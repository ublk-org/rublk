//! smol-based implementation of libublk's executor contract.
//!
//! The drive loop is the tuning surface this file exists for: drain every
//! runnable task, poll the root future, then park in the ublk reactor.
//! Plain parking batches completions hard (measured ~0.92 io_uring_enter
//! per IO vs Tokio's ~1.15): best CPU/IO, ~10us extra latency in configs
//! with CPU headroom. Adjust here, not in targets.
//!
//! Limitation (inherited design constraint): when the root future is
//! pending, no task is runnable and NO ring op is in flight, the loop
//! busy-spins (wait_and_reap_events returns immediately with no ring to
//! wait on). Every rublk target keeps ring ops in flight while idle, so
//! this does not occur in practice.

use libublk::{TaskHandle, UblkError, UblkExecutor, UblkSpawner, UblkTask};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

pub(crate) struct SmolRuntime {
    exe: smol::LocalExecutor<'static>,
}

/// smol's Task cancels on drop; the contract wants detach-on-drop and
/// explicit cancel, so wrap it.
struct SmolTask(Option<smol::Task<()>>);

impl Future for SmolTask {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        match self.0.as_mut() {
            Some(t) => {
                let r = Pin::new(t).poll(cx);
                if r.is_ready() {
                    self.0 = None;
                }
                r
            }
            None => Poll::Ready(()),
        }
    }
}

impl UblkTask for SmolTask {
    fn cancel(mut self: Box<Self>) {
        // Dropping a smol Task cancels it.
        drop(self.0.take());
    }
}

impl Drop for SmolTask {
    fn drop(&mut self) {
        if let Some(t) = self.0.take() {
            t.detach();
        }
    }
}

impl UblkSpawner for SmolRuntime {
    fn spawn_boxed(&self, fut: Pin<Box<dyn Future<Output = ()>>>) -> TaskHandle {
        TaskHandle::new(Box::new(SmolTask(Some(self.exe.spawn(fut)))))
    }
}

impl UblkExecutor for SmolRuntime {
    fn new() -> Result<Self, UblkError> {
        Ok(SmolRuntime {
            exe: smol::LocalExecutor::new(),
        })
    }

    fn block_on<F: Future>(&self, future: F) -> F::Output {
        libublk::with_ambient_spawner(self, || {
            let mut fut = std::pin::pin!(future);
            // The root is re-polled every loop iteration, so a no-op
            // waker is sufficient: progress is driven by task ticks and
            // reactor wakes, never by the root's own waker.
            // (Waker::noop() returns &'static Waker.)
            let mut cx = Context::from_waker(Waker::noop());
            loop {
                while self.exe.try_tick() {}
                if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
                    return v;
                }
                if self.exe.try_tick() {
                    continue;
                }
                // Idle: park in io_uring_enter until a CQE (or the
                // reactor's safety timeout) wakes something. A loop that
                // simply re-enters is naturally live per the reactor docs.
                let _ = libublk::reactor::wait_and_reap_events();
            }
        })
    }

    // Override the provided default with a specialized join loop: spawn
    // the per-tag futures directly on the concrete executor (no
    // `spawn_boxed` boxing), and instead of parking a root join future in
    // `block_on`, drive the executor with a plain `is_finished()` check —
    // no root-future poll per park wake. This is the measured-fastest
    // shape (matches rublk main's historical loop within noise).
    fn run_io_tasks<F, Fut>(
        dev: &std::sync::Arc<libublk::io::UblkDev>,
        qid: u16,
        io_task: F,
    ) -> Result<(), UblkError>
    where
        F: Fn(std::rc::Rc<libublk::io::UblkQueue>, u16) -> Fut + 'static,
        Fut: Future<Output = Result<(), UblkError>> + 'static,
    {
        let rt = <Self as UblkExecutor>::new()?;
        let dev = dev.clone();
        libublk::with_ambient_spawner(&rt, || {
            let q = std::rc::Rc::new(libublk::io::UblkQueue::new(qid, &dev)?);
            let tasks: Vec<smol::Task<()>> = (0..dev.dev_info.queue_depth)
                .map(|tag| {
                    let task = io_task(q.clone(), tag);
                    rt.exe.spawn(async move {
                        match task.await {
                            Err(UblkError::QueueIsDown) | Ok(_) => {}
                            Err(e) => log::error!("io task failed for tag {}: {}", tag, e),
                        }
                    })
                })
                .collect();
            while !tasks.iter().all(|t| t.is_finished()) {
                while rt.exe.try_tick() {}
                // Park in io_uring_enter until a CQE (or the reactor's
                // safety timeout) wakes a task; re-entering is naturally
                // live per the reactor docs.
                let _ = libublk::reactor::wait_and_reap_events();
            }
            Ok(())
        })
    }
}

// Inherent delegates to the trait items above, so call sites that use
// `crate::Rt::new()` / `crate::Rt::run_io_tasks(...)` without importing
// `UblkExecutor` resolve the same way whether `Rt` is `SmolRuntime` or
// `libublk::UblkRuntime` (which exposes these as inherent methods).
impl SmolRuntime {
    pub(crate) fn new() -> Result<Self, UblkError> {
        <Self as UblkExecutor>::new()
    }

    pub(crate) fn block_on<F: Future>(&self, future: F) -> F::Output {
        <Self as UblkExecutor>::block_on(self, future)
    }

    pub(crate) fn run_io_tasks<F, Fut>(
        dev: &std::sync::Arc<libublk::io::UblkDev>,
        qid: u16,
        io_task: F,
    ) -> Result<(), UblkError>
    where
        F: Fn(std::rc::Rc<libublk::io::UblkQueue>, u16) -> Fut + 'static,
        Fut: std::future::Future<Output = Result<(), UblkError>> + 'static,
    {
        <Self as UblkExecutor>::run_io_tasks(dev, qid, io_task)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use libublk::spawn_local;
    use std::cell::Cell;
    use std::rc::Rc;

    struct YieldNow(bool);
    impl Future for YieldNow {
        type Output = ();
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.0 {
                Poll::Ready(())
            } else {
                self.0 = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }

    #[test]
    fn block_on_returns_value() {
        let rt = SmolRuntime::new().unwrap();
        assert_eq!(rt.block_on(async { 7 }), 7);
    }

    #[test]
    fn spawn_await_cancel_detach() {
        let rt = SmolRuntime::new().unwrap();
        let ran = Rc::new(Cell::new(0u32));
        let (r1, r2) = (ran.clone(), ran.clone());
        rt.block_on(async move {
            let h = spawn_local(async move {
                r1.set(r1.get() + 1);
            });
            h.await;
            drop(spawn_local(async move {
                r2.set(r2.get() + 1);
            }));
            let h = spawn_local(async {
                std::future::pending::<()>().await;
            });
            h.cancel();
            YieldNow(false).await;
            YieldNow(false).await;
        });
        assert_eq!(ran.get(), 2);
    }
}
