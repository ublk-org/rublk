use io_uring::cqueue;
use libublk::io::{UblkIOCtx, UblkQueue};
use std::cell::RefCell;
use std::collections::HashMap;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

struct FutureData {
    pub waker: Waker,
    pub result: Option<i32>,
}

std::thread_local! {
    static MY_THREAD_WAKER: RefCell<HashMap<u64, FutureData>> = RefCell::new(Default::default());
}

/// User code creates one future with user_data used for submitting
/// uring OP, then future.await returns this uring OP's result.
pub(crate) struct UblkUringOpFuture {
    pub user_data: u64,
}

impl Future for UblkUringOpFuture {
    type Output = i32;
    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        MY_THREAD_WAKER.with(|refcell| {
            let mut map = refcell.borrow_mut();
            match map.get(&self.user_data) {
                None => {
                    map.insert(
                        self.user_data,
                        FutureData {
                            waker: cx.waker().clone(),
                            result: None,
                        },
                    );
                    //log::debug!("rublk: uring io pending userdata {:x}", self.user_data);
                    Poll::Pending
                }
                Some(fd) => {
                    match fd.result {
                        Some(result) => {
                            map.remove(&self.user_data);
                            log::debug!(
                                "rublk: uring io ready userdata {:x} ready",
                                self.user_data
                            );
                            Poll::Ready(result)
                        }
                        None => {
                            //log::debug!("rublk: uring io pending userdata {:x}", self.user_data);
                            Poll::Pending
                        }
                    }
                }
            }
        })
    }
}

#[inline]
pub(crate) fn ublk_wake_task(data: u64, cqe: &cqueue::Entry) {
    MY_THREAD_WAKER.with(|refcell| {
        let mut map = refcell.borrow_mut();

        match map.get_mut(&data) {
            Some(fd) => {
                fd.result = Some(cqe.result());
                fd.waker.clone().wake();
            }
            None => {}
        }
    })
}

std::thread_local! {
    static MY_THREAD_SEQ: RefCell<u64> = RefCell::new(0);
}

#[inline]
pub(crate) fn ublk_get_uring_io_seq() -> u64 {
    MY_THREAD_SEQ.with(|count| {
        let a = *count.borrow();

        if a == 0x00ff_ffff_ffff_ffff {
            *count.borrow_mut() = 0;
        } else {
            *count.borrow_mut() += 1;
        }
        a
    })
}

#[inline]
pub(crate) fn ublk_submit_sqe(
    q: &UblkQueue,
    sqe: &io_uring::squeue::Entry,
    user_data: u64,
) -> UblkUringOpFuture {
    loop {
        let res = unsafe { q.q_ring.borrow_mut().submission().push(sqe) };

        match res {
            Ok(_) => break,
            Err(_) => {
                log::debug!("ublk_submit_sqe: flush and retry");
                q.q_ring.borrow().submit_and_wait(0).unwrap();
            }
        }
    }

    UblkUringOpFuture { user_data }
}

pub(crate) fn ublk_run_task<T>(
    q: &UblkQueue,
    exe: &smol::LocalExecutor,
    task: &smol::Task<T>,
    nr_waits: usize,
) {
    while exe.try_tick() {}
    while !task.is_finished() {
        match q.q_ring.borrow().submit_and_wait(nr_waits) {
            Err(_) => break,
            _ => {}
        }
        let cqe = {
            match q.q_ring.borrow_mut().completion().next() {
                None => {
                    exe.try_tick();
                    std::thread::sleep(std::time::Duration::from_millis(10));
                    continue;
                }
                Some(r) => r,
            }
        };
        let user_data = cqe.user_data();
        ublk_wake_task(user_data, &cqe);
        while exe.try_tick() {}
    }
}

#[inline]
pub(crate) fn ublk_submit_io_cmd(
    q: &UblkQueue,
    tag: u16,
    cmd_op: u32,
    buf_addr: *mut u8,
    result: i32,
) -> UblkUringOpFuture {
    let user_data = UblkIOCtx::build_user_data(tag, cmd_op, 0, false);

    q.__submit_io_cmd(tag, cmd_op, buf_addr as u64, user_data, result);

    UblkUringOpFuture { user_data }
}
