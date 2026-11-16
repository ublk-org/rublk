# qcow2 target: why no multi-queue, and how to add it

Status: investigation and design only. Nothing here is implemented yet.

## 1. Summary

| Question | Answer |
|---|---|
| What is the limit? | `rublk add qcow2` only works with one queue (`-q 1`). |
| Where is the check? | `src/qcow2.rs`: `"qcow2 doesn't support MQ yet"` |
| Where is the real cause? | In the `qcow2-rs` library, plus how rublk waits for events. |
| Is it the async API shape? | No. The `?Send` futures are fine. |
| How many causes? | Two. Both must be fixed. |
| Proposed fix | One lock around every poll of qcow2-rs code, plus an eventfd wakeup. |
| Change needed in qcow2-rs | None. |

What was checked, and how:

| Claim | How it was checked |
|---|---|
| `Qcow2Dev` is not `Sync` | Compile test against qcow2-rs 0.1.5 (the version in `Cargo.lock`) |
| `Qcow2Dev` is `Send` | Same compile test |
| Lost wakeup across threads | Code reading only (libublk 0.4.8, async-executor 1.13.1, futures-locks 0.7.1) |
| Design in sections 6 to 9 | Reviewed against the code once. Open points are listed in section 7.1 and section 9. |
| Performance of the fix | Reasoning only. Not measured. |

## 2. Background

### Today: one queue, one thread

```
              one thread
  +-------------------------------------+
  |  smol LocalExecutor                 |
  |    task(tag 0) task(tag 1) ...      |
  |    flush-meta task                  |
  |              |                      |
  |              v                      |
  |        Qcow2Dev  (L1, L2 cache,     |
  |                   refcount, locks)  |
  |              |                      |
  |              v                      |
  |        io_uring (this thread)       |
  +-------------------------------------+
```

- One task per tag. Each task loops: fetch command, handle it, commit result.
- Tasks switch only at `.await`. Code between two `.await`s runs without interruption.
- The thread sleeps in `io_uring_enter` when no task can run.

### Wanted: N queues, N threads, one image

```
   thread 0            thread 1            thread N-1
  +-----------+       +-----------+       +-----------+
  | executor  |       | executor  |       | executor  |
  | io_uring  |       | io_uring  |       | io_uring  |
  +-----+-----+       +-----+-----+       +-----+-----+
        |                   |                   |
        +---------+---------+---------+---------+
                  |
                  v
           one shared Qcow2Dev
```

The image metadata (L1, L2, refcount) belongs to the image, not to a queue.
So all queue threads must share one `Qcow2Dev`.

## 3. Root cause 1: `Qcow2Dev` cannot be shared between threads

The compiler rejects sharing it:

```
`RefCell<VecDeque<u32>>` cannot be shared between threads safely
  within `L1Table` ...
  required for `futures_locks::RwLock<L1Table>` to implement `Sync`
```

| Item | Detail |
|---|---|
| Field | `dirty_blocks: RefCell<VecDeque<u32>>` |
| Types | `L1Table` and `RefTable` |
| Writers | `set_dirty()` and `pop_dirty_blk_idx()`, both take `&self` |
| Where (qcow2-rs 0.2.0) | `src/meta/l1.rs`, `src/meta/refcount.rs`, `src/meta/table.rs` |
| Where (qcow2-rs 0.1.5) | all in `src/meta.rs` |

This is a real race, not only a type-system detail:

```
  thread A (write with COW)          thread B (flush_meta)
  -------------------------          ---------------------
  reftable.read()   <- shared lock   reftable.read()   <- shared lock
  pop_dirty_blk_idx()                pop_dirty_blk_idx()
        \                                 /
         +--- both mutate the same VecDeque ---+
```

- A read lock allows many holders.
- `pop_dirty_blk_idx()` changes the queue while holding only a read lock.
- With one thread this is safe: no `.await` happens inside the change.
- With two threads it is a data race.

## 4. Root cause 2: a wakeup from another thread is lost

qcow2-rs keeps async locks while it waits for IO. Examples:

| Function | Lock held while waiting for IO |
|---|---|
| `do_write_cow` | L2 slice write lock |
| `flush_meta` | `flush_lock` and L1 read lock |
| `flush_refcount` | refcount table read lock |

So a task on one thread can wait for a lock held by a task on another thread.

```
  thread A                              thread B
  --------                              --------
  task takes L2 write lock
  task waits for its IO
                                        task wants the same lock -> pending
                                        no runnable task left
                                        sleeps in io_uring_enter(wait 1 CQE)
  IO done, task drops the lock
  lock wakes B's task:
    push task to B's run queue   ---->  (B is asleep in the kernel)
    notify B's executor          ---->  nobody is listening
                                        ...
                                        B wakes only when some other CQE
                                        arrives on B's ring, or when the
                                        20s idle timeout of libublk fires.
```

Why the notify does nothing:

- B drives its executor with `try_tick()`, then sleeps in the kernel.
- The executor's `notify()` only wakes threads that sleep *inside the executor*.
- Nothing writes to B's ring, so `io_uring_enter` does not return.

Result: the IO does not hang forever, but it can stall for up to 20 seconds.

| Constant | Where |
|---|---|
| `UBLK_QUEUE_IDLE_SECS = 20` | libublk `src/io.rs`, used as the wait timeout |

Warning for testing: without the fix a multi-queue test still passes, only slowly.
A passing test does not prove the wakeup works. Check latency too.

With one queue this cannot happen: the lock is always released by a task on the same thread.

## 5. Things that are *not* the cause

| Suspect | Verdict | Why |
|---|---|---|
| `async_trait(?Send)` / `?Send` futures | Not a problem | Each task is polled only on its own thread. |
| Thread-local `UblkQueue` pointer in `qcow2.rs` | Not a problem | An IO future is created and polled by the same task. |
| `Rc`, hard-coded queue id 0 in `qcow2.rs` | Must change, but simple | Plain rublk-side rework. |

## 6. Proposed solution

Idea: let many threads share `Qcow2Dev`, but allow only one thread at a time to run qcow2-rs code.

### 6.1 Overview

```
   thread 0                 thread 1                 thread N-1
  +----------------+       +----------------+       +----------------+
  | io tasks       |       | io tasks       |       | io tasks       |
  | flush task     |       | flush task     |       | flush task     |
  | eventfd task   |       | eventfd task   |       | eventfd task   |
  | io_uring       |       | io_uring       |       | io_uring       |
  +-------+--------+       +-------+--------+       +-------+--------+
          |                        |                        |
          +-----------+------------+------------+-----------+
                      |
              +-------v--------+
              |  SharedQcow2   |   lock taken for each poll,
              |   lock + dev   |   released before sleeping
              +----------------+
```

### 6.2 Part A: the lock wrapper

All access to `Qcow2Dev` goes through one small type in rublk.

```rust
struct SharedQcow2<T> { dev: Qcow2Dev<T>, gil: Mutex<()> }
unsafe impl<T: Send> Sync for SharedQcow2<T> {}   // the only unsafe

// the only way to run async qcow2-rs code
fn run<F: Future>(&self, f: impl FnOnce(&Qcow2Dev<T>) -> F) -> Locked<F>
```

Two details:

- `f` must only build the future. It is called under the lock, or on the first poll.
- Two things are read without the lock. Both are safe.

| Read without the lock | Why it is safe |
|---|---|
| `need_flush_meta()` | It is one `AtomicBool`. |
| `info` (image size and so on) | Plain data, never changed after setup. |

| Event on `Locked<F>` | What it does |
|---|---|
| `poll()` | take lock, poll inner future, release lock |
| `drop()` | take lock, drop inner future, release lock |
| thread goes to sleep | lock is never held here |

### 6.3 Part B: the cross-thread wakeup

`Locked::poll` gives the inner future a wrapped waker.

```
  wake() called
       |
       v
  call the original waker        (task goes to owner's run queue)
       |
       v
  caller thread == owner thread? --yes--> done
       |
       no
       v
  write owner's eventfd
       |
       v
  owner's ring gets a CQE  ->  owner leaves io_uring_enter
       |
       v
  owner runs `while exe.try_tick() {}`  ->  woken task runs
```

- Each queue runs one small task: `loop { read(eventfd).await }`.
- The read goes through `q.ublk_submit_sqe()`, like the existing flush timer.
- Order matters: queue the task first, write the eventfd second.
- The eventfd count stays until it is read, so a wakeup cannot be missed.

Note: `src/notifier.rs` (used by `compress`) is close, but its counter is a `RefCell`.
It needs an atomic counter, or no counter at all, to be used across threads.

## 7. Why the solution is correct

| Risk | Why it is covered |
|---|---|
| Two threads change metadata at once | Every poll and drop of qcow2-rs futures holds the lock. |
| New interleavings qcow2-rs never saw | Threads swap only between whole polls. One executor with many tasks already does exactly that today. |
| Data race on the `RefCell`s | The lock orders every access. `Qcow2Dev` is `Send`. |
| Thread-bound state in qcow2-rs | None found: no `thread_local`, no `Rc` in the library. |
| IO future polled on the wrong thread | qcow2-rs never spawns tasks. Creator and poller are the same task. |
| Lost wakeup | Covered by Part B. |
| Deadlock on the lock itself | It is never held across an `.await` or a wait for completions. One exception: see 7.1. |

In short: any multi-queue run behaves like some single-queue run. Single-queue is already known to work.

### 7.1 Open points

These are not solved by the design above. Each needs a decision.

| Open point | Detail | Possible answer |
|---|---|---|
| Submission queue full | `ublk_submit_sqe()` calls `submit_and_wait(0)` inside the poll and retries in a loop. This runs under the lock. It does not wait for completions, but it submits the whole queue. | Make the SQ large enough, or accept and document it. |
| How to reach a full SQ | `flush_cache_entries` starts one write per dirty slice at once. SQ depth is `queue_depth * 4`. | Same as above. |
| Panic under the lock | Every qcow2 error is `.unwrap()`ed in `src/qcow2.rs`. A panic poisons the `Mutex` for all queues. | Abort the process on panic, or return `-EIO` instead of `unwrap()`. |

## 8. Performance

| Work | Runs in parallel? |
|---|---|
| SQE submit, inline IO issue | Yes (inside `io_uring_enter`, lock not held). Exception: SQ full, see 7.1. |
| CQE reaping | Yes |
| ublk data copy between kernel and daemon | Yes |
| L2 cache lookup, SQE build | No (under the lock) |
| Decompression of compressed clusters | No (under the lock) |

| Extra cost | When |
|---|---|
| One mutex lock/unlock | Every poll. Cheap with one queue. With many busy queues, threads wait here. |
| One eventfd write per woken task | Only when a lock is released to a task on another thread. A writer unlock wakes all waiting readers. |

Not measured yet. To get the scaling limit before writing code:

1. Run fio on a `-q 1` qcow2 device.
2. Compare `utime` and `stime` of the queue thread.
3. The user-time share is the part that will not scale.

## 9. Changes needed in rublk

| Area | Change |
|---|---|
| Sharing | `Rc<Qcow2Tgt>` becomes `Arc`. One thread, one `UblkQueue`, one executor per queue. |
| Startup | Queue 0 runs `qcow2_prep_io()` before the device starts. |
| Periodic flush | Every queue runs the same task: if `need_flush_meta()`, wait 50ms, call `flush_meta()`. |
| Shutdown | Each thread finishes its flush task, then calls `flush_meta()` once more. |
| MQ check | Keep `nr_hw_queues != 1` rejected until MQ tests pass. |

Notes:

- Flush on every queue is safe: `flush_lock` in qcow2-rs already serializes it.
- It also avoids this case: queue 0 sleeps while queue 1 makes metadata dirty.
- The last thread to exit flushes whatever is left.

Shutdown needs care. A thread that stops driving its ring must not hold a qcow2 lock.

| Task | Can it hold a qcow2 lock when the queue goes down? | What to do |
|---|---|---|
| Flush task | Yes. It can be in the middle of a flush. | Drive it to completion first. Today's code already does this. |
| IO task | Not verified. | See below. |

- libublk reports "queue is down" when no ublk command is in the kernel and the SQ is empty.
- That does not count target IO, such as a qcow2 write still in flight.
- So the IO-task case depends on the kernel: does it finish all requests before it aborts the fetch commands?
- This must be checked in the kernel source before implementing shutdown.

## 10. Alternatives considered

| Option | Change in qcow2-rs | Easy to prove correct? | Performance | Verdict |
|---|---|---|---|---|
| **Lock around every poll (this doc)** | None | Yes, by reduction to one queue | Kernel part scales | **Chosen** |
| Make `Qcow2Dev` really `Sync`, fine-grained locks | Small diff | No, needs a full audit of "check then act" code | Best | Rejected for now |
| One metadata thread, other queues forward requests | None | Yes | Two thread hops per IO | Rejected |
| Queues do data IO, ask owner thread for mappings | New public API | No, COW and discard order must be re-proven | Good | Rejected |

## 11. Optional follow-up

Replace the two `RefCell<VecDeque<u32>>` fields with `std::sync::Mutex`.

| Gain | Cost |
|---|---|
| The compiler proves `Sync`. The one `unsafe impl` goes away. | Needs a new qcow2-rs release. |
| | rublk must move from qcow2-rs 0.1.5 to the 0.2 API. |

This is independent of the main fix and can be done later.
