//! NBD client target: exposes a remote NBD export as /dev/ublkbN.
//!
//! One NBD connection per queue (negotiated at add time, requiring
//! `NBD_FLAG_CAN_MULTI_CONN` for more than one queue), driven entirely
//! through the libublk v0.5 typed op catalog on the queue's io_uring:
//! per-tag tasks serialize request sends through an async lock, and one
//! receive task per queue demuxes replies back to the waiting tags by
//! handle. Reads land straight in the tag's buffer (or, with `-z`, in
//! the ublk-registered kernel buffer via a fixed-buffer receive);
//! writes go out as a header send plus a payload send, optionally
//! `IORING_OP_SEND_ZC` (`--send-zc`) and straight from the registered
//! buffer with `-z`.

use anyhow::Context as _;
use io_uring::{opcode, squeue, types};
use libublk::ctrl::UblkCtrl;
use libublk::helpers::IoBuf;
use libublk::io::{BufDesc, UblkDev, UblkQueue};
use libublk::ops::{self, TgtFd};
use libublk::UblkError;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::io::{Read, Write};
use std::os::fd::{AsRawFd, OwnedFd, RawFd};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Poll, Waker};

// Transmission phase
const NBD_REQUEST_MAGIC: u32 = 0x25609513;
const NBD_SIMPLE_REPLY_MAGIC: u32 = 0x67446698;
const NBD_REQUEST_LEN: usize = 28;
const NBD_REPLY_LEN: usize = 16;

const NBD_CMD_READ: u16 = 0;
const NBD_CMD_WRITE: u16 = 1;
const NBD_CMD_FLUSH: u16 = 3;
const NBD_CMD_TRIM: u16 = 4;
const NBD_CMD_WRITE_ZEROES: u16 = 6;

// Transmission flags (from the handshake)
const NBD_FLAG_READ_ONLY: u16 = 1 << 1;
const NBD_FLAG_SEND_FLUSH: u16 = 1 << 2;
const NBD_FLAG_SEND_TRIM: u16 = 1 << 5;
const NBD_FLAG_SEND_WRITE_ZEROES: u16 = 1 << 6;
const NBD_FLAG_CAN_MULTI_CONN: u16 = 1 << 8;

// Fixed-newstyle handshake
const NBDMAGIC: u64 = 0x4e42_444d_4147_4943; // "NBDMAGIC"
const IHAVEOPT: u64 = 0x4948_4156_454f_5054; // "IHAVEOPT"
const NBD_REP_MAGIC: u64 = 0x3e88_9045_565a_9;
const NBD_OPT_GO: u32 = 7;
const NBD_REP_ACK: u32 = 1;
const NBD_REP_INFO: u32 = 3;
const NBD_INFO_EXPORT: u16 = 0;
const NBD_FLAG_FIXED_NEWSTYLE: u16 = 1 << 0;
const NBD_FLAG_NO_ZEROES: u16 = 1 << 1;
const NBD_FLAG_C_FIXED_NEWSTYLE: u32 = 1 << 0;
const NBD_FLAG_C_NO_ZEROES: u32 = 1 << 1;

#[derive(clap::Args, Debug)]
pub(crate) struct NbdAddArgs {
    #[command(flatten)]
    pub(crate) gen_arg: super::args::GenAddArgs,

    /// NBD server host
    #[clap(long, default_value = "127.0.0.1")]
    host: String,

    /// NBD server TCP port
    #[clap(long, default_value_t = 10809)]
    port: u16,

    /// unix domain socket path of the NBD server (overrides host/port)
    #[clap(long)]
    unix: Option<PathBuf>,

    /// export name
    #[clap(long, default_value = "")]
    export: String,

    /// send write payloads with IORING_OP_SEND_ZC
    #[clap(long, default_value_t = false)]
    send_zc: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct NbdJson {
    host: String,
    port: u16,
    unix: Option<PathBuf>,
    export: String,
    send_zc: bool,
}

fn read_exact_sync(s: &mut impl Read, buf: &mut [u8]) -> anyhow::Result<()> {
    s.read_exact(buf).context("short read in NBD handshake")
}

fn read_be32(s: &mut impl Read) -> anyhow::Result<u32> {
    let mut b = [0u8; 4];
    read_exact_sync(s, &mut b)?;
    Ok(u32::from_be_bytes(b))
}

fn read_be64(s: &mut impl Read) -> anyhow::Result<u64> {
    let mut b = [0u8; 8];
    read_exact_sync(s, &mut b)?;
    Ok(u64::from_be_bytes(b))
}

/// Fixed-newstyle negotiation with `NBD_OPT_GO`; returns the export
/// size and its transmission flags.
fn nbd_negotiate(s: &mut (impl Read + Write), export: &str) -> anyhow::Result<(u64, u16)> {
    if read_be64(s)? != NBDMAGIC {
        anyhow::bail!("not an NBD server (bad magic)");
    }
    if read_be64(s)? != IHAVEOPT {
        anyhow::bail!("NBD server does not speak the newstyle handshake");
    }
    let mut hs_flags = [0u8; 2];
    read_exact_sync(s, &mut hs_flags)?;
    let hs_flags = u16::from_be_bytes(hs_flags);
    if hs_flags & NBD_FLAG_FIXED_NEWSTYLE == 0 {
        anyhow::bail!("NBD server does not support the fixed newstyle handshake");
    }

    let mut cflags = NBD_FLAG_C_FIXED_NEWSTYLE;
    if hs_flags & NBD_FLAG_NO_ZEROES != 0 {
        cflags |= NBD_FLAG_C_NO_ZEROES;
    }
    s.write_all(&cflags.to_be_bytes())?;

    // NBD_OPT_GO: export name + "no extra info requests"
    let name = export.as_bytes();
    let opt_len = 4 + name.len() + 2;
    s.write_all(&IHAVEOPT.to_be_bytes())?;
    s.write_all(&NBD_OPT_GO.to_be_bytes())?;
    s.write_all(&(opt_len as u32).to_be_bytes())?;
    s.write_all(&(name.len() as u32).to_be_bytes())?;
    s.write_all(name)?;
    s.write_all(&0u16.to_be_bytes())?;
    s.flush()?;

    let mut export_info: Option<(u64, u16)> = None;
    loop {
        if read_be64(s)? != NBD_REP_MAGIC {
            anyhow::bail!("bad NBD option reply magic");
        }
        let _opt = read_be32(s)?;
        let rep_type = read_be32(s)?;
        let len = read_be32(s)? as usize;
        let mut payload = vec![0u8; len];
        read_exact_sync(s, &mut payload)?;

        if rep_type & 0x8000_0000 != 0 {
            anyhow::bail!(
                "NBD server rejected export {:?}: error {:#x} {:?}",
                export,
                rep_type,
                String::from_utf8_lossy(&payload)
            );
        }
        match rep_type {
            NBD_REP_INFO => {
                if len >= 12 {
                    let info_type = u16::from_be_bytes([payload[0], payload[1]]);
                    if info_type == NBD_INFO_EXPORT {
                        let size = u64::from_be_bytes(payload[2..10].try_into().unwrap());
                        let flags = u16::from_be_bytes([payload[10], payload[11]]);
                        export_info = Some((size, flags));
                    }
                }
            }
            NBD_REP_ACK => break,
            _ => {}
        }
    }
    export_info.context("NBD server sent no export info")
}

/// Open and negotiate one connection.
fn nbd_connect(json: &NbdJson) -> anyhow::Result<(OwnedFd, u64, u16)> {
    if let Some(path) = &json.unix {
        let mut s = std::os::unix::net::UnixStream::connect(path)
            .with_context(|| format!("connect NBD server at {:?}", path))?;
        let (size, flags) = nbd_negotiate(&mut s, &json.export)?;
        Ok((OwnedFd::from(s), size, flags))
    } else {
        let mut s = std::net::TcpStream::connect((json.host.as_str(), json.port))
            .with_context(|| format!("connect NBD server at {}:{}", json.host, json.port))?;
        s.set_nodelay(true)?;
        let (size, flags) = nbd_negotiate(&mut s, &json.export)?;
        Ok((OwnedFd::from(s), size, flags))
    }
}

/// Per-tag reply rendezvous filled by the queue's receive task.
#[derive(Default)]
struct Slot {
    /// 0 on success, negative errno on failure; `None` while in flight
    result: Option<i32>,
    waker: Option<Waker>,
    /// Payload length the receive task must read for an in-flight
    /// `NBD_CMD_READ`; 0 for every other command.
    read_len: u32,
    /// Staleness guard: replies must echo the cookie of the request.
    cookie: u32,
}

/// One request queued for the sender task: the prebuilt header plus
/// the payload source for writes.
struct SendItem {
    hdr: [u8; NBD_REQUEST_LEN],
    /// `(ptr, len)` of the write payload; ptr is null in `-z` mode
    /// (fixed-buffer send from the registered buffer `tag`).
    payload: Option<(*const u8, u32)>,
    tag: u16,
}

#[derive(Default)]
struct SendQueue {
    items: std::collections::VecDeque<SendItem>,
    waker: Option<Waker>,
    /// The connection is poisoned; enqueue nothing more.
    dead: bool,
}

struct NbdQueue {
    q: Rc<UblkQueue>,
    /// This queue's connection, as the fixed-file index registered by
    /// UblkQueue::new() (tgt.fds[qid + 1]).
    sock: TgtFd,
    sock_raw: RawFd,
    /// Per-tag IO buffers; empty in `-z` mode, where reads and writes
    /// use the ublk-registered kernel buffers instead.
    bufs: Vec<IoBuf<u8>>,
    zc: bool,
    send_zc: bool,
    slots: RefCell<Vec<Slot>>,
    sendq: RefCell<SendQueue>,
}

impl NbdQueue {
    fn handle(&self, tag: u16) -> u64 {
        ((self.slots.borrow()[tag as usize].cookie as u64) << 32) | tag as u64
    }

    /// Arm `tag`'s slot for one request; returns its handle.
    fn arm(&self, tag: u16, read_len: u32) -> u64 {
        let mut slots = self.slots.borrow_mut();
        let slot = &mut slots[tag as usize];
        slot.cookie = slot.cookie.wrapping_add(1);
        slot.result = None;
        slot.read_len = read_len;
        drop(slots);
        self.handle(tag)
    }

    fn complete(&self, tag: u16, result: i32) {
        let mut slots = self.slots.borrow_mut();
        let slot = &mut slots[tag as usize];
        slot.result = Some(result);
        if let Some(waker) = slot.waker.take() {
            waker.wake();
        }
    }

    /// Fail every armed slot (connection loss).
    fn complete_all(&self, result: i32) {
        let mut slots = self.slots.borrow_mut();
        for slot in slots.iter_mut() {
            if slot.result.is_none() {
                slot.result = Some(result);
                if let Some(waker) = slot.waker.take() {
                    waker.wake();
                }
            }
        }
    }

    async fn wait_reply(&self, tag: u16) -> i32 {
        std::future::poll_fn(|cx| {
            let mut slots = self.slots.borrow_mut();
            let slot = &mut slots[tag as usize];
            match slot.result.take() {
                Some(res) => Poll::Ready(res),
                None => {
                    if slot
                        .waker
                        .as_ref()
                        .map_or(true, |w| !w.will_wake(cx.waker()))
                    {
                        slot.waker = Some(cx.waker().clone());
                    }
                    Poll::Pending
                }
            }
        })
        .await
    }
}

fn build_req(
    cmd: u16,
    cmd_flags: u16,
    handle: u64,
    offset: u64,
    len: u32,
) -> [u8; NBD_REQUEST_LEN] {
    let mut hdr = [0u8; NBD_REQUEST_LEN];
    hdr[0..4].copy_from_slice(&NBD_REQUEST_MAGIC.to_be_bytes());
    hdr[4..6].copy_from_slice(&cmd_flags.to_be_bytes());
    hdr[6..8].copy_from_slice(&cmd.to_be_bytes());
    hdr[8..16].copy_from_slice(&handle.to_be_bytes());
    hdr[16..24].copy_from_slice(&offset.to_be_bytes());
    hdr[24..28].copy_from_slice(&len.to_be_bytes());
    hdr
}

/// Bounded non-blocking recv(2) fast path: the bytes we want are
/// usually already queued on the socket, so drain them with plain
/// syscalls and skip the ring round-trip (SQE/CQE/waker per op) -- the
/// same trick as ublksrv's nbd_do_recv. Spinning continues only while
/// data is flowing; an empty socket costs one failed recv.
///
/// Returns the bytes consumed (possibly 0), or a negative errno on a
/// hard failure (peer close included).
///
/// # Safety (caller contract)
///
/// `[ptr, ptr + len)` must be valid for writes for the whole call.
#[inline]
fn sync_recv_some(sock_raw: RawFd, ptr: *mut u8, len: u32) -> i32 {
    let mut done = 0u32;
    let mut spins: u32 = if len < 512 { 16 } else { 32 };
    while done < len && spins > 0 {
        spins -= 1;
        // SAFETY: per the caller contract above
        let res = unsafe {
            libc::recv(
                sock_raw,
                ptr.wrapping_add(done as usize) as *mut libc::c_void,
                (len - done) as usize,
                libc::MSG_DONTWAIT | libc::MSG_WAITALL,
            )
        };
        if res > 0 {
            done += res as u32;
        } else if res == 0 {
            return -libc::ENOTCONN;
        } else {
            let err = std::io::Error::last_os_error()
                .raw_os_error()
                .unwrap_or(libc::EIO);
            if err == libc::EINTR {
                continue;
            }
            if err != libc::EAGAIN && err != libc::EWOULDBLOCK {
                return -err;
            }
            // Nothing there yet: only keep spinning when a reply is
            // already flowing (partial bytes consumed).
            if done == 0 {
                break;
            }
        }
    }
    done as i32
}

/// Receive exactly `len` bytes into caller-managed memory. Returns 0,
/// or a negative errno (peer close included).
///
/// Fast path first ([`sync_recv_some`]); only an empty socket falls
/// back to an io_uring recv, resuming at the consumed offset.
async fn recv_all(sock: TgtFd, sock_raw: RawFd, ptr: *mut u8, len: u32) -> i32 {
    let drained = sync_recv_some(sock_raw, ptr, len);
    if drained < 0 {
        return drained;
    }
    let mut done = drained as u32;
    while done < len {
        // SAFETY: caller guarantees the memory outlives the await
        let res = match unsafe {
            ops::recv_raw(
                sock,
                ptr.wrapping_add(done as usize),
                len - done,
                libc::MSG_WAITALL,
            )
        } {
            Ok(op) => op.await,
            Err(e) => e.errno(),
        };
        if res <= 0 {
            return if res == 0 { -libc::ENOTCONN } else { res };
        }
        done += res as u32;
    }
    0
}

/// Receive exactly `len` bytes into the ublk-registered fixed buffer
/// `tag`, resuming at the consumed offset on short receives.
async fn recv_all_fixed(sock: TgtFd, tag: u16, len: u32) -> i32 {
    let mut done = 0u32;
    while done < len {
        // SAFETY: the ublk request buffer stays registered until the io
        // is committed, which happens only after this future resolves.
        let res =
            match unsafe { ops::recv_fixed(sock, tag, done as u64, len - done, libc::MSG_WAITALL) }
            {
                Ok(op) => op.await,
                Err(e) => e.errno(),
            };
        if res <= 0 {
            return if res == 0 { -libc::ENOTCONN } else { res };
        }
        done += res as u32;
    }
    0
}

/// The connection framing is broken (short or failed send): make every
/// waiter fail fast and unblock the receive task.
fn nbd_poison(nq: &NbdQueue) {
    nq.complete_all(-libc::EPIPE);
    // SAFETY: plain shutdown(2) on this queue's connection
    unsafe { libc::shutdown(nq.sock_raw, libc::SHUT_RDWR) };
}

/// Issue one NBD request for `tag`'s current io and await its reply;
/// returns the ublk completion result.
async fn nbd_handle_io(nq: &NbdQueue, tag: u16, tflags: u16) -> i32 {
    let iod = nq.q.get_iod(tag);
    let op = iod.op_flags & 0xff;
    let off = iod.start_sector << 9;
    let bytes = iod.nr_sectors << 9;

    let (cmd, data_out, data_in) = match op {
        libublk::sys::UBLK_IO_OP_READ => (NBD_CMD_READ, false, true),
        libublk::sys::UBLK_IO_OP_WRITE => (NBD_CMD_WRITE, true, false),
        libublk::sys::UBLK_IO_OP_FLUSH => (NBD_CMD_FLUSH, false, false),
        libublk::sys::UBLK_IO_OP_DISCARD => (NBD_CMD_TRIM, false, false),
        libublk::sys::UBLK_IO_OP_WRITE_ZEROES => (NBD_CMD_WRITE_ZEROES, false, false),
        _ => return -libc::EOPNOTSUPP,
    };
    if (cmd == NBD_CMD_TRIM && tflags & NBD_FLAG_SEND_TRIM == 0)
        || (cmd == NBD_CMD_WRITE_ZEROES && tflags & NBD_FLAG_SEND_WRITE_ZEROES == 0)
    {
        return -libc::EOPNOTSUPP;
    }

    let handle = nq.arm(tag, if data_in { bytes } else { 0 });
    // NBD_CMD_FLUSH must carry zero offset and length
    let (req_off, req_len) = if cmd == NBD_CMD_FLUSH {
        (0, 0)
    } else {
        (off, bytes)
    };
    let hdr = build_req(cmd, 0, handle, req_off, req_len);

    let payload = if data_out {
        if nq.zc {
            Some((std::ptr::null(), bytes))
        } else {
            Some((nq.bufs[tag as usize].as_mut_ptr() as *const u8, bytes))
        }
    } else {
        None
    };
    {
        let mut sendq = nq.sendq.borrow_mut();
        if sendq.dead {
            return -libc::EPIPE;
        }
        sendq.items.push_back(SendItem { hdr, payload, tag });
        if let Some(waker) = sendq.waker.take() {
            waker.wake();
        }
    }

    let res = nq.wait_reply(tag).await;
    if res < 0 {
        return res;
    }
    match op {
        libublk::sys::UBLK_IO_OP_READ | libublk::sys::UBLK_IO_OP_WRITE => bytes as i32,
        _ => 0,
    }
}

/// The queue's sender task: the single point that puts bytes on the
/// socket. Draining the whole backlog as one IO_LINK chain gives every
/// queued request a serial position in the stream -- concurrent sends
/// without a global order can interleave mid-frame the moment one of
/// them blocks on socket pressure (ublksrv's nbd chains its sends for
/// the same reason).
///
/// KNOWN ISSUE (scheduling order): by design this task should run only
/// after every currently-runnable io task has run, so a full round of
/// pushes lands in the backlog before one drain. The current
/// implementation does not guarantee that, and Tokio actively inverts
/// it: the wake from the round's FIRST push lands this task in the
/// scheduler's LIFO slot, running it ahead of the remaining io tasks
/// (measured: ~1.2 items per drain at q=1). It is tolerable today only
/// because of pipeline equilibrium -- pushes made while a send is in
/// flight accumulate wake-free and drain together on completion, and
/// an experimental run-last variant (yield_now before the drain, which
/// re-queues behind the woken io tasks and cannot stall since yield
/// self-wakes) measured neutral to -1.6%. The clean fix is a
/// customized executor via libublk's UblkExecutor trait: one without a
/// LIFO slot (plain FIFO, as smol's LocalExecutor behaves) or with an
/// explicit low-priority lane for this task, making run-last a
/// scheduling property instead of a per-wake workaround.
async fn nbd_send_task(nq: &NbdQueue) {
    let mut chain_ops = ChunkOps::default();
    loop {
        // Wait for work
        let more = std::future::poll_fn(|cx| {
            let mut sendq = nq.sendq.borrow_mut();
            if sendq.dead {
                return Poll::Ready(false);
            }
            if sendq.items.is_empty() {
                if sendq
                    .waker
                    .as_ref()
                    .map_or(true, |w| !w.will_wake(cx.waker()))
                {
                    sendq.waker = Some(cx.waker().clone());
                }
                Poll::Pending
            } else {
                Poll::Ready(true)
            }
        })
        .await;
        if !more {
            return;
        }

        // Drain the backlog and ship it in stream order. Nothing else
        // runs between the pushes on this single-threaded executor, so
        // ordering holds without a lock.
        let batch: Vec<SendItem> = nq.sendq.borrow_mut().items.drain(..).collect();
        let failed = if nq.zc {
            send_batch_zc_chain(nq, &batch, &mut chain_ops).await
        } else {
            send_batch_gather(nq, &batch).await
        };
        if failed {
            nq.sendq.borrow_mut().dead = true;
            nbd_poison(nq);
            return;
        }
    }
}

/// Ship one batch as a single vectored SENDMSG (SENDMSG_ZC with
/// `--send-zc`): every header and payload becomes one iovec entry, so
/// the whole batch has a fixed serial position in the stream and costs
/// one op -- ioutgt's gather-send shape.
async fn send_batch_gather(nq: &NbdQueue, batch: &[SendItem]) -> bool {
    let mut iovs: Vec<libc::iovec> = Vec::with_capacity(batch.len() * 2);
    let mut expected: usize = 0;
    let mut payload_bytes: usize = 0;
    for item in batch {
        iovs.push(libc::iovec {
            iov_base: item.hdr.as_ptr() as *mut libc::c_void,
            iov_len: NBD_REQUEST_LEN,
        });
        expected += NBD_REQUEST_LEN;
        if let Some((ptr, len)) = item.payload {
            iovs.push(libc::iovec {
                iov_base: ptr as *mut libc::c_void,
                iov_len: len as usize,
            });
            expected += len as usize;
            payload_bytes += len as usize;
        }
    }
    // SAFETY: zero-initialized msghdr with only iov fields set
    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_iov = iovs.as_mut_ptr();
    msg.msg_iovlen = iovs.len();

    // ZC pays a per-send page-pin; for small batches (headers only, or
    // little payload) the copy is cheaper.
    const SEND_ZC_MIN_BYTES: usize = 32 << 10;
    let sent = if nq.send_zc && payload_bytes >= SEND_ZC_MIN_BYTES {
        // SAFETY: `batch`, `iovs` and `msg` outlive the await; payload
        // buffers are reused only after the replies resolve
        match unsafe { ops::sendmsg_zc(nq.sock, &msg, libc::MSG_WAITALL) } {
            Ok(mut op) => {
                let sent = op.sent().await;
                op.into_notif().await;
                sent
            }
            Err(e) => e.errno(),
        }
    } else {
        // SAFETY: as above
        match unsafe { ops::sendmsg_raw(nq.sock, &msg, libc::MSG_WAITALL) } {
            Ok(op) => op.await,
            Err(e) => e.errno(),
        }
    };
    if sent != expected as i32 {
        log::error!("nbd: gather send returned {} (expected {})", sent, expected);
        return true;
    }
    false
}

/// One chunk's in-flight sends, awaited by [`await_chunk_sends`].
/// Allocated once per queue in [`nbd_send_task`]; the await drains the
/// vectors in place, so their capacity is reused chunk after chunk.
#[derive(Default)]
struct ChunkOps {
    /// (op, expected byte count) for headers and non-ZC payload sends
    plain: Vec<(ops::RawOp, i32)>,
    /// (op, expected byte count) for `--send-zc` payload sends
    zc: Vec<(ops::SendZcOp, i32)>,
}

/// Free SQ slots after guaranteeing at least `min`, flushing the ring
/// once if needed (the caller must have no open link chain). `None`
/// means the ring is broken.
fn ensure_sq_room(min: usize) -> Option<usize> {
    let free_slots = || {
        libublk::io::with_task_io_ring_mut(|r| {
            let sq = r.submission();
            sq.capacity() - sq.len()
        })
    };
    let free = free_slots();
    if free >= min {
        return Some(free);
    }
    libublk::io::with_task_io_ring_mut(|r| r.submit()).ok()?;
    let free = free_slots();
    (free >= min).then_some(free)
}

/// How many leading `batch` items fit in `free` SQ slots (a header SQE
/// each, plus a payload SQE for writes). `free >= 2` guarantees at
/// least one.
fn chunk_items(batch: &[SendItem], free: usize) -> usize {
    let mut sqes = 0;
    for (i, item) in batch.iter().enumerate() {
        sqes += 1 + item.payload.is_some() as usize;
        if sqes > free {
            return i;
        }
    }
    batch.len()
}

/// Build the header-send SQE for `item` on the fixed socket file `idx`,
/// linked to the following SQE unless it terminates the chain.
fn hdr_send_sqe(idx: u16, item: &SendItem, linked: bool) -> squeue::Entry {
    let sqe = opcode::Send::new(
        types::Fixed(idx as u32),
        item.hdr.as_ptr(),
        NBD_REQUEST_LEN as u32,
    )
    .flags(libc::MSG_WAITALL)
    .build();
    if linked {
        sqe.flags(squeue::Flags::IO_LINK)
    } else {
        sqe
    }
}

/// Push one chunk as a single IO_LINK chain: header before payload,
/// every element linked to the next, the chunk's last element unlinked.
/// Deliberately NOT async -- no await point can slip between the
/// caller's SQ-space check and these pushes, which is what guarantees
/// the chain reaches the kernel in one submission.
///
/// Returns the ops actually pushed plus whether a push failed; the
/// caller must await the ops either way, since their SQEs reference
/// `chunk`'s memory.
fn push_chunk_chain(nq: &NbdQueue, idx: u16, chunk: &[SendItem], ops_out: &mut ChunkOps) -> bool {
    debug_assert!(ops_out.plain.is_empty() && ops_out.zc.is_empty());
    let last = chunk.len() - 1;
    for (i, item) in chunk.iter().enumerate() {
        let link_hdr = item.payload.is_some() || i != last;
        // SAFETY: the caller awaits every pushed op before `chunk` dies
        match unsafe { ops::submit_sqe(hdr_send_sqe(idx, item, link_hdr)) } {
            Ok(op) => ops_out.plain.push((op, NBD_REQUEST_LEN as i32)),
            Err(_) => return true,
        }
        let Some((_, len)) = item.payload else {
            continue;
        };
        // SAFETY: the ublk request buffer stays registered until the
        // reply resolves and the io is committed
        if nq.send_zc {
            match unsafe {
                ops::send_zc_fixed(nq.sock, item.tag, 0, len, libc::MSG_WAITALL, i != last)
            } {
                Ok(op) => ops_out.zc.push((op, len as i32)),
                Err(_) => return true,
            }
        } else {
            match unsafe {
                ops::send_fixed(nq.sock, item.tag, 0, len, libc::MSG_WAITALL, i != last)
            } {
                Ok(op) => ops_out.plain.push((op, len as i32)),
                Err(_) => return true,
            }
        }
    }
    false
}

/// Await every send of one chunk, draining `ops` in place (the vector
/// capacity survives for the next chunk); true if any send failed or
/// fell short.
async fn await_chunk_sends(ops: &mut ChunkOps) -> bool {
    let mut failed = false;
    for (op, expected) in ops.plain.drain(..) {
        let res = op.await;
        if res != expected {
            log::error!("nbd: send returned {} (expected {})", res, expected);
            failed = true;
        }
    }
    for (mut op, expected) in ops.zc.drain(..) {
        let sent = op.sent().await;
        op.into_notif().await;
        if sent != expected {
            log::error!("nbd: zc send returned {} (expected {})", sent, expected);
            failed = true;
        }
    }
    failed
}

/// Ship one `-z` batch as IO_LINK chains: a write's payload has no
/// userspace address (it lives in the ublk-registered kernel buffer),
/// so it cannot join a gather iovec; instead each header is linked
/// before its fixed-buffer send.
///
/// A link chain only holds together inside ONE ring submission: the
/// kernel's link state ends with each io_uring_enter, and libublk's
/// SQE push flushes the ring mid-loop when the SQ fills -- a flushed
/// half-chain executes concurrently with the rest and interleaves
/// mid-frame on the socket. So the batch is shipped in chunks sized by
/// [`ensure_sq_room`]/[`chunk_items`] to what fits in one submission,
/// and each chunk is awaited to completion before the next is pushed;
/// completion, like the cross-batch case, preserves stream order.
async fn send_batch_zc_chain(nq: &NbdQueue, batch: &[SendItem], ops: &mut ChunkOps) -> bool {
    let TgtFd::Fixed(idx) = nq.sock else {
        unreachable!()
    };
    let mut rest = batch;
    while !rest.is_empty() {
        let Some(free) = ensure_sq_room(2) else {
            return true;
        };
        let (chunk, tail) = rest.split_at(chunk_items(rest, free));
        rest = tail;

        let push_failed = push_chunk_chain(nq, idx, chunk, ops);
        // Await also on the push-error path: the in-flight SQEs
        // reference `batch`, which the caller frees on return.
        let send_failed = await_chunk_sends(ops).await;
        if push_failed || send_failed {
            return true;
        }
    }
    false
}

/// The queue's receive task: demux simple replies (and read payloads)
/// back to the waiting tags.
async fn nbd_recv_task(nq: &NbdQueue) {
    let mut hdr = [0u8; NBD_REPLY_LEN];
    loop {
        let res = recv_all(nq.sock, nq.sock_raw, hdr.as_mut_ptr(), NBD_REPLY_LEN as u32).await;
        if res < 0 {
            // Connection gone (or shutdown at queue teardown): fail
            // whatever is still waiting.
            nq.complete_all(res);
            return;
        }
        let magic = u32::from_be_bytes(hdr[0..4].try_into().unwrap());
        let error = u32::from_be_bytes(hdr[4..8].try_into().unwrap());
        let handle = u64::from_be_bytes(hdr[8..16].try_into().unwrap());
        let tag = handle as u32 as u16;
        let cookie = (handle >> 32) as u32;

        if magic != NBD_SIMPLE_REPLY_MAGIC || (tag as usize) >= nq.slots.borrow().len() {
            log::error!("nbd: bad reply magic {:#x} or tag {}", magic, tag);
            nq.complete_all(-libc::EPROTO);
            return;
        }
        {
            let slots = nq.slots.borrow();
            let slot = &slots[tag as usize];
            if slot.cookie != cookie || slot.result.is_some() {
                log::error!("nbd: stale reply for tag {} cookie {}", tag, cookie);
                drop(slots);
                nq.complete_all(-libc::EPROTO);
                return;
            }
        }

        let read_len = nq.slots.borrow()[tag as usize].read_len;
        if error != 0 {
            nq.complete(tag, -(error.min(libc::EIO as u32 * 25) as i32));
            continue;
        }
        if read_len > 0 {
            let res = if nq.zc {
                recv_all_fixed(nq.sock, tag, read_len).await
            } else {
                recv_all(
                    nq.sock,
                    nq.sock_raw,
                    nq.bufs[tag as usize].as_mut_ptr(),
                    read_len,
                )
                .await
            };
            if res < 0 {
                nq.complete_all(res);
                return;
            }
        }
        nq.complete(tag, 0);
    }
}

async fn nbd_io_task(nq: &NbdQueue, tag: u16, tflags: u16) -> Result<(), UblkError> {
    let auto_buf_reg = libublk::sys::ublk_auto_buf_reg {
        index: tag,
        flags: 0,
        ..Default::default()
    };
    let buf_desc = if nq.zc {
        BufDesc::AutoReg(auto_buf_reg)
    } else {
        BufDesc::Slice(nq.bufs[tag as usize].as_slice())
    };
    let io_buf = if nq.zc {
        None
    } else {
        Some(&nq.bufs[tag as usize])
    };

    nq.q.submit_io_prep_cmd(tag, buf_desc.clone(), 0, io_buf)
        .await?;
    loop {
        let res = nbd_handle_io(nq, tag, tflags).await;
        nq.q.submit_io_commit_cmd(tag, buf_desc.clone(), res)
            .await?;
    }
}

fn q_fn(qid: u16, dev: &Arc<UblkDev>, tflags: u16, send_zc: bool) -> Result<(), UblkError> {
    let depth = dev.dev_info.queue_depth;
    let zc = (dev.dev_info.flags & libublk::sys::UBLK_F_AUTO_BUF_REG as u64) != 0;
    let sock_raw = dev.tgt.fds[(qid + 1) as usize];
    let dev = dev.clone();

    let rt = crate::Rt::new()?;
    rt.block_on(async move {
        let q = Rc::new(UblkQueue::new(qid, &dev)?);
        let bufs = if zc {
            Vec::new()
        } else {
            (0..depth)
                .map(|_| IoBuf::<u8>::new(dev.dev_info.max_io_buf_bytes as usize))
                .collect()
        };
        let nq = Rc::new(NbdQueue {
            q,
            sock: TgtFd::Fixed(qid + 1),
            sock_raw,
            bufs,
            zc,
            send_zc,
            slots: RefCell::new((0..depth).map(|_| Slot::default()).collect()),
            sendq: RefCell::new(SendQueue::default()),
        });

        let mut handles = Vec::new();
        for tag in 0..depth {
            let nq = nq.clone();
            handles.push(libublk::spawn_local(async move {
                match nbd_io_task(&nq, tag, tflags).await {
                    Err(UblkError::QueueIsDown) | Ok(_) => {}
                    Err(e) => log::error!("nbd io task failed for tag {}: {}", tag, e),
                }
            }));
        }
        let recv_nq = nq.clone();
        let recv_task = libublk::spawn_local(async move { nbd_recv_task(&recv_nq).await });
        let send_nq = nq.clone();
        let send_task = libublk::spawn_local(async move { nbd_send_task(&send_nq).await });

        for handle in handles {
            let _ = handle.await;
        }
        // Stop the sender and unblock the receive task's pending recv
        {
            let mut sendq = nq.sendq.borrow_mut();
            sendq.dead = true;
            if let Some(waker) = sendq.waker.take() {
                waker.wake();
            }
        }
        // SAFETY: plain shutdown(2) on this queue's connection
        unsafe { libc::shutdown(nq.sock_raw, libc::SHUT_RDWR) };
        let _ = send_task.await;
        let _ = recv_task.await;
        Ok(())
    })
}

pub(crate) fn ublk_add_nbd(
    ctrl: UblkCtrl,
    opt: Option<NbdAddArgs>,
    comm_arc: &Arc<crate::DevIdComm>,
) -> anyhow::Result<i32> {
    let info = ctrl.dev_info();
    if (info.flags & libublk::sys::UBLK_F_USER_COPY as u64) != 0 {
        return Err(anyhow::anyhow!("nbd doesn't support user copy"));
    }

    let json = match opt {
        Some(ref o) => NbdJson {
            host: o.host.clone(),
            port: o.port,
            unix: o.unix.clone(),
            export: o.export.clone(),
            send_zc: o.send_zc,
        },
        None => {
            let val = ctrl
                .get_target_data_from_json()
                .ok_or_else(|| anyhow::anyhow!("no json data"))?;
            serde_json::from_value(val["nbd"].clone())
                .map_err(|e| anyhow::anyhow!("invalid nbd json: {}", e))?
        }
    };

    // One negotiated connection per queue
    let mut socks = Vec::new();
    let mut size = 0u64;
    let mut tflags = 0u16;
    for i in 0..info.nr_hw_queues {
        let (sock, s, f) = nbd_connect(&json)?;
        if i == 0 {
            size = s;
            tflags = f;
        }
        socks.push(sock);
    }
    if info.nr_hw_queues > 1 && tflags & NBD_FLAG_CAN_MULTI_CONN == 0 {
        return Err(anyhow::anyhow!(
            "server does not allow multiple connections; use -q 1"
        ));
    }
    log::info!(
        "nbd: connected, size {}MB transmission flags {:#x}",
        size >> 20,
        tflags
    );

    // SEND_ZC does not support AF_UNIX; fall back to plain sends there
    // (the -z data path then uses fixed-buffer sends).
    let send_zc = json.send_zc && json.unix.is_none();
    if json.send_zc && !send_zc {
        log::warn!("nbd: --send-zc disabled on unix socket");
    }
    let tgt_init = |dev: &mut UblkDev| {
        let tgt = &mut dev.tgt;
        for sock in &socks {
            let nr_fds = tgt.nr_fds;
            tgt.fds[nr_fds as usize] = sock.as_raw_fd();
            tgt.nr_fds = nr_fds + 1;
        }
        tgt.dev_size = size;
        // Sends and receives per tag can overlap; give the rings room
        tgt.sq_depth = dev.dev_info.queue_depth * 2;
        tgt.cq_depth = dev.dev_info.queue_depth * 2;

        let mut attrs = 0;
        if tflags & NBD_FLAG_SEND_FLUSH != 0 {
            attrs |= libublk::sys::UBLK_ATTR_VOLATILE_CACHE;
        }
        if tflags & NBD_FLAG_READ_ONLY != 0 {
            attrs |= libublk::sys::UBLK_ATTR_READ_ONLY;
        }
        tgt.params = libublk::sys::ublk_params {
            types: libublk::sys::UBLK_PARAM_TYPE_BASIC
                | if tflags & (NBD_FLAG_SEND_TRIM | NBD_FLAG_SEND_WRITE_ZEROES) != 0 {
                    libublk::sys::UBLK_PARAM_TYPE_DISCARD
                } else {
                    0
                },
            basic: libublk::sys::ublk_param_basic {
                attrs,
                logical_bs_shift: 9,
                physical_bs_shift: 12,
                io_opt_shift: 12,
                io_min_shift: 9,
                max_sectors: dev.dev_info.max_io_buf_bytes >> 9,
                dev_sectors: size >> 9,
                ..Default::default()
            },
            discard: libublk::sys::ublk_param_discard {
                discard_granularity: 512,
                max_discard_sectors: if tflags & NBD_FLAG_SEND_TRIM != 0 {
                    (1 << 30) >> 9
                } else {
                    0
                },
                max_write_zeroes_sectors: if tflags & NBD_FLAG_SEND_WRITE_ZEROES != 0 {
                    (1 << 30) >> 9
                } else {
                    0
                },
                max_discard_segments: 1,
                ..Default::default()
            },
            ..Default::default()
        };

        if let Some(ref o) = opt {
            o.gen_arg.apply_block_size(dev);
            o.gen_arg.apply_read_only(dev);
        }

        dev.set_target_json(serde_json::json!({"nbd": json }));
        Ok(())
    };

    let comm = comm_arc.clone();
    ctrl.run_target(
        tgt_init,
        move |qid, dev: &Arc<UblkDev>| {
            if let Err(e) = q_fn(qid, dev, tflags, send_zc) {
                log::error!("nbd queue {} failed: {}", qid, e);
            }
        },
        move |ctrl: &UblkCtrl| {
            if let Err(e) = comm.send_dev_id(ctrl.dev_info().dev_id) {
                log::error!("Failed to send device ID: {}", e);
            }
        },
    )?;

    Ok(0)
}
