# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`rublk` is a CLI that creates Linux block devices whose IO logic runs in userspace, via the
kernel `ublk_drv` driver. It is a thin binary on top of the [`libublk`](https://crates.io/crates/libublk)
crate: `libublk` owns the io_uring/control-device plumbing, `rublk` supplies the per-target IO
logic (loop, null, zoned, qcow2, compress, vram) and the command-line/daemon layer.

Anything touching `UblkCtrl`, `UblkQueue`, `UblkDev`, `BufDesc`, `libublk::sys::*` is
libublk API — check the installed version's docs (`~/.cargo/registry/src/*/libublk-0.4.6/`)
rather than guessing, since this crate tracks libublk closely (see git log: most releases are
"depend on libublk X" + fixups).

## Build / test

```bash
cargo build                       # default features (= vram)
cargo build --features compress   # RocksDB target; compiles bundled RocksDB, slow, needs C++ toolchain + libclang
cargo build --no-default-features # drops the OpenCL/vram target
cargo fmt                         # .rustfmt.toml is enforced; several commits exist purely to re-pass `cargo fmt`
```

- The `vram` feature is on by default and links `opencl3`. It builds and links fine without any
  OpenCL runtime installed — a missing ICD only shows up at runtime (`rublk vram --list-opencl-dev`).
- CI builds with `RUSTFLAGS="--cfg=io_uring_skip_arch_check"` (both x86_64 and i686, stable + nightly).
  Use the same flag when reproducing CI failures.

### Tests

`tests/basic.rs` is the only test file. These are **integration tests that shell out to the built
`rublk` binary** (located via `current_exe()` → `target/debug/rublk`), create real ublk devices,
run `dd`/`mkfs`/`mount`/`blkdiscard` against them, then delete them.

```bash
sudo modprobe ublk_drv
cargo build                                   # or: cargo build --features compress
sudo -E $(which cargo) test
sudo -E $(which cargo) test test_ublk_loop_recover -- --nocapture   # single test
sudo -E $(which cargo) test -- --test-threads=1                     # serialize if devices interfere
```

- Root is required (ublk control device + mount). Use `sudo -E $(which cargo)` because `cargo`
  is usually not on root's `PATH`.
- **Every test silently returns success when `/dev/ublk-control` is missing** (`support_ublk()`),
  and zoned/compress/mkfs tests skip when their prerequisites are absent. A green `cargo test` run
  proves nothing unless the module is loaded — check that tests actually did work before claiming
  a fix passes.
- Optional external tools that unlock more coverage: `qemu-img` (qcow2), `mkfs.ext4`, `mkfs.btrfs`
  (zoned), `blkdiscard`, `fio`.
- `ci/` builds a Fedora VM image with mkosi (`mkosi.build` installs Rust, `cargo install`s rublk,
  clones blktests) for running the suite against a real kernel. `scripts/zfio` is a fio wrapper
  for zoned-device throughput testing.

## Architecture

### Command flow (`src/main.rs`, `src/args.rs`)

`clap` parses into `Commands` / `AddCommands` (`args.rs`). For `add`:

1. `ublk_add` — unless `--foreground`, **daemonizes**. Parent and child communicate over an
   `eventfd` wrapped in `DevIdComm`: the child writes `dev_id + 1` once the device is live (or
   `i64::MAX` on failure), the parent blocks on read and then dumps device info. `dev_id + 1`
   because writing 0 to an eventfd doesn't wake the reader.
2. `GenAddArgs::new_ublk_ctrl()` translates the shared flags (`-q/-d/-r/-u/-z/--mlock/...`) into
   `UBLK_F_*` control flags and `UblkFlags` dev flags, validating block sizes and IO buffer size.
3. `ublk_add_worker` dispatches to `<target>::ublk_add_<target>(ctrl, Some(args), &comm)`.

Because the daemon's stderr goes to /dev/null, a failing `add` gives a useless error — that is why
the error text tells users to re-run with `--foreground`. Do the same when debugging.

### Recovery

`recover` re-creates the device from state the kernel kept, with no CLI args available:

- The target type comes from `ctrl.get_target_type_from_json()`; per-target config comes from
  `ctrl.get_target_data_from_json()` (each target stores its own JSON via `dev.set_target_json()`
  during init — `LoJson`, `Qcow2Json`, `ZoneJson`, `{"compress": {"dir": …}}`).
- The original `UblkFlags` are smuggled through the **high 32 bits of `ublksrv_flags`**
  (`args.rs: combined_target_flags`, restored in `main.rs: ublk_recover_work`); the low bits hold
  `target_flags::TGT_QUIET`.
- This is why every target entry point takes `Option<Args>`: `Some` = fresh `add` (parse CLI,
  write JSON), `None` = recovery (read JSON back). Any new target option that must survive
  recovery has to go into that JSON blob.

### Target modules

Each target is one file and follows the same shape:

```rust
pub(crate) fn ublk_add_X(ctrl: UblkCtrl, opt: Option<XArgs>, comm: &Arc<DevIdComm>) -> anyhow::Result<i32> {
    ctrl.run_target(
        tgt_init,        // fill dev.tgt.params / dev_size, apply_block_size/apply_read_only, set_target_json
        q_handler,       // per-queue thread body, called once per qid
        dev_ready_cb,    // comm.send_dev_id(...) — unblocks the daemonizing parent
    )
}
```

Two queue-handling styles coexist; several targets support both:

- **Sync**: `UblkQueue::new(...).submit_fetch_commands_unified(...).wait_and_handle_io(closure)`.
  The closure is re-entered per CQE, so target IO completion and new commands are distinguished
  with `UblkIOCtx::is_tgt_io()` (see `__lo_handle_io_cmd_sync`).
- **Async** (`-a`/`--async-await`, and the only mode for compress/vram/zoned): one `smol`
  `LocalExecutor` per queue with **one task per tag**, each looping
  `submit_io_prep_cmd` → handle → `submit_io_commit_cmd`, driven by
  `wait_and_handle_io_events(...)`. `Err(UblkError::QueueIsDown)` is the normal shutdown path and
  must not be logged as an error.

Buffer handling is selected per-device and shows up as `BufDesc`:
`BufDesc::Slice` (normal copy or user-copy with an empty slice) vs `BufDesc::AutoReg` for
zero copy (`-z`, `UBLK_F_AUTO_BUF_REG`, currently only loop/null).

Per-target notes:

- `loop.rs` — builds raw io_uring SQEs (`Read`/`Write`/`ReadFixed`/`WriteFixed`/`Fsync`/`Fallocate`)
  against the backing file registered as fixed fd index 1; discard/write-zeroes mirror the kernel
  loop driver's `fallocate` mode selection.
- `null.rs` — the reference/minimal target; read it first when learning the queue APIs.
- `zoned.rs` — the largest module: emulated zoned device, ram-backed or file-backed via `--path`
  (a `superblock` file persists geometry). Forces `UBLK_F_ZONED | UBLK_F_USER_COPY`.
- `qcow2.rs` — delegates to the `qcow2-rs` crate; bridges its `Qcow2IoOps` trait to the queue
  through a **thread-local raw `UblkQueue` pointer**, because the trait methods have no place to
  carry queue context. Also drives periodic metadata flush.
- `compress.rs` (feature `compress`) — RocksDB-backed device, one key per logical block. RocksDB
  calls are blocking, so they are offloaded with `smol::unblock()`; completion is signalled back
  into the queue's io_uring loop through `notifier.rs` (an eventfd whose writes are coalesced by a
  counter). This target therefore runs a custom `run_uring_tasks` loop instead of
  `wait_and_handle_io_events`.
- `vram.rs` + `opencl/` (feature `vram`, default) — block device backed by GPU memory buffers via
  OpenCL. No recovery support.

### Cross-cutting constraints already encoded in the code

These are validated at add time; keep them in sync if you change target capabilities:

- `--zero-copy` only for `loop` and `null`.
- `--mlock` rejected for `compress`/`qcow2`/`zoned`, and rejected together with `--user-copy`
  (deadlock risk).
- `loop` rejects `--user-copy`; `null` rejects `--unprivileged`; `vram` and ram-backed `zoned`
  reject `--user-recovery`.
- Single-CPU affinity (`UBLK_DEV_F_SINGLE_CPU_AFFINITY`) is the default; `--multi-cpus-affinity`
  opts out.

## Adding a new target

1. New `src/<name>.rs` with a `#[derive(clap::Args)]` struct that `#[command(flatten)]`s
   `super::args::GenAddArgs`, plus a serde struct for anything recovery needs.
2. Implement `ublk_add_<name>(ctrl, Option<Args>, &Arc<DevIdComm>)` following the `run_target`
   shape above, and `dev.set_target_json()` in `tgt_init`.
3. Register in `main.rs`: `mod`, `ublk_parse_add_args`, `ublk_add_worker`, and the
   `ublk_recover_work` match arm (return a clear error if recovery is unsupported).
4. Register the variant in `args.rs: AddCommands`; add any incompatibility checks to
   `new_ublk_ctrl` / `validate_mlock_compatibility`.
5. Gate optional heavy dependencies behind a Cargo feature and `#[cfg(feature = "...")]` on every
   touch point (see how `compress` and `vram` thread through `main.rs`, `args.rs`, `tests/basic.rs`).
6. Add tests to `tests/basic.rs` using `run_rublk_add_dev` / `run_rublk_del_dev`, guarded by
   `support_ublk()`.

## Conventions

- Errors: `anyhow` at the CLI/setup layer, `UblkError` / raw `-libc::E*` inside IO paths (an IO
  handler returns a negative errno as the completion result rather than propagating).
- Commit subjects follow `component: summary` (`rublk: …`, `compress: …`, `Cargo: bump to vX.Y.Z`)
  with occasional `fix:` / `refactor:` / `cleanup:` prefixes.
- Releases are a `Cargo.toml` version bump commit; the crate is published to crates.io.
