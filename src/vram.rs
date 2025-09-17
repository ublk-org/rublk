use anyhow::Result;
use libublk::{
    ctrl::UblkCtrl,
    helpers::IoBuf,
    io::{UblkDev, UblkQueue},
    uring_async::ublk_wait_and_handle_ios,
    UblkError,
};
use std::rc::Rc;
use std::sync::Arc;

use crate::opencl::{list_opencl_devices, VRamBuffer, VRamBufferConfig, VramDevice};

#[derive(clap::Args, Debug)]
pub(crate) struct VramAddArgs {
    #[command(flatten)]
    pub(crate) gen_arg: super::args::GenAddArgs,

    /// Size of the block device (e.g., 512MiB, 2GiB).
    #[clap(long, default_value = "2GiB")]
    size: Option<String>,

    /// OCL device index to use (0 for first OCL)
    #[clap(long, default_value = "0")]
    device: usize,

    /// OpenCL platform index
    #[clap(long, default_value = "0")]
    platform: usize,

    /// Read/Write via memory mapping
    #[clap(short, long)]
    mmap: bool,

    /// How many block buffers
    #[clap(long, default_value = "1")]
    blocks: usize,

    /// CPU device
    #[clap(long)]
    cpu: bool,
}

#[derive(clap::Args, Debug)]
pub(crate) struct VramCmd {
    /// List available OpenCL devices
    #[clap(long)]
    pub(crate) list_opencl_dev: bool,
}

fn handle_io_cmd(
    q: &UblkQueue<'_>,
    tag: u16,
    buf: &IoBuf<u8>,
    vrams: &Arc<Vec<VRamBuffer>>,
) -> i32 {
    let iod = q.get_iod(tag);
    let global_limit = q.dev.tgt.dev_size;

    let mut global_offset = global_limit.min(iod.start_sector << 9);
    let mut global_length = (iod.nr_sectors << 9) as usize;
    if global_offset + global_length as u64 >= global_limit {
        global_length = (global_limit - global_offset) as usize;
    }

    if global_length > 0 {
        let op = iod.op_flags & 0xff;
        match op {
            libublk::sys::UBLK_IO_OP_READ | libublk::sys::UBLK_IO_OP_WRITE => {
                let operate = match op {
                    libublk::sys::UBLK_IO_OP_READ => "Read",
                    _ => "Write",
                };
                let mut local_offset = 0;
                let mut global_remaining = global_length;

                for (i, vram) in vrams.iter().enumerate() {
                    let local_remaining = vram.remaining(global_offset);
                    if local_remaining.is_none() {
                        continue;
                    }

                    let local_length = global_remaining.min(local_remaining.unwrap());

                    if libublk::sys::UBLK_IO_OP_READ == op {
                        let array = unsafe {
                            std::slice::from_raw_parts_mut(
                                buf.as_mut_ptr().add(local_offset),
                                local_length,
                            )
                        };
                        if let Err(e) = vram.read(global_offset, array) {
                            log::error!(
                                "{} error, device vram-{} offset {} size {}, code {}",
                                operate,
                                i,
                                global_offset,
                                local_length,
                                e
                            );
                            return -libc::EIO;
                        }
                    } else {
                        let array = unsafe {
                            std::slice::from_raw_parts(buf.as_ptr().add(local_offset), local_length)
                        };
                        if let Err(e) = vram.write(global_offset, array) {
                            log::error!(
                                "{} error, device vram-{} offset {} size {}, code {}",
                                operate,
                                i,
                                global_offset,
                                local_length,
                                e
                            );
                            return -libc::EIO;
                        }
                    }

                    global_remaining -= local_length;
                    if global_remaining == 0 {
                        break;
                    }
                    local_offset += local_length;
                    global_offset += local_length as u64;
                }

                if global_remaining > 0 {
                    log::error!(
                        "{} error, offset {} size {}",
                        operate,
                        global_offset,
                        global_remaining
                    );
                    return -libc::EIO;
                }
            }
            libublk::sys::UBLK_IO_OP_FLUSH => {}
            _ => {
                return -libc::EINVAL;
            }
        }
    }
    global_length as i32
}

async fn io_task(
    q: &UblkQueue<'_>,
    tag: u16,
    vrams: Arc<Vec<VRamBuffer>>,
) -> Result<(), UblkError> {
    let buf_bytes = q.dev.dev_info.max_io_buf_bytes as usize;
    let buf = IoBuf::<u8>::new(buf_bytes);
    let buf_desc = libublk::BufDesc::Slice(buf.as_slice());

    // Submit initial prep command
    q.submit_io_prep_cmd(tag, buf_desc.clone(), 0, Some(&buf))
        .await?;

    loop {
        let res = handle_io_cmd(q, tag, &buf, &vrams);
        q.submit_io_commit_cmd(tag, buf_desc.clone(), res).await?;
    }
}

fn q_fn(qid: u16, dev: &UblkDev, vrams: Arc<Vec<VRamBuffer>>) {
    let q_rc = match UblkQueue::new(qid, dev) {
        Ok(queue) => Rc::new(queue),
        Err(e) => {
            log::error!("Failed to create queue {}: {}", qid, e);
            return;
        }
    };
    let exe = smol::LocalExecutor::new();
    let mut f_vec = Vec::new();

    for tag in 0..dev.dev_info.queue_depth {
        let q = q_rc.clone();
        let use_vram = vrams.clone();
        f_vec.push(exe.spawn(async move {
            match io_task(&q, tag, use_vram).await {
                Err(UblkError::QueueIsDown) | Ok(_) => {}
                Err(e) => log::error!("vram io_task failed for tag {}: {}", tag, e),
            }
        }));
    }

    ublk_wait_and_handle_ios(&exe, &q_rc);
    smol::block_on(async { futures::future::join_all(f_vec).await });
}

pub(crate) fn ublk_add_vram(
    ctrl: UblkCtrl,
    vram_args: &VramAddArgs,
    comm_rc: &Arc<crate::DevIdComm>,
) -> Result<()> {
    if ctrl.dev_info().flags & (libublk::sys::UBLK_F_USER_RECOVERY as u64) != 0 {
        anyhow::bail!("vram device can't support recovery");
    }

    let size = match &vram_args.size {
        Some(size_str) => parse_size::parse_size(size_str)?,
        _ => 2 << 30,
    };
    let mut config = VRamBufferConfig {
        platform_index: vram_args.platform,
        device_index: vram_args.device,
        size: size as usize,
        mmap: vram_args.mmap,
        ..Default::default()
    };

    if vram_args.cpu {
        config.with_cpu();
    }

    log::info!(
        "Allocating {} bytes ({} MB) on OCL device {} (Platform {})",
        size * vram_args.blocks.max(1) as u64,
        size * vram_args.blocks.max(1) as u64 / (1024 * 1024),
        config.device_index,
        config.platform_index
    );

    let device = VramDevice::new(&config)?;
    let mut vrams: Vec<VRamBuffer> = Vec::new();
    for _ in 0..vram_args.blocks.max(1) {
        vrams.push(VRamBuffer::new(&device, config.size, config.mmap)?);
    }

    log::info!(
        "Successfully allocated {} bytes ({} MB) on {}",
        size * vram_args.blocks.max(1) as u64,
        size * vram_args.blocks.max(1) as u64 / (1024 * 1024),
        device.name()
    );

    let mut dev_size: u64 = 0;
    for v in vrams.iter_mut() {
        v.offset(dev_size);
        dev_size += v.size() as u64;
    }
    let dev_blocks = vrams.len();
    let use_vram = Arc::new(vrams);

    let comm = comm_rc.clone();
    ctrl.run_target(
        |dev| {
            dev.set_default_params(dev_size);
            dev.tgt.params.basic.attrs &= !libublk::sys::UBLK_ATTR_VOLATILE_CACHE;

            dev.set_target_json(serde_json::json!({
                "type": "vram",
                "blocks": dev_blocks,
                "size": dev_size,
            }));
            Ok(())
        },
        move |qid, dev| q_fn(qid, dev, use_vram.clone()),
        move |ctrl: &UblkCtrl| {
            if let Err(e) = comm.send_dev_id(ctrl.dev_info().dev_id) {
                log::error!("Failed to send device ID: {}", e);
            }
        },
    )?;

    Ok(())
}

pub(crate) fn ublk_vram_cmd(args: &VramCmd) -> Result<()> {
    if args.list_opencl_dev {
        let config = VRamBufferConfig {
            ..Default::default()
        };
        match list_opencl_devices(&config) {
            Ok(_) => {}
            Err(e) => {
                eprintln!("Error: {}", e);
                eprintln!("No OpenCL platforms found. Please ensure OpenCL drivers are installed.");
                return Ok(()); // Return success to avoid panic in main
            }
        }
    }
    Ok(())
}
