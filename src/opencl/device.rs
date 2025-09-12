use std::ptr;

use super::VRamBufferConfig;
use anyhow::{Context, Result, bail};
use opencl3::{
    command_queue::{self as cl_command_queue, CommandQueue},
    context::Context as ClContext,
    device::{Device as CLDevice, get_device_ids},
    memory::Buffer,
    memory::{self as cl_memory},
    platform::get_platforms,
};

pub struct VramDevice {
    dev: CLDevice,
    ctx: ClContext,
}

impl VramDevice {
    pub fn new(config: &VRamBufferConfig) -> Result<Self> {
        let platforms = get_platforms().context("Failed to get OpenCL platforms")?;

        if platforms.is_empty() {
            bail!("No OpenCL platforms available");
        }

        if config.platform_index >= platforms.len() {
            bail!(
                "Platform index {} is out of bounds (max: {})",
                config.platform_index,
                platforms.len() - 1
            );
        }
        let platform = &platforms[config.platform_index];

        let device_ids = platform
            .get_devices(config.device)
            .context("Failed to get device list")?;

        if device_ids.is_empty() {
            bail!(
                "No OCL devices found for platform {}",
                config.platform_index
            );
        }

        if config.device_index >= device_ids.len() {
            bail!(
                "Device index {} is out of bounds (max: {})",
                config.device_index,
                device_ids.len() - 1
            );
        }
        let device = CLDevice::new(device_ids[config.device_index]);
        let context = ClContext::from_device(&device).context("Failed to create OpenCL context")?;
        Ok(Self {
            dev: device,
            ctx: context,
        })
    }

    /// Get the device name
    pub fn name(&self) -> String {
        self.dev
            .name()
            .unwrap_or_else(|_| "Unknown device".to_string())
    }

    /// Create a new CommandQueue
    pub fn create_queue(&self) -> Result<CommandQueue> {
        unsafe {
            CommandQueue::create_with_properties(
                &self.ctx,
                self.dev.id(),
                cl_command_queue::CL_QUEUE_OUT_OF_ORDER_EXEC_MODE_ENABLE,
                0,
            )
            .context("Failed to create command queue")
        }
    }

    /// create a new Buffer
    pub fn create_buffer(&self, queue: &CommandQueue, size: usize) -> Result<Buffer<u8>> {
        unsafe {
            let mut buffer = Buffer::<u8>::create(
                &self.ctx,
                cl_memory::CL_MEM_READ_WRITE,
                size,
                ptr::null_mut(),
            )
            .context("Failed to allocate OCL memory")?;

            log::info!(
                "Created OpenCL buffer of size {} bytes on device: {}",
                size,
                self.name()
            );
            let _ = queue
                .enqueue_fill_buffer(&mut buffer, &[0u8], 0, size, &[])
                .context("Failed to reset OCL memory")?
                .wait();
            Ok(buffer)
        }
    }
}
impl Drop for VramDevice {
    fn drop(&mut self) {
        log::debug!("Freeing OCL device");
    }
}
/// Lists available OpenCL devices.
#[allow(dead_code)]
pub(crate) fn list_opencl_devices(config: &VRamBufferConfig) -> Result<()> {
    println!("Available OpenCL Platforms and Devices:");
    let platforms = get_platforms().context("Failed to get OpenCL platforms")?;
    if platforms.is_empty() {
        println!("  No OpenCL platforms found.");
        return Ok(());
    }

    for (plat_idx, platform) in platforms.iter().enumerate() {
        let plat_name = platform
            .name()
            .unwrap_or_else(|_| "Unknown Platform".to_string());
        println!("\nPlatform {}: {}", plat_idx, plat_name);

        match get_device_ids(platform.id(), config.device) {
            Ok(device_ids) => {
                if device_ids.is_empty() {
                    println!("  No OCL devices found on this platform.");
                } else {
                    for (dev_idx, device_id) in device_ids.iter().enumerate() {
                        let device = CLDevice::new(*device_id);
                        let dev_name = device
                            .name()
                            .unwrap_or_else(|_| "Unknown Device".to_string());
                        let dev_vendor = device
                            .vendor()
                            .unwrap_or_else(|_| "Unknown Vendor".to_string());
                        let dev_mem = device.global_mem_size().unwrap_or(0);
                        println!(
                            "  Device {}: {} ({}) - Memory: {} MB",
                            dev_idx,
                            dev_name,
                            dev_vendor,
                            dev_mem / (1024 * 1024)
                        );
                    }
                }
            }
            Err(e) => {
                println!("  Error getting devices for this platform: {}", e);
            }
        }
    }
    Ok(())
}
