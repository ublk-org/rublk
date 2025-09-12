//! OCL memory management via OpenCL
//!
//! This module provides functionality to allocate and manage
//! OCL memory buffers that will be exposed as block devices.

use super::VramDevice;
use anyhow::{Context, Result, bail};
use opencl3::{
    command_queue::CommandQueue,
    memory::{self as cl_memory, Buffer, ClMem},
    device::{self as cl_device},
    types,
};
// Use std::sync::RwLock for thread-safe interior mutability
use std::ptr;
use std::sync::RwLock;

/// Configuration for a OCL memory buffer
#[derive(Debug, Clone)]
pub struct VRamBufferConfig {
    /// Size of the buffer in bytes
    pub size: usize,
    /// OCL device index to use (0 for first OCL)
    pub device_index: usize,
    /// Optional platform index (defaults to 0)
    pub platform_index: usize,
    /// Read/Write via mmap
    pub mmap: bool,
    /// Device
    pub device: u64,
}

impl VRamBufferConfig {
    pub fn with_cpu(&mut self) {
        self.device = cl_device::CL_DEVICE_TYPE_CPU;
    }
}

impl Default for VRamBufferConfig {
    fn default() -> Self {
        Self {
            size: 2048 * 1024 * 1024, // 2 GB default size
            device_index: 0,
            platform_index: 0,
            mmap: false,
            device: cl_device::CL_DEVICE_TYPE_GPU | cl_device::CL_DEVICE_TYPE_ACCELERATOR
        }
    }
}

/// A buffer allocated in OCL VRAM via OpenCL
// Make VRamBuffer Send + Sync by using RwLock for the buffer
pub struct VRamBuffer {
    queue: CommandQueue,
    buffer: RwLock<Buffer<u8>>,
    offset: u64,
    size: usize,
    mmap: bool,
}

impl VRamBuffer {
    /// Create a new OCL memory buffer with the specified configuration
    pub fn new(device: &VramDevice, size: usize, mmap: bool) -> Result<Self> {
        let queue = device.create_queue()?;
        let buffer = RwLock::new(device.create_buffer(&queue, size)?);
        Ok(Self {
            queue,
            buffer,
            offset: 0,
            size,
            mmap,
        })
    }

    /// Get the buffer size in bytes
    pub fn size(&self) -> usize {
        self.size
    }

    /// update the global offset
    pub fn offset(&mut self, offset: u64) {
        self.offset = offset;
    }

    // check offset in this vram
    #[inline]
    fn within(&self, offset: u64) -> bool {
        offset >= self.offset && offset < self.offset + self.size as u64
    }

    // the reamaining after current position
    pub fn remaining(&self, offset: u64) -> Option<usize> {
        if self.within(offset) {
            Some((self.size as u64 + self.offset - offset) as usize)
        } else {
            None
        }
    }

    /// Read data from the OCL buffer
    pub fn read(&self, offset: u64, data: &mut [u8]) -> Result<()> {
        if !self.within(offset) {
            bail!("Attempted to read out of buffer");
        }
        let local_offset = (offset - self.offset) as usize;
        let length = data.len();
        if local_offset + length > self.size {
            bail!("Attempted to read past end of buffer");
        }
        unsafe {
            if self.mmap {
                let buffer_guard = self
                    .buffer
                    .write()
                    .map_err(|_| anyhow::anyhow!("Failed to lock buffer RwLock for read"))?;
                let mut host_ptr = ptr::null_mut();
                let _ = self
                    .queue
                    .enqueue_map_buffer(
                        &*buffer_guard,
                        types::CL_TRUE,
                        cl_memory::CL_MEM_READ_ONLY,
                        local_offset,
                        length,
                        &mut host_ptr,
                        &[],
                    )
                    .context("Failed to mmap from buffer")?;

                data.as_mut_ptr().copy_from(host_ptr as *mut u8, length);

                let _ = self
                    .queue
                    .enqueue_unmap_mem_object(buffer_guard.get(), host_ptr, &[])
                    .context("Failed to unmmap from buffer")?
                    .wait();
            } else {
                let buffer_guard = self
                    .buffer
                    .read()
                    .map_err(|_| anyhow::anyhow!("Failed to lock buffer RwLock for read"))?;
                self.queue
                    .enqueue_read_buffer(&*buffer_guard, types::CL_TRUE, local_offset, data, &[])
                    .context("Failed to enqueue blocking read from buffer")?;
            }
        }

        Ok(())
    }

    /// Write data to the OCL buffer
    pub fn write(&self, offset: u64, data: &[u8]) -> Result<()> {
        if !self.within(offset) {
            bail!("Attempted to write out of buffer");
        }
        let local_offset = (offset - self.offset) as usize;
        let length = data.len();
        if local_offset + length > self.size {
            bail!("Attempted to write past end of buffer");
        }

        let mut buffer_guard = self
            .buffer
            .write()
            .map_err(|_| anyhow::anyhow!("Failed to lock buffer RwLock for write"))?;

        unsafe {
            if self.mmap {
                let mut host_ptr = ptr::null_mut();
                let _ = self
                    .queue
                    .enqueue_map_buffer(
                        &*buffer_guard,
                        types::CL_TRUE,
                        cl_memory::CL_MEM_WRITE_ONLY,
                        local_offset,
                        length,
                        &mut host_ptr,
                        &[],
                    )
                    .context("Failed to mmap from buffer")?;

                data.as_ptr().copy_to(host_ptr as *mut u8, length);

                let _ = self
                    .queue
                    .enqueue_unmap_mem_object(buffer_guard.get(), host_ptr, &[])
                    .context("Failed to unmmap from buffer")?
                    .wait();
            } else {
                self.queue
                    .enqueue_write_buffer(
                        &mut *buffer_guard,
                        types::CL_TRUE,
                        local_offset,
                        data,
                        &[],
                    )
                    .context("Failed to enqueue blocking write to buffer")?;
            }
        }

        Ok(())
    }
}

impl Drop for VRamBuffer {
    fn drop(&mut self) {
        log::debug!("Freeing OCL memory buffer");
    }
}
