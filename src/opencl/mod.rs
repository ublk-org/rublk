//! OpenCL module for OCL memory allocation and management
//!
//! This module handles interaction with the OCL via OpenCL,
//! including device selection, memory allocation, and data transfer.

mod device;
mod memory;

pub(crate) use device::{list_opencl_devices, VramDevice};
pub(crate) use memory::{VRamBuffer, VRamBufferConfig};
