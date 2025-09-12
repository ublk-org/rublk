//! OpenCL module for OCL memory allocation and management
//!
//! This module handles interaction with the OCL via OpenCL,
//! including device selection, memory allocation, and data transfer.

mod memory;
mod device;

pub(crate) use memory::{VRamBuffer, VRamBufferConfig};
pub(crate) use device::{VramDevice, list_opencl_devices};
