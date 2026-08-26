//! `--shmem-zc` plumbing shared by the targets that support it.
//!
//! With `UBLK_F_SHMEM_ZC` the daemon registers a shared mapping with the
//! driver; requests an application issues from the same memory (say `fio
//! --mem=mmaphuge:FILE` over the hugetlbfs file given with `--htlb`) arrive
//! with `UBLK_IO_F_SHMEM_ZC` and are served straight from the mapping, no
//! copy on either side. Everything else keeps the target's normal buffer
//! path, so a target consults [`Shmem::bufs`] first and falls through.
//!
//! Registration happens before `START_DEV`, when the device has no disk yet
//! and REG_BUF needs no queue freeze. The driver keeps registrations for the
//! life of the device, so on `-r` recovery the file is mapped again and
//! adopted under the index recorded in the device JSON -- no REG_BUF, which
//! would have to freeze a quiesced queue that cannot drain.

use crate::args::GenAddArgs;
use libublk::{ctrl::UblkCtrl, io::UblkDev, ShmemBuf, ShmemBufs};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;

/// What recovery needs: the file to map again, the driver index it is
/// registered under, and the file's identity, since the driver pinned
/// *pages* -- a file deleted and recreated at the same path is different
/// memory and must not be adopted under the old index.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ShmemJson {
    htlb: PathBuf,
    index: u16,
    dev: u64,
    ino: u64,
    size: u64,
}

impl ShmemJson {
    fn new(htlb: PathBuf, index: u16) -> anyhow::Result<Self> {
        use std::os::linux::fs::MetadataExt;
        let md = std::fs::metadata(&htlb)?;
        Ok(Self {
            htlb,
            index,
            dev: md.st_dev(),
            ino: md.st_ino(),
            size: md.st_size(),
        })
    }

    fn same_file(&self) -> anyhow::Result<bool> {
        use std::os::linux::fs::MetadataExt;
        let md = std::fs::metadata(&self.htlb)?;
        Ok(md.st_dev() == self.dev && md.st_ino() == self.ino && md.st_size() == self.size)
    }
}

/// The registered shared buffers of one device.
pub(crate) struct Shmem {
    /// Consulted by the queue handlers; empty when `--shmem-zc` is off.
    pub bufs: Arc<ShmemBufs>,
    json: Option<ShmemJson>,
}

impl Shmem {
    /// Map and register the `--htlb` file of a new device, or, when `gen`
    /// is `None` (recovery), re-map and adopt what the device JSON records.
    pub fn new(ctrl: &UblkCtrl, gen: Option<&GenAddArgs>) -> anyhow::Result<Self> {
        match gen {
            Some(gen) => Self::setup(ctrl, gen),
            None => Self::recover(ctrl),
        }
    }

    fn setup(ctrl: &UblkCtrl, gen: &GenAddArgs) -> anyhow::Result<Self> {
        let bufs = Arc::new(ShmemBufs::new());
        let Some(htlb) = gen.htlb.as_ref() else {
            return Ok(Self { bufs, json: None });
        };
        let htlb = gen.build_abs_path(htlb.clone());
        let buf = ShmemBuf::open(&htlb, false)
            .map_err(|e| anyhow::anyhow!("map hugetlb file {}: {}", htlb.display(), e))?;
        let index = bufs
            .register(ctrl, buf)
            .map_err(|e| anyhow::anyhow!("register shmem buffer: {}", e))?;
        log::info!("shmem: {} registered as buffer {}", htlb.display(), index);
        Ok(Self {
            bufs,
            json: Some(ShmemJson::new(htlb, index)?),
        })
    }

    fn recover(ctrl: &UblkCtrl) -> anyhow::Result<Self> {
        let bufs = Arc::new(ShmemBufs::new());
        let json: Option<ShmemJson> = match ctrl.get_target_data_from_json() {
            Some(val) if !val["shmem"].is_null() => Some(
                serde_json::from_value(val["shmem"].clone())
                    .map_err(|e| anyhow::anyhow!("invalid shmem json: {}", e))?,
            ),
            _ => None,
        };
        if let Some(j) = &json {
            if !j.same_file()? {
                anyhow::bail!(
                    "shmem: {} is not the file registered as buffer {}; refusing to adopt it",
                    j.htlb.display(),
                    j.index
                );
            }
            let buf = ShmemBuf::open(&j.htlb, false)
                .map_err(|e| anyhow::anyhow!("map hugetlb file {}: {}", j.htlb.display(), e))?;
            bufs.adopt(j.index, buf);
            log::info!("shmem: {} adopted as buffer {}", j.htlb.display(), j.index);
        }
        Ok(Self { bufs, json })
    }

    /// Record the registration in the device JSON for recovery. Call at the
    /// end of the target's init hook, after it stored its own data.
    pub fn save_json(&self, dev: &mut UblkDev) {
        let Some(json) = &self.json else {
            return;
        };
        let mut val = dev
            .get_target_json()
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));
        val["shmem"] = serde_json::json!(json);
        dev.set_target_json(val);
    }
}
