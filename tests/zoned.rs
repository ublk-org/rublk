mod common;

#[cfg(test)]
mod zoned {
    use super::common::*;
    use libublk::ctrl::UblkCtrl;
    use std::path::Path;
    use std::process::{Command, Stdio};

    fn __test_ublk_add_del_zoned<F>(bs: u32, queues: u32, dir: Option<&String>, r: bool, tf: F)
    where
        F: Fn(&UblkCtrl, u32, usize),
    {
        let bs_str = format!("{}", bs);
        let queues_str = format!("{}", queues);
        let mut cmdline = [
            "add",
            "zoned",
            "-q",
            &queues_str,
            "--zone-size",
            "4",
            "--logical-block-size",
            &bs_str,
            "--conv-zones",
            "0",
        ]
        .to_vec();

        if let Some(d) = dir {
            cmdline.push("--path");
            cmdline.push(d);
        };
        if r {
            cmdline.push("-r");
        }

        let ctrl = run_rublk_add_dev(cmdline);
        tf(&ctrl, bs, 4 << 20);
        run_rublk_del_dev(ctrl, false);
    }

    #[test]
    fn test_ublk_add_del_zoned() {
        if !support_ublk() {
            return;
        }
        if !support_zoned() {
            return;
        }
        let tf = |ctrl: &UblkCtrl, bs: u32, _file_size: usize| {
            read_ublk_disk(ctrl);
            check_block_size(ctrl, bs);
        };
        __test_ublk_add_del_zoned(512, 1, None, false, tf);
        __test_ublk_add_del_zoned(4096, 1, None, false, tf);
    }

    /// A zone management command rejected by the target has to fail,
    /// instead of being left uncompleted forever.
    #[test]
    fn test_ublk_zoned_failed_mgmt_cmd() {
        if !support_ublk() {
            return;
        }
        if !support_zoned() {
            return;
        }
        if !has_blkzone() {
            return;
        }
        let tf = |ctrl: &UblkCtrl, _bs: u32, _file_size: usize| {
            // closing one empty zone is rejected
            let expect = ExpectIoErrors::new("zoned_failed_mgmt_cmd", ctrl);
            let mut child = Command::new("blkzone")
                .args(["close", "-o", "0", "-c", "1", &ctrl.get_bdev_path()])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap();
            let mut status = None;
            for _ in 0..100 {
                status = child.try_wait().unwrap();
                if status.is_some() {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
            let status = status.expect("zone close isn't completed");
            assert!(!status.success());
            drop(expect);

            // the device still works
            read_ublk_disk(ctrl);
        };
        __test_ublk_add_del_zoned(4096, 1, None, false, tf);
    }

    /// Reading the unwritten part of one zone has to return zeroes.
    #[test]
    fn test_ublk_zoned_read_unwritten() {
        if !support_ublk() {
            return;
        }
        if !support_zoned() {
            return;
        }
        let tf = |ctrl: &UblkCtrl, _bs: u32, _file_size: usize| {
            const BS: usize = 64 << 10;
            let dev = ctrl.get_bdev_path();
            let tmp_dir = tempfile::TempDir::new().unwrap();
            let pattern = tmp_dir.path().join("pattern");
            let out = tmp_dir.path().join("out");

            // write the 1st 64k of zone 0
            std::fs::write(&pattern, vec![0xaa_u8; BS]).unwrap();
            let res = Command::new("dd")
                .arg(format!("if={}", pattern.display()))
                .arg(format!("of={}", dev))
                .args(["bs=64k", "count=1", "oflag=direct"])
                .output()
                .unwrap();
            assert!(res.status.success());

            // read the 1st 128k by one IO, and its 2nd half isn't written
            let res = Command::new("dd")
                .arg(format!("if={}", dev))
                .arg(format!("of={}", out.display()))
                .args(["bs=128k", "count=1", "iflag=direct"])
                .output()
                .unwrap();
            assert!(res.status.success());

            let data = std::fs::read(&out).unwrap();
            assert!(data.len() == 2 * BS);
            assert!(data[..BS].iter().all(|b| *b == 0xaa));
            assert!(data[BS..].iter().all(|b| *b == 0));

            // and read the 2nd 64k only, which is behind the write pointer
            let res = Command::new("dd")
                .arg(format!("if={}", dev))
                .arg(format!("of={}", out.display()))
                .args(["bs=64k", "skip=1", "count=1", "iflag=direct"])
                .output()
                .unwrap();
            assert!(res.status.success());

            let data = std::fs::read(&out).unwrap();
            assert!(data.len() == BS);
            assert!(data.iter().all(|b| *b == 0));
        };
        __test_ublk_add_del_zoned(4096, 1, None, false, tf);

        let path_dir = tempfile::TempDir::new().unwrap();
        let path_str = path_dir.path().to_string_lossy().to_string();

        __test_ublk_add_del_zoned(4096, 1, Some(&path_str), false, tf);
    }

    /// One zone parsed from `blkzone report`, everything is in 512b sectors
    /// except for `cond`, and `wptr` is relative to `start`.
    #[derive(Debug)]
    struct ZoneRep {
        start: u64,
        len: u64,
        cap: u64,
        wptr: u64,
        cond: u32,
    }

    const ZONE_COND_NOT_WP: u32 = 0;
    const ZONE_COND_EMPTY: u32 = 1;
    const ZONE_COND_IMP_OPEN: u32 = 2;
    const ZONE_COND_CLOSED: u32 = 4;
    const ZONE_COND_FULL: u32 = 14;

    /// Report all zones, and `None` is returned if this `blkzone` is too old
    /// to show zone capacity.
    fn blkzone_report(ctrl: &UblkCtrl) -> Option<Vec<ZoneRep>> {
        let res = Command::new("blkzone")
            .args(["report", &ctrl.get_bdev_path()])
            .output()
            .unwrap();
        assert!(res.status.success());

        let out = String::from_utf8_lossy(&res.stdout).to_string();
        let re = regex::Regex::new(
            r"start: 0x([0-9a-f]+), len 0x([0-9a-f]+), cap 0x([0-9a-f]+), wptr 0x([0-9a-f]+) reset:(\d+) non-seq:(\d+).*zcond:\s*(\d+)\(",
        )
        .unwrap();
        let hex = |s: &str| u64::from_str_radix(s, 16).unwrap();
        let zones: Vec<ZoneRep> = re
            .captures_iter(&out)
            .map(|c| {
                // both aren't supported, and have to be reported as zero
                assert!(&c[5] == "0" && &c[6] == "0", "bad reset/non-seq: {}", &c[0]);
                ZoneRep {
                    start: hex(&c[1]),
                    len: hex(&c[2]),
                    cap: hex(&c[3]),
                    wptr: hex(&c[4]),
                    cond: c[7].parse().unwrap(),
                }
            })
            .collect();

        // Zone capacity isn't reported by old blkzone, which is the only
        // reason for returning nothing, then the caller can skip its test.
        let nr_zones = out.lines().filter(|l| l.contains("start: 0x")).count();
        if nr_zones > 0 && !out.contains(", cap 0x") {
            println!("SKIP: blkzone doesn't report zone capacity");
            return None;
        }
        // otherwise every zone has to be parsed
        assert!(
            nr_zones > 0 && zones.len() == nr_zones,
            "can't parse blkzone report: {}",
            out
        );
        Some(zones)
    }

    fn blkzone_mgmt(ctrl: &UblkCtrl, op: &str, start: u64) -> bool {
        Command::new("timeout")
            .args(["30", "blkzone", op, "-o", &start.to_string(), "-c", "1"])
            .arg(ctrl.get_bdev_path())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap()
            .success()
    }

    /// Verify zoned device with `zs_mb` zone size and `zc_mb` zone capacity,
    /// which has 4 zones.
    ///
    /// The 1st zone is conventional if `conv` is 1, otherwise all zones are
    /// sequential, such as ZNS SSD, and the 1st zone is just left as empty.
    fn __test_ublk_zoned_capacity(zs_mb: u64, zc_mb: u64, conv: u32, dir: Option<&String>) {
        const MB: u64 = (1 << 20) >> 9; // sectors
        let size = (4 * zs_mb).to_string();
        let zs_str = zs_mb.to_string();
        let zc_str = zc_mb.to_string();
        let conv_str = conv.to_string();
        assert!(conv <= 1);
        let mut cmdline = [
            "add",
            "zoned",
            "--size",
            &size,
            "--zone-size",
            &zs_str,
            "--zone-capacity",
            &zc_str,
            "--conv-zones",
            &conv_str,
            // 1M write is used for covering rejected write, and keep it as
            // one single request, otherwise the split 2nd half is aborted by
            // kernel with one extra warning
            "--io-buf-size",
            "1MiB",
        ]
        .to_vec();
        if let Some(d) = dir {
            cmdline.extend(["--path", d, "-r"]);
        }
        println!("zoned capacity: zone size {zs_mb}M capacity {zc_mb}M conv {conv} dir {dir:?}");

        let ctrl = run_rublk_add_dev(cmdline);
        let check_geometry = |zones: &Vec<ZoneRep>| {
            assert!(zones.len() == 4);
            for (i, z) in zones.iter().enumerate() {
                assert!(z.start == i as u64 * zs_mb * MB, "{:?}", z);
                assert!(z.len == zs_mb * MB, "{:?}", z);
                // conventional zone is always writable completely
                let cap = if (i as u32) < conv { zs_mb } else { zc_mb };
                assert!(z.cap == cap * MB, "{:?}", z);
            }
            // the 1st zone is never written
            let cond = if conv > 0 {
                ZONE_COND_NOT_WP
            } else {
                ZONE_COND_EMPTY
            };
            assert!(zones[0].cond == cond, "{:?}", zones[0]);
        };
        let report = || blkzone_report(&ctrl).unwrap();

        let zones = match blkzone_report(&ctrl) {
            Some(z) => z,
            None => {
                run_rublk_del_dev(ctrl, false);
                return;
            }
        };
        let expect = ExpectIoErrors::new("zoned_capacity", &ctrl);
        check_geometry(&zones);
        assert!(zones[1..].iter().all(|z| z.cond == ZONE_COND_EMPTY));

        let tmp_dir = tempfile::TempDir::new().unwrap();
        let pattern = tmp_dir.path().join("pattern");
        let pattern_str = pattern.to_string_lossy().to_string();
        std::fs::write(&pattern, vec![0xaa_u8; 2 << 20]).unwrap();

        // Zone 1: write until zone capacity is reached. Filling the whole
        // capacity takes too long for big zone, which is covered by finish.
        if zc_mb <= 256 {
            if zc_mb > 1 {
                assert!(dd_write_dev(&ctrl, "/dev/zero", 1, zs_mb, zc_mb - 1));
            }
            // and leave the last 128K of zone capacity unwritten
            let cap_end_kb = (zs_mb + zc_mb) << 10;
            assert!(dd_write_dev_kb(
                &ctrl,
                &pattern_str,
                128,
                cap_end_kb - 1024,
                7
            ));
            let z = &report()[1];
            assert!(z.wptr == zc_mb * MB - 256, "{:?}", z);

            // one 256K write crosses zone capacity, and has to fail without
            // moving write pointer
            if zc_mb < zs_mb {
                assert!(!dd_write_dev_kb(
                    &ctrl,
                    &pattern_str,
                    256,
                    cap_end_kb - 128,
                    1
                ));
                let z = &report()[1];
                assert!(z.wptr == zc_mb * MB - 256, "{:?}", z);
            }

            // the last 128K of zone capacity makes this zone full
            assert!(dd_write_dev_kb(
                &ctrl,
                &pattern_str,
                128,
                cap_end_kb - 128,
                1
            ));
            let z = &report()[1];
            assert!(z.cond == ZONE_COND_FULL, "{:?}", z);

            // no more write is allowed
            if zc_mb < zs_mb {
                assert!(!dd_write_dev(&ctrl, &pattern_str, 1, zs_mb + zc_mb, 1));
            }
            assert!(!dd_write_dev(&ctrl, &pattern_str, 1, zs_mb, 1));
        }

        // Zone 2: finish one partially written zone, then reset it
        assert!(dd_write_dev(&ctrl, &pattern_str, 1, 2 * zs_mb, 1));
        let z = &report()[2];
        assert!(z.cond == ZONE_COND_IMP_OPEN && z.wptr == MB, "{:?}", z);

        assert!(blkzone_mgmt(&ctrl, "finish", 2 * zs_mb * MB));
        let z = &report()[2];
        assert!(z.cond == ZONE_COND_FULL, "{:?}", z);
        assert!(!dd_write_dev(&ctrl, &pattern_str, 1, 2 * zs_mb + 1, 1));
        if let Some(d) = dir {
            // zone file never grows over zone capacity
            let zf = Path::new(d).join("seq-000002");
            assert!(std::fs::metadata(&zf).unwrap().len() == zc_mb << 20);
        }

        assert!(blkzone_mgmt(&ctrl, "reset", 2 * zs_mb * MB));
        let z = &report()[2];
        assert!(z.cond == ZONE_COND_EMPTY && z.wptr == 0, "{:?}", z);

        // and finish it again for the following recovery
        assert!(dd_write_dev(&ctrl, &pattern_str, 1, 2 * zs_mb, 1));
        assert!(blkzone_mgmt(&ctrl, "finish", 2 * zs_mb * MB));

        // Zone 3: partially written
        assert!(dd_write_dev(&ctrl, &pattern_str, 1, 3 * zs_mb, 2));
        let z = &report()[3];
        assert!(z.cond == ZONE_COND_IMP_OPEN && z.wptr == 2 * MB, "{:?}", z);

        // data is read back, and what is behind write pointer is zero
        let read_mb = |skip_mb: u64, count: u64| {
            let out = tmp_dir.path().join("out");
            let res = Command::new("dd")
                .arg(format!("if={}", ctrl.get_bdev_path()))
                .arg(format!("of={}", out.display()))
                .arg(format!("skip={}", skip_mb))
                .arg(format!("count={}", count))
                .args(["bs=1M", "iflag=direct"])
                .output()
                .unwrap();
            assert!(res.status.success());
            std::fs::read(&out).unwrap()
        };
        let check_data = || {
            let data = read_mb(3 * zs_mb, 3);
            assert!(data.len() == 3 << 20);
            assert!(data[..2 << 20].iter().all(|b| *b == 0xaa));
            assert!(data[2 << 20..].iter().all(|b| *b == 0));

            // the hole between zone capacity and zone size is read as zero
            if zc_mb < zs_mb {
                let data = read_mb(2 * zs_mb + zc_mb, 1);
                assert!(data.len() == 1 << 20);
                assert!(data.iter().all(|b| *b == 0));
            }
        };
        check_data();

        // geometry and write pointers survive recovery
        if dir.is_some() {
            run_ublk_recover(&ctrl);

            let zones = report();
            check_geometry(&zones);
            if zc_mb <= 256 {
                assert!(zones[1].cond == ZONE_COND_FULL, "{:?}", zones[1]);
            }
            assert!(zones[2].cond == ZONE_COND_FULL, "{:?}", zones[2]);
            let z = &zones[3];
            assert!(z.wptr == 2 * MB, "{:?}", z);
            assert!(
                z.cond == ZONE_COND_CLOSED || z.cond == ZONE_COND_IMP_OPEN,
                "{:?}",
                z
            );
            check_data();

            // the recovered zone can be written from its write pointer
            assert!(dd_write_dev(&ctrl, &pattern_str, 1, 3 * zs_mb + 2, 1));
            assert!(report()[3].wptr == 3 * MB);
        }
        drop(expect);
        run_rublk_del_dev(ctrl, false);

        // Add the device again from the existing directory: parameters are
        // always from `superblock.json`, and the ones from command line
        // are ignored.
        if let Some(d) = dir {
            let ctrl = run_rublk_add_dev(
                [
                    "add",
                    "zoned",
                    "--path",
                    d,
                    "--zone-size",
                    "8",
                    "--zone-capacity",
                    "8",
                ]
                .to_vec(),
            );
            let zones = blkzone_report(&ctrl).unwrap();

            check_geometry(&zones);
            assert!(zones[2].cond == ZONE_COND_FULL, "{:?}", zones[2]);
            // 3M is written to zone 3
            let z = &zones[3];
            if zc_mb == 3 {
                assert!(z.cond == ZONE_COND_FULL, "{:?}", z);
            } else {
                assert!(z.cond == ZONE_COND_CLOSED && z.wptr == 3 * MB, "{:?}", z);
            }
            run_rublk_del_dev(ctrl, false);
        }
    }

    /// Zone capacity can be smaller than zone size, such as ZNS SSD.
    #[test]
    fn test_ublk_zoned_capacity() {
        if !support_ublk() {
            return;
        }
        if !support_zoned() {
            return;
        }
        if !has_blkzone() {
            return;
        }

        // (zone size, zone capacity) in MiB:
        // - 4/4: capacity is specified as same with zone size
        // - 4/3: typical small setting for test, such as null_blk
        // - 256/192: the default zone size
        // - 2048/1077: WD ZN540 ZNS SSD
        //
        // Each one is covered with one conventional zone, and with
        // sequential zones only, such as ZNS SSD.
        for conv in [1, 0] {
            for (zs, zc) in [(4, 4), (4, 3), (256, 192)] {
                __test_ublk_zoned_capacity(zs, zc, conv, None);
            }
            for (zs, zc) in [(4, 3), (256, 192), (2048, 1077)] {
                let path_dir = tempfile::TempDir::new().unwrap();
                let path_str = path_dir.path().to_string_lossy().to_string();

                __test_ublk_zoned_capacity(zs, zc, conv, Some(&path_str));
            }
        }
    }

    /// Bad zone capacity has to be rejected
    #[test]
    fn test_ublk_zoned_bad_capacity() {
        if !support_ublk() {
            return;
        }
        if !support_zoned() {
            return;
        }

        let path_dir = tempfile::TempDir::new().unwrap();
        let path_str = path_dir.path().to_string_lossy().to_string();

        // bigger than zone size, zero, and not multiple of 1MiB
        for zc in ["5", "0", "3584KiB"] {
            for dir in [None, Some(&path_str)] {
                let mut args = ["--zone-size", "4", "--zone-capacity", zc].to_vec();
                if let Some(d) = dir {
                    args.extend(["--path", d]);
                }
                let err = add_zoned_expect_failure(&args);

                assert!(
                    err.contains("zone_capacity"),
                    "zone capacity {}: unexpected failure {}",
                    zc,
                    err
                );
                // bad parameters can't be stored
                assert!(!path_dir.path().join("superblock.json").exists());
            }
        }

        // the same directory is still usable with good zone capacity
        let cmdline = ["add", "zoned", "--zone-size", "4", "--zone-capacity", "3"];
        let ctrl = run_rublk_add_dev([&cmdline[..], &["--path", &path_str]].concat());
        if has_blkzone() {
            if let Some(zones) = blkzone_report(&ctrl) {
                assert!(zones.last().unwrap().cap == (3 << 20) >> 9);
            }
        }
        run_rublk_del_dev(ctrl, false);
    }

    /// btrfs works on zoned device with smaller zone capacity
    #[test]
    fn test_ublk_format_mount_zoned_capacity() {
        if !support_ublk() {
            return;
        }
        if !support_zoned() {
            return;
        }
        if !has_mkfs_btrfs() {
            return;
        }

        let path_dir = tempfile::TempDir::new().unwrap();
        let path_str = path_dir.path().to_string_lossy().to_string();

        for dir in [None, Some(&path_str)] {
            let mut cmdline = ["add", "zoned", "--zone-size", "4", "--zone-capacity", "3"].to_vec();
            cmdline.extend(["--logical-block-size", "4096", "--conv-zones", "0"]);
            if let Some(d) = dir {
                cmdline.extend(["--path", d]);
            }

            let ctrl = run_rublk_add_dev(cmdline);
            let tmp_dir = tempfile::TempDir::new().unwrap();

            mkfs(&ctrl, "btrfs", ["-O", "zoned", "-f"].to_vec());
            mount_fs_and_io(&ctrl, &tmp_dir, |dir| {
                let tstr = dir.path().to_string_lossy().to_string();

                dd_rw_file(&tstr, true, 8192, 16 * 1024);
                dd_rw_file(&tstr, false, 8192, 16 * 1024);
            });

            // Zone append can't be issued from userspace directly, and btrfs
            // uses it for writing data: some zones have to be filled up to
            // zone capacity, and no one is written over it.
            if has_blkzone() {
                if let Some(zones) = blkzone_report(&ctrl) {
                    let cap = (3 << 20) >> 9;
                    assert!(zones.iter().any(|z| z.cond == ZONE_COND_FULL));
                    assert!(zones
                        .iter()
                        .all(|z| z.cap == cap && (z.cond == ZONE_COND_FULL || z.wptr <= cap)));
                }
            }
            run_rublk_del_dev(ctrl, false);
        }
    }

    /// Add one zoned device with 4M zone size, 3M zone capacity and no
    /// conventional zone
    fn add_zoned_4m_3m(extra: &[&str]) -> UblkCtrl {
        let cmdline = ["add", "zoned", "--zone-size", "4", "--zone-capacity", "3"];
        // 1M rejected write is handled as one single request, see
        // __test_ublk_zoned_capacity()
        let conv = ["--conv-zones", "0", "--io-buf-size", "1MiB"];

        run_rublk_add_dev([&cmdline[..], &conv[..], extra].concat())
    }

    /// Zone management commands and open/active zone limits, with smaller
    /// zone capacity
    #[test]
    fn test_ublk_zoned_mgmt_limits() {
        if !support_ublk() {
            return;
        }
        if !support_zoned() {
            return;
        }
        if !has_blkzone() {
            return;
        }

        const MB: u64 = (1 << 20) >> 9; // sectors
        const ZS: u64 = 4; // MiB
        const ZONE_COND_EXP_OPEN: u32 = 3;
        const TEST: &str = "zoned_mgmt_limits";
        let limits = ["--max-open-zones", "2", "--max-active-zones", "3"];
        let ctrl = add_zoned_4m_3m(&[&["--size", "32"][..], &limits[..]].concat());
        if blkzone_report(&ctrl).is_none() {
            run_rublk_del_dev(ctrl, false);
            return;
        }
        let expect = ExpectIoErrors::new(TEST, &ctrl);
        let mgmt = |op: &str, zno: u64| blkzone_mgmt(&ctrl, op, zno * ZS * MB);
        let write = |zno: u64, off_mb: u64, mbs: u64| {
            dd_write_dev(&ctrl, "/dev/zero", 1, zno * ZS + off_mb, mbs)
        };
        let cond = |zno: usize| blkzone_report(&ctrl).unwrap()[zno].cond;

        // max open zones is 2, and explicitly opened zone can't be closed
        // implicitly
        assert!(mgmt("open", 0) && mgmt("open", 1));
        assert!(cond(0) == ZONE_COND_EXP_OPEN && cond(1) == ZONE_COND_EXP_OPEN);
        assert!(!mgmt("open", 2));
        assert!(cond(2) == ZONE_COND_EMPTY);

        // closing one opened zone without any data makes it empty
        assert!(mgmt("close", 0));
        assert!(cond(0) == ZONE_COND_EMPTY);

        // open: zone 1(exp) & zone 0(imp)
        assert!(write(0, 0, 1));
        assert!(cond(0) == ZONE_COND_IMP_OPEN);

        // zone 0 is closed implicitly for writing to zone 2, then active
        // zones are zone 0(closed), zone 1(exp) and zone 2(imp)
        assert!(write(2, 0, 1));
        assert!(cond(0) == ZONE_COND_CLOSED && cond(2) == ZONE_COND_IMP_OPEN);

        // max active zones is 3
        assert!(!write(3, 0, 1));
        assert!(!mgmt("open", 3));
        assert!(cond(3) == ZONE_COND_EMPTY);

        // finish implicitly opened zone, which isn't active any more
        assert!(mgmt("finish", 2));
        assert!(cond(2) == ZONE_COND_FULL);
        assert!(write(3, 0, 1));
        assert!(!write(4, 0, 1));

        // zone 3 becomes full after zone capacity is reached, and it isn't
        // active any more too
        assert!(write(3, 1, 2));
        assert!(cond(3) == ZONE_COND_FULL);
        assert!(write(4, 0, 1));

        // active zones: zone 0(closed), zone 1(exp) and zone 4(imp)
        assert!(mgmt("close", 1));
        assert!(cond(1) == ZONE_COND_EMPTY);

        // finish empty zone
        assert!(mgmt("finish", 5));
        assert!(cond(5) == ZONE_COND_FULL);
        // finish full zone
        assert!(mgmt("finish", 5));
        assert!(cond(5) == ZONE_COND_FULL);

        // finish explicitly opened zone
        assert!(mgmt("open", 0));
        assert!(cond(0) == ZONE_COND_EXP_OPEN);
        assert!(mgmt("finish", 0));
        assert!(cond(0) == ZONE_COND_FULL);

        // finish closed zone, and now zone 4 is the only active one
        assert!(write(6, 0, 1));
        assert!(mgmt("close", 6));
        assert!(cond(6) == ZONE_COND_CLOSED);
        assert!(mgmt("finish", 6));
        assert!(cond(6) == ZONE_COND_FULL);

        // reset all zones
        let res = Command::new("timeout")
            .args(["30", "blkzone", "reset", &ctrl.get_bdev_path()])
            .output()
            .unwrap();
        assert!(res.status.success());
        let zones = blkzone_report(&ctrl).unwrap();
        assert!(zones
            .iter()
            .all(|z| z.cond == ZONE_COND_EMPTY && z.wptr == 0));

        // and all open & active zones are released
        assert!(mgmt("open", 6) && mgmt("open", 7));
        assert!(!mgmt("open", 5));
        assert!(mgmt("reset", 6) && mgmt("reset", 7));
        assert!(write(0, 0, 1) && write(1, 0, 1) && write(2, 0, 1));
        assert!(!write(3, 0, 1));

        drop(expect);
        run_rublk_del_dev(ctrl, false);
    }

    /// Open/active zones accounting has to be correct after zone file
    /// operation fails.
    #[test]
    fn test_ublk_zoned_accounting_after_failure() {
        if !support_ublk() {
            return;
        }
        if !support_zoned() {
            return;
        }
        if !has_blkzone() {
            return;
        }

        const MB: u64 = (1 << 20) >> 9; // sectors
        const ZS: u64 = 4; // MiB
        let path_dir = tempfile::TempDir::new().unwrap();
        let path_str = path_dir.path().to_string_lossy().to_string();

        // Truncating append-only file fails, which is used for injecting
        // failure to finish & reset zone
        let chattr = |zno: u32, flag: &str| {
            let zf = path_dir.path().join(format!("seq-{:06}", zno));
            Command::new("chattr")
                .arg(flag)
                .arg(&zf)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .map(|s| s.success())
                .unwrap_or(false)
        };
        let probe = path_dir.path().join("seq-000000");
        std::fs::write(&probe, "").unwrap();
        if !chattr(0, "+a") {
            println!("SKIP: append-only file isn't supported in {}", path_str);
            return;
        }
        assert!(chattr(0, "-a"));
        std::fs::remove_file(&probe).unwrap();

        const TEST: &str = "zoned_accounting_after_failure";
        let limits = ["--max-open-zones", "1", "--max-active-zones", "1"];
        let ctrl =
            add_zoned_4m_3m(&[&["--size", "32", "--path", &path_str][..], &limits[..]].concat());
        if blkzone_report(&ctrl).is_none() {
            run_rublk_del_dev(ctrl, false);
            return;
        }
        let expect = ExpectIoErrors::new(TEST, &ctrl);
        let mgmt = |op: &str, zno: u64| blkzone_mgmt(&ctrl, op, zno * ZS * MB);
        let write = |zno: u64, off_mb: u64, mbs: u64| {
            dd_write_dev(&ctrl, "/dev/zero", 1, zno * ZS + off_mb, mbs)
        };
        let zone = |zno: usize| blkzone_report(&ctrl).unwrap().remove(zno);

        // Zone 0: finish fails, then the zone is written & finished without
        // reporting zones
        assert!(write(0, 0, 1));
        assert!(chattr(0, "+a"));
        assert!(!mgmt("finish", 0));
        assert!(chattr(0, "-a"));
        assert!(write(0, 1, 1));
        let z = zone(0);
        assert!(z.cond == ZONE_COND_IMP_OPEN && z.wptr == 2 * MB, "{:?}", z);
        assert!(mgmt("finish", 0));
        assert!(zone(0).cond == ZONE_COND_FULL);

        // Zone 1: finish fails, then zone state is updated from zone file by
        // reporting zones
        assert!(write(1, 0, 1));
        assert!(chattr(1, "+a"));
        assert!(!mgmt("finish", 1));
        assert!(chattr(1, "-a"));
        let z = zone(1);
        assert!(z.cond == ZONE_COND_CLOSED && z.wptr == MB, "{:?}", z);
        // it is the only active zone
        assert!(!write(2, 0, 1));
        assert!(write(1, 1, 1));
        let z = zone(1);
        assert!(z.cond == ZONE_COND_IMP_OPEN && z.wptr == 2 * MB, "{:?}", z);

        // Zone 1: reset fails, and it is still the only active zone
        assert!(chattr(1, "+a"));
        assert!(!mgmt("reset", 1));
        assert!(chattr(1, "-a"));
        assert!(!write(2, 0, 1));
        assert!(mgmt("reset", 1));
        assert!(zone(1).cond == ZONE_COND_EMPTY);

        // no zone is active now, and the limit of one active zone still
        // works
        assert!(write(2, 0, 1));
        assert!(!write(3, 0, 1));
        assert!(mgmt("finish", 2));
        // kernel doesn't allow to write this zone until its write pointer
        // is updated by reporting zones after the write failure
        assert!(zone(3).cond == ZONE_COND_EMPTY);
        assert!(write(3, 0, 3));
        assert!(zone(3).cond == ZONE_COND_FULL);
        assert!(write(4, 0, 1));
        assert!(!write(5, 0, 1));

        drop(expect);
        run_rublk_del_dev(ctrl, false);
    }

    /// One zone may be closed implicitly when its last write is in progress,
    /// and this zone has to be released from active zones after it becomes
    /// full.
    #[test]
    fn test_ublk_zoned_full_after_implicit_close() {
        if !support_ublk() {
            return;
        }
        if !support_zoned() {
            return;
        }

        const NR_ZONES: u64 = 64;
        const ZS: u64 = 4; // MiB
        let path_dir = tempfile::TempDir::new().unwrap();
        let path_str = path_dir.path().to_string_lossy().to_string();
        let size = (NR_ZONES * ZS).to_string();
        // zone capacity is 1M, so one 1M write fills the whole zone
        let ctrl = run_rublk_add_dev(
            [
                "add",
                "zoned",
                "--zone-size",
                "4",
                "--zone-capacity",
                "1",
                "--conv-zones",
                "0",
                "--io-buf-size",
                "1MiB",
                "--max-open-zones",
                "1",
                "--max-active-zones",
                "2",
                "--size",
                &size,
                "--path",
                &path_str,
            ]
            .to_vec(),
        );

        // Fill two zones concurrently: only one zone can be open, so the
        // 1st zone is closed implicitly by the write to the 2nd one, very
        // likely before its only write is completed.
        //
        // Every write is failed for exceeding max active zones if any
        // filled zone is still counted as active.
        let bdev = ctrl.get_bdev_path();
        for pair in 0..NR_ZONES / 2 {
            let children: Vec<_> = (0..2)
                .map(|i| {
                    Command::new("timeout")
                        .args(["60", "dd", "if=/dev/zero", "bs=1M", "count=1"])
                        .arg(format!("of={}", bdev))
                        .arg(format!("seek={}", (pair * 2 + i) * ZS))
                        .arg("oflag=direct")
                        .stdout(Stdio::null())
                        .stderr(Stdio::null())
                        .spawn()
                        .unwrap()
                })
                .collect();
            for (i, mut c) in children.into_iter().enumerate() {
                assert!(
                    c.wait().unwrap().success(),
                    "write to zone {} fails",
                    pair * 2 + i as u64
                );
            }
        }

        run_rublk_del_dev(ctrl, false);
    }

    /// Add zoned device and return its stderr if adding fails
    fn add_zoned_expect_failure(args: &[&str]) -> String {
        let rublk = get_curr_bin_dir().unwrap().join("rublk");
        // `timeout` avoids to hang if the device is added
        let res = Command::new("timeout")
            .arg("30")
            .arg(&rublk)
            .args(["add", "zoned", "--foreground"])
            .args(args)
            .output()
            .unwrap();
        let err = String::from_utf8_lossy(&res.stderr).to_string();

        // the device is added if `timeout` expires
        if res.status.code() == Some(124) {
            panic!("{:?} is accepted", args);
        }
        assert!(!res.status.success(), "{:?} isn't rejected", args);
        err
    }

    /// The directory is still usable after setting up new device fails
    #[test]
    fn test_ublk_zoned_failed_setup() {
        if !support_ublk() {
            return;
        }
        if !support_zoned() {
            return;
        }

        let path_dir = tempfile::TempDir::new().unwrap();
        let path_str = path_dir.path().to_string_lossy().to_string();
        let args = [
            "--zone-size",
            "4",
            "--size",
            "32",
            "--conv-zones",
            "0",
            "--path",
            &path_str,
        ];

        // zone file can't be created because one directory takes its name
        let blocker = path_dir.path().join("seq-000001");
        std::fs::create_dir(&blocker).unwrap();
        add_zoned_expect_failure(&args);
        assert!(!path_dir.path().join("superblock.json").exists());

        std::fs::remove_dir(&blocker).unwrap();
        let ctrl = run_rublk_add_dev([&["add", "zoned"][..], &args[..]].concat());
        read_ublk_disk(&ctrl);
        run_rublk_del_dev(ctrl, false);
    }

    /// The directory of one live device can't be used by another device, and
    /// it isn't touched by the failed one.
    #[test]
    fn test_ublk_zoned_dir_in_use() {
        if !support_ublk() {
            return;
        }
        if !support_zoned() {
            return;
        }

        let path_dir = tempfile::TempDir::new().unwrap();
        let path_str = path_dir.path().to_string_lossy().to_string();
        let sb = path_dir.path().join("superblock.json");
        let args = ["--zone-size", "4", "--size", "32", "--path", &path_str];

        let ctrl = run_rublk_add_dev([&["add", "zoned"][..], &args[..]].concat());
        let sb_data = std::fs::read(&sb).unwrap();

        // different geometry is ignored because of existing superblock,
        // and the directory can't be locked
        for extra in [&[][..], &["--zone-capacity", "3"][..]] {
            let err = add_zoned_expect_failure(&[&args[..], extra].concat());
            assert!(err.contains("fail to lock"), "unexpected failure: {}", err);
            assert!(std::fs::read(&sb).unwrap() == sb_data);
        }
        read_ublk_disk(&ctrl);
        run_rublk_del_dev(ctrl, false);

        // and the directory is still usable
        let ctrl = run_rublk_add_dev([&["add", "zoned"][..], &args[..]].concat());
        read_ublk_disk(&ctrl);
        run_rublk_del_dev(ctrl, false);
    }

    /// Bad pre-alloc-size has to be rejected
    #[test]
    fn test_ublk_zoned_bad_pre_alloc_size() {
        if !support_ublk() {
            return;
        }
        if !support_zoned() {
            return;
        }

        for sz in ["3MiB", "0", "2KiB"] {
            let path_dir = tempfile::TempDir::new().unwrap();
            let path_str = path_dir.path().to_string_lossy().to_string();
            let args = [
                "--zone-size",
                "4",
                "--path",
                &path_str,
                "--pre-alloc-size",
                sz,
            ];
            let err = add_zoned_expect_failure(&args);

            assert!(
                err.contains("pre_alloc_size"),
                "{}: unexpected failure {}",
                sz,
                err
            );
            assert!(!path_dir.path().join("superblock.json").exists());
        }
    }

    /// Max open/active zones are adjusted if they can't be enforced
    #[test]
    fn test_ublk_zoned_adjust_limits() {
        if !support_ublk() {
            return;
        }
        if !support_zoned() {
            return;
        }

        let read_limit = |ctrl: &UblkCtrl, name: &str| -> u32 {
            let bdev = ctrl.get_bdev_path();
            let disk = Path::new(&bdev)
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string();
            let path = format!("/sys/block/{}/queue/{}", disk, name);

            std::fs::read_to_string(&path)
                .unwrap()
                .trim()
                .parse()
                .unwrap()
        };

        // 8 sequential zones: (max open, max active) -> expected ones
        for ((open, active), (exp_open, exp_active)) in [
            ((2, 3), (2, 3)),
            // max open zones can't be bigger than max active zones
            ((6, 4), (4, 4)),
            // the limit can't be enforced if it isn't less than zones
            ((8, 0), (0, 0)),
            ((100, 200), (0, 0)),
            ((100, 4), (4, 4)),
        ] {
            let (open, active) = (open.to_string(), active.to_string());
            let limits = ["--max-open-zones", &open, "--max-active-zones", &active];
            let ctrl = add_zoned_4m_3m(&[&["--size", "32"][..], &limits[..]].concat());

            assert!(
                read_limit(&ctrl, "max_open_zones") == exp_open,
                "{} {}",
                open,
                active
            );
            assert!(
                read_limit(&ctrl, "max_active_zones") == exp_active,
                "{} {}",
                open,
                active
            );
            run_rublk_del_dev(ctrl, false);
        }
    }

    #[test]
    fn test_ublk_format_mount_zoned() {
        if !support_ublk() {
            return;
        }
        if !support_zoned() {
            return;
        }
        if !has_mkfs_btrfs() {
            return;
        }

        let tf = |ctrl: &UblkCtrl, _bs: u32, _file_size: usize| {
            let tmp_dir = tempfile::TempDir::new().unwrap();

            mkfs(ctrl, "btrfs", ["-O", "zoned", "-f"].to_vec());
            mount_fs_and_io(ctrl, &tmp_dir, |dir| {
                let tstr = dir.path().to_string_lossy().to_string();

                dd_rw_file(&tstr, true, 8192, 16 * 1024);
                dd_rw_file(&tstr, false, 8192, 16 * 1024);
            });
        };

        __test_ublk_add_del_zoned(4096, 1, None, false, tf);

        let path_dir = tempfile::TempDir::new().unwrap();
        let path_str = path_dir.path().to_string_lossy().to_string();

        __test_ublk_add_del_zoned(4096, 1, Some(&path_str), false, tf);
        __test_ublk_add_del_zoned(4096, 2, Some(&path_str), false, tf);
    }

    /// Run fio random write & verify in zbd mode over one zoned device of 16
    /// zones, in which each zone is `zs_mb` MiB with `zc_mb` MiB capacity.
    ///
    /// In zbd mode, fio turns a random write into one write at the write
    /// pointer of a randomly chosen sequential zone, so writes are spread
    /// over many zones, each of which is filled gradually up to its
    /// capacity, and at most `max_open` (if it isn't zero) zones are written
    /// at the same time. Conventional zones are written in place.
    fn __test_ublk_fio_verify_zoned(
        zs_mb: u64,
        zc_mb: u64,
        conv: u32,
        max_open: u32,
        dir: Option<&String>,
    ) {
        const NR_ZONES: u64 = 16;
        let size = (NR_ZONES * zs_mb).to_string();
        let zs_str = zs_mb.to_string();
        let zc_str = zc_mb.to_string();
        let conv_str = conv.to_string();
        let max_open_str = max_open.to_string();
        let mut cmdline = [
            "add",
            "zoned",
            "--size",
            &size,
            "--zone-size",
            &zs_str,
            "--zone-capacity",
            &zc_str,
            "--conv-zones",
            &conv_str,
            "--max-open-zones",
            &max_open_str,
        ]
        .to_vec();
        if let Some(d) = dir {
            cmdline.extend(["--path", d]);
        }

        let fio_max_open = format!("--max_open_zones={}", max_open);
        let mut fio_args = ["--zonemode=zbd"].to_vec();
        if max_open > 0 {
            fio_args.push(&fio_max_open);
        }

        // Write 3/4 of the writable space each time, so the 2nd run can't
        // complete without resetting and rewriting zones filled by the 1st
        // one: fio resets the zones before writing if verify is enabled.
        let io_mb = (conv as u64 * zs_mb + (NR_ZONES - conv as u64) * zc_mb) * 3 / 4;

        let ctrl = run_rublk_add_dev(cmdline);
        fio_randwrite_verify(&ctrl, io_mb, 1, &fio_args);
        fio_randwrite_verify(&ctrl, io_mb, 2, &fio_args);
        run_rublk_del_dev(ctrl, false);
    }

    #[test]
    fn test_ublk_fio_verify_zoned() {
        if !support_ublk() {
            return;
        }
        if !support_zoned() {
            return;
        }
        if !has_fio() {
            return;
        }

        // (zone size, zone capacity, conventional zones, max open zones)
        let cfgs = [(4, 4, 0, 0), (4, 3, 0, 4), (4, 3, 1, 2)];

        // ram-backed
        for (zs, zc, conv, max_open) in cfgs {
            __test_ublk_fio_verify_zoned(zs, zc, conv, max_open, None);
        }

        // file-backed
        for (zs, zc, conv, max_open) in cfgs {
            let path_dir = tempfile::TempDir::new().unwrap();
            let path_str = path_dir.path().to_string_lossy().to_string();

            __test_ublk_fio_verify_zoned(zs, zc, conv, max_open, Some(&path_str));
        }
    }

    #[test]
    fn test_ublk_zoned_recover() {
        if !support_ublk() {
            return;
        }

        if !support_zoned() {
            return;
        }

        let path_dir = tempfile::TempDir::new().unwrap();
        let path_str = path_dir.path().to_string_lossy().to_string();

        __test_ublk_add_del_zoned(4096, 1, Some(&path_str), true, |ctrl, _bs, _file_size| {
            run_ublk_recover(ctrl);
        });
    }
}
