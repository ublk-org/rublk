#[cfg(test)]
mod integration {
    use libublk::{ctrl::UblkCtrl, sys};
    use std::env;
    use std::fs::File;
    use std::path::Path;
    use std::process::{Command, Stdio};

    #[ctor::ctor]
    fn init_logger() {
        let _ = env_logger::builder()
            .format_target(false)
            .format_timestamp(None)
            .is_test(true)
            .try_init();
    }

    fn has_mkfs_ext4() -> bool {
        match Command::new("mkfs.ext4")
            .arg("-V")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
        {
            Ok(res) => res.success(),
            _ => false,
        }
    }
    fn has_mkfs_btrfs() -> bool {
        match Command::new("mkfs.btrfs")
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
        {
            Ok(res) => res.success(),
            _ => false,
        }
    }

    fn has_blkdiscard() -> bool {
        Command::new("blkdiscard")
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok()
    }

    fn has_blkzone() -> bool {
        Command::new("blkzone")
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok()
    }

    fn support_zoned() -> bool {
        match UblkCtrl::get_features() {
            Some(f) => {
                if (f & sys::UBLK_F_ZONED as u64) == 0 {
                    return false;
                }
            }
            _ => return false,
        };
        match libublk::ctrl::UblkCtrlBuilder::default()
            .name("zoned_test")
            .depth(4)
            .nr_queues(1)
            .id(-1)
            .ctrl_flags((libublk::sys::UBLK_F_USER_COPY | libublk::sys::UBLK_F_ZONED).into())
            .dev_flags(libublk::UblkFlags::UBLK_DEV_F_ADD_DEV)
            .io_buf_bytes(512 * 1024)
            .build()
        {
            Ok(_) => true,
            _ => false,
        }
    }

    fn mkfs(ctrl: &UblkCtrl, fs: &str, args: Vec<&str>) {
        let bdev = ctrl.get_bdev_path();
        let cmd = "mkfs.".to_string() + fs;

        let res = Command::new(cmd.clone())
            .args(args)
            .args([&bdev])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .expect(&cmd);
        assert!(res.success());
    }

    fn mount_fs_and_io<F>(ctrl: &UblkCtrl, dir: &tempfile::TempDir, f: F)
    where
        F: Fn(&tempfile::TempDir),
    {
        let dstr = dir.path().to_string_lossy().to_string();
        let bdev = ctrl.get_bdev_path();

        let res = Command::new("mount")
            .args([&bdev, &dstr])
            .stdout(std::process::Stdio::null())
            .status()
            .expect("Failed to execute mount");
        assert!(res.success());

        f(&dir);

        let res = Command::new("umount")
            .args([&dstr])
            .stdout(std::process::Stdio::null())
            .status()
            .expect("Failed to execute umount");
        assert!(res.success());
    }

    fn support_ublk() -> bool {
        if !Path::new("/dev/ublk-control").exists() {
            eprintln!("ublk isn't supported or its module isn't loaded");
            return false;
        }
        return true;
    }

    fn ext4_format_and_mount(ctrl: &UblkCtrl) {
        let tmp_dir = tempfile::TempDir::new().unwrap();

        if has_mkfs_ext4() {
            mkfs(&ctrl, "ext4", ["-F"].to_vec());
            mount_fs_and_io(&ctrl, &tmp_dir, |dir| {
                let tstr = dir.path().to_string_lossy().to_string();
                dd_rw_file(&tstr, true, 4096, 128);
            });
        }
    }

    fn check_ro(ctrl: &UblkCtrl, exp_ro: bool) {
        let mut params: sys::ublk_params = { Default::default() };
        ctrl.get_params(&mut params).unwrap();

        let ro = (params.basic.attrs & libublk::sys::UBLK_ATTR_READ_ONLY) != 0;
        assert!(ro == exp_ro);
    }

    fn check_block_size(ctrl: &UblkCtrl, exp_bs: u32) {
        let mut params: sys::ublk_params = { Default::default() };
        ctrl.get_params(&mut params).unwrap();

        let bs = 1_u32 << params.basic.logical_bs_shift;
        assert!(bs == exp_bs);
        assert!(params.basic.physical_bs_shift >= params.basic.logical_bs_shift);
    }

    fn dd_rw_file(dir: &String, write: bool, bs: u32, count: u32) {
        let mut arg_list: Vec<String> = Vec::new();
        let rw_file = if write {
            format!("of={}/temp.img", &dir)
        } else {
            format!("if={}/temp.img", &dir)
        };

        arg_list.push(rw_file);
        if write {
            arg_list.push("if=/dev/zero".to_string());
        } else {
            arg_list.push("of=/dev/null".to_string());
        }
        arg_list.push(format!("bs={}", bs).to_string());
        arg_list.push(format!("count={}", count).to_string());
        let out = Command::new("dd").args(arg_list).output().unwrap();

        assert!(out.status.success() == true);
    }

    fn read_ublk_disk(ctrl: &UblkCtrl) {
        let dev_path = ctrl.get_bdev_path();
        let mut arg_list: Vec<String> = Vec::new();
        let if_dev = format!("if={}", &dev_path);

        arg_list.push(if_dev);
        arg_list.push("of=/dev/null".to_string());
        arg_list.push("bs=4096".to_string());
        arg_list.push("count=64k".to_string());
        let out = Command::new("dd").args(arg_list).output().unwrap();

        assert!(out.status.success() == true);
    }

    fn write_ublk_disk(ctrl: &UblkCtrl, bs: u32, size: usize) {
        let dev_path = ctrl.get_bdev_path();
        let mut arg_list: Vec<String> = Vec::new();

        arg_list.push("if=/dev/zero".to_string());
        arg_list.push(format!("of={}", dev_path));
        arg_list.push(format!("bs={}", bs));
        arg_list.push(format!("count={}", size / (bs as usize)));
        let out = Command::new("dd").args(arg_list).output().unwrap();

        assert!(out.status.success() == true);
    }

    fn create_file_with_size(p: &Path, size: u64) -> std::io::Result<()> {
        // Open the file for writing. This will create the file if it doesn't exist.
        let file = File::create(p)?;

        // Seek to the desired size (e.g., 1 MB).
        file.set_len(size)?;

        Ok(())
    }

    // qemu-img package is needed
    fn create_qcow2_image(p: &Path, size: usize) {
        let mut arg_list: Vec<String> = Vec::new();
        let name = format!("{}", p.to_str().unwrap());
        let size = format!("{}", size);

        arg_list.push("create".to_string());
        arg_list.push("-f".to_string());
        arg_list.push("qcow2".to_string());
        arg_list.push(name);
        arg_list.push(size);
        let out = Command::new("qemu-img").args(arg_list).output().unwrap();
        assert!(out.status.success() == true);
    }

    fn ublk_state_wait_until(ctrl: &UblkCtrl, state: u16, timeout: u32) {
        let mut count = 0;
        let unit = 100_u32;
        loop {
            std::thread::sleep(std::time::Duration::from_millis(unit as u64));

            ctrl.read_dev_info().unwrap();
            if ctrl.dev_info().state == state {
                std::thread::sleep(std::time::Duration::from_millis(20));
                break;
            }
            count += unit;
            assert!(count < timeout);
        }
    }

    fn get_curr_bin_dir() -> Option<std::path::PathBuf> {
        if let Err(_current_exe) = env::current_exe() {
            None
        } else {
            env::current_exe().ok().map(|mut path| {
                path.pop();
                if path.ends_with("deps") {
                    path.pop();
                }
                path
            })
        }
    }

    fn run_rublk_cmd(s: Vec<&str>, exp_len: usize) -> String {
        let tgt_dir = get_curr_bin_dir().unwrap();
        let tmpfile = tempfile::NamedTempFile::new().unwrap();
        let file = std::fs::File::create(tmpfile.path()).unwrap();
        let fg = s.contains(&"--foreground");

        //println!("top dir: path {:?} {:?}", &tgt_dir, &file);
        let rd_path = tgt_dir.display().to_string() + &"/rublk".to_string();
        let mut cmd = Command::new(&rd_path)
            .args(s)
            .stdout(Stdio::from(file))
            .spawn()
            .expect("Failed to execute process");

        if !fg {
            cmd.wait().unwrap();
        }
        let buf = loop {
            std::thread::sleep(std::time::Duration::from_millis(200));
            let _buf = std::fs::read_to_string(tmpfile.path()).unwrap();

            if _buf.len() >= exp_len {
                break _buf;
            }
        };

        buf
    }

    fn run_rublk_add_dev(s: Vec<&str>) -> UblkCtrl {
        let buf = run_rublk_cmd(s, 64);
        let id_regx = regex::Regex::new(r"dev id (\d+)").unwrap();

        let id = {
            if let Some(c) = id_regx.captures(&buf.as_str()) {
                c.get(1).unwrap().as_str().parse().unwrap()
            } else {
                -1_i32
            }
        };
        assert!(id >= 0);

        let ctrl = UblkCtrl::new_simple(id).unwrap();
        ublk_state_wait_until(&ctrl, sys::UBLK_S_DEV_LIVE as u16, 5000);

        //ublk block device should be observed now
        let dev_path = ctrl.get_bdev_path();
        assert!(Path::new(&dev_path).exists() == true);

        ctrl
    }

    fn run_rublk_del_dev(ctrl: UblkCtrl, async_del: bool) {
        let id = ctrl.dev_info().dev_id;
        let id_str = id.to_string();

        std::thread::sleep(std::time::Duration::from_millis(500));
        let mut para = ["del", "-n", &id_str].to_vec();
        if async_del {
            para.push("--async");
        }
        let _ = run_rublk_cmd(para.to_vec(), 0);
    }

    fn __test_ublk_add_del_null(bs: u32, aa: bool) {
        let binding = bs.to_string();
        let mut cmd_line = ["add", "null", "--logical-block-size", &binding].to_vec();
        if aa {
            cmd_line.push("-a");
        }
        let ctrl = run_rublk_add_dev(cmd_line);
        read_ublk_disk(&ctrl);
        check_block_size(&ctrl, bs);
        run_rublk_del_dev(ctrl, aa);
    }
    #[test]
    fn test_ublk_add_del_null() {
        if !support_ublk() {
            return;
        }

        let mut aa = false;
        for bs in [512, 1024, 4096] {
            __test_ublk_add_del_null(bs, aa);
            aa = !aa;
        }
    }

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

        if zones.is_empty() {
            eprintln!("blkzone doesn't report zone capacity: {}", out);
            None
        } else {
            Some(zones)
        }
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

    /// Write `count` blocks of `bs_mb` MiB from `src` to the offset of `seek_mb` MiB
    fn dd_write_dev(ctrl: &UblkCtrl, src: &str, bs_mb: u64, seek_mb: u64, count: u64) -> bool {
        dd_write_dev_kb(ctrl, src, bs_mb << 10, seek_mb << 10, count)
    }

    /// Write `count` blocks of `bs_kb` KiB from `src` to the offset of `seek_kb` KiB
    fn dd_write_dev_kb(ctrl: &UblkCtrl, src: &str, bs_kb: u64, seek_kb: u64, count: u64) -> bool {
        Command::new("timeout")
            .args(["60", "dd"])
            .arg(format!("if={}", src))
            .arg(format!("of={}", ctrl.get_bdev_path()))
            .arg(format!("bs={}K", bs_kb))
            .arg(format!("seek={}", seek_kb << 10))
            .arg(format!("count={}", count))
            .arg("oflag=direct,seek_bytes")
            .output()
            .unwrap()
            .status
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
        let conv = ["--conv-zones", "0"];

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
        let limits = ["--max-open-zones", "2", "--max-active-zones", "3"];
        let ctrl = add_zoned_4m_3m(&[&["--size", "32"][..], &limits[..]].concat());
        if blkzone_report(&ctrl).is_none() {
            run_rublk_del_dev(ctrl, false);
            return;
        }
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
            eprintln!("append-only file isn't supported in {}", path_str);
            return;
        }
        assert!(chattr(0, "-a"));
        std::fs::remove_file(&probe).unwrap();

        let limits = ["--max-open-zones", "1", "--max-active-zones", "1"];
        let ctrl =
            add_zoned_4m_3m(&[&["--size", "32", "--path", &path_str][..], &limits[..]].concat());
        if blkzone_report(&ctrl).is_none() {
            run_rublk_del_dev(ctrl, false);
            return;
        }
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

    fn __test_ublk_add_del_loop<F>(bs: u32, aa: bool, recover: bool, zc: bool, f: F)
    where
        F: Fn(&UblkCtrl, u32, usize, &str),
    {
        let tmp_file = tempfile::NamedTempFile::new().unwrap();
        let file_size = 32 * 1024 * 1024; // 1 MB
        let p = tmp_file.path();

        create_file_with_size(&p, file_size).unwrap();
        let pstr = match p.to_str() {
            Some(p) => p,
            _ => panic!(),
        };

        let binding = bs.to_string();
        let mut cmd_line = ["add", "loop", "-f", &pstr, "--logical-block-size", &binding].to_vec();
        if aa {
            cmd_line.push("-a");
        }
        if recover {
            cmd_line.push("-r");
        }
        if zc {
            cmd_line.push("--zero-copy");
        }

        let ctrl = run_rublk_add_dev(cmd_line);
        f(&ctrl, bs, file_size.try_into().unwrap(), pstr);
        run_rublk_del_dev(ctrl, false);
    }
    #[test]
    fn test_ublk_add_del_loop() {
        if !support_ublk() {
            return;
        }

        let tf = |ctrl: &UblkCtrl, bs: u32, _file_size: usize, _path: &str| {
            read_ublk_disk(ctrl);
            check_block_size(ctrl, bs);
        };

        __test_ublk_add_del_loop(4096, false, false, false, tf);
        __test_ublk_add_del_loop(4096, true, false, false, tf);
    }

    fn __test_ublk_null_read_only(cmds: &[&str], exp_ro: bool) {
        let ctrl = run_rublk_add_dev(cmds.to_vec());
        check_ro(&ctrl, exp_ro);
        run_rublk_del_dev(ctrl, false);
    }
    #[test]
    fn test_ublk_null_read_only() {
        if !support_ublk() {
            return;
        }
        __test_ublk_null_read_only(&["add", "null"], false);
        __test_ublk_null_read_only(&["add", "null", "--read-only"], true);
        __test_ublk_null_read_only(&["add", "null", "--foreground"], false);
    }

    fn __test_ublk_add_del_qcow2<F>(bs: u32, recover: bool, f: F)
    where
        F: Fn(&UblkCtrl, u32, usize),
    {
        let tmp_file = tempfile::NamedTempFile::new().unwrap();
        let file_size = 32 * 1024 * 1024;
        let p = tmp_file.path();

        create_qcow2_image(&p, file_size);
        let pstr = match p.to_str() {
            Some(p) => p,
            _ => panic!(),
        };

        let binding = bs.to_string();
        let mut cmd_line = [
            "add",
            "qcow2",
            "-f",
            &pstr,
            "--logical-block-size",
            &binding,
        ]
        .to_vec();
        if recover {
            cmd_line.push("-r");
        }
        let ctrl = run_rublk_add_dev(cmd_line);
        f(&ctrl, bs, file_size);
        run_rublk_del_dev(ctrl, false);
    }
    #[test]
    fn test_ublk_add_del_qcow2() {
        if !support_ublk() {
            return;
        }
        __test_ublk_add_del_qcow2(4096, false, |ctrl, bs, file_size| {
            read_ublk_disk(ctrl);
            write_ublk_disk(ctrl, bs, file_size);
            check_block_size(ctrl, bs);
        });
    }

    #[test]
    fn test_ublk_default_block_sizes() {
        if !support_ublk() {
            return;
        }

        for target in ["loop", "qcow2"] {
            let tmp_file = tempfile::NamedTempFile::new().unwrap();
            let size = 32 * 1024 * 1024;
            if target == "qcow2" {
                create_qcow2_image(tmp_file.path(), size);
            } else {
                create_file_with_size(tmp_file.path(), size as u64).unwrap();
            }
            let path = tmp_file.path().to_str().unwrap();
            let ctrl = run_rublk_add_dev(vec!["add", target, "-f", path]);
            let mut params: sys::ublk_params = Default::default();
            ctrl.get_params(&mut params).unwrap();
            for shift in [
                params.basic.logical_bs_shift,
                params.basic.physical_bs_shift,
                params.basic.io_min_shift,
                params.basic.io_opt_shift,
            ] {
                assert!((9..=12).contains(&shift));
            }
            let bs = 1_u32 << params.basic.logical_bs_shift;
            check_block_size(&ctrl, bs);
            read_ublk_disk(&ctrl);
            write_ublk_disk(&ctrl, bs, size);
            run_rublk_del_dev(ctrl, false);
        }
    }

    #[test]
    fn test_ublk_format_mount_loop() {
        if !support_ublk() {
            return;
        }
        __test_ublk_add_del_loop(4096, true, false, false, |ctrl, _bs, _file_size, _path| {
            ext4_format_and_mount(ctrl);
        });
    }

    #[test]
    fn test_ublk_format_mount_loop_zero_copy() {
        if !support_ublk() {
            return;
        }
        __test_ublk_add_del_loop(4096, true, false, true, |ctrl, _bs, _file_size, _path| {
            ext4_format_and_mount(ctrl);
        });
        __test_ublk_add_del_loop(4096, false, false, true, |ctrl, _bs, _file_size, _path| {
            ext4_format_and_mount(ctrl);
        });
    }

    #[test]
    fn test_ublk_format_mount_qcow2() {
        if !support_ublk() {
            return;
        }
        __test_ublk_add_del_qcow2(4096, false, |ctrl, _bs, _file_size| {
            ext4_format_and_mount(ctrl);
        });
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

    fn run_ublk_recover(ctrl: &UblkCtrl) {
        let id = ctrl.dev_info().dev_id.to_string();
        let pid = ctrl.dev_info().ublksrv_pid.to_string();
        let res = Command::new("kill")
            .args(["-9", &pid])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .expect(&format!("kill -9 {} failed", pid));
        assert!(res.success());
        ublk_state_wait_until(ctrl, sys::UBLK_S_DEV_QUIESCED as u16, 5000);
        let para = ["recover", "-n", &id].to_vec();
        let _ = run_rublk_cmd(para.to_vec(), 64);
        ublk_state_wait_until(ctrl, sys::UBLK_S_DEV_LIVE as u16, 5000);

        read_ublk_disk(ctrl);
    }

    #[test]
    fn test_ublk_loop_recover() {
        if !support_ublk() {
            return;
        }
        __test_ublk_add_del_loop(4096, true, true, false, |ctrl, _bs, _file_size, _path| {
            run_ublk_recover(ctrl);
        });
    }

    #[test]
    fn test_ublk_qcow2_recover() {
        if !support_ublk() {
            return;
        }
        __test_ublk_add_del_qcow2(4096, true, |ctrl, _bs, _file_size| {
            run_ublk_recover(ctrl);
        });
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

    #[cfg(feature = "compress")]
    fn __test_ublk_add_del_compress<F>(recover: bool, f: F)
    where
        F: Fn(&UblkCtrl),
    {
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let pstr = tmp_dir.path().to_str().unwrap();

        let mut cmd_line = vec!["add", "compress", "--dir", pstr, "--size", "8G"];
        if recover {
            cmd_line.push("-r");
        }

        let ctrl = run_rublk_add_dev(cmd_line);
        f(&ctrl);

        if recover {
            run_ublk_recover(&ctrl);
        }

        run_rublk_del_dev(ctrl, false);
    }

    #[test]
    #[cfg(feature = "compress")]
    fn test_ublk_add_del_compress() {
        if !support_ublk() {
            return;
        }
        __test_ublk_add_del_compress(false, |ctrl| {
            write_ublk_disk(ctrl, 4096, 1024);
            read_ublk_disk(ctrl);
        });
    }

    #[test]
    #[cfg(feature = "compress")]
    fn test_ublk_compress_recover() {
        if !support_ublk() {
            return;
        }
        __test_ublk_add_del_compress(true, |ctrl| {
            write_ublk_disk(ctrl, 4096, 1024);
            read_ublk_disk(ctrl);
        });
    }

    #[test]
    #[cfg(feature = "compress")]
    fn test_ublk_format_mount_compress() {
        if !support_ublk() {
            return;
        }
        __test_ublk_add_del_compress(false, |ctrl| {
            ext4_format_and_mount(ctrl);
        });
    }

    #[cfg(feature = "compress")]
    fn __test_ublk_compress_type(comp_type: &str, res: &str) {
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let pstr = tmp_dir.path().to_str().unwrap();

        let cmd_line = vec![
            "add",
            "compress",
            "--dir",
            pstr,
            "--size",
            "8GiB",
            "--compression",
            comp_type,
        ];

        let ctrl = run_rublk_add_dev(cmd_line);

        let log_path = tmp_dir.path().join("LOG");
        let log_content = std::fs::read_to_string(log_path).unwrap();
        let comp_str = format!("Options.compression: {}", res);
        assert!(log_content.contains(&comp_str));

        run_rublk_del_dev(ctrl, false);
    }
    #[test]
    #[cfg(feature = "compress")]
    fn test_ublk_compress_type() {
        if !support_ublk() {
            return;
        }
        let comp_types = vec!["none", "lz4", "zstd", "snappy", "zlib"];
        let comp_res = vec!["NoCompression", "LZ4", "ZSTD", "Snappy", "Zlib"];
        for (i, t) in comp_types.iter().enumerate() {
            __test_ublk_compress_type(&t, &comp_res[i]);
        }
    }

    #[test]
    fn test_ublk_loop_discard() {
        if !support_ublk() {
            return;
        }

        if !has_blkdiscard() {
            return;
        }

        __test_ublk_add_del_loop(4096, false, false, false, |ctrl, _, _, file_path| {
            let dev_path = ctrl.get_bdev_path();

            // 1. write dev with random data
            let mut arg_list: Vec<String> = Vec::new();
            arg_list.push("if=/dev/urandom".to_string());
            arg_list.push(format!("of={}", dev_path));
            arg_list.push("bs=4096".to_string());
            arg_list.push("count=1024".to_string());
            let out = Command::new("dd").args(arg_list).output().unwrap();
            assert!(out.status.success());

            // 2. punch a hole in the middle
            let res = Command::new("blkdiscard")
                .args(["-o", "4096", "-l", "4096", &dev_path])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .expect("blkdiscard failed");
            assert!(res.success());

            // 3. verify the hole by checking if it is all zero
            let cmp_status = Command::new("cmp")
                .args(["-i", "4096", "--bytes", "4096", &dev_path, "/dev/zero"])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .expect("cmp failed");
            assert!(cmp_status.success());

            let cmp_status = Command::new("cmp")
                .args(["-i", "4096", "--bytes", "4096", &file_path, "/dev/zero"])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .expect("cmp failed");
            assert!(cmp_status.success());

            // 4. write zeroes
            let res = Command::new("blkdiscard")
                .args(["-z", "-o", "8192", "-l", "4096", &dev_path])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .expect("blkdiscard failed");
            assert!(res.success());

            // 5. verify zeroes
            let cmp_status = Command::new("cmp")
                .args(["-i", "8192", "--bytes", "4096", &dev_path, "/dev/zero"])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .expect("cmp failed");
            assert!(cmp_status.success());

            let cmp_status = Command::new("cmp")
                .args(["-i", "8192", "--bytes", "4096", &file_path, "/dev/zero"])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .expect("cmp failed");
            assert!(cmp_status.success());
        });
    }

    #[test]
    fn test_ublk_null_discard() {
        if !support_ublk() {
            return;
        }

        if !has_blkdiscard() {
            return;
        }

        let ctrl = run_rublk_add_dev(["add", "null"].to_vec());
        let dev_path = ctrl.get_bdev_path();

        let res = Command::new("blkdiscard")
            .args([&dev_path])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .expect("blkdiscard failed");
        assert!(res.success());

        run_rublk_del_dev(ctrl, false);
    }

    #[test]
    fn test_rublk_add_no_hang() {
        if !support_ublk() {
            return;
        }

        let tgt_dir = get_curr_bin_dir().unwrap();
        let rublk_path = tgt_dir.join("rublk");

        let output = Command::new(rublk_path)
            .args(["add", "null"])
            .output()
            .expect("Failed to execute rublk add null");

        assert!(output.status.success());

        let stdout = String::from_utf8_lossy(&output.stdout);
        let id_regx = regex::Regex::new(r"dev id (\d+)").unwrap();
        let id: i32 = id_regx
            .captures(&stdout)
            .and_then(|c| c.get(1))
            .and_then(|m| m.as_str().parse().ok())
            .expect("Failed to parse device ID");

        let ctrl = UblkCtrl::new_simple(id).unwrap();
        run_rublk_del_dev(ctrl, false);
    }
}
