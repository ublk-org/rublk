#[cfg(test)]
mod integration {
    use libublk::{ctrl::UblkCtrl, sys};
    use std::env;
    use std::fs::File;
    use std::path::Path;
    use std::process::{Command, Stdio};

    fn has_mkfs_btrfs() -> bool {
        match Command::new("mkfs.btrfs")
            .arg("--version") // Try running the binary with a harmless argument
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
        {
            Ok(res) => res.success(),
            _ => false,
        }
    }

    fn support_ublk() -> bool {
        if !Path::new("/dev/ublk-control").exists() {
            eprintln!("ublk isn't supported or its module isn't loaded");
            return false;
        }
        return true;
    }

    fn format_and_mount(ctrl: &UblkCtrl) {
        let dev_path = ctrl.get_bdev_path();

        let ext4_options = block_utils::Filesystem::Ext4 {
            inode_size: 512,
            stride: Some(2),
            stripe_width: None,
            reserved_blocks_percentage: 10,
        };
        block_utils::format_block_device(&Path::new(&dev_path), &ext4_options).unwrap();

        let tmp_dir = tempfile::TempDir::new().unwrap();
        let bdev = block_utils::get_device_info(Path::new(&dev_path)).unwrap();

        block_utils::mount_device(&bdev, tmp_dir.path()).unwrap();
        block_utils::unmount_device(tmp_dir.path()).unwrap();
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

    fn run_rublk_add_dev(s: Vec<&str>) -> i32 {
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

        id
    }

    fn run_rublk_del_dev(id: i32) {
        let id_str = id.to_string();

        std::thread::sleep(std::time::Duration::from_millis(500));
        let para = ["del", "-n", &id_str].to_vec();
        let _ = run_rublk_cmd(para, 0);
    }

    fn __test_ublk_add_del_null(bs: u32, aa: bool) {
        let binding = bs.to_string();
        let mut cmd_line = ["add", "null", "--logical-block-size", &binding].to_vec();
        if aa {
            cmd_line.push("-a");
        }
        let id = run_rublk_add_dev(cmd_line);
        let ctrl = UblkCtrl::new_simple(id).unwrap();

        read_ublk_disk(&ctrl);
        check_block_size(&ctrl, bs);
        run_rublk_del_dev(id);
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

    fn __test_ublk_add_del_zoned<F>(bs: u32, tf: F)
    where
        F: Fn(i32, u32, usize),
    {
        match UblkCtrl::get_features() {
            Some(f) => {
                if (f & sys::UBLK_F_ZONED as u64) != 0 {
                    let id = run_rublk_add_dev(
                        [
                            "add",
                            "zoned",
                            "--zone-size",
                            "4",
                            "--logical-block-size",
                            &bs.to_string(),
                        ]
                        .to_vec(),
                    );
                    tf(id, bs, 4 << 20);
                    run_rublk_del_dev(id);
                }
            }
            _ => {}
        }
    }

    #[test]
    fn test_ublk_add_del_zoned() {
        if !support_ublk() {
            return;
        }
        match UblkCtrl::get_features() {
            Some(f) => {
                if f & (libublk::sys::UBLK_F_ZONED as u64) != 0 {
                    let tf = |id: i32, bs: u32, _file_size: usize| {
                        let ctrl = UblkCtrl::new_simple(id).unwrap();
                        read_ublk_disk(&ctrl);
                        check_block_size(&ctrl, bs);
                    };
                    __test_ublk_add_del_zoned(512, tf);
                    __test_ublk_add_del_zoned(4096, tf);
                }
            }
            None => {}
        }
    }

    fn __test_ublk_add_del_loop<F>(bs: u32, aa: bool, f: F)
    where
        F: Fn(i32, u32, usize),
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
        let id = run_rublk_add_dev(cmd_line);

        f(id, bs, file_size.try_into().unwrap());

        run_rublk_del_dev(id);
    }
    #[test]
    fn test_ublk_add_del_loop() {
        if !support_ublk() {
            return;
        }

        let tf = |id: i32, bs: u32, _file_size: usize| {
            let ctrl = UblkCtrl::new_simple(id).unwrap();
            read_ublk_disk(&ctrl);
            check_block_size(&ctrl, bs);
        };

        __test_ublk_add_del_loop(4096, false, tf);
        __test_ublk_add_del_loop(4096, true, tf);
    }

    fn __test_ublk_null_read_only(cmds: &[&str], exp_ro: bool) {
        let id = run_rublk_add_dev(cmds.to_vec());
        let ctrl = UblkCtrl::new_simple(id).unwrap();
        check_ro(&ctrl, exp_ro);
        run_rublk_del_dev(id);
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

    fn __test_ublk_add_del_qcow2<F>(bs: u32, f: F)
    where
        F: Fn(i32, u32, usize),
    {
        let tmp_file = tempfile::NamedTempFile::new().unwrap();
        let file_size = 32 * 1024 * 1024;
        let p = tmp_file.path();

        create_qcow2_image(&p, file_size);
        let pstr = match p.to_str() {
            Some(p) => p,
            _ => panic!(),
        };

        let id = run_rublk_add_dev(
            [
                "add",
                "qcow2",
                "-f",
                &pstr,
                "--logical-block-size",
                &bs.to_string(),
            ]
            .to_vec(),
        );

        f(id, bs, file_size);

        run_rublk_del_dev(id);
    }
    #[test]
    fn test_ublk_add_del_qcow2() {
        if !support_ublk() {
            return;
        }
        __test_ublk_add_del_qcow2(4096, |id, bs, file_size| {
            let ctrl = UblkCtrl::new_simple(id).unwrap();

            read_ublk_disk(&ctrl);
            write_ublk_disk(&ctrl, bs, file_size);
            check_block_size(&ctrl, bs);
        });
    }

    #[test]
    fn test_ublk_format_mount_loop() {
        if !support_ublk() {
            return;
        }
        __test_ublk_add_del_loop(4096, true, |id, _bs, _file_size| {
            let ctrl = UblkCtrl::new_simple(id).unwrap();
            format_and_mount(&ctrl);
        });
    }

    #[test]
    fn test_ublk_format_mount_qcow2() {
        if !support_ublk() {
            return;
        }
        __test_ublk_add_del_qcow2(4096, |id, _bs, _file_size| {
            let ctrl = UblkCtrl::new_simple(id).unwrap();
            format_and_mount(&ctrl);
        });
    }

    #[test]
    fn test_ublk_format_mount_zoned() {
        if !support_ublk() {
            return;
        }

        if !has_mkfs_btrfs() {
            return;
        }
        __test_ublk_add_del_zoned(4096, |id, _bs, _file_size| {
            let ctrl = UblkCtrl::new_simple(id).unwrap();
            let bdev = ctrl.get_bdev_path();
            let tmp_dir = tempfile::TempDir::new().unwrap();

            let res = Command::new("mkfs.btrfs")
                .args(["-O", "zoned", &bdev])
                .stdout(std::process::Stdio::null())
                .status()
                .expect("Failed to execute mkfs.btrfs");
            assert!(res.success());

            let res = Command::new("mount")
                .args([&bdev, &tmp_dir.path().to_string_lossy().to_string()])
                .stdout(std::process::Stdio::null())
                .status()
                .expect("Failed to execute mount");
            assert!(res.success());
            let res = Command::new("umount")
                .args([&tmp_dir.path().to_string_lossy().to_string()])
                .stdout(std::process::Stdio::null())
                .status()
                .expect("Failed to execute umount");
            assert!(res.success());
        });
    }
}
