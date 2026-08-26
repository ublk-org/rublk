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

    fn support_nbd() -> bool {
        Command::new("nbdkit")
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map_or(false, |s| s.success())
    }

    /// Start nbdkit serving a memory export on a unix socket; killed by
    /// the caller.
    fn start_nbd_server(sock: &str) -> std::process::Child {
        let child = Command::new("nbdkit")
            .args(["-f", "-U", sock, "memory", "64M"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("start nbdkit");
        for _ in 0..100 {
            if Path::new(sock).exists() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        child
    }

    fn __test_ublk_add_del_nbd<F>(extra: &[&str], recover: bool, f: F)
    where
        F: Fn(&UblkCtrl, u32, usize),
    {
        let dir = tempfile::TempDir::new().unwrap();
        let sock = dir.path().join("nbd.sock");
        let sock_str = sock.to_string_lossy().to_string();
        let mut server = start_nbd_server(&sock_str);

        let mut cmd_line = ["add", "nbd", "--unix", &sock_str].to_vec();
        cmd_line.extend_from_slice(extra);
        if recover {
            cmd_line.push("-r");
        }
        let ctrl = run_rublk_add_dev(cmd_line);
        f(&ctrl, 512, 64 * 1024 * 1024);
        run_rublk_del_dev(ctrl, false);

        let _ = server.kill();
        let _ = server.wait();
    }

    #[test]
    fn test_ublk_add_del_nbd() {
        if !support_ublk() || !support_nbd() {
            return;
        }
        let tf = |ctrl: &UblkCtrl, bs: u32, file_size: usize| {
            read_ublk_disk(ctrl);
            write_ublk_disk(ctrl, bs, file_size);
        };
        __test_ublk_add_del_nbd(&[], false, tf);
        __test_ublk_add_del_nbd(&["--send-zc"], false, tf);
        __test_ublk_add_del_nbd(&["-z"], false, tf);
        __test_ublk_add_del_nbd(&["-z", "--send-zc"], false, tf);
        __test_ublk_add_del_nbd(&["-q", "2"], false, tf);
    }

    #[test]
    fn test_ublk_format_mount_nbd() {
        if !support_ublk() || !support_nbd() {
            return;
        }
        __test_ublk_add_del_nbd(&[], false, |ctrl, _bs, _file_size| {
            ext4_format_and_mount(ctrl);
        });
        __test_ublk_add_del_nbd(&["-z", "--send-zc"], false, |ctrl, _bs, _file_size| {
            ext4_format_and_mount(ctrl);
        });
    }

    #[test]
    fn test_ublk_nbd_recover() {
        if !support_ublk() || !support_nbd() {
            return;
        }
        __test_ublk_add_del_nbd(&[], true, |ctrl, _bs, _file_size| {
            run_ublk_recover(ctrl);
        });
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

    // ---- --shmem-zc ---------------------------------------------------------

    /// hugetlb setup touches global state (nr_hugepages); one test at a time.
    static HUGETLB_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn support_shmem_zc() -> bool {
        if UblkCtrl::get_features().unwrap_or_default() & sys::UBLK_F_SHMEM_ZC as u64 == 0 {
            return false;
        }
        libublk::ctrl::UblkCtrlBuilder::default()
            .name("shmem_zc_test")
            .depth(4)
            .nr_queues(1)
            .id(-1)
            .ctrl_flags(sys::UBLK_F_SHMEM_ZC.into())
            .dev_flags(libublk::UblkFlags::UBLK_DEV_F_ADD_DEV)
            .build()
            .is_ok()
    }

    fn has_fio() -> bool {
        Command::new("fio")
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|s| s.success())
    }

    /// A file on a private hugetlbfs mount, with the hugepages it needs
    /// reserved; everything is torn down (file, mount, reservation) on drop.
    struct HugetlbFile {
        _guard: std::sync::MutexGuard<'static, ()>,
        mnt: tempfile::TempDir,
        path: std::path::PathBuf,
        size: usize,
        prev_nr_hugepages: String,
    }

    impl HugetlbFile {
        const CTL: &'static str = "/proc/sys/vm/nr_hugepages";

        /// `None` (with a message) when hugepages cannot be had here.
        fn new(pages: usize) -> Option<Self> {
            let guard = HUGETLB_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
            let kb: usize = meminfo
                .lines()
                .find(|l| l.starts_with("Hugepagesize:"))?
                .split_whitespace()
                .nth(1)?
                .parse()
                .ok()?;
            let size = pages * (kb << 10);

            let mnt = tempfile::TempDir::new().ok()?;
            let prev = std::fs::read_to_string(Self::CTL).ok()?;
            let have: usize = prev.trim().parse().ok()?;
            std::fs::write(Self::CTL, format!("{}\n", have.max(pages))).ok()?;
            let mounted = Command::new("mount")
                .args(["-t", "hugetlbfs", "none"])
                .arg(mnt.path())
                .status()
                .is_ok_and(|s| s.success());
            if !mounted {
                let _ = std::fs::write(Self::CTL, &prev);
                eprintln!("skipping: cannot mount hugetlbfs");
                return None;
            }
            // from here on Drop undoes the mount and the reservation
            let this = Self {
                _guard: guard,
                path: mnt.path().join("ublk_shmem_buf"),
                mnt,
                size,
                prev_nr_hugepages: prev,
            };

            // fallocate takes the pages from the pool right away, so a
            // pool that is too small (or held by someone else) shows up
            // here as a skip rather than as a failed mmap in the daemon
            use std::os::fd::AsRawFd;
            let file = std::fs::File::create(&this.path).ok()?;
            if unsafe { libc::fallocate(file.as_raw_fd(), 0, 0, size as libc::off_t) } != 0 {
                eprintln!(
                    "skipping: cannot reserve {} hugepages: {}",
                    pages,
                    std::io::Error::last_os_error()
                );
                return None;
            }
            Some(this)
        }

        fn path_str(&self) -> String {
            self.path.to_string_lossy().to_string()
        }
    }

    impl Drop for HugetlbFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
            let _ = Command::new("umount")
                .arg(self.mnt.path())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
            let _ = std::fs::write(Self::CTL, &self.prev_nr_hugepages);
        }
    }

    /// O_DIRECT IO on the device issued straight from the hugetlbfs file the
    /// daemon registered -- the same pages, so the driver's match fires --
    /// cross-checked against IO through an ordinary heap buffer.
    /// `verify` is false for targets that do not keep data (null).
    fn shmem_zc_io_check(ctrl: &UblkCtrl, htlb: &HugetlbFile, verify: bool) {
        use std::os::fd::AsRawFd;
        use std::os::unix::fs::OpenOptionsExt;

        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&htlb.path)
            .unwrap();
        let shmem = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                htlb.size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED | libc::MAP_POPULATE,
                file.as_raw_fd(),
                0,
            )
        };
        assert_ne!(
            shmem,
            libc::MAP_FAILED,
            "{}",
            std::io::Error::last_os_error()
        );
        let shmem = shmem as *mut u8;

        let bdev = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(ctrl.get_bdev_path())
            .unwrap();
        let fd = bdev.as_raw_fd();
        let pwrite = |buf: *const u8, len: usize, off: u64| {
            let n = unsafe { libc::pwrite(fd, buf as *const _, len, off as libc::off_t) };
            assert_eq!(
                n as usize,
                len,
                "pwrite: {}",
                std::io::Error::last_os_error()
            );
        };
        let pread = |buf: *mut u8, len: usize, off: u64| {
            let n = unsafe { libc::pread(fd, buf as *mut _, len, off as libc::off_t) };
            assert_eq!(
                n as usize,
                len,
                "pread: {}",
                std::io::Error::last_os_error()
            );
        };
        let fill = |buf: *mut u8, len: usize, seed: u8| {
            for i in 0..len {
                unsafe { *buf.add(i) = seed.wrapping_add((i / 512) as u8) ^ (i as u8) };
            }
        };
        let equal = |a: *const u8, b: *const u8, len: usize| unsafe {
            !verify || std::slice::from_raw_parts(a, len) == std::slice::from_raw_parts(b, len)
        };

        let io_len = 256_usize << 10;
        assert!(htlb.size >= 4 * io_len);
        let heap = libublk::helpers::IoBuf::<u8>::new(io_len);
        let heap_ptr = heap.as_mut_ptr();
        let zc_a = unsafe { shmem.add(4096) };
        let zc_b = unsafe { shmem.add(htlb.size - io_len) };

        // shmem -> device -> heap
        fill(zc_a, io_len, 0x11);
        pwrite(zc_a, io_len, 1 << 20);
        fill(heap_ptr, io_len, 0);
        pread(heap_ptr, io_len, 1 << 20);
        assert!(equal(zc_a, heap_ptr, io_len));

        // heap -> device -> shmem
        fill(heap_ptr, io_len, 0x77);
        pwrite(heap_ptr, io_len, 8 << 20);
        fill(zc_b, io_len, 0);
        pread(zc_b, io_len, 8 << 20);
        assert!(equal(heap_ptr, zc_b, io_len));

        // shmem -> device -> shmem, 4k at a time
        for i in 0..8_u64 {
            let p = unsafe { zc_a.add(i as usize * 4096) };
            fill(p, 4096, 0xa0 + i as u8);
            pwrite(p, 4096, (16 << 20) + i * 4096);
        }
        pread(zc_b, 8 * 4096, 16 << 20);
        assert!(equal(zc_a, zc_b, 8 * 4096));

        unsafe { libc::munmap(shmem as *mut _, htlb.size) };
    }

    /// fio over the same hugetlbfs file (`--mem=mmaphuge`): a short randrw
    /// burst, then a write/verify pass when the target keeps data.
    fn shmem_zc_fio(ctrl: &UblkCtrl, htlb: &HugetlbFile, verify: bool) {
        if !has_fio() {
            return;
        }
        let dev = ctrl.get_bdev_path();
        let mem = format!("--mem=mmaphuge:{}", htlb.path_str());
        let common = [
            "--ioengine=libaio",
            "--direct=1",
            "--bs=4k",
            "--iodepth=32",
            "--numjobs=1",
            "--size=16M",
        ];
        let run = |args: &[&str]| {
            let status = Command::new("fio")
                .args(common)
                .arg(&mem)
                .arg(format!("--filename={}", dev))
                .args(args)
                .stdout(Stdio::null())
                .status()
                .expect("run fio");
            assert!(status.success(), "fio {:?} failed", args);
        };
        run(&[
            "--name=randrw",
            "--rw=randrw",
            "--runtime=2",
            "--time_based",
        ]);
        if verify {
            run(&[
                "--name=write",
                "--rw=write",
                "--verify=crc32c",
                "--do_verify=0",
            ]);
            run(&[
                "--name=verify",
                "--rw=read",
                "--verify=crc32c",
                "--verify_only",
            ]);
        }
    }

    fn __test_ublk_null_shmem_zc(htlb: &HugetlbFile, extra: &[&str]) {
        let htlb_path = htlb.path_str();
        let mut cmd_line = ["add", "null", "--shmem-zc", "--htlb", &htlb_path].to_vec();
        cmd_line.extend_from_slice(extra);
        let ctrl = run_rublk_add_dev(cmd_line);
        shmem_zc_io_check(&ctrl, htlb, false);
        shmem_zc_fio(&ctrl, htlb, false);
        run_rublk_del_dev(ctrl, false);
    }

    #[test]
    fn test_ublk_null_shmem_zc() {
        if !support_ublk() || !support_shmem_zc() {
            return;
        }
        let Some(htlb) = HugetlbFile::new(8) else {
            return;
        };
        __test_ublk_null_shmem_zc(&htlb, &[]);
        __test_ublk_null_shmem_zc(&htlb, &["-a"]);
        __test_ublk_null_shmem_zc(&htlb, &["-a", "-z"]);
    }

    fn __test_ublk_loop_shmem_zc<F>(htlb: &HugetlbFile, extra: &[&str], f: F)
    where
        F: Fn(&UblkCtrl),
    {
        let tmp_file = tempfile::NamedTempFile::new().unwrap();
        create_file_with_size(tmp_file.path(), 64 << 20).unwrap();
        let file = tmp_file.path().to_string_lossy().to_string();
        let htlb_path = htlb.path_str();

        let mut cmd_line = [
            "add",
            "loop",
            "-f",
            &file,
            "--shmem-zc",
            "--htlb",
            &htlb_path,
        ]
        .to_vec();
        cmd_line.extend_from_slice(extra);
        let ctrl = run_rublk_add_dev(cmd_line);
        f(&ctrl);
        run_rublk_del_dev(ctrl, false);
    }

    #[test]
    fn test_ublk_loop_shmem_zc() {
        if !support_ublk() || !support_shmem_zc() {
            return;
        }
        let Some(htlb) = HugetlbFile::new(8) else {
            return;
        };
        let tf = |ctrl: &UblkCtrl| {
            shmem_zc_io_check(ctrl, &htlb, true);
            shmem_zc_fio(ctrl, &htlb, true);
        };
        __test_ublk_loop_shmem_zc(&htlb, &[], tf);
        __test_ublk_loop_shmem_zc(&htlb, &["-a"], tf);
        __test_ublk_loop_shmem_zc(&htlb, &["-z"], tf);
        __test_ublk_loop_shmem_zc(&htlb, &["-a", "-z"], tf);
    }

    /// The registration outlives the daemon: after `kill -9` and `recover`
    /// the new daemon adopts the buffer under its old index, and IO from the
    /// hugetlbfs file keeps working with the data written before.
    #[test]
    fn test_ublk_loop_shmem_zc_recover() {
        if !support_ublk() || !support_shmem_zc() {
            return;
        }
        let Some(htlb) = HugetlbFile::new(8) else {
            return;
        };
        __test_ublk_loop_shmem_zc(&htlb, &["-a", "-r"], |ctrl| {
            shmem_zc_io_check(ctrl, &htlb, true);
            run_ublk_recover(ctrl);
            shmem_zc_io_check(ctrl, &htlb, true);
            shmem_zc_fio(ctrl, &htlb, true);
        });
    }

    #[test]
    fn test_ublk_nbd_shmem_zc() {
        if !support_ublk() || !support_shmem_zc() || !support_nbd() {
            return;
        }
        let Some(htlb) = HugetlbFile::new(8) else {
            return;
        };
        let htlb_path = htlb.path_str();
        let tf = |ctrl: &UblkCtrl, _bs: u32, _size: usize| {
            shmem_zc_io_check(ctrl, &htlb, true);
            shmem_zc_fio(ctrl, &htlb, true);
        };
        __test_ublk_add_del_nbd(&["--shmem-zc", "--htlb", &htlb_path], false, tf);
        __test_ublk_add_del_nbd(&["--shmem-zc", "--htlb", &htlb_path, "-z"], false, tf);
        __test_ublk_add_del_nbd(
            &["--shmem-zc", "--htlb", &htlb_path, "-z", "--send-zc"],
            false,
            tf,
        );
    }
}
