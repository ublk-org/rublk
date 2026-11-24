mod common;

#[cfg(test)]
mod r#loop {
    use super::common::*;
    use libublk::ctrl::UblkCtrl;
    use std::process::{Command, Stdio};

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
    fn test_ublk_fio_verify_loop() {
        if !support_ublk() {
            return;
        }
        if !has_fio() {
            return;
        }

        let tf = |ctrl: &UblkCtrl, _bs: u32, file_size: usize, _path: &str| {
            let io_mb = (file_size >> 20) as u64 / 2;

            fio_randwrite_verify(ctrl, io_mb, 1, &[]);
            fio_randwrite_verify(ctrl, io_mb, 2, &[]);
        };

        __test_ublk_add_del_loop(4096, false, false, false, tf);
        __test_ublk_add_del_loop(512, true, false, false, tf);
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
}
