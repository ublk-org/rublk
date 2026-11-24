mod common;

#[cfg(test)]
mod qcow2 {
    use super::common::*;
    use libublk::ctrl::UblkCtrl;

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
    fn test_ublk_format_mount_qcow2() {
        if !support_ublk() {
            return;
        }
        __test_ublk_add_del_qcow2(4096, false, |ctrl, _bs, _file_size| {
            ext4_format_and_mount(ctrl);
        });
    }

    #[test]
    fn test_ublk_fio_verify_qcow2() {
        if !support_ublk() {
            return;
        }
        if !has_fio() {
            return;
        }

        __test_ublk_add_del_qcow2(4096, false, |ctrl, _bs, file_size| {
            let io_mb = (file_size >> 20) as u64 / 2;

            fio_randwrite_verify(ctrl, io_mb, 1, &[]);
            fio_randwrite_verify(ctrl, io_mb, 2, &[]);
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
}
