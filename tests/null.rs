mod common;

#[cfg(test)]
mod null {
    use super::common::*;
    use std::process::{Command, Stdio};

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
}
