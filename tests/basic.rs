mod common;

#[cfg(test)]
mod basic {
    use super::common::*;
    use libublk::{ctrl::UblkCtrl, sys};
    use std::process::Command;

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
