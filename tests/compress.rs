//! Tests of compress target, which is only built with `--features compress`
#![cfg(feature = "compress")]

mod common;

#[cfg(test)]
mod compress {
    use super::common::*;
    use libublk::ctrl::UblkCtrl;

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
    fn test_ublk_format_mount_compress() {
        if !support_ublk() {
            return;
        }
        __test_ublk_add_del_compress(false, |ctrl| {
            ext4_format_and_mount(ctrl);
        });
    }

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
}
