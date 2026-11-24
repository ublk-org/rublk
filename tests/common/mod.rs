//! Helpers shared by all integration tests
#![allow(dead_code)]

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

pub fn has_mkfs_ext4() -> bool {
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
pub fn has_mkfs_btrfs() -> bool {
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

pub fn has_blkdiscard() -> bool {
    Command::new("blkdiscard")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok()
}

/// Mark the io errors of one ublk device as expected in kernel log, until
/// this guard is dropped.
///
/// Tests covering rejected io make kernel log io errors, which look
/// like real failures for people reading the kernel log.
pub struct ExpectIoErrors(String);

impl ExpectIoErrors {
    pub fn new(test: &str, ctrl: &UblkCtrl) -> Self {
        let bdev = ctrl.get_bdev_path();
        let name = bdev.rsplit('/').next().unwrap_or(&bdev);
        let tag = format!("{} {}", test, name);

        Self::kmsg(&format!("BEGIN {}", tag));
        ExpectIoErrors(tag)
    }

    // it is just one hint, so ignore any failure
    fn kmsg(msg: &str) {
        use std::io::Write;

        if let Ok(mut f) = std::fs::OpenOptions::new().write(true).open("/dev/kmsg") {
            let _ = f.write_all(format!("[RUBLK-TEST] EXPECTED IO ERRORS {}\n", msg).as_bytes());
        }
    }
}

impl Drop for ExpectIoErrors {
    fn drop(&mut self) {
        Self::kmsg(&format!("END {}", self.0));
    }
}

pub fn has_blkzone() -> bool {
    Command::new("blkzone")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok()
}

pub fn support_zoned() -> bool {
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

pub fn mkfs(ctrl: &UblkCtrl, fs: &str, args: Vec<&str>) {
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

pub fn mount_fs_and_io<F>(ctrl: &UblkCtrl, dir: &tempfile::TempDir, f: F)
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

pub fn support_ublk() -> bool {
    if !Path::new("/dev/ublk-control").exists() {
        eprintln!("ublk isn't supported or its module isn't loaded");
        return false;
    }
    return true;
}

pub fn ext4_format_and_mount(ctrl: &UblkCtrl) {
    let tmp_dir = tempfile::TempDir::new().unwrap();

    if has_mkfs_ext4() {
        mkfs(&ctrl, "ext4", ["-F"].to_vec());
        mount_fs_and_io(&ctrl, &tmp_dir, |dir| {
            let tstr = dir.path().to_string_lossy().to_string();
            dd_rw_file(&tstr, true, 4096, 128);
        });
    }
}

pub fn check_ro(ctrl: &UblkCtrl, exp_ro: bool) {
    let mut params: sys::ublk_params = { Default::default() };
    ctrl.get_params(&mut params).unwrap();

    let ro = (params.basic.attrs & libublk::sys::UBLK_ATTR_READ_ONLY) != 0;
    assert!(ro == exp_ro);
}

pub fn check_block_size(ctrl: &UblkCtrl, exp_bs: u32) {
    let mut params: sys::ublk_params = { Default::default() };
    ctrl.get_params(&mut params).unwrap();

    let bs = 1_u32 << params.basic.logical_bs_shift;
    assert!(bs == exp_bs);
    assert!(params.basic.physical_bs_shift >= params.basic.logical_bs_shift);
}

pub fn dd_rw_file(dir: &String, write: bool, bs: u32, count: u32) {
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

pub fn read_ublk_disk(ctrl: &UblkCtrl) {
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

pub fn write_ublk_disk(ctrl: &UblkCtrl, bs: u32, size: usize) {
    let dev_path = ctrl.get_bdev_path();
    let mut arg_list: Vec<String> = Vec::new();

    arg_list.push("if=/dev/zero".to_string());
    arg_list.push(format!("of={}", dev_path));
    arg_list.push(format!("bs={}", bs));
    arg_list.push(format!("count={}", size / (bs as usize)));
    let out = Command::new("dd").args(arg_list).output().unwrap();

    assert!(out.status.success() == true);
}

pub fn create_file_with_size(p: &Path, size: u64) -> std::io::Result<()> {
    // Open the file for writing. This will create the file if it doesn't exist.
    let file = File::create(p)?;

    // Seek to the desired size (e.g., 1 MB).
    file.set_len(size)?;

    Ok(())
}

// qemu-img package is needed
pub fn create_qcow2_image(p: &Path, size: usize) {
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

pub fn ublk_state_wait_until(ctrl: &UblkCtrl, state: u16, timeout: u32) {
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

pub fn get_curr_bin_dir() -> Option<std::path::PathBuf> {
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

/// Kill every process which is started by `argv`, or forked from that
/// process without exec, such as the daemon of `rublk add`, which runs in
/// its own session and isn't killed with its parent.
fn kill_by_cmdline(argv: &[String]) {
    let mut cmdline = argv.join("\0").into_bytes();
    cmdline.push(0);

    let Ok(dir) = std::fs::read_dir("/proc") else {
        return;
    };
    for entry in dir.flatten() {
        let name = entry.file_name();
        let Some(pid) = name
            .to_str()
            .filter(|n| n.bytes().all(|b| b.is_ascii_digit()))
        else {
            continue;
        };
        if std::fs::read(entry.path().join("cmdline")).ok().as_deref() == Some(&cmdline[..]) {
            let _ = Command::new("kill")
                .args(["-9", pid])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
    }
}

/// Run rublk command and return its stdout, which has `exp_len` bytes at
/// least.
///
/// Panic if the command fails or the expected output can't be observed,
/// instead of waiting for the output forever.
pub fn run_rublk_cmd(s: Vec<&str>, exp_len: usize) -> String {
    let tgt_dir = get_curr_bin_dir().unwrap();
    let tmpfile = tempfile::NamedTempFile::new().unwrap();
    let file = std::fs::File::create(tmpfile.path()).unwrap();
    let fg = s.contains(&"--foreground");

    //println!("top dir: path {:?} {:?}", &tgt_dir, &file);
    let rd_path = tgt_dir.display().to_string() + &"/rublk".to_string();
    let cmdline = s.join(" ");
    let argv: Vec<String> = std::iter::once(rd_path.clone())
        .chain(s.iter().map(|a| a.to_string()))
        .collect();
    let mut cmd = Command::new(&rd_path)
        .args(s)
        .stdout(Stdio::from(file))
        .spawn()
        .expect("Failed to execute process");

    // Wait for 60 seconds at most. The background command may never exit
    // if its daemon dies without telling it anything.
    for _ in 0..3000 {
        let status = cmd.try_wait().unwrap();
        let buf = std::fs::read_to_string(tmpfile.path()).unwrap();

        match (fg, status) {
            // foreground command keeps running after its output is done
            (true, _) if buf.len() >= exp_len => return buf,
            (true, Some(status)) => panic!("`rublk {}` exits early: {}", cmdline, status),
            // the whole output is available after background command exits
            (false, Some(status)) => {
                assert!(status.success(), "`rublk {}` fails: {}", cmdline, status);
                let buf = std::fs::read_to_string(tmpfile.path()).unwrap();
                assert!(buf.len() >= exp_len, "`rublk {}`: short output", cmdline);
                return buf;
            }
            _ => {}
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let _ = cmd.kill();
    let _ = cmd.wait();
    // the daemon may be still running
    kill_by_cmdline(&argv);
    panic!("`rublk {}` isn't done in 60 seconds", cmdline);
}

pub fn run_rublk_add_dev(s: Vec<&str>) -> UblkCtrl {
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

pub fn run_rublk_del_dev(ctrl: UblkCtrl, async_del: bool) {
    let id = ctrl.dev_info().dev_id;
    let id_str = id.to_string();

    std::thread::sleep(std::time::Duration::from_millis(500));
    let mut para = ["del", "-n", &id_str].to_vec();
    if async_del {
        para.push("--async");
    }
    let _ = run_rublk_cmd(para.to_vec(), 0);
}

/// Write `count` blocks of `bs_mb` MiB from `src` to the offset of `seek_mb` MiB
pub fn dd_write_dev(ctrl: &UblkCtrl, src: &str, bs_mb: u64, seek_mb: u64, count: u64) -> bool {
    dd_write_dev_kb(ctrl, src, bs_mb << 10, seek_mb << 10, count)
}

/// Write `count` blocks of `bs_kb` KiB from `src` to the offset of `seek_kb` KiB
pub fn dd_write_dev_kb(ctrl: &UblkCtrl, src: &str, bs_kb: u64, seek_kb: u64, count: u64) -> bool {
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

pub fn has_fio() -> bool {
    match Command::new("fio")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
    {
        Ok(res) => res.success(),
        _ => false,
    }
}

/// Write `io_mb` MiB to random offsets of the ublk disk with fio, then
/// read every written block back and verify its checksum.
///
/// `seed` selects both the offsets and the data, so running this twice
/// over one device with different seeds makes sure that data from the
/// earlier run is really replaced. `extra` is appended to fio's command
/// line, for target specific options such as `--zonemode=zbd`.
///
/// Return bytes verified.
pub fn fio_randwrite_verify(ctrl: &UblkCtrl, io_mb: u64, seed: u32, extra: &[&str]) -> u64 {
    let bdev = ctrl.get_bdev_path();
    // The whole `io_size` has to be written and verified, so don't limit
    // runtime of fio, and `timeout` is for avoiding to hang forever.
    let res = Command::new("timeout")
        .args(["600", "fio"])
        .args(["--name=randwrite_verify", "--output-format=json"])
        .arg(format!("--filename={}", bdev))
        .args(["--direct=1", "--ioengine=libaio", "--iodepth=16"])
        .args(["--rw=randwrite", "--bsrange=4k-128k"])
        .arg(format!("--io_size={}M", io_mb))
        .arg(format!("--randseed={}", seed))
        .args(["--verify=crc32c", "--do_verify=1", "--verify_fatal=1"])
        .arg("--verify_state_save=0")
        .args(extra)
        .output()
        .expect("Failed to execute fio");
    let out = String::from_utf8_lossy(&res.stdout).to_string();
    let err = String::from_utf8_lossy(&res.stderr).to_string();

    assert!(
        res.status.code() != Some(124),
        "fio isn't done on {} {:?} in 600 seconds",
        bdev,
        extra
    );
    // any verify failure or io error makes fio exit with failure
    assert!(
        res.status.success(),
        "fio failed on {} {:?}: {}\n{}",
        bdev,
        extra,
        res.status,
        err
    );

    // fio may log something before the json document
    let json: serde_json::Value =
        serde_json::from_str(&out[out.find('{').expect("no fio json output")..]).unwrap();
    let job = &json["jobs"][0];
    let written = job["write"]["io_bytes"].as_u64().unwrap();
    let read = job["read"]["io_bytes"].as_u64().unwrap();

    println!(
        "fio randwrite & verify {} {:?}: write {} bytes/{} ios, verify read {} bytes/{} ios, err {}",
        bdev,
        extra,
        written,
        job["write"]["total_ios"],
        read,
        job["read"]["total_ios"],
        job["error"]
    );

    assert_eq!(job["error"].as_i64(), Some(0));
    assert_eq!(job["total_err"].as_u64().unwrap_or(0), 0);
    // the workload is really run, and each written byte is verified
    assert!(
        written + (128 << 10) >= io_mb << 20,
        "short write {}",
        written
    );
    assert_eq!(read, written);

    read
}

pub fn run_ublk_recover(ctrl: &UblkCtrl) {
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
