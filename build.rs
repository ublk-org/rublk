use libbpf_cargo::SkeletonBuilder;
use std::fs;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

fn find_prog_files(dir: &Path, matching_files: &mut Vec<PathBuf>) {
    // Iterate over the entries in the directory
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();

            if !path.is_dir() {
                let pp = path.clone();
                if let Some(path_str) = pp.to_str() {
                    if path_str.ends_with(".bpf.c") {
                        matching_files.push(path);
                    }
                }
            }
        }
    }
}

fn generate_new_path(old_path: &Path) -> Option<PathBuf> {
    // Get the parent directory
    let parent = old_path.parent()?;

    // Get the file stem (without ".bpf.c")
    let file_stem = old_path.file_stem()?.to_str()?;

    // Remove the ".bpf" part of the stem
    let new_stem = &file_stem[..file_stem.len() - 4];

    // Create the new filename with ".skel.rs" extension
    let new_filename = format!("{}.skel.rs", new_stem);

    // Join the parent directory with the new filename
    let new_path = parent.join(new_filename);

    Some(new_path)
}

fn build_vmlinux_header() {
    //bpftool btf dump file $(VMLINUX_BTF) format c
    let arg_list = vec![
        "btf",
        "dump",
        "file",
        "/sys/kernel/btf/ublk_drv",
        "format",
        "c",
    ];

    let output_file_path = "src/bpf/vmlinux.h";
    // Open the file for writing
    let mut file = File::create(output_file_path).expect("Failed to create file");

    // Run the shell command
    let mut command = Command::new("bpftool")
        .args(arg_list)
        .stdout(Stdio::piped()) // Redirect the stdout to be captured
        .spawn() // Execute the command
        .expect("Failed to execute command");

    // Capture the output from the command
    if let Some(mut stdout) = command.stdout.take() {
        std::io::copy(&mut stdout, &mut file).expect("Failed to write to file");
    }

    // Wait for the command to finish
    command.wait().expect("Failed to wait on command");
}

fn main() {
    // Define the root directory to start searching from
    let root_dir = Path::new("src/bpf");

    // Collect all matching file paths
    let mut prog_files = Vec::new();
    find_prog_files(root_dir, &mut prog_files);

    build_vmlinux_header();

    // Print out the matching file paths
    for file in prog_files {
        if let Some(out) = generate_new_path(&file) {
            if let Some(src_str) = file.to_str() {
                SkeletonBuilder::new()
                    .source(src_str)
                    .build_and_generate(&out)
                    .unwrap();
            }
        }
    }

    println!("cargo:rerun-if-changed=src/bpf/*.bpf.c");
}
