use std::{fs::{create_dir, read_dir, remove_file, metadata, OpenOptions, File, remove_dir}, path::{PathBuf, Path}, env::current_dir};
use crate::log;

use super::diskseg::DiskSegment::{self, *};

pub const LOG_EXT: &str = "log";
pub const DATA_EXT: &str = "data";

pub fn get_wal(name: &str, create: bool) -> File {
    // Check for existing log for this DB, then we are not creating new DB and should
    // restore log to memory
    let path_str = format!("{}.{}", name, LOG_EXT);
    let log_file = Path::new(&path_str);
    let full_file_path = get_lsmdir(name).join(log_file);
    log(&format!("{:?}", full_file_path.as_os_str()));
    if create {
        OpenOptions::new().create(true).write(true).append(true).open(full_file_path).unwrap()
    }
    else {
        OpenOptions::new().read(true).write(true).append(true).open(full_file_path).unwrap()
    }
}

pub fn get_seg_path(name: &str, seg_num: usize) -> PathBuf {
    // Check for existing log for this DB, then we are not creating new DB and should
    // restore log to memory
    let path_str = format!("segment_{}.{}", seg_num, LOG_EXT);
    let seg_path = Path::new(&path_str);
    get_lsmdir(name).join(seg_path)
}

pub fn get_seg_path_s(name: &str, seg_num: usize) -> String {
    // Check for existing log for this DB, then we are not creating new DB and should
    // restore log to memory
    let path_str = format!("segment_{}.{}", seg_num, LOG_EXT);
    let seg_path = Path::new(&path_str);
    get_lsmdir(name).join(seg_path).to_str().unwrap().to_string()
}

pub fn get_segment(name: &str, seg_num: usize, create: bool) -> File {
    let full_file_path = get_seg_path(name, seg_num);
    log(&format!("Creating segment file {:?}", full_file_path.as_os_str()));
    if create {
        OpenOptions::new().create(true).write(true).append(true).open(full_file_path).unwrap()
    }
    else {
        OpenOptions::new().read(true).write(true).append(true).open(full_file_path).unwrap()
    }
}

pub fn reclaim_segments(name: &str) -> Vec<DiskSegment> {
    let mut old_segments: Vec<DiskSegment> = Vec::new();

    let lsm_dir = get_lsmdir(name);

    if lsm_dir.is_dir() {
        for item in read_dir(lsm_dir).unwrap() {
            let item = item.unwrap();
            let path = item.path();
            
            if let Some(ext) = path.extension() {
                let md = metadata(&path).unwrap();
                if md.is_file() && ext.eq(DATA_EXT) {
                    let segment = ClosedSegment{
                        path_s: String::from(name), 
                        file: OpenOptions::new().read(true).write(false).open(path).unwrap()};
                    match old_segments.binary_search(&segment) {
                        // Place log segments in order by name
                        // TODO: We need to probably extract the numeric ordering
                        // from the data files to properly order this vector e.g.
                        // currently foo_10.data would be before foo_9.data
                        Err(pos) => old_segments.insert(pos, segment),
                        _ => {},
                    }
                }
            }
        }
    }
    else {
        // For new LSM, we should have created the LSM dir for the WAL already, panic if not
        panic!("No LSM dir found for LSM {}!", name);
    }
    old_segments
}

/*
Purge LSM directory: Delete LSM directory and all log segment/WAL files.
Will panic if we try to purge the LSM directory for a non-existing LSM
*/
pub fn purge_lsm_dir(name: &str) {
    let lsm_dir = get_lsmdir(name);

    if lsm_dir.is_dir() {
        for item in read_dir(lsm_dir.clone()).unwrap() {
            let item = item.unwrap();
            let path = item.path();
            
            let md = metadata(&path).unwrap();
            if md.is_file() {
                if let Err(e) = remove_file(path) {
                    panic!("unable to delete existing LSM files with error {}", e);
                }
            }
        }
        if let Err(e) = remove_dir(lsm_dir) {
            panic!("unable to delete existing LSM directory with error {}", e);
        }
    }
}

/*
Create LSM Directory: Create a new LSM directory for the specified LSM.
Will panic if attempt to create LSM directory already occupied by existing LSM
*/
pub fn create_lsm_dir(name: &str) -> Result<(), &'static str> {
    let lsm_dir = get_lsmdir(name);

    if !lsm_dir.is_dir() {
        if let Err(e) = create_dir(lsm_dir) {
            panic!("failed to create log segments directory {}, with error {}!", name, e);
        }
    }
    else {
        return Err("Attempting to create LSM directory for existing LSM");
    }
    Ok(())
}

pub fn lsm_exists(name: &str) -> bool {
    let lsm_dir = get_lsmdir(name);
    lsm_dir.is_dir()
}

pub fn get_lsmdir(name: &str) -> PathBuf {
    get_currdir().join(Path::new(name))
}

pub fn get_currdir() -> PathBuf {
    current_dir().expect("Unable to get cwd")
}