use std::{io::{BufReader, Write, BufRead}, path::{Path, PathBuf}, fs::{metadata, create_dir, read_dir, remove_file, OpenOptions, File, remove_dir}, env};
use crate::storage::tree::*;
use crate::kvpair::*;

use crate::storage::diskseg::DiskSegment::{self, *};

const LOG_EXT: &str = "log";
const DATA_EXT: &str = "data";

pub struct LsmTree {
    name: String,
    log_file: File,
    tree: LogSegment<String>,
    log_segments: Vec<DiskSegment>,
}

impl LsmTree {
    /*
    New: Either creates a new DB, including creating a new write ahead log, or,
    if the DB already exists and we are not purging this as part of creation, conusmes
    and restores from the pre-existing WAL.
    */
    pub fn new(name: &str) -> LsmTree {
        // Check for existing log for this DB, then we are not creating new DB and should
        // restore log to memory
        let path_str = format!("{}.{}", name, LOG_EXT);
        let log_file = Path::new(&path_str);
        if Path::exists(log_file) {
            let existing_log = OpenOptions::new().read(true).write(true).append(true).open(log_file).unwrap();
            let mut tree = LsmTree{
                name: name.to_string(), 
                log_file: existing_log, 
                tree: LogSegment::new(), 
                log_segments: reclaim_segments(name)};
            let restore_result = tree.restore();
            assert!(restore_result, "Failed to restore WAL!");
            return tree;
        }
        LsmTree{
            name: name.to_string(), 
            log_file: OpenOptions::new().create(true).write(true).append(true).open(log_file).unwrap(), 
            tree: LogSegment::new(), 
            log_segments: reclaim_segments(name)}
    }

    /*
    New Delete Existing: Creates DB from scratch, deleting any existing DB with this name
    */
    pub fn new_delete_existing(name: &str) -> LsmTree {
        // Check for existing log for this DB, then we are not creating new DB and should
        // restore log to memory
        let path_str = format!("{}.log", name);
        let log_file = Path::new(&path_str);

        if Path::exists(log_file) {
            if let Err(e) = remove_file(log_file) {
                panic!("unable to delete existing wal with error {}", e);
            }
        }

        let lsm_dir = get_lsmdir(name);
        if lsm_dir.is_dir() {
            if let Err(e) = remove_dir(lsm_dir) {
                panic!("unable to delete existing log segment directory with error {}", e);
            }
        }
        
        LsmTree{
            name: name.to_string(), 
            log_file: OpenOptions::new().create(true).write(true).append(true).open(log_file).unwrap(), 
            tree: LogSegment::new(),
            log_segments: reclaim_segments(name)}
    }

    /*
    Log: On each DB operation, we write ahead to log to ensure durability of all operations. This is a persisted
    log that will reflect any actions prior to mutating the in memory log segment(s)
    */
    fn log(&mut self, entry: &KVPair) -> bool {
        match self.log_file.write(format!("{} {}\n", entry.key, entry.value).as_bytes()) {
            Err(e) => {println!("failed to log {} to db {} with error {}", entry, self.name, e); false}
            Ok(_) => {println!("logged entry for {} for db {}", entry, self.name); true}
        }
    }

    /*
    Restore: On DB startup, if this is an existing DB, we will need to restore the existing WAL prior
    to the latest start up. Consumes each entry of the WAL beyond the latest non-persisted log entry
    and builds a new in-memory log segment
    */
    fn restore(&mut self) -> bool {
        // Clone WAL handle since we cannot move WAL behind ref to LSM
        let restore_handle = self.log_file.try_clone().unwrap();
        let wal_contents = BufReader::new(restore_handle).lines();
        for line in wal_contents {
            if let Ok(line) = line {
                // WAL format is an append-only log of entries like below
                // key: foo, value: bar, we can re-create segment from log
                // by iterating these entries and applying as a sequence of writes
                let mut line = line.split(' ');
                let tuple = (line.next().unwrap().into(), line.next().unwrap().into());
                self.tree.insert(tuple);
            }
            else {
                panic!("unable to get entry from WAL for {}, terminating", self.name);
            }
        }
        true
    }

    /*
    Get: Queries LSM for value for the given key, will traverse log segments in newest
    to oldest fashion to preverse append-only deletion semantics
    */
    pub fn get(&self, key: &str) -> Option<&String> {
        return self.tree.get(key.to_string());
    }

    /*
    Write: Appends a new entry to the latest log segment, after first preserving the
    operation to the WAL
    */
    pub fn write(&mut self, key: &str, value: &str) -> bool {
        let kvp = KVPair::new(key.into(), value.into());
        match self.log(&kvp) {
            true => {
                self.tree.insert((key.to_string(), value.to_string()));
                true
            },
            false => {false}
        }
    }

    /*
    Total Segments: Gets the total number of log segments on disk
    */
    pub fn total_segments(&self) -> usize {
        self.log_segments.len()
    }
}

fn reclaim_segments(name: &str) -> Vec<DiskSegment> {
    let mut old_segments: Vec<DiskSegment> = Vec::new();

    let lsm_dir = get_lsmdir(name);

    if lsm_dir.is_dir() {
        for item in read_dir(lsm_dir).unwrap() {
            let item = item.unwrap();
            let path = item.path();
            
            if let Some(ext) = path.extension() {
                let md = metadata(&path).unwrap();
                if md.is_file() && ext.eq(DATA_EXT) {
                    let segment = ClosedSegment{path_s: String::from(name)};
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
        // For new LSM, we don't reclaim any segments, but instead create the segment directory
        if let Err(e) = create_dir(lsm_dir) {
            panic!("failed to create log segments directory {}!", name);
        }
    }
    old_segments
}

fn get_lsmdir(name: &str) -> PathBuf {
    get_currdir().join(Path::new(name))
}

fn get_currdir() -> PathBuf {
    env::current_dir().expect("Unable to get cwd")
}