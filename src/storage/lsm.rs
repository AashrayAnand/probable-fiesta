use std::{io::{Lines, Result, BufReader, BufRead, Write}, fs::File, path::Path};
use crate::storage::tree::*;
use crate::kvpair::*;

use crate::storage::{diskseg::{extract_seg_id, DiskSegment::{self, *}}, files::*};

const MAX_TREE_SIZE: usize = 100;

pub struct LsmTree {
    name: String,
    log_file: File,
    tree: LogSegment<String>,
    max_tree_size: usize,
    log_segments: Vec<DiskSegment>,
}

impl LsmTree {
    /*
    New: Either creates a new DB, including creating a new write ahead log, or,
    if the DB already exists and we are not purging this as part of creation, conusmes
    and restores from the pre-existing WAL.
    */
    pub fn new(name: &str) -> LsmTree {

        if lsm_exists(name) {
            let existing_log = get_wal(name, false);
            let mut tree = LsmTree{
                name: name.to_string(),
                log_file: existing_log,
                tree: LogSegment::new(),
                max_tree_size: MAX_TREE_SIZE,
                log_segments: reclaim_segments(name)};
            let restore_result = tree.restore();
            assert!(restore_result, "Failed to restore WAL!");
            return tree;
        }

        // Note: we only create LSM directory when the LSM does not exist already, replacing 
        // this elsewhere in this ctor e.g. prior to above case for existing LSM will cause panic
        if let Err(e) = create_lsm_dir(name) {
            panic!("{}", e);
        }

        LsmTree{
            name: name.to_string(),
            log_file: get_wal(name, true),
            tree: LogSegment::new(),
            max_tree_size: MAX_TREE_SIZE,
            log_segments: reclaim_segments(name)}
    }

    /*
    New Delete Existing: Creates DB from scratch, deleting any existing DB with this name
    */
    pub fn new_delete_existing(name: &str) -> LsmTree {
        // Note: we only purge LSM directory because we are creating an LSM and deleting the
        // existing LSM of this name, should not purge elsewhere
        purge_lsm_dir(name);

        // Once we have purged the existing LSM directory, this ctor operates
        // the same as the default ctor
        LsmTree::new(name)
    }

    /*
    Log: On each DB operation, we write ahead to log to ensure durability of all operations. This is a persisted
    log that will reflect any actions prior to mutating the in memory log segment(s)
    */
    fn log(&mut self, entry: &KVPair) -> bool {
        match self.log_file.write(format!("{} {}\n", entry.key, entry.value).as_bytes()) {
            Err(e) => {println!("failed to log {} to wal for db {} with error {}", entry, self.name, e); false}
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
    Flushes Tree: Flushes current tree to new disk segment. Note that flushing the in-memory
    tree is lazy, so we can read this tree until another write occurs
    */
    fn flush_tree(&mut self) {
        let total_segments = self.total_segments();
        let mut segment_buf = get_segment(&self.name, total_segments, true);
        self.tree.write_to_disk(&mut segment_buf);
        let new_seg = ClosedSegment{path_s: get_seg_path_s(&self.name, total_segments), file: segment_buf};
        match self.log_segments.binary_search(&new_seg) {
            // Place log segments in order by name
            Err(pos) => self.log_segments.insert(pos, new_seg),
            _ => {},
        }
        self.tree = LogSegment::new();
    }

    pub fn num_entries(&self) -> usize {
        self.tree.size()
    }

    pub fn max_entries(&self) -> usize {
        self.max_tree_size
    }

    /*
    Get: Queries LSM for value for the given key, will traverse log segments in newest
    to oldest fashion to preverse append-only deletion semantics
    */
    pub fn get(&mut self, key: &str) -> Option<&String> {
        let result = self.tree.get(key.to_string());

        // If the key is not already in memory, traverse prior log
        // segments in newest-to-oldest order until we get a result
        if let None = result {
            for segment in &mut self.log_segments {
                println!("Checking for {} in segment {}", key, extract_seg_id(segment.value().to_string()));
                let tree = get_tree_from_segment(segment);

                if let Some(result) = tree.get(key.to_string()) {
                    return Some(result);
                }
            }
        }
        result
    }

    /*
    Write: Appends a new entry to the latest log segment, after first preserving the
    operation to the WAL
    */
    pub fn write(&mut self, key: &str, value: &str) -> bool {
        if self.num_entries() >= self.max_tree_size {
                self.flush_tree();
        }

        let kvp = KVPair::new(key.into(), value.into());
        match self.log(&kvp) {
            true => {
                println!("Adding in {} {}", key, value);
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

fn get_tree_from_segment(segment: &mut DiskSegment) -> &LogSegment<String> {
    match segment {
        OpenSegment{path_s: _, file: _, tree} => {tree},
        ClosedSegment{path_s, file} => {
            let tree = read_segment(Path::new(path_s));
            *segment = OpenSegment{path_s: path_s.to_string(), file: file.try_clone().unwrap(), tree: tree};
            if let OpenSegment { path_s: _, file: _, tree } = segment {
                tree
            }
            else {
                panic!("Failed to open disk segment");
            }
        }
    }
}

fn read_segment<P: AsRef<Path>>(path: P) -> LogSegment<String> {
    let mut root = LogSegment::new();
    if let Ok(lines) = read_lines(path) {
        for line in lines {
            if let Ok(line) = line {
                let mut tuple = line.split(" ");
                let key = tuple.next().unwrap();
                let value = tuple.next().unwrap();
                root.insert((key.to_string(), value.to_string()));
            }
        }
    }
    root
}

// Generic read lines helper, returns a Result which on success is an iterator over
// the lines of a BufReader, where the BufReader consumes the result of opening file
pub fn read_lines<P: AsRef<Path>>(path: P) -> Result<Lines<BufReader<File>>>
{
    let input = File::open(path).expect("Unable to open up input file");
    Ok(BufReader::new(input).lines())
}