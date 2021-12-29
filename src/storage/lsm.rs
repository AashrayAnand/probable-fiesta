use std::{io::{BufReader, Write, BufRead}, path::Path, fs::{remove_file, OpenOptions, File}, borrow::Cow};
use crate::storage::tree::*;
use crate::kvpair::*;

pub struct LsmTree<'a> {
    name: &'a str,
    log_file: File,
    tree: LogSegment<String>
}

impl LsmTree {
    /*
    New: Either creates a new DB, including creating a new write ahead log, or,
    if the DB already exists and we are not purging this as part of creation, conusmes
    and restores from the pre-existing WAL.
    */
    pub fn new(name: &'static str) -> LsmTree {
        // Check for existing log for this DB, then we are not creating new DB and should
        // restore log to memory
        let path_str = format!("{}.log", name);
        let log_file = Path::new(&path_str);
        if Path::exists(log_file) {
            let existing_log = OpenOptions::new().read(true).write(true).append(true).open(log_file).unwrap();
            let tree = LsmTree{name, log_file: existing_log, tree: LogSegment::new()};
            let (tree, restore_result) = tree.restore();
            assert!(restore_result, "Failed to restore WAL!");
            return tree;
        }
        LsmTree{name, log_file: OpenOptions::new().create(true).write(true).append(true).open(log_file).unwrap(), tree: LogSegment::new()}
    }

    /*
    New Delete Existing: Creates DB from scratch, deleting any existing DB with this name
    */
    pub fn new_delete_existing(name: &'static str) -> LsmTree {
        // Check for existing log for this DB, then we are not creating new DB and should
        // restore log to memory
        let path_str = format!("{}.log", name);
        let log_file = Path::new(&path_str);
        if Path::exists(log_file) {
            delete_wal(log_file);
        }
        LsmTree{name, log_file: OpenOptions::new().create(true).write(true).append(true).open(log_file).unwrap(), tree: LogSegment::new()}
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
    fn restore(mut self) -> (LsmTree, bool) {
        // Restore moves ex
        let new_handle = self.log_file.try_clone().unwrap();
        let wal_contents = BufReader::new(self.log_file).lines();
        for line in wal_contents {
            if let Ok(line) = line {
                // WAL format is an append-only log of entries like below
                // key: foo, value: bar, we can re-create segment from log
                // by iterating these entries and applying as a sequence of writes
                let mut line = line.split(' ');
                let tuple = (line.next().unwrap().into(), line.next().unwrap().into());
                self.tree = self.tree.insert(tuple);
            }
        }
        self.log_file = new_handle;
        (self, true)
    }

    /*
    Write: Appends a new entry to the latest log segment, after first preserving the
    operation to the WAL
    */
    pub fn write(mut self, key: &'static str, value: &'static str) -> (LsmTree, bool) {
        let kvp = KVPair::new(key.into(), value.into());
        match self.log(&kvp) {
            true => {
                self.tree.insert((key.to_string(), value.to_string()));
                (self, true)
            },
            false => {(self, false)}
        }
    }

    pub fn get(&self, key: &'a str) -> Option<&String> {
        return self.tree.get(key.to_string());
    }
}