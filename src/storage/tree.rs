use std::fmt::{Debug, Display};
use std::{cmp::*};
use std::fs::File;
use std::io::{Write};
use crate::log;
use crate::storage::tree::LogSegment::*;

#[derive(Debug)]
pub enum TriOption<T> {
    TriSome(T),
    TriNone,
    Tombstoned
}

use self::TriOption::*;

pub enum LogSegment<T: Ord + Clone + Debug + Display> {
    TreeNode{k: T, v: TriOption<T>, left: Option<Box<LogSegment<T>>>, right: Option<Box<LogSegment<T>>>},
    Nil
}

impl<T: Ord + Clone + Debug + Display> LogSegment<T> {
    pub fn new() -> LogSegment<T> {
        Nil
    }

    pub fn insert(&mut self, pair: (T, T)) {
        match self {
            Nil => {
                *self = TreeNode { k: pair.0, v: TriSome(pair.1), left: None, right: None };
            },
            TreeNode{k, v, left, right} => {
                match pair.0.cmp(k) {
                    Ordering::Equal => *v = TriSome(pair.1),
                    Ordering::Greater => {
                        if let Some(right) = right {
                            right.insert(pair);
                        }
                        else {
                            *right = Some(Box::new(TreeNode { k: pair.0, v: TriSome(pair.1), left: None, right: None }));
                        }
                    },
                    Ordering::Less => {
                        if let Some(left) = left {
                            left.insert(pair);
                        }
                        else {
                            *left = Some(Box::new(TreeNode { k: pair.0, v: TriSome(pair.1), left: None, right: None }));
                        }
                    },
                }
            }
        }
    }

    pub fn delete(&mut self, del_key: T) {
        match self {
            Nil => {
                *self = TreeNode{k: del_key, v: Tombstoned, left: None, right: None};
            },
            TreeNode { k, v, left, right } => {
                match del_key.cmp(k) {
                    Ordering::Equal => *v = Tombstoned,
                    Ordering::Greater => {
                        if let Some(right) = right {
                            right.delete(del_key);
                        }
                        else {
                            *right = Some(Box::new(TreeNode { k: del_key, v: Tombstoned, left: None, right: None }));
                        }
                    },
                    Ordering::Less => {
                        if let Some(left) = left {
                            left.delete(del_key);
                        }
                        else {
                            *left = Some(Box::new(TreeNode { k: del_key, v: Tombstoned, left: None, right: None }));
                        }
                    },
                }
            }
        }
    }

    pub fn get(&self, get_key: T) -> TriOption<&T> {
        match self {
            Nil => TriNone,
            TreeNode { k, v, left, right } => {
                match get_key.cmp(k) {
                    Ordering::Equal => {
                        log(&format!("{} is {:?}", get_key, v));
                        match v {
                            TriNone => return TriNone,
                            TriSome(v) => return TriSome(v),
                            Tombstoned => return Tombstoned
                        };
                    }
                    Ordering::Greater => {
                        if let Some(right) = right {
                            return right.get(get_key);
                        }
                        else {
                            return TriNone;
                        }
                    },
                    Ordering::Less => {
                        if let Some(left) = left {
                            return left.get(get_key);
                        }
                        else {
                            return TriNone;
                        }
                    },
                }
            }
        }
    }

    // in-order traversal and write to disk of the tree.
    pub fn write_to_disk(&mut self, file: &mut File) {
        match self {
            Nil => {},
            TreeNode { k, v, left, right } => {
                if let Some(left) = left {
                    left.write_to_disk(file);
                }

                if let TriSome(v) = v {
                    if let Err(e) = file.write(format!("{} {}\n", k, v).as_bytes()) {
                        panic!("{}", e);
                    }
                }
                else {
                    // Tombstoned entries will be written to disk as keys only
                    if let Err(e) = file.write(format!("{}\n", k).as_bytes()) {
                        panic!("{}", e);
                    }
                }
                if let Some(right) = right {
                    right.write_to_disk(file);
                }
            }
        }
    }

    pub fn exists(&self, ex_key: T) -> bool {
        match self.get(ex_key) {
            TriSome(_) => true,
            _ => false
        }
    }

    pub fn size(&self) -> usize {
        match self {
            Nil => {
                0
            }
            TreeNode { k: _, v: _, left, right } => {
                let mut sum = 1;
                if let Some (left) = left {
                    sum += left.size();
                }
                if let Some(right) = right {
                    sum += right.size();
                }
                sum
            }
        }
    }
}