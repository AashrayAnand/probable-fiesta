use std::fmt::{Debug, Display};
use std::cmp::*;
use crate::storage::tree::LogSegment::*;

pub enum LogSegment<T: Ord + Copy + Debug + Display> {
    TreeNode(T, T, Box<LogSegment<T>>, Box<LogSegment<T>>),
    Nil
}

impl<T: Ord + Copy + Debug + Display> LogSegment<T> {
    pub fn new() -> LogSegment<T> {
        Nil
    }

    pub fn insert(self, pair: (T, T)) -> LogSegment<T> {
        match self {
            Nil => {
                TreeNode(pair.0, pair.1, Box::new(Nil), Box::new(Nil))
            },
            TreeNode(key, value, left, right) => {
                if key > pair.0  {
                    TreeNode(key, value, Box::new(left.insert(pair)), right)
                }
                else if key < pair.0 {
                    TreeNode(key, value, left, Box::new(right.insert(pair)))
                }
                else {
                    TreeNode(key, pair.1, left, right)
                }
            }
        }
    }

    pub fn delete(self, del_key: T) -> LogSegment<T> {
        match self {
            Nil => {
                self
            },
            TreeNode(key, value, left, right) => {
                if key == del_key  {
                    if let Some(right_min) = right.min() {
                        // Intermediate node, need to swap with min of right sub
                        // tree to maintain bst ordering property
                        TreeNode(right_min.0, right_min.1, left, Box::new(right.delete(right_min.0)))
                    }
                    else if let Some(left_max) = left.max() {
                        // Intermediate node, need to swap with min of right sub
                        // tree to maintain bst ordering property
                        TreeNode(left_max.0, left_max.1, Box::new(left.delete(left_max.0)), right)
                    }
                    else {
                        Nil
                    }
                }
                else if key > del_key {
                    TreeNode(key, value, Box::new(left.delete(del_key)), right)
                }
                else {
                    TreeNode(key, value, left, Box::new(right.delete(del_key)))
                }
            }
        }
    }

    pub fn get(&self, get_key: T) -> Option<T> {
        match self {
            Nil => {None},
            TreeNode(key, value, left, right) => {
                if *key == get_key {
                    Some(*value)
                }
                else if *key > get_key {
                    left.get(get_key)
                }
                else {
                    right.get(get_key)
                }
            }
        }
    }

    pub fn exists(&self, ex_key: T) -> bool {
        match self.get(ex_key) {
            Some(_) => true,
            _ => false
        }
    }

    pub fn size(&self) -> usize {
        match self {
            Nil => {
                0
            }
            TreeNode(_, _, left, right) => {
                1 + left.size() + right.size()
            }
        }
    }

    fn min(&self) -> Option<(T, T)> {
        match self {
            Nil => {
               None 
            }
            TreeNode(key, value, left, _) => {
                match **left {
                    Nil => {Some((*key, *value))}
                    TreeNode(_, _, _, _) => {
                        left.min()
                    }
                } 
            }
        }
    }

    fn max(&self) -> Option<(T, T)> {
        match self {
            Nil => {
               None 
            }
            TreeNode(key, value, _, right) => {
                match **right {
                    Nil => {Some((*key, *value))},
                    TreeNode(_, _, _, _) => {
                        right.max()
                    }
                }
            }
        }
    }
}