use std::{cmp::Ordering, fs::File};

use self::DiskSegment::*;

use super::tree::LogSegment;

pub enum DiskSegment {
    OpenSegment{path_s: String, file: File, tree: LogSegment<String>},
    ClosedSegment{path_s: String, file: File},
}

impl DiskSegment {
    pub fn value(&self) -> &str {
        match self {
            OpenSegment { path_s, file: _, tree: _ } => {
                &path_s
            }
            ClosedSegment{path_s, file: _} => {
                &path_s
            }
        }
    }
}

pub fn extract_seg_id(path: String) -> i32 {
    path.split('/').last().unwrap().split('.').next().unwrap().split('_').nth(1).unwrap().parse::<i32>().unwrap()
}


impl Ord for DiskSegment
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let own_path_id = match self {
            OpenSegment{path_s, file: _, tree: _} => {path_s},
            ClosedSegment{path_s, file: _} => {path_s}
        };

        let own_path = own_path_id.clone();
        let own_path_id = extract_seg_id(own_path);

        let other_path = match other {
            OpenSegment{path_s, file: _, tree: _} => {path_s},
            ClosedSegment{path_s, file: _} => {path_s}
        };

        let other_path = other_path.clone();
        let other_path_id = extract_seg_id(other_path);

        other_path_id.cmp(&own_path_id)
    }
}

impl Eq for DiskSegment {
}

impl PartialEq for DiskSegment {
    fn eq(&self, other: &Self) -> bool {
        let own_path_id = match self {
            OpenSegment{path_s, file: _, tree: _} => {path_s},
            ClosedSegment{path_s, file: _} => {path_s}
        };

        let own_path = own_path_id.clone();
        let own_path_id = extract_seg_id(own_path);

        let other_path = match other {
            OpenSegment{path_s, file: _, tree: _} => {path_s},
            ClosedSegment{path_s, file: _} => {path_s}
        };

        let other_path = other_path.clone();
        let other_path_id = extract_seg_id(other_path);

        own_path_id == other_path_id
    }
}

impl PartialOrd for DiskSegment {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let own_path_id = match self {
            OpenSegment{path_s, file: _, tree: _} => {path_s},
            ClosedSegment{path_s, file: _} => {path_s}
        };

        let own_path = own_path_id.clone();
        let own_path_id = extract_seg_id(own_path);

        let other_path = match other {
            OpenSegment{path_s, file: _, tree: _} => {path_s},
            ClosedSegment{path_s, file: _} => {path_s}
        };

        let other_path = other_path.clone();
        let other_path_id = extract_seg_id(other_path);

        other_path_id.partial_cmp(&own_path_id)
    }
}