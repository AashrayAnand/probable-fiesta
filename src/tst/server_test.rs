#[cfg(test)]
use crate::client::construct_frame;
#[cfg(test)]
use std::str;

#[test]
pub fn test_new_db() {
    let frame = construct_frame(String::from("NEW foo"));
    
    // expected frame is op byte for NEW + foo -> "0foo"
    let ex_frame = "0foo";
    if let Ok(frame) = frame {
        assert!(ex_frame.as_bytes() == frame,  "Expected frame {}, actually {}", ex_frame, str::from_utf8(&frame).unwrap());
    }
}

pub async fn start_server() {
    
}