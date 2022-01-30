pub mod storage {
    pub mod tree;
    pub mod lsm;
    pub mod diskseg;
    pub mod files;
}

pub mod client;
use client::ClientError;
pub mod server;
use server::ServerError;

use Operation::*;
use Response::*;

pub mod tst {
    // declaring module inline and placing all tests there as per this convention
    // https://stackoverflow.com/questions/58935890/how-to-import-from-a-file-in-a-subfolder-of-src
    pub mod bst_test;
    pub mod lsm_test;
    pub mod tst_util;
    pub mod client_test;
    pub mod server_test;
}

pub enum Operation {
    NEW,
    USE,
    GET,
    SET,
    DELETE,
    PUB,
    SUB,
}

// Serializes plaintext query operator to byte for passing in frame
fn op_byte(operation: &[u8]) -> Result<u8, ClientError> {
    match operation {
        b"NEW" => Ok(b'0'),
        b"USE" => Ok(b'1'),
        b"GET" => Ok(b'2'),
        b"SET" => Ok(b'3'),
        b"DEL" => Ok(b'4'),
        b"PUB" => Ok(b'5'),
        b"SUB" => Ok(b'6'),
        _ => Err(ClientError::SerializeOpError)
    }
}

// De-serializes query byte to operator enum
fn byte_op(op_byte: u8) -> Result<Operation, ServerError> {
    match op_byte {
        b'0' => Ok(NEW),
        b'1' => Ok(GET),
        b'2' => Ok(SET),
        b'3' => Ok(DELETE),
        b'4' => Ok(PUB),
        b'5' => Ok(SUB),
        _ => Err(ServerError::DeserializeOpError)
    }
}

pub enum Response {
    PASS,
    FAIL,
    ACK,
}

// Serializes plaintext query response to byte for passing in frame
fn res_byte(operation: &[u8]) -> Result<u8, ServerError> {
    match operation {
        b"PASS" => Ok(b'0'),
        b"FAIL" => Ok(b'1'),
        b"ACK" => Ok(b'2'),
        _ => Err(ServerError::SerializeResError)
    }
}

// De-serializes query byte to response enum
fn byte_res(op_byte: u8) -> Result<Response, ClientError> {
    match op_byte {
        b'0' => Ok(PASS),
        b'1' => Ok(FAIL),
        b'2' => Ok(ACK),
        _ => Err(ClientError::DeserializeResError)
    }
}

pub const LOGGING: bool = false;
pub const LOCALHOST: &str = "127.0.0.1";
pub const SRV_PORT: u16 = 3620;

pub fn log(msg: &str) {
    if LOGGING {
        println!("{}", msg);
    }
}

// trim whitespace either from head or tail, head when start < end, tail otherwise
fn skip_ws(start: &mut usize, end: usize, input: &[u8]) {
    if *start < end {
        while *start < end && input[*start] == b' ' {
            *start += 1;
        }
    } 
    else {
        while *start > end && input[*start] == b' ' {
            *start -= 1;
        }
    }
}