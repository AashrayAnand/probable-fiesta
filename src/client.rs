// Client library, implements all functionality for taking command line user
// input and processing/requesting from server. Mostly used by client.rs executable
// in bin, which alone is just an input reader, leveraging this library
use crate::{skip_ws, Operation::*, byte_op, op_byte};
use tokio::{net::TcpStream, io::AsyncWriteExt};
use std::{str, io::stdin};

#[derive(Debug)]
pub enum ClientError {
    ParseError,
    SerializeOpError,
    DeserializeResError,
}

impl std::error::Error for ClientError {}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
    match self {
        ClientError::ParseError => write!(f, "Error parsing query"),
        ClientError::SerializeOpError => write!(f, "Error serializing operation"),
        ClientError::DeserializeResError => write!(f, "Error de-serializing response"),
    }
    }
}

pub async fn start_conn(mut conn: TcpStream) {
    // take byte strings from user and execute frame protocol, as well as receiving byte strings
    // from servers to parse and present to user
    loop {
        print!("PF$ ");
        // get input from user, can be in the form of

        // NEW db
        // USE db
        // GET key
        // SET key value
        // DELETE key
        // PUB key value
        // SUB key
        // EXIT

        // client -> server wire protocol is a minimal byte stream, where we compress operators to a byte
        // we can then frame the parameters of the operator as pascal strings, with a length value preceding each parameter
        // e.g. SET FOO BAR would be encoded as '133FOOBAR' -> 1 (SET) 3 (len FOO) 3 (len BAR) FOO BAR
        let mut input = String::new();
        match stdin().read_line(&mut input) {
            Ok(num_bytes) => {
                // construct frame from query text in client lib and send frame for processing to server
                if let Ok(frame) = construct_frame(input) {
                    println!("Frame is {:?}, from size {} input", str::from_utf8(&frame).unwrap(), num_bytes);
                    conn.write(&frame).await;
                }
            },
            Err(e) => eprint!("==> Failed to read client input, with error: {}", e)
        }
        print!("\n");
    }
}

// Parse CLI to operation and args to pass to server
pub fn construct_frame(input: String) -> Result<Vec<u8>, ClientError> {
    // construct frame from input and send to server, block on result depending on operation
    // we should block only on GET, SET is async, as well as PUB/SUB, while EXIT kills client proc.
    let in_slice = input.as_bytes();
    let mut frame: Vec<u8> = Vec::new();
    if let Ok(arg_start) = add_op_to_frame(in_slice, &mut frame)  {
        if let Ok(()) = add_args_to_frame(&in_slice[arg_start..], &mut frame) {
            return Ok(frame);
        }
    }
    Err(ClientError::ParseError)
}

// Parse operation query text and add corresponding op byte to frame
fn add_op_to_frame(input: &[u8], frame: &mut Vec<u8>) -> Result<usize, ClientError> {
    let mut i = 0;
    while input[i] != b' ' {
        i += 1;
    }

    if i < input.len() {
        if let Ok(op_byte) = op_byte(&input[0..i]) {
            frame.push(op_byte);
            return Ok(i);
        }
    }

    Err(ClientError::ParseError)
}

// Parse arguments from query text and add to frame
fn add_args_to_frame(input_args: &[u8], frame: &mut Vec<u8>) -> Result<(), ClientError> {
    // Assume at this point we parsed the operation type to the head of the frame, as we
    // rely on this to enumerate the operation and parse accordingly
    if let Ok(operation) = byte_op(frame[0]) {
        match operation {
            NEW | USE | GET | DELETE | SUB => {
                // expected input args for NEW/USE: {NAME}
                // expected input args for GET/DEL: {KEY}
                let mut head = 0;
                skip_ws(&mut head, input_args.len(), input_args);

                let mut tail = input_args.len() - 1;
                skip_ws(&mut tail, head, input_args);

                // Fail only on NEW being called without corresponding DB name
                // where we purge trailing/leading white spaces from the name
                if head <= tail {
                    for i in head..tail+1 {
                        frame.push(input_args[i]);
                    }
                    // Delimit frame with CRLF, segments each frame when
                    // we send in batch or through a queue
                    frame.push(b'\r');
                    frame.push(b'\n');
                    return Ok(());
                }
            },
            SET | PUB => {
                // expected input args for SET: {KEY} {VALUE}
                // keys cannot have whitespaces, but values can
                let mut head = 0;
                skip_ws(&mut head, input_args.len(), input_args);

                while input_args[head] != b' ' {
                    frame.push(input_args[head]);
                }

                // second argument parsing for SET is equivalent to first arg for NEW/USE/GET/DELETE,
                // skip the head and tail whitespace and take the remaining
                skip_ws(&mut head, input_args.len(), input_args);

                // delimit key and value with whitespace
                frame.push(b' ');

                let mut tail = input_args.len() - 1;
                skip_ws(&mut tail, head, input_args);

                // Fail only on NEW being called without corresponding DB name
                // where we purge trailing/leading white spaces from the name
                if head <= tail {
                    for i in head..tail+1 {
                        frame.push(input_args[i]);
                    }
                    // Delimit frame with CRLF, segments each frame when
                    // we send in batch or through a queue
                    frame.push(b'\r');
                    frame.push(b'\n');
                    return Ok(());
                }
            }
        }
    }

    Err(ClientError::ParseError)
}