use probable_fiesta::{LOCALHOST, SRV_PORT};
use tokio::net::TcpStream;
use std::fmt::Result;
use std::io::stdin;

enum Operation {
    GET,
    SET,
    DELETE,
    PUB,
    SUB,
}

#[tokio::main]
async fn main() {
    let mut client = TcpStream::connect(format!("{}:{}", LOCALHOST, SRV_PORT)).await.unwrap();

    // take byte strings from user and execute frame protocol, as well as receiving byte strings
    // from servers to parse and present to user
    loop {
        print!("PF$ ");
        // get input from user, can be in the form of

        // GET key
        // SET key value
        // DELETE key
        // PUB key value
        // SUB key
        // EXIT

        // client -> server wire protocol can be minimized to a simple byte stream, where we compress each operator
        // to a single byte value (enumerating operators).
        // we can then frame the parameters of the operator as pascal strings, with a length value preceding each parameter
        // e.g. SET FOO BAR would be encoded as '133FOOBAR' -> 1 (SET) 3 (len FOO) 3 (len BAR) FOO BAR
        let mut input = String::new();
        match stdin().read_line(&mut input) {
            Ok(num_bytes) => {
                construct_frame(input);
            },
            Err(e) => eprint!("==> Failed to read client input, with error {}", e)
        }
        print!("\n");
    }
}

pub fn construct_frame(input: String) -> Result<Option<Vec<u8>>> {
    // construct frame from input and send to server, block on result depending on operation
    // we should block only on GET, SET is async, as well as PUB/SUB, while EXIT kills client proc.
    let in_slice = input.as_bytes();
    if in_slice.length() < 3 {
        let out_bytes: Vec<u8> = Vec::new();
        match (in_slice[0], in_slice[1], in_slice[2]) {
            (b'g' | b'G', b'e' | b'E', b't' | b'T') => {
                out_bytes.push(Operation::GET)
            },
            (b's' | b'S', b'e' | b'E', b't' | b'T') => {
                out_bytes.push(Operation::SET)
            },
            (b'd' | b'D', b'e' | b'E', b'l' | b'L') => {
                out_bytes.push(Operation::DELETE)
            },
            (b'p' | b'P', b'u' | b'U', b'b' | b'B') => {
                out_bytes.push(Operation::PUB)
            },
            (b's' | b'S', b'u' | b'U', b'b' | b'B') => {
                out_bytes.push(Operation::SUB)
            },
            (b'e' | b'E', b's' | b'S', b'c' | b'C') => Ok(None),
            _ => {}
        }
    }
    Err("Invalid query")
}