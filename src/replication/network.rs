use std::{net::{SocketAddr, TcpStream, AddrParseError}, io::Write};
use serde::{Serialize, Deserialize, Deserializer};
use crate::log;

#[derive(Debug, Serialize, Deserialize)]
pub enum MsgType {
    Election,
    Log,
    Ack,
    Commit,
    Abort,
}

// Sum type structure for generically representing different
// message params. Some parameters are option-able and selectively
// used depending on the message type
#[derive(Debug, Serialize, Deserialize)]
pub struct Message {
    source: SocketAddr,
    msg_t: MsgType,
    pid: Option<u32>,
    log: Option<String>
}

// Represents a replica in the cluster, each replica maintains 
// a sequence of replicas which are all the other members of the 
// cluster, includes the requisite state for:
// 1. leader election
// 2. socket dst. address 
// 3. last sent and received messages to/from replica
pub struct Replica {
    pid: u32,
    sock_addr: SocketAddr,
    last_sent: Option<MsgType>,
    last_rcv: Option<MsgType>
}

impl Replica {
    pub fn new(pid: u32, ip_addr: &str, port: &str) -> Option<Replica> {
        match parse_ip(ip_addr, port) {
            Ok(sock_addr) => {
                Some(Replica{pid, sock_addr, last_sent: None, last_rcv: None})
            },
            Err(e) => {
                log(&format!("Failed to open socket with error {}", e));
                None
            }
        }
    }

    pub fn send(&mut self, msg_p: Message) {
        let buf = serde_json::to_string(&msg_p).unwrap();
        match open_tcp_stream(&self.sock_addr) {
            Ok(mut stream) => {
                match stream.write(&buf.as_bytes()) {
                    Ok(num_written) => {
                        if num_written != buf.len() {
                            log(&format!("Failed to write entire message, only wrote {} bytes out of {}", num_written, buf.len()));
                        }
                    }
                    Err(e) => log(&format!("Failed to write msg {:?} to {} with error {}", msg_p, self.sock_addr, e))
                }
            },
            Err(e) => log(&format!("Failed to open tcp stream with error {}", e))
        }
    }
}

fn parse_ip(ip_addr: &str, port: &str) -> Result<SocketAddr, AddrParseError> {
    format!("{}:{}", ip_addr, port).parse::<SocketAddr>()
}

fn open_tcp_stream(addr: &SocketAddr) -> Result<TcpStream, std::io::Error> {
    TcpStream::connect(addr)
}

pub struct ReplicaContext {
    pub replicas: Vec<Replica>
}

impl ReplicaContext {
    pub fn new() -> ReplicaContext {
        ReplicaContext{replicas: Vec::new()}
    }
}