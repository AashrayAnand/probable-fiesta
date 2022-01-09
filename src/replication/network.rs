use std::{net::{SocketAddr, TcpStream, AddrParseError, TcpListener}, io::Write};
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
pub struct MsgParameters {
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
// 3. tcp stream
// 4. state (based on most recent message to this replica)
pub struct Replica {
    pid: u32,
    sock_addr: SocketAddr,
    stream: TcpStream,
    listener: TcpListener,
    last_sent: Option<MsgType>,
    last_rcv: Option<MsgType>
}

impl Replica {
    pub fn new(pid: u32, ip_addr: &str, port: &str) -> Option<Replica> {
        match parse_ip(ip_addr, port) {
            Ok(sock_addr) => {
                match open_tcp_stream(&sock_addr) {
                    Ok(stream) => {
                        match open_tcp_listener(&sock_addr) {
                            Ok(listener) => Some(Replica{pid, sock_addr, stream, listener, last_sent: None, last_rcv: None}),
                            Err(e) => {
                                log(&format!("Failed to open tcp stream with error {}", e));
                                None
                            }
                        }
                    },
                    Err(e) => {
                        log(&format!("Failed to open tcp stream with error {}", e));
                        None
                    }
                }
            },
            Err(e) => {
                log(&format!("Failed to open socket with error {}", e));
                None
            }
        }
    }

    pub fn send_msg(&mut self, msg_p: MsgParameters) {
        let buf = serde_json::to_string(&msg_p).unwrap();
        match self.stream.write(&buf.as_bytes()) {
            Ok(num_written) => {
                if num_written != buf.len() {
                    log(&format!("Failed to write entire message, only wrote {} bytes out of {}", num_written, buf.len()));
                }
            }
            Err(e) => log(&format!("Failed to write msg {:?} to {} with error {}", msg_p, self.sock_addr, e))
        }
    }
}

fn parse_ip(ip_addr: &str, port: &str) -> Result<SocketAddr, AddrParseError> {
    format!("{}:{}", ip_addr, port).parse::<SocketAddr>()
}

fn open_tcp_stream(addr: &SocketAddr) -> Result<TcpStream, std::io::Error> {
    TcpStream::connect(addr)
}

fn open_tcp_listener(addr: &SocketAddr) -> Result<TcpListener, std::io::Error> {
    TcpListener::bind(addr)
}

pub struct ReplicaContext {
    replicas: Vec<Replica>
}

impl ReplicaContext {
    pub fn new() -> ReplicaContext {
        ReplicaContext{replicas: Vec::new()}
    }
}