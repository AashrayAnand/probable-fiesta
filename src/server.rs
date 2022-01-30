use std::{collections::HashMap, sync::{Arc, Mutex}};
use crate::{Operation::*, Response::*, skip_ws, byte_op, LOCALHOST, SRV_PORT, log, storage::lsm::LsmTree};
use tokio::{net::{TcpListener}, io::AsyncReadExt};
use std::net::SocketAddr;
use std::str;
use chrono::{DateTime, Utc};
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum MsgType {
    Election,
    Log,
    Ack,
    Commit,
    Abort,
}

#[derive(Debug)]
pub enum ServerError {
    ParseError,
    DeserializeOpError,
    SerializeResError,
}

impl std::error::Error for ServerError {}

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
    match self {
        ServerError::ParseError => write!(f, "Error parsing query"),
        ServerError::DeserializeOpError => write!(f, "Error de-serializing operation"),
        ServerError::SerializeResError => write!(f, "Error serializing response"),
        }
    }
}

pub struct DB {
    data: Arc<Mutex<LsmTree>>
}

// server maintains sessions which map unique client connections to
// the DB they are using, and also maps DB names to the corresponding LSMs
pub struct Server {
    is_primary: bool, 
    listener: TcpListener,
    pub dbs: HashMap<String, DB>,
    pub sessions: HashMap<SocketAddr, String>,
    start_time: DateTime<Utc>,
}

impl Server {
    pub fn new(listener: TcpListener) -> Server {
        Server{
            is_primary: true,
            listener: listener,
            dbs: HashMap::new(), 
            sessions: HashMap::new(), 
            start_time: Utc::now()}
    }

    pub async fn start(self) {
        loop {
            let (mut socket, addr) = self.listener.accept().await.unwrap();
            // A new task is spawned for each inbound socket. The socket is
            // moved to the new task and processed there.
            tokio::spawn(async move {
                let mut bytes: Vec<u8> = Vec::new();
                log(&format!("New incoming connection from {}", addr));
                if let Ok(_) = socket.read_exact(&mut bytes).await {

                    // de-construct frame to determine handle request
                    if let Ok(()) = deconstruct_frame(&bytes) {
                        log(&format!("Received operation {} from {}", str::from_utf8(&bytes).unwrap(), addr));

                        // construct response in server lib to send back to client
                    }
                }


            });
        }
    }

    pub fn start_session(&mut self, addr: SocketAddr, name: String) {
        self.sessions.insert(addr, String::from(&name));
        if !self.dbs.contains_key(&name) {
            let tree = LsmTree::new(&name);
            let db = DB {data: Arc::new(Mutex::new(tree))};
            self.dbs.insert(String::from(&name), db);
        }
    }

    pub fn drop(&mut self, addr: SocketAddr, name: String) {
        self.sessions.insert(addr, String::from(&name));
        if self.dbs.contains_key(&name) {
            self.dbs.remove(&name);
        }
    }
}


pub fn deconstruct_frame(frame: &[u8]) -> Result<(), ServerError> {
    if !frame.is_empty() {
        if let Ok(operation) = byte_op(frame[0]) {
            let mut head = 1;
            match operation {
                NEW | USE | GET | DELETE | SUB => {
                    // expected input args for NEW/USE: {NAME}
                    // expected input args for GET/DEL: {KEY}
                    let mut key: Vec<u8> = Vec::new();
                    let mut tail = frame.len() - 1;
                    skip_ws(&mut tail, frame.len(), frame);

                    // Fail only on NEW being called without corresponding DB name
                    // where we purge trailing/leading white spaces from the name
                    if head <= tail {
                        for i in head..tail+1 {
                            key.push(frame[i]);
                        }
                    }
                    match operation {
                        GET => {

                        }
                        _ => {}
                    }
                },
                SET | PUB => {
                    let mut key: Vec<u8> = Vec::new();
                    let mut value: Vec<u8> = Vec::new();
                    // expected input args for SET: {KEY} {VALUE}
                    // keys cannot have whitespaces, but values can
                    let mut head = 0;

                    // Expect no extra whitespace on input but skip anyways
                    skip_ws(&mut head, frame.len(), frame);

                    // Expect no white space in key, but skip anyways
                    while frame[head] != b' ' {
                        key.push(frame[head]);
                    }

                    // Skip delimiter
                    skip_ws(&mut head, frame.len(), frame);

                    let mut tail = frame.len() - 1;

                    // Fail only on NEW being called without corresponding DB name
                    // where we purge trailing/leading white spaces from the name
                    if head <= tail {
                        for i in head..tail+1 {
                            value.push(frame[i]);
                        }
                        return Ok(());
                    }
                }
            }
        }
    }
    Err(ServerError::DeserializeOpError)
}