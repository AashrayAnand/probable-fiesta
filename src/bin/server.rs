use probable_fiesta::storage::{lsm::LsmTree};
use probable_fiesta::{LOCALHOST, SRV_PORT, log};
use probable_fiesta::replication::network::Message;
use tokio::{net::{TcpListener}, io::AsyncReadExt};
use std::mem::size_of;

#[tokio::main]
async fn main() {
    match std::env::args().nth(1) {
        Some(pattern) => {
            let listener = TcpListener::bind(format!("{}:{}", LOCALHOST, SRV_PORT)).await.unwrap();

            loop {
                let msg_size = size_of::<Message>();
                let (mut socket, addr) = listener.accept().await.unwrap();
                // A new task is spawned for each inbound socket. The socket is
                // moved to the new task and processed there.
                tokio::spawn(async move {
                    let mut bytes = vec![0_u8; msg_size];
                    log(&format!("New incoming connection from {}", addr));
                    socket.read_exact(&mut bytes).await;
                });
            }
        }
        None => log(&format!("Did not get DB name, please provide as parameter"))
    };
}