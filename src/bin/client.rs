use probable_fiesta::client::*;
use tokio::{net::TcpStream, io::AsyncWriteExt};
use std::io::stdin;
use probable_fiesta::{LOCALHOST, SRV_PORT};

#[tokio::main]
async fn main() {
    let mut client = TcpStream::connect(format!("{}:{}", LOCALHOST, SRV_PORT)).await.unwrap();

    start_conn(client).await;
}