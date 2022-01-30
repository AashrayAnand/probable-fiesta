use probable_fiesta::{SRV_PORT, LOCALHOST, server::*};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    let listener =  TcpListener::bind(format!("{}:{}", LOCALHOST, SRV_PORT)).await.unwrap();
    
    Server::new(listener).start().await;
}