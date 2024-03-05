use tokio::{
    io::{AsyncWriteExt, AsyncReadExt},
    net::{TcpListener, TcpStream},
};

async fn stream_handler(mut stream: TcpStream) {

    let mut buffer: [u8; 256] = [0; 256];

    while let Ok(buf_len) = stream.read(&mut buffer).await {
        if &buffer[..buf_len] == b"*1\r\n$4\r\nping\r\n" {
            stream.write_all(b"+PONG\r\n").await.unwrap()
        }
        else {
            stream.write_all(&buffer[..buf_len]).await.unwrap()
        }
    }
    ()
}

#[tokio::main]
async fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        match listener.accept().await {
            Ok((mut stream, _)) => {
                 tokio::spawn(async move { stream_handler(stream).await }); 
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
