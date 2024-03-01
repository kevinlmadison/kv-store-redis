use std::error::Error;
use std::io::{Write, Read};
use std::net::{TcpListener, TcpStream};

fn stream_handler(mut stream: TcpStream)  -> Result<(), Box<dyn Error>> {

    let mut buffer: [u8; 256] = [0; 256];

    while let Ok(buf_len) = stream.read(&mut buffer) {

        if &buffer[..buf_len] == b"*1\r\n$4\r\nping\r\n" {

            stream.write_all(b"+PONG\r\n")?

        }
        else {

            stream.write_all(&buffer[..buf_len])?

        }
    }

    Ok(())
}

fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                if let Ok(_res) = stream_handler(stream) {
                    println!("Handled the response");
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
