#![allow(unused_imports)]

use std::io::{Read, Write};
use std::net::TcpListener;

fn main() {
    println!("Broker has commenced.");
    // TODO: Refactor so its fixed size later
    // let mut request_buffer: Vec<u8> = Vec::new();
    let mut request_buffer = vec![0u8; 1024];

    let mut response_buffer: Vec<u8> = Vec::new();
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                let message_size: i32 = 4;
                let mut correlation_id: u32 = 7;
                while (stream.read(&mut request_buffer).unwrap() > 0) {
                    println!("First bytes are read");
                    if let Ok(bytes) = request_buffer[8..12].try_into() {
                        correlation_id = u32::from_be_bytes(bytes);
                        println!("correlation id is {}", correlation_id);
                    }
                    // TODO: Refactor message handling for scalability later.
                    response_buffer.extend(&message_size.to_be_bytes());
                    response_buffer.extend(&correlation_id.to_be_bytes());

                    stream.write_all(&response_buffer).unwrap();
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
    
}