#![allow(unused_imports)]

use std::io::{Read, Write};
use std::net::TcpListener;

fn main() {
    println!("Broker has commenced.");

    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                let message_size: i32 = 4;
                let correlation_id: i32 = 7;
                // TODO: Refactor message handling for scalability later.
                let mut response = Vec::new();
                response.extend(&message_size.to_be_bytes());
                response.extend(&correlation_id.to_be_bytes());

                stream.write_all(&response).unwrap();
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}