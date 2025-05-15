#![allow(unused_imports)]

use std::io::{Read, Write};
use std::net::TcpListener;

// TODO: make more flexible for other requests
struct ApiVersionsRequest {
    // preliminary
    size: u32,
    // header start request v2
    api_key: u16,
    api_version: u16,
    correlation_id: u32,
}

impl ApiVersionsRequest {
    pub fn new(size: u32, api_key: u16, api_version: u16, correlation_id: u32) -> Self {
        Self { size, api_key, api_version, correlation_id }
    }
}

struct ApiVersionsResponse {
    // preliminary
    size: u32,
    // header start response v1
    correlation_id: u32,
    // body start
    error_code: u16,
}

impl ApiVersionsResponse {
    pub fn new(size: u32, correlation_id: u32, error_code: u16) -> Self {
        Self { size, correlation_id, error_code }
    }
}

fn main() {
    println!("Broker has commenced.");
    let mut request = ApiVersionsRequest::new(0, 0, 0, 0);
    let mut request_buffer = vec![0u8; 1024];
    // TODO: Refactor so its fixed size later
    let mut response_buffer= Vec::new();
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                let n = stream.read(&mut request_buffer).unwrap();
                println!("{} bytes are read", n);
                if let Some((size, api_key, api_version, correlation_id)) = process_request(&request_buffer[..n]) {
                    request.size = size;
                    request.api_key = api_key;
                    request.api_version = api_version;
                    request.correlation_id = correlation_id;
                }

                let response = ApiVersionsResponse::new(0, request.correlation_id,35);
                // TODO: Refactor message handling for scalability later.
                response_buffer.extend(&request.size.to_be_bytes());
                response_buffer.extend(&request.correlation_id.to_be_bytes());

                if request.api_version > 4 {
                    response_buffer.extend(&response.error_code.to_be_bytes());
                }
                stream.write_all(&response_buffer).unwrap();
                response_buffer.clear();
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn process_request(buffer: &[u8]) -> Option<(u32, u16, u16, u32)> {
    let (size_bytes, tail) = buffer.split_at(4);
    let (api_key_bytes, tail) = tail.split_at(2);
    let (api_version_bytes, tail) = tail.split_at(2);
    let (correlation_id_bytes, tail) = tail.split_at(4);


    Some((
        u32::from_be_bytes(size_bytes.try_into().unwrap()),
        u16::from_be_bytes(api_key_bytes.try_into().unwrap()),
        u16::from_be_bytes(api_version_bytes.try_into().unwrap()),
        u32::from_be_bytes(correlation_id_bytes.try_into().unwrap()),
    ))
}