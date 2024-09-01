use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;

enum Command {
    PING,
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                println!("accepted new connection");

                let mut command = "".to_owned();
                let mut buf_reader = BufReader::new(&_stream);

                match buf_reader.read_line(&mut command) {
                    Ok(_) => {
                        _stream
                            .write("+PONG\r\n".to_owned().as_bytes())
                            .expect("Failed to send bytes");
                    }
                    Err(_) => {}
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
