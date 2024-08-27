use std::io::Write;
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
                _stream
                    .write("+PONG\r\n".to_owned().as_bytes())
                    .expect("Failed to send bytes");
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
