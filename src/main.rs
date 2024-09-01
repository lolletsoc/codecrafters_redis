use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::TcpListener;

enum Command {
    PING,
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(_stream) => {
                println!("accepted new connection");

                let mut command = "".to_owned();
                let mut buf_reader = BufReader::new(&_stream);

                let mut buf_writer = BufWriter::new(&_stream);

                loop {
                    let num_bytes = buf_reader
                        .read_line(&mut command)
                        .expect("Failed to read from stream");

                    println!("Read {} bytes", num_bytes);
                    println!("Received: {}", command);

                    if num_bytes == 0 {
                        break;
                    }

                    if command.ends_with("PING\r\n") {
                        buf_writer
                            .write("+PONG\r\n".to_owned().as_bytes())
                            .expect("Failed to send bytes");

                        buf_writer.flush().unwrap();
                    }
                    command.clear();
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
