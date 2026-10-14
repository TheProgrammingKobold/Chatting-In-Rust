use std::io::{BufRead, BufReader, Write};
use std::net::{TcpStream, Shutdown};
use std::println;

use chat_common::{ClientMessage, MessageType};

fn recieve_data(mut stream: TcpStream) {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();

    loop{
        line.clear();


        match reader.read_line(&mut line) {
            Ok(0) => {
                println!("Lost connection to server!");
                break; // Important to break from the loop here!
            }
            Ok(_) => {
                let chat_message: ClientMessage = 
                match serde_json::from_str(&line) {
                    Ok(message) => message,
                    Err(e) => {
                        println!("Failed to parse JSON: {}", e);
                        continue;
                    }
                };

                println!("[{}]: {}", chat_message.username, chat_message.message);
            }
            Err(e) => {
                println!("Failed to recive data: {}", e);
                break; // Also importnat to break here!
            }
        }
    }
}

fn send_data(client_message: ClientMessage, stream: &mut TcpStream) -> bool {
    let json = serde_json::to_string(&client_message).unwrap();
    let json = format!("{}\n", json);

    match stream.write_all(json.as_bytes()) {
        Ok(()) => {
            //println!("Sent: {}", message);
            return true;
        }
        Err(e) => {
            println!("Error sending bytes: {}", e);
            return false;
        }
    }
}

fn main() {
    let mut username = String::new();

    println!("Please Enter the username you'd like to use");

    match std::io::stdin().read_line(&mut username) {
        Ok(0) => {
            username = String::from("Default");
        }
        Ok(_) => {

        }
        Err(e) => {
            println!("Error capturing username: {}", e);
        }
    }

    let username = username.trim().to_string();

    let mut stream = TcpStream::connect("127.0.0.1:7878").unwrap();
    let mut stream_copy: TcpStream;
    match stream.try_clone(){
        Ok(cloned_stream) => {
            stream_copy = cloned_stream;
        }
        Err(e) => {
            println!("Error Copying the handle to the stream: {}", e);
            return;
        }
    }

    let announcement = ClientMessage {
        kind: MessageType::Join,
        username: username.clone(),
        message: String::new(),
    };

    send_data(announcement, &mut stream_copy);

    let receiver = std::thread::spawn(move || {
        recieve_data(stream);
    });

    loop {
        let mut message = String::new();

        match std::io::stdin().read_line(&mut message){
            Ok(0) => {
                break;
            }
            Ok(_) => {

                let chat_message = ClientMessage {
                    kind: MessageType::Chat,
                    username: username.clone(),
                    message: message.trim_end().to_string()
                };

                send_data(chat_message, &mut stream_copy);

            }
            Err(e) => {
                println!("Failed to get message: {}", e);
                break;
            }
        }
    }

    let disconnect_message = ClientMessage {
        kind: MessageType::Disconnect,
        username: username.clone(),
        message: String::new(),
    };

    send_data(disconnect_message, &mut stream_copy);

    if let Err(e) = stream_copy.shutdown(Shutdown::Both) {
        println!("Error shutting down connection: {}", e);
    }

    match receiver.join() {
        Ok(_) => {
            
        }
        Err(e) => {
            println!("Thread Failed to join: {:?}", e);
        }
    }
}
