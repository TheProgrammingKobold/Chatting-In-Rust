use std::io::{BufReader, BufRead, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::{println, thread};

use chat_common::{ClientMessage, MessageType};

type Clients = Arc<Mutex<Vec<TcpStream>>>;

fn broadcast_message(clients: &Clients, message: &ClientMessage) {
    let json = match serde_json::to_string(message) {
        Ok(json) => format!("{}\n", json),
        Err(e) => {
            println!("Failed to steralize announcement: {}", e);
            return;
        }
    };

    let mut connected = match clients.lock() {
        Ok(connected) => connected,
        Err(e) => {
            println!("Failed to lock clients: {}", e);
            return;
        }
    };

    connected.retain_mut(|client| {
        match client.write_all(json.as_bytes()) {
            Ok(()) => true,
            Err(e) => {
                println!("Failed to send to a client: {}", e);
                false
            }
        }
    });
}

fn handle_client(mut stream: TcpStream, clients: Clients) {
    let address = match stream.peer_addr() {
        Ok(addr) => addr,
        Err(e) => {
            println!("Failed to get client address: {}", e);
            return;
        }
    };

    // Clone the stream so the shared client list can send messages to this client.
    let client_stream = match stream.try_clone() {
        Ok(s) => s,
        Err(e) => {
            println!("Failed to clone client stream: {}", e);
            return;
        }
    };

    // Register this client.
    match clients.lock() {
        Ok(mut connected) => connected.push(client_stream),
        Err(e) => {
            println!("Failed to lock client list: {}", e);
            return;
        }
    }

    println!("Registered client: {}", address);

    let mut reader = BufReader::new(stream);
    let mut line = String::new();

    let mut username = String::new();

    loop {
        line.clear();

        match reader.read_line(&mut line) {
            Ok(0) => {
                println!("Client disconnected: {}", address);
                break;
            }
            Ok(_) => {
                let message: ClientMessage = 
                match serde_json::from_str(&line) {
                    Ok(message) => message,
                    Err(e) => {
                        println!("Invalide message from {}: {}", address, e);
                        continue;
                    }
                };

                match message.kind {
                    MessageType::Join => {
                        // Only accept the first Join request.
                        if !username.is_empty() {
                            continue;
                        }

                        username = message.username.trim().to_string();

                        let announcement = ClientMessage {
                            kind: MessageType::Chat,
                            username: "SERVER".to_string(),
                            message: format!("{} has joined the channel!", username),
                        };

                        broadcast_message(&clients, &announcement);
                    }

                    MessageType::Chat => {

                        if username.is_empty() {
                            println!("Client tried to chat before joining: {}", address);
                            continue;
                        }

                        let json = match serde_json::to_string(&message) {
                            Ok(json) => format!("{}\n", json),
                            Err(e) => {
                                println!("Failed to serialize message: {}", e);
                                continue;
                            }
                        };

                        // Broadcast the message to all connected clients.
                        let mut connected = match clients.lock() {
                            Ok(connected) => connected,
                            Err(e) => {
                                println!("Failed to lock client list: {}", e);
                                break;
                            }
                        };

                        connected.retain_mut(|client| {
                            
                            let is_sender = client.peer_addr().map(|addr| addr == address).unwrap_or(false);

                            if is_sender {
                                return true;
                            }
                            
                            match client.write_all(json.as_bytes()) {
                                Ok(()) => true,
                                Err(e) => {
                                    println!("Removing client after send failure: {}", e);
                                    false
                                }
                            }
                        });

                        println!("{}: {}", message.username, message.message);

                        // Broadcast the chat message to other clients.
                    }

                    MessageType::Disconnect => {
                        break;
                    }
                }


            }
            Err(e) => {
                println!("Error reading from {}: {}", address, e);
                break;
            }
        }
    }
    // Save the username before cleaning up the connection.
    let disconnected_username = username.clone();

    // Remove this client's connection.
    match clients.lock() {
        Ok(mut connected) => {
            connected.retain(|client| {
                client.peer_addr()
                    .map(|addr| addr != address)
                    .unwrap_or(false)
            });
        }
        Err(e) => {
            println!("Failed to lock clients during cleanup: {}", e);
        }
    }

    // Announce the departure after removing the disconnected client.
    if !disconnected_username.is_empty() {
        let announcement = ClientMessage {
            kind: MessageType::Chat,
            username: "SERVER".to_string(),
            message: format!(
                "{} has disconnected from the channel!",
                disconnected_username
            ),
        };

        broadcast_message(&clients, &announcement);
    }
}

fn main() {
    let listener = match TcpListener::bind("127.0.0.1:7878") {
        Ok(listener) => listener,
        Err(e) => {
            println!("Failed to bind server: {}", e);
            return;
        }
    };

    println!("Server listening on 127.0.0.1:7878!");

    let clients: Clients = Arc::new(Mutex::new(Vec::new()));

    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                let clients = Arc::clone(&clients);

                thread::spawn(move || {
                    handle_client(stream, clients);
                });
            }
            Err(e) => println!("Failed to accept connection: {}", e),
        }
    }
}
