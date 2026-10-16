use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;
use tokio::net::{TcpListener, TcpStream};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::{eprint, io, println};

use chat_common::{ClientMessage, ServerMessage, OUTGOING_QUEUE_CAPACITY, MAX_MESSAGE_SIZE};

struct ConnectedClient {
    username: String,
    outgoing: mpsc::Sender<Vec<u8>>
}

type Clients = Arc<Mutex<HashMap<SocketAddr, ConnectedClient>>>;

fn encode_server_message(message: &ServerMessage) -> io::Result<Vec<u8>> {
    let payload = serde_json::to_vec(message)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    if payload.len() > MAX_MESSAGE_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Server message exceeds maximum size",
        ));
    }

    let size = u32::try_from(payload.len())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&size.to_be_bytes());
    frame.extend_from_slice(&payload);

    Ok(frame)
}

fn broadcast_message(clients: &Clients, message: &ServerMessage, excluded: Option<SocketAddr>) {
     // Encode the message once, before distributing it.
    let frame = match encode_server_message(message) {
        Ok(frame) => frame,
        Err(e) => {
            eprintln!("Failed to encode server message: {}", e);
            return;
    }
};

    let mut connected = match clients.lock() {
        Ok(connected) => connected,
        Err(e) => {
            eprintln!("Failed to lock client registry: {}", e);
            return;
        }
    };

    let mut closed_clients = Vec::new();

    for (address, client) in connected.iter() {
        if Some(*address) == excluded {
            continue;
        }

        match client.outgoing.try_send(frame.clone()) {
            Ok(()) => {}

            Err(mpsc::error::TrySendError::Full(_)) => {
                // Don't let a slow client block everybody else.
                // This individual message is dropped for that client.
                eprintln!(
                    "Outgoing queue full for {}; dropping message",
                    address
                );
            }

            Err(mpsc::error::TrySendError::Closed(_)) => {
                closed_clients.push(*address);
            }
        }
    }

    for address in closed_clients {
        connected.remove(&address);
    }
}

async fn handle_client(
    stream: TcpStream,
    address: SocketAddr,
    clients: Clients,
) {
    println!("TCP connection from {}", address);

    // Split this socket into an independently owned reader and writer.
    let (read_half, mut write_half) = stream.into_split();

    // The reader task sends outgoing messages through this channel.
    let (outgoing_tx, mut outgoing_rx) =
        mpsc::channel::<Vec<u8>>(OUTGOING_QUEUE_CAPACITY);

    // Dedicated writer task: wait for queued messages and send them.
    tokio::spawn(async move {
        while let Some(frame) = outgoing_rx.recv().await {
            if let Err(e) = write_half.write_all(&frame).await {
                eprintln!("Failed to write to {}: {}", address, e);
                break;
            }
        }
    });

    let mut reader = BufReader::new(read_half);
    let mut size_buffer = [0u8; 4];
    let mut username = String::new();

    loop {
        if let Err(e) = reader.read_exact(&mut size_buffer).await {
            eprintln!("Failed to read frame header from {}: {}", address, e);
            break;
        }
        let size_in_bytes = u32::from_be_bytes(size_buffer) as usize;

        if size_in_bytes > MAX_MESSAGE_SIZE {
            println!("Warning: Tried to recieve data that was larger than the maximum size! Packet Dropped!");
            break;
        }

        let mut message_buffer = vec![0u8; size_in_bytes];

        if let Err(e) = reader.read_exact(&mut message_buffer).await {
            eprint!("Failed to read frame from payload {}: {}", address, e);
        };

        let message: ClientMessage = match serde_json::from_slice(&message_buffer) {
            Ok(message) => message,
            Err(e) => {
                eprintln!("Invalid JSON from {}: {}", address, e);
                continue;
            }
        };

        match message {
            ClientMessage::Join { username: requested_username } => {
                if !username.is_empty() {
                    continue;
                }

                let requested_username = requested_username.trim();

                if requested_username.is_empty() {
                    let rejection = ServerMessage::Reject {
                        reason: "Username cannot be empty.".to_string(),
                    };

                    match encode_server_message(&rejection) {
                        Ok(frame) => {
                            if let Err(e) = outgoing_tx.send(frame).await {
                                eprintln!("Failed to send rejection to {}: {}", address, e);
                            }
                        }
                        Err(e) => {
                            eprintln!("Failed to encode rejection: {}", e);
                        }
                    }

                    // Reject the connection instead of continuing to process requests.
                    break;
                }

                username = requested_username.to_string();

                // Only joined clients are added to the broadcast registry.
                match clients.lock() {
                    Ok(mut connected) => {
                        connected.insert(
                            address,
                            ConnectedClient {
                                username: username.clone(),
                                outgoing: outgoing_tx.clone(),
                            },
                        );
                    }

                    Err(e) => {
                        eprintln!("Failed to lock client registry: {}", e);
                        username.clear();
                        break;
                    }
                }

                println!("{} joined from {}", username, address);

                let announcement = ServerMessage::Announce {
                    message: format!("{} has joined the channel!", username)
                };

                // Include the newly joined client in the announcement.
                broadcast_message(&clients, &announcement, None);
            }

            ClientMessage::Chat { message: text } => {
                if username.is_empty() {
                    eprintln!("Client tried to chat before joining: {}", address);
                    continue;
                }

                println!("{}: {}", username, text);

                let chat_message = ServerMessage::Chat {
                    username: username.clone(),
                    message: text,
                };

                broadcast_message(&clients, &chat_message, Some(address));
            }

            ClientMessage::Disconnect => {
                break;
            }
        }
    }

    // Remove the client before broadcasting its departure.
    match clients.lock() {
        Ok(mut connected) => {
            connected.remove(&address);
        }

        Err(e) => {
            eprintln!("Failed to lock client registry during cleanup: {}", e);
        }
    }

    println!("Client disconnected: {}", address);

    if !username.is_empty() {
        let announcement = ServerMessage::Announce {
             message: format!("{} has disconnected from the channel!", username)
        };

        broadcast_message(&clients, &announcement, None);
    }

    // Once all registry references are removed, dropping this sender lets
    // the writer task finish after its queued messages have been sent.
    drop(outgoing_tx);
}

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:7878").await.unwrap();

    println!("Server listening on 127.0.0.1:7878!");

    let clients: Clients = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let (socket, address) = listener.accept().await.unwrap();
        let clients = Arc::clone(&clients);


        tokio::spawn(async move {
            handle_client(socket, address, clients).await;
        });
    }
}
