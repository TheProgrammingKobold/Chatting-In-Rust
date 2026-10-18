use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;
use tokio::net::{TcpListener, TcpStream};
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::{eprint, eprintln, io, println};

use chat_common::{ClientMessage, ServerMessage, OUTGOING_QUEUE_CAPACITY, MAX_MESSAGE_SIZE};

struct ConnectedClient {
    username: String,
    outgoing: mpsc::Sender<Vec<u8>>
}

type Clients = Arc<Mutex<HashMap<SocketAddr, ConnectedClient>>>;
type MessageData = Vec<u8>;

const MAX_MESSAGES_PER_ROOM: usize = 100;
type MessageRoom = Arc<Mutex<VecDeque<MessageData>>>;

fn add_message_to_room(message_room: &MessageRoom, message: MessageData) {
    let mut connected = match message_room.lock() {
        Ok(connected) => connected,
        Err(e) => {
            eprintln!("Failed to lock message room: {}", e);
            return;
        }
    };

    connected.push_back(message);

    // If its above the message size we just pop the back before adding anotehr
    while connected.len() > MAX_MESSAGES_PER_ROOM {
        let _ = connected.pop_front();
    }

}

fn send_all_messages_from_room(client_to_send: SocketAddr, clients: &Clients, message_room: &MessageRoom) {
    let connected = match clients.lock() {
        Ok(connected) => connected,
        Err(e) => {
            eprintln!("Failed to lock client registry: {}", e);
            return;
        }
    };
    
    let client = connected.get(&client_to_send).unwrap();

    let room = match message_room.lock() {
        Ok(room) => room,
        Err(e) => {
            eprintln!("Failed to lock room: {}", e);
            return;
        }
    };

    for message in room.iter() {
        match client.outgoing.try_send(message.clone()) {
            Ok(()) => {},

            Err(mpsc::error::TrySendError::Full(_)) => {
                eprintln!("Outgoing message queue is full for {}, dropping message", client_to_send);
                break; // sense there's no point in trying again if its full
            },

            Err(mpsc::error::TrySendError::Closed(_)) => {
                eprintln!("Client, {}, has closed their queue!", client_to_send);
                break;
            }
        }
    }
}

fn encode_server_message(message: &ServerMessage) -> io::Result<MessageData> {
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

fn broadcast_message(clients: &Clients, message: &ServerMessage, excluded: Option<SocketAddr>, message_room: &MessageRoom) {
     // Encode the message once, before distributing it.
    let frame = match encode_server_message(message) {
        Ok(frame) => frame,
        Err(e) => {
            eprintln!("Failed to encode server message: {}", e);
            return;
        }
    };

    add_message_to_room(&message_room, frame.clone());

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
    message_room: MessageRoom
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
            break;
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

                let username_taken = {
                    let connected = clients.lock().unwrap();

                    connected.values().any(|client| {
                        client.username.eq_ignore_ascii_case(requested_username)
                    })
                }; 
                
                if username_taken {
                    let rejection = ServerMessage::Reject { reason: "Someone already has this username.".to_string() };
                    
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

                    username = String::from("USER_TAKEN");

                    // Important to break from the connection! 
                    break;
                }

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
                broadcast_message(&clients, &announcement, Some(address), &message_room);

                send_all_messages_from_room(address, &clients, &message_room);
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

                broadcast_message(&clients, &chat_message, Some(address), &message_room);

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

        broadcast_message(&clients, &announcement, None, &message_room);
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
    let message_room: MessageRoom = Arc::new(Mutex::new(VecDeque::new()));

    loop {
        let (socket, address) = listener.accept().await.unwrap();
        let clients = Arc::clone(&clients);
        let message_room = Arc::clone(&message_room);


        tokio::spawn(async move {
            handle_client(socket, address, clients, message_room).await;
        });
    }
}
