use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpStream};
use std::{println, io};

use chat_common::{ClientMessage, ServerMessage, MAX_MESSAGE_SIZE};

async fn recieve_data(stream: OwnedReadHalf) {

    let mut reader = BufReader::new(stream);
    let mut size_buffer = [0u8; 4];

    loop{

        reader.read_exact(&mut size_buffer).await.unwrap();
        let size_in_bytes = u32::from_be_bytes(size_buffer) as usize;

        if size_in_bytes > MAX_MESSAGE_SIZE {
            println!("Warning: Tried to recieve data that was larger than the maximum size! Packet Dropped!");
            break;
        }

        let mut message_buffer = vec![0u8; size_in_bytes];

        reader.read_exact(&mut message_buffer).await.unwrap();

        let message: ServerMessage = match serde_json::from_slice(&message_buffer) {
            Ok(message) => message,
            Err(e) => {
                eprintln!("Invalid JSON from SERVER: {}", e);
                continue;
            }
        };


        match message {
            ServerMessage::Accept => {
                println!("Server has accepted your connection!");
            },
            ServerMessage::Reject { reason } => {
                println!("Server has rejected your connection: {}", reason);
                break;
            },
            ServerMessage::Chat { username, message } => {
                println!("[{}]: {}", username, message);
            },
            ServerMessage::Disconnect => {
                println!("You have been disconnected from the server!");
                break;
            },
            ServerMessage::Announce { message } => {
                println!("[SERVER]: {}", message);
            },
        }
    }
}

async fn send_data(client_message: ClientMessage, stream: &mut OwnedWriteHalf) -> io::Result<()> {
    let payload = serde_json::to_vec(&client_message)
    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    if payload.len() > MAX_MESSAGE_SIZE {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "Tried to send a message that was above the max message size!"));
    }


    let size = u32::try_from(payload.len())
    .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;


    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&size.to_be_bytes());
    frame.extend_from_slice(&payload);


    stream.write_all(&frame).await?;

    Ok(())
}

#[tokio::main]
async fn main() {
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

    let mut stream = TcpStream::connect("127.0.0.1:7878").await.unwrap();

    let announcement = ClientMessage::Join { username };

    let (read_half, mut write_half) = stream.into_split();

    send_data(announcement, &mut write_half).await.unwrap();

    let receiver = tokio::spawn(async move {
        recieve_data(read_half).await;
    });

    loop {
        let mut message = String::new();

        match std::io::stdin().read_line(&mut message){
            Ok(0) => {
                break;
            }
            Ok(_) => {

                let chat_message = ClientMessage::Chat { message };

                send_data(chat_message, &mut write_half).await.unwrap();

            }
            Err(e) => {
                println!("Failed to get message: {}", e);
                break;
            }
        }
    }

    let disconnect_message = ClientMessage::Disconnect {};

    send_data(disconnect_message, &mut write_half).await.unwrap();

    match receiver.await {
        Ok(()) => {
            
        }
        Err(e) => {
            println!("Receiver Failed to join: {}", e);
        }
    }
}
