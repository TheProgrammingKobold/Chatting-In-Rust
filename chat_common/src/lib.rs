use std::io;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufRead, AsyncBufReadExt};

pub const MAX_MESSAGE_SIZE: usize = 64 * 1024;
pub const OUTGOING_QUEUE_CAPACITY: usize = 32;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ClientMessage {
    Join {
        username: String,
    },
    Chat { 
        message: String,
    },
    Disconnect,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ServerMessage {
    Accept,
    Reject { 
        reason: String,
    },
    Chat {
        username: String,
        message: String,
    },
    Disconnect, 
    Announce {
        message: String,
    },
}


pub async fn read_limited_line<R>(
    reader: &mut R,
    buffer: &mut Vec<u8>,
) -> io::Result<usize>
where
    R: AsyncBufRead + Unpin,
    {
        buffer.clear();

        loop {
            let (bytes_to_consume, found_newline) = {
                let available = reader.fill_buf().await?;

                if available.is_empty() {
                    return Ok(buffer.len());
                }

                let newline = available.iter().position(|&byte| byte == b'\n');
                let count = newline.map_or(available.len(), |index| index + 1);

                if buffer.len() + count > MAX_MESSAGE_SIZE {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, 
                    "Message exceeded the maximum size of 64KiB",));
                }

                buffer.extend_from_slice(&available[..count]);

                (count, newline.is_some())
            };

            reader.consume(bytes_to_consume);
            if found_newline {
                return Ok(buffer.len());
            }
        }
    }