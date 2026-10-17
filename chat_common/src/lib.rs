
use serde::{Deserialize, Serialize};

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
