use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum MessageType {
    Join,
    Chat,
    Disconnect,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ClientMessage {
    pub kind: MessageType,
    pub username: String,
    pub message: String,
}