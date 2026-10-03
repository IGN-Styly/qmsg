//! Types shared by the orchestrator and providers.
//!
//! Providers talk to the orchestrator over a WebSocket. Each binary frame holds
//! one postcard-encoded [`ProviderMessage`] or [`HostMessage`].
//!
//! These are encoded with postcard, which is not self-describing. Only append
//! new enum variants and fields at the end, and bump [`ABI_VERSION`] for any
//! change that breaks existing providers.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize, de::DeserializeOwned};

/// Checked against each provider when it is loaded.
pub const ABI_VERSION: u32 = 1;

/// Passed to a provider when it starts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderConfig {
    pub name: String,
    /// WebSocket URL the provider connects to for talking to the orchestrator.
    pub host_url: String,
    pub settings: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    pub chat: String,
    pub author: String,
    pub body: String,
}

/// Work the orchestrator hands to a provider.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    Send { chat: String, body: String },
    Shutdown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

/// Sent from a provider to the orchestrator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProviderMessage {
    Log {
        level: LogLevel,
        message: String,
    },
    /// A message received from the platform.
    Emit(Message),
    /// Answered with [`HostMessage::Value`].
    KvGet {
        key: String,
    },
    KvSet {
        key: String,
        value: Vec<u8>,
    },
}

/// Sent from the orchestrator to a provider.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum HostMessage {
    Command(Command),
    /// The answer to a [`ProviderMessage::KvGet`].
    Value {
        key: String,
        value: Option<Vec<u8>>,
    },
}

pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_allocvec(value)
}

pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, postcard::Error> {
    postcard::from_bytes(bytes)
}
