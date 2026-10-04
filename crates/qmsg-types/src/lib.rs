//! Types shared by the orchestrator and providers.
//!
//! Providers talk to the orchestrator over a WebSocket. Each binary frame holds
//! one postcard-encoded [`ProviderMessage`] or [`HostMessage`].
//!
//! These are encoded with postcard, which is not self-describing, so both
//! sides must agree on the exact layout. Adding an enum variant at the end is
//! compatible as long as it is only sent to code that knows it. Any other
//! change, including adding a field, breaks existing providers and needs an
//! [`ABI_VERSION`] bump.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize, de::DeserializeOwned};

/// Checked against each provider when it is loaded.
pub const ABI_VERSION: u32 = 3;

/// Passed to a provider when it starts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderConfig {
    pub name: String,
    /// WebSocket URL the provider connects to for talking to the orchestrator.
    pub host_url: String,
    pub settings: BTreeMap<String, String>,
}

/// A message sent to or received from a channel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    /// The [`Organization`] the channel belongs to, or `None` for channels
    /// outside any, such as direct messages.
    pub organization: Option<String>,
    pub channel: String,
    /// The author's [`User`] id.
    pub author: String,
    /// The parts of the message, in order, such as a caption and its image.
    pub content: Vec<Content>,
}

/// One part of a [`Message`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Content {
    Text(String),
    Image(Media),
    Video(Media),
    Audio(Media),
    File(Media),
    /// Anything else the platform has, such as a sticker or a poll, in a
    /// format the provider defines.
    Custom {
        kind: String,
        data: Vec<u8>,
    },
}

impl Content {
    pub fn kind(&self) -> ContentKind {
        match self {
            Self::Text(_) => ContentKind::Text,
            Self::Image(_) => ContentKind::Image,
            Self::Video(_) => ContentKind::Video,
            Self::Audio(_) => ContentKind::Audio,
            Self::File(_) => ContentKind::File,
            Self::Custom { kind, .. } => ContentKind::Custom(kind.clone()),
        }
    }
}

/// A file carried by a [`Content`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Media {
    /// The file name, if the platform has one.
    pub name: Option<String>,
    /// The MIME type, such as `image/png`, if known.
    pub mime: Option<String>,
    pub source: MediaSource,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MediaSource {
    Bytes(Vec<u8>),
    /// Where the platform hosts the file.
    Url(String),
}

/// The kinds of [`Content`], used to say what a [`Channel`] accepts.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ContentKind {
    Text,
    Image,
    Video,
    Audio,
    File,
    /// Matches [`Content::Custom`] with the same `kind`.
    Custom(String),
}

/// A group on a platform, such as a Discord server or a Slack workspace.
///
/// Ids are chosen by the provider and only need to be unique within it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Organization {
    pub id: String,
    pub name: String,
    pub users: Vec<User>,
    pub channels: Vec<Channel>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct User {
    /// Unique within the user's organization.
    pub id: String,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Channel {
    /// Unique within the channel's organization, or among the provider's
    /// channels outside any.
    pub id: String,
    pub name: String,
    pub kind: ChannelKind,
    /// What can be sent to the channel. Empty for channels that can't be
    /// written to, such as a voice channel without text chat.
    pub inputs: Vec<ContentKind>,
}

impl Channel {
    /// Whether the channel takes `content`.
    pub fn accepts(&self, content: &Content) -> bool {
        self.inputs.contains(&content.kind())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChannelKind {
    Text,
    Voice,
    Video,
    /// Only some users can post, such as a news channel.
    Announcement,
    /// Holds threads rather than messages.
    Forum,
    /// A conversation between two users.
    Direct,
    /// A conversation between a few users, outside any channel list.
    Group,
    /// Anything else the platform has, named by the provider.
    Custom(String),
}

/// A change to the organizations and channels a provider is part of.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DirectoryUpdate {
    /// Adds an organization, or replaces it along with its users and channels.
    OrganizationSet(Organization),
    OrganizationRemoved {
        id: String,
    },
    /// Adds a user to an organization, or replaces the one with its id.
    UserSet {
        organization: String,
        user: User,
    },
    UserRemoved {
        organization: String,
        id: String,
    },
    /// Adds a channel, or replaces the one with its id. Channels outside any
    /// organization, such as direct messages, have no `organization`.
    ChannelSet {
        organization: Option<String>,
        channel: Channel,
    },
    ChannelRemoved {
        organization: Option<String>,
        id: String,
    },
}

/// Work the orchestrator hands to a provider.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    Send {
        organization: Option<String>,
        channel: String,
        content: Vec<Content>,
    },
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
    /// Answered with [`HostMessage::Secret`].
    SecretGet {
        key: String,
    },
    /// Answered with [`HostMessage::SecretStored`].
    SecretSet {
        key: String,
        value: Vec<u8>,
    },
    /// Answered with [`HostMessage::SecretStored`].
    SecretDelete {
        key: String,
    },
    /// A change to the organizations and channels the provider is part of.
    /// Sent before any message in them, and again whenever they change.
    Directory(DirectoryUpdate),
}

/// Sent from the orchestrator to a provider.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum HostMessage {
    Command(Command),
    /// The answer to a [`ProviderMessage::SecretGet`]. Fails when the
    /// secret can't be read or decrypted, which is not the same as it
    /// never having been set.
    Secret {
        key: String,
        result: Result<Option<Vec<u8>>, String>,
    },
    /// The answer to a [`ProviderMessage::SecretSet`] or
    /// [`ProviderMessage::SecretDelete`]. A set fails when the value doesn't
    /// fit in the provider's secret storage.
    SecretStored {
        key: String,
        result: Result<(), String>,
    },
}

pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_allocvec(value)
}

pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, postcard::Error> {
    postcard::from_bytes(bytes)
}
