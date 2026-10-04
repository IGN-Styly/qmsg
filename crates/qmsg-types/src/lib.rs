//! Types shared by the orchestrator and providers.
//!
//! Providers talk to the orchestrator over a WebSocket. Each binary frame holds
//! one postcard-encoded [`ProviderMessage`] or [`HostMessage`].
//!
//! These are encoded with postcard, which is not self-describing, so both
//! sides must agree on the exact layout. A provider only loads when its
//! [`ABI_VERSION`] matches the orchestrator's exactly, so any change to these
//! types, including a new enum variant, needs a bump.
//!
//! # Addressing
//!
//! A provider is part of any number of [`Organization`]s, each with its own
//! users and channels. Platforms that have users and channels outside any
//! container, such as Discord direct messages or Signal groups, put them in
//! the provider's standalone scope instead, addressed with no organization.
//! Use an organization whenever the platform has one: Slack direct messages
//! belong to their workspace.
//!
//! Ids are chosen by the provider. A user or channel id is unique within its
//! scope, a message id within its channel. A [`Message`]'s author, a
//! channel's members and its parent are all in the channel's scope.

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

/// Largest encoded message either side may send. Bigger ones are refused
/// before they are sent, rather than dropping the connection.
pub const MAX_FRAME: usize = 64 * 1024 * 1024;

/// Where a channel is: in an organization, or in the provider's standalone
/// scope when `organization` is `None`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ChannelRef {
    pub organization: Option<String>,
    pub channel: String,
}

impl ChannelRef {
    pub fn new(organization: Option<&str>, channel: impl Into<String>) -> Self {
        Self {
            organization: organization.map(str::to_owned),
            channel: channel.into(),
        }
    }
}

/// A message in a channel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    /// Unique within the channel.
    pub id: String,
    pub channel: ChannelRef,
    /// The author's [`User`] id, in the channel's scope.
    pub author: String,
    /// When it was sent, in milliseconds since the Unix epoch.
    pub sent_at: u64,
    /// The id of the message this replies to, in the same channel.
    pub reply_to: Option<String>,
    /// The parts of the message, in order, such as a caption and its image.
    pub content: Vec<Content>,
}

/// Something that happened to a message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageEvent {
    /// A new message, including ones the account sent itself.
    Received(Message),
    Edited {
        channel: ChannelRef,
        id: String,
        content: Vec<Content>,
    },
    Deleted {
        channel: ChannelRef,
        id: String,
    },
}

impl From<Message> for MessageEvent {
    fn from(message: Message) -> Self {
        Self::Received(message)
    }
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
    /// The file itself. The whole message must fit in [`MAX_FRAME`].
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

impl ContentKind {
    pub fn matches(&self, content: &Content) -> bool {
        match (self, content) {
            (Self::Text, Content::Text(_))
            | (Self::Image, Content::Image(_))
            | (Self::Video, Content::Video(_))
            | (Self::Audio, Content::Audio(_))
            | (Self::File, Content::File(_)) => true,
            (Self::Custom(a), Content::Custom { kind: b, .. }) => a == b,
            _ => false,
        }
    }
}

/// A group on a platform, such as a Discord server or a Slack workspace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Organization {
    /// Unique within the provider.
    pub id: String,
    pub name: String,
    /// The account's own user id in the organization, if it has one.
    pub me: Option<String>,
    /// Can be partial, such as when the platform sends large member lists in
    /// chunks; send the rest with [`DirectoryUpdate::UserUpserted`].
    pub users: Vec<User>,
    pub channels: Vec<Channel>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct User {
    /// Unique within the user's scope.
    pub id: String,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Channel {
    /// Unique within the channel's scope.
    pub id: String,
    pub name: String,
    pub kind: ChannelKind,
    /// The channel this one is in, such as a thread's channel or a channel's
    /// category.
    pub parent: Option<String>,
    /// Where the channel sorts among its siblings, lowest first.
    pub position: Option<u32>,
    /// The ids of the users in the channel, or `None` when it is open to its
    /// whole organization or the platform doesn't say.
    pub members: Option<Vec<String>>,
    /// What can be sent to the channel. Empty for channels that can't be
    /// written to, such as a voice channel without text chat. Only a hint:
    /// the platform can still refuse a message, such as for its size or the
    /// account's permissions.
    pub accepted_content: Vec<ContentKind>,
}

impl Channel {
    /// A channel with nothing but its id, name and kind set.
    pub fn new(id: impl Into<String>, name: impl Into<String>, kind: ChannelKind) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            kind,
            parent: None,
            position: None,
            members: None,
            accepted_content: Vec::new(),
        }
    }

    /// Whether `content`'s kind is in [`Channel::accepted_content`].
    pub fn accepts(&self, content: &Content) -> bool {
        self.accepted_content
            .iter()
            .any(|kind| kind.matches(content))
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
    /// A conversation branched off another channel, its parent.
    Thread,
    /// Groups other channels, which name it as their parent.
    Category,
    /// A conversation between two users.
    Direct,
    /// A conversation between a few users, outside any channel list.
    Group,
    /// Anything else the platform has, named by the provider.
    Custom(String),
}

/// A change to the organizations, users and channels a provider is part of.
///
/// Upserts add the item, or replace the one with the same id. Removals of
/// items that aren't there are not an error for the provider.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DirectoryUpdate {
    /// Adds an organization, or replaces it along with its users and channels.
    OrganizationUpserted(Organization),
    /// The account left the organization, or it was deleted.
    OrganizationRemoved {
        id: String,
    },
    UserUpserted {
        organization: Option<String>,
        user: User,
    },
    UserRemoved {
        organization: Option<String>,
        id: String,
    },
    ChannelUpserted {
        organization: Option<String>,
        channel: Channel,
    },
    ChannelRemoved(ChannelRef),
    /// Sets the account's own user id in a scope.
    Me {
        organization: Option<String>,
        id: String,
    },
}

/// Work the orchestrator hands to a provider.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    /// Sends a message. The provider answers with
    /// [`ProviderMessage::Sent`], and reports the message itself as
    /// [`MessageEvent::Received`] like any other.
    Send {
        /// Chosen by the orchestrator, to match the answer to the command.
        request: u64,
        channel: ChannelRef,
        reply_to: Option<String>,
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
    /// Something happened to a message on the platform.
    Message(MessageEvent),
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
    /// A change to the organizations, users and channels the provider is
    /// part of. Sent before any message in them, and again whenever they
    /// change.
    Directory(DirectoryUpdate),
    /// The answer to a [`Command::Send`]: the sent message's id, or why it
    /// wasn't sent.
    Sent {
        request: u64,
        result: Result<String, String>,
    },
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
