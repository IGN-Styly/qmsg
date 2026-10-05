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

use std::fmt;

use serde::{Deserialize, Serialize, de::DeserializeOwned};

mod limits;

pub use limits::{ContentRule, MessageLimits, TextUnit, Violation, check};

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
/// before they are sent, rather than dropping the connection. Send bigger
/// files as a [`MediaSource::Blob`].
pub const MAX_FRAME: usize = 64 * 1024 * 1024;

/// The most bytes one blob read returns.
pub const MAX_READ: u32 = 4 * 1024 * 1024;

/// The longest blob id or secret key, in bytes. The orchestrator disconnects
/// a provider that sends a longer one.
pub const MAX_ID: usize = 1024;

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
    /// The size in bytes, if known. Ignored for [`MediaSource::Bytes`].
    pub size: Option<u64>,
    pub source: MediaSource,
}

impl Media {
    /// The size in bytes, if known.
    pub fn size(&self) -> Option<u64> {
        match &self.source {
            MediaSource::Bytes(bytes) => Some(bytes.len() as u64),
            _ => self.size,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MediaSource {
    /// The file itself. The whole message must fit in [`MAX_FRAME`].
    Bytes(Vec<u8>),
    /// Where the file can be downloaded without credentials.
    Url(String),
    /// A file held by the side that sent the message: the provider in the
    /// messages it emits, the orchestrator in its commands. The other side
    /// reads it in pieces, so it can be any size. Use it too for files only
    /// the provider can download, such as ones behind the platform's login.
    ///
    /// A provider's blob stays readable until the orchestrator sends
    /// [`Command::ReleaseBlob`]. At most [`MAX_ID`] bytes.
    Blob(String),
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
    /// What can be sent to the channel, and how much. Empty for channels that
    /// can't be written to, such as a voice channel without text chat.
    pub accepted_content: Vec<ContentRule>,
    pub limits: MessageLimits,
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
            limits: MessageLimits::default(),
        }
    }

    /// Takes `kinds` with no limits.
    pub fn accepting(self, kinds: impl IntoIterator<Item = ContentKind>) -> Self {
        Self {
            accepted_content: kinds.into_iter().map(ContentRule::new).collect(),
            ..self
        }
    }

    /// Checks a message against the channel's rules and limits.
    ///
    /// Passing is only a good sign: the platform can still refuse a message,
    /// such as for the account's permissions.
    pub fn check(&self, content: &[Content]) -> Result<(), Violation> {
        check(&self.accepted_content, &self.limits, content)
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
    /// Changes an organization's details, keeping its users and channels.
    OrganizationUpdated {
        id: String,
        name: String,
    },
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
    /// Sets the account's own user id in a scope, or clears it with `None`.
    /// Removing that user clears it too.
    Me {
        organization: Option<String>,
        id: Option<String>,
    },
}

/// Work the orchestrator hands to a provider.
///
/// The provider answers each command but `ReleaseBlob` and `Shutdown` with a
/// [`ProviderMessage::Reply`] carrying the command's `request`, which the
/// orchestrator chooses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    /// Sends a message, answered with [`Reply::Sent`]. The provider also
    /// reports the message as [`MessageEvent::Received`], like any other.
    Send {
        request: u64,
        channel: ChannelRef,
        reply_to: Option<String>,
        content: Vec<Content>,
    },
    /// Opens a conversation with `members`, or finds the one already open,
    /// answered with [`Reply::Opened`]. The provider reports the channel
    /// before answering, so its directory update comes first.
    ///
    /// Members are user ids in the organization's scope, or addresses the
    /// provider hasn't reported, such as an email address or phone number.
    OpenChannel {
        request: u64,
        organization: Option<String>,
        members: Vec<String>,
    },
    /// Reads up to `len` bytes, at most [`MAX_READ`], of a
    /// [`MediaSource::Blob`] the provider sent, starting at `offset`.
    /// Answered with [`Reply::Blob`], which is shorter than `len` only at the
    /// end of the blob.
    ReadBlob {
        request: u64,
        blob: String,
        offset: u64,
        len: u32,
    },
    /// The orchestrator is done with a [`MediaSource::Blob`] the provider
    /// sent, so the provider can free it. Not answered.
    ReleaseBlob {
        blob: String,
    },
    Shutdown,
}

/// A provider's answer to a [`Command`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Reply {
    /// The id of the message that was sent.
    Sent {
        id: String,
    },
    Opened(ChannelRef),
    Blob(Vec<u8>),
}

/// Why a provider couldn't carry out a [`Command`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommandError {
    /// The provider or platform can't do this.
    Unsupported,
    UnknownOrganization(String),
    UnknownChannel(ChannelRef),
    UnknownUser(String),
    UnknownBlob(String),
    /// The content breaks the channel's rules or limits.
    Rejected(Violation),
    /// Too many requests; try again after this many milliseconds, if the
    /// platform says.
    RateLimited {
        retry_after_ms: Option<u64>,
    },
    /// The account isn't allowed to, such as post in an announcement channel.
    Forbidden(String),
    /// Anything else, as the provider describes it.
    Failed(String),
}

impl fmt::Display for CommandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsupported => write!(f, "not supported"),
            Self::UnknownOrganization(id) => write!(f, "unknown organization `{id}`"),
            Self::UnknownChannel(channel) => write!(f, "unknown channel {channel:?}"),
            Self::UnknownUser(id) => write!(f, "unknown user `{id}`"),
            Self::UnknownBlob(id) => write!(f, "unknown blob `{id}`"),
            Self::Rejected(violation) => write!(f, "rejected: {violation}"),
            Self::RateLimited {
                retry_after_ms: Some(ms),
            } => write!(f, "rate limited, retry in {ms} ms"),
            Self::RateLimited {
                retry_after_ms: None,
            } => write!(f, "rate limited"),
            Self::Forbidden(reason) => write!(f, "forbidden: {reason}"),
            Self::Failed(reason) => write!(f, "{reason}"),
        }
    }
}

impl std::error::Error for CommandError {}

impl From<Violation> for CommandError {
    fn from(violation: Violation) -> Self {
        Self::Rejected(violation)
    }
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
    /// The answer to a [`Command`].
    Reply {
        request: u64,
        result: Result<Reply, CommandError>,
    },
    /// Reads up to `len` bytes, at most [`MAX_READ`], of a
    /// [`MediaSource::Blob`] the orchestrator sent, starting at `offset`.
    /// Answered with [`HostMessage::Blob`].
    ReadBlob {
        blob: String,
        offset: u64,
        len: u32,
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
    /// The answer to a [`ProviderMessage::ReadBlob`]: shorter than asked only
    /// at the end of the blob.
    Blob {
        blob: String,
        offset: u64,
        result: Result<Vec<u8>, String>,
    },
}

pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_allocvec(value)
}

pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, postcard::Error> {
    postcard::from_bytes(bytes)
}
