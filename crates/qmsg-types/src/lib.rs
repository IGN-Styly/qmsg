//! Types shared by the orchestrator and providers.
//!
//! Providers talk to the orchestrator over a WebSocket. Each binary frame holds
//! one postcard-encoded [`ProviderMessage`] or [`HostMessage`].
//!
//! These are encoded with postcard, which is not self-describing, so both
//! sides must agree on the exact layout. A provider only loads when its
//! [`ABI_VERSION`] matches the orchestrator's exactly. During development we
//! rebuild both sides together without bumping the version for each change.
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

mod formatting;
mod functions;
mod limits;

pub use formatting::{FormattedText, Mention, Reaction, Span, Style};
pub use functions::*;

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

/// The most blob reads a provider can have running at once. The
/// orchestrator fails more at once, without reading, until one finishes.
pub const MAX_BLOB_READS: usize = 4;

/// The most name lookups a provider can have running at once. The
/// orchestrator fails more at once, without looking up, until one finishes.
pub const MAX_LOOKUPS: usize = 4;

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
    /// When it was last edited, in milliseconds since the Unix epoch.
    pub edited_at: Option<u64>,
    pub reactions: Vec<Reaction>,
    /// The client's id for a message the account sent through
    /// [`Command::Send`], when the provider can tell which send it is. It
    /// settles a send whose answer never came.
    pub nonce: Option<String>,
}

impl Message {
    /// A message with no reply, edit, reactions or nonce.
    pub fn new(
        id: impl Into<String>,
        channel: ChannelRef,
        author: impl Into<String>,
        sent_at: u64,
        content: Vec<Content>,
    ) -> Self {
        Self {
            id: id.into(),
            channel,
            author: author.into(),
            sent_at,
            reply_to: None,
            content,
            edited_at: None,
            reactions: Vec::new(),
            nonce: None,
        }
    }
}

/// Something that happened to a message or in a channel.
///
/// Events can repeat, such as when history overlaps live messages or a
/// provider replays what it missed. Clients keep messages by channel and id:
/// a [`MessageEvent::Received`] for a known id replaces it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageEvent {
    /// A new message, including ones the account sent itself.
    Received(Message),
    Edited {
        channel: ChannelRef,
        id: String,
        content: Vec<Content>,
        /// In milliseconds since the Unix epoch.
        edited_at: u64,
    },
    Deleted {
        channel: ChannelRef,
        id: String,
    },
    /// A user added or removed a reaction.
    Reacted {
        channel: ChannelRef,
        id: String,
        user: String,
        key: String,
        added: bool,
    },
    /// `user`, which can be the account, has read the channel up to and
    /// including `up_to`, or nothing in it when `None`.
    Read {
        channel: ChannelRef,
        user: String,
        up_to: Option<String>,
    },
    /// `user` is typing. It lasts until they send a message, or for
    /// [`TYPING_TIMEOUT_MS`] unless repeated.
    Typing {
        channel: ChannelRef,
        user: String,
    },
    /// The provider may have missed events in this channel and can't replay
    /// them, such as after a reconnect. Load its history again.
    Gap {
        channel: ChannelRef,
    },
    /// A [`Command::Send`] with this nonce never reached the platform, so it
    /// is safe to send again. Without this or a [`Message`] carrying the
    /// nonce, delivery is unknown.
    NotSent {
        channel: ChannelRef,
        nonce: String,
        error: CommandError,
    },
}

/// How long a [`MessageEvent::Typing`] lasts unless repeated.
pub const TYPING_TIMEOUT_MS: u64 = 10_000;

impl From<Message> for MessageEvent {
    fn from(message: Message) -> Self {
        Self::Received(message)
    }
}

/// One part of a [`Message`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Content {
    Text(String),
    /// Text with styles and mentions. Counts toward the text limit.
    Formatted(FormattedText),
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
            Self::Formatted(_) => ContentKind::Formatted,
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
    /// [`Command::ReleaseBlob`]. Each id belongs to the one event or reply
    /// it came in: a provider that returns the same file again, such as in
    /// history, gives it a new id, so releasing one never takes the file from
    /// another reader. At most [`MAX_ID`] bytes.
    Blob(String),
}

/// The kinds of [`Content`], used to say what a [`Channel`] accepts.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ContentKind {
    Text,
    Formatted,
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
            | (Self::Formatted, Content::Formatted(_))
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
    /// Whether a user is around. Users can have a presence before they are
    /// reported, such as in large organizations.
    Presence {
        organization: Option<String>,
        user: String,
        presence: Presence,
    },
    /// Forgets everything the provider reported, such as before a full
    /// snapshot after reconnecting.
    Reset,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Presence {
    Online,
    Idle,
    /// Do not disturb.
    Busy,
    Offline,
}

/// Where a provider is in connecting to its platform. Reported with
/// [`ProviderMessage::Status`]; a provider starts as `Connecting`.
///
/// A provider reports `Syncing` before its directory snapshot, then `Ready`
/// once it has sent it, along with replays or [`MessageEvent::Gap`]s for
/// what it missed. Commands sent before `Ready` can fail, such as with
/// [`CommandError::LoginRequired`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProviderStatus {
    Connecting,
    /// The account must log in; the step says how. Reported again for each
    /// following step.
    LoginRequired(LoginStep),
    Syncing,
    Ready,
    /// The connection was lost, and the provider is trying again.
    Disconnected {
        reason: String,
    },
    /// The provider can't go on without help, such as for a banned account.
    Failed(String),
}

/// One step of logging in. Show the message along with whatever else the
/// step has.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoginStep {
    /// What to tell the user, such as "Enter the code we texted you".
    pub message: String,
    /// An [`ActionKind::Login`] function in the provider context to fill in
    /// and call, such as with a password or a code. The provider reports the
    /// next status before answering it.
    pub function: Option<String>,
    /// A page to open in a browser, such as an OAuth login.
    pub link: Option<String>,
    /// Text to show as a QR code for the platform's phone app to scan.
    /// Reported again when it changes.
    pub qr_code: Option<String>,
}

/// Work the orchestrator hands to a provider.
///
/// The provider answers each command but `ReleaseBlob` and `Shutdown` with a
/// [`ProviderMessage::Reply`] carrying the command's `request`, which the
/// orchestrator chooses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    /// Lists the functions currently available in this context.
    Functions {
        request: u64,
        context: FunctionContext,
    },
    /// Runs an action or lookup, answered with `Reply::Value`.
    Call {
        request: u64,
        call: FunctionCall,
    },
    /// Checks an input without carrying out an action.
    Verify {
        request: u64,
        verification: Box<VerificationRequest>,
    },
    /// Suggests values without carrying out an action.
    Complete {
        request: u64,
        completion: Box<CompletionRequest>,
    },
    /// Sends a message, answered with [`Reply::Sent`]. The provider also
    /// reports the message as [`MessageEvent::Received`], like any other,
    /// with `nonce` when it can tell. A failed send with a nonce that never
    /// reached the platform is reported as [`MessageEvent::NotSent`] too.
    Send {
        request: u64,
        channel: ChannelRef,
        reply_to: Option<String>,
        content: Vec<Content>,
        /// The client's id for this send, unique to it.
        nonce: Option<String>,
    },
    /// Reads a channel's messages, newest page first, answered with
    /// [`Reply::History`]. Start without a cursor, then pass `next_cursor`
    /// for older ones.
    History {
        request: u64,
        channel: ChannelRef,
        cursor: Option<String>,
        limit: u32,
    },
    /// Reads one message, answered with [`Reply::Message`].
    GetMessage {
        request: u64,
        channel: ChannelRef,
        id: String,
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
    /// The orchestrator stopped waiting for `request`, such as on a
    /// timeout. Not answered itself. A command not yet started can be skipped
    /// and answered with [`CommandError::Cancelled`]; one already carried out
    /// stays carried out and is answered as usual. Either way the request
    /// is answered, which frees its place in the orchestrator's limits.
    Cancel {
        request: u64,
    },
    Shutdown,
}

impl Command {
    /// The request the command is answered with, if any.
    pub fn request(&self) -> Option<u64> {
        match self {
            Self::Functions { request, .. }
            | Self::Call { request, .. }
            | Self::Verify { request, .. }
            | Self::Complete { request, .. }
            | Self::Send { request, .. }
            | Self::History { request, .. }
            | Self::GetMessage { request, .. }
            | Self::OpenChannel { request, .. }
            | Self::ReadBlob { request, .. } => Some(*request),
            Self::ReleaseBlob { .. } | Self::Cancel { .. } | Self::Shutdown => None,
        }
    }
}

/// Some of a channel's messages, oldest first.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryPage {
    /// At most the requested limit, all in the requested channel.
    pub messages: Vec<Message>,
    /// For older messages, or `None` at the start of the channel.
    pub next_cursor: Option<String>,
}

/// A provider's answer to a [`Command`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Reply {
    Functions(Vec<Function>),
    Value(Value),
    Verified(Verification),
    Completed(CompletionPage),
    /// The id of the message that was sent.
    Sent {
        id: String,
    },
    Opened(ChannelRef),
    History(HistoryPage),
    Message(Message),
    Blob(Vec<u8>),
}

/// Why a provider couldn't carry out a [`Command`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommandError {
    /// The provider or platform can't do this.
    Unsupported,
    UnknownFunction(String),
    /// Invalid function arguments, with codes and messages for each input.
    InvalidInput(Vec<InputIssue>),
    /// A platform-specific failure with a stable code and optional data,
    /// such as confirmed progress after a partial failure.
    Provider {
        code: String,
        message: String,
        details: Option<Box<Value>>,
    },
    UnknownOrganization(String),
    UnknownChannel(ChannelRef),
    UnknownUser(String),
    UnknownMessage(String),
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
    /// The account must log in first; see [`ProviderStatus::LoginRequired`].
    LoginRequired,
    /// The orchestrator cancelled the command before it was carried out.
    Cancelled,
    /// Anything else, as the provider describes it.
    Failed(String),
}

impl fmt::Display for CommandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsupported => write!(f, "not supported"),
            Self::UnknownFunction(id) => write!(f, "unknown function `{id}`"),
            Self::Provider { code, message, .. } => write!(f, "{code}: {message}"),
            Self::InvalidInput(issues) => {
                write!(f, "invalid input")?;
                for issue in issues {
                    write!(
                        f,
                        "; {}: {}",
                        issue.input.as_deref().unwrap_or("inputs"),
                        issue.message
                    )?;
                }
                Ok(())
            }
            Self::UnknownOrganization(id) => write!(f, "unknown organization `{id}`"),
            Self::UnknownChannel(channel) => write!(f, "unknown channel {channel:?}"),
            Self::UnknownUser(id) => write!(f, "unknown user `{id}`"),
            Self::UnknownMessage(id) => write!(f, "unknown message `{id}`"),
            Self::UnknownBlob(id) => write!(f, "unknown blob `{id}`"),
            Self::Rejected(violation) => write!(f, "rejected: {violation}"),
            Self::RateLimited {
                retry_after_ms: Some(ms),
            } => write!(f, "rate limited, retry in {ms} ms"),
            Self::RateLimited {
                retry_after_ms: None,
            } => write!(f, "rate limited"),
            Self::Forbidden(reason) => write!(f, "forbidden: {reason}"),
            Self::LoginRequired => write!(f, "login required"),
            Self::Cancelled => write!(f, "cancelled"),
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
    /// Where the provider is in connecting to its platform.
    Status(ProviderStatus),
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
    /// Answered with [`HostMessage::Blob`], which fails at once while
    /// [`MAX_BLOB_READS`] others are running.
    ReadBlob {
        blob: String,
        offset: u64,
        len: u32,
    },
    /// Looks up the addresses of `name`, a host and port such as
    /// `example.com:443`, at most [`MAX_ID`] bytes. Answered with
    /// [`HostMessage::Addresses`], which fails at once while [`MAX_LOOKUPS`]
    /// others are running. Lookups in a provider would block it, since WASI
    /// has no way to wait for one and for other sockets at once.
    Lookup {
        name: String,
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
    /// The answer to a [`ProviderMessage::Lookup`].
    Addresses {
        name: String,
        result: Result<Vec<std::net::SocketAddr>, String>,
    },
}

pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_allocvec(value)
}

pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, postcard::Error> {
    postcard::from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip<T: Serialize + DeserializeOwned + PartialEq + fmt::Debug>(value: T) {
        assert_eq!(decode::<T>(&encode(&value).unwrap()).unwrap(), value);
    }

    #[test]
    fn statuses_history_and_message_features_survive_the_wire() {
        let channel = ChannelRef::new(Some("org"), "general");
        round_trip(ProviderMessage::Status(ProviderStatus::LoginRequired(
            LoginStep {
                message: "Scan the code".into(),
                function: None,
                link: Some("https://example.com/login".into()),
                qr_code: Some("qr-data".into()),
            },
        )));
        let mut message = Message::new(
            "1",
            channel.clone(),
            "ana",
            5,
            vec![Content::Formatted(FormattedText {
                text: "@Bo hi".into(),
                spans: vec![Span {
                    start: 0,
                    end: 3,
                    style: Style::Mention(Mention::User("bo".into())),
                }],
            })],
        );
        message.edited_at = Some(6);
        message.nonce = Some("n".into());
        message.reactions.push(Reaction {
            key: "👍".into(),
            count: 2,
            me: true,
        });
        round_trip(ProviderMessage::Reply {
            request: 1,
            result: Ok(Reply::History(HistoryPage {
                messages: vec![message],
                next_cursor: Some("1".into()),
            })),
        });
        for event in [
            MessageEvent::Typing {
                channel: channel.clone(),
                user: "ana".into(),
            },
            MessageEvent::Read {
                channel: channel.clone(),
                user: "me".into(),
                up_to: None,
            },
            MessageEvent::Gap {
                channel: channel.clone(),
            },
            MessageEvent::NotSent {
                channel: channel.clone(),
                nonce: "n".into(),
                error: CommandError::Cancelled,
            },
        ] {
            round_trip(ProviderMessage::Message(event));
        }
        round_trip(ProviderMessage::Directory(DirectoryUpdate::Presence {
            organization: None,
            user: "ana".into(),
            presence: Presence::Busy,
        }));
        round_trip(HostMessage::Command(Command::GetMessage {
            request: 2,
            channel,
            id: "1".into(),
        }));
        round_trip(HostMessage::Command(Command::Cancel { request: 2 }));
    }
}
