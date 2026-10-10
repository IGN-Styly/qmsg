# qmsg
simple messaging for everything and everyone.

This Cargo workspace contains:

| Crate | Output | Purpose |
| --- | --- | --- |
| `crates/qmsg` | `qmsg` binary | Main app |
| `crates/qmsg-orchestrator` | `qmsg-orchestrator` binary | Runs providers with wasmtime |
| `crates/qmsg-sdk` | `qmsg_sdk` library | What providers are built with |
| `crates/qmsg-types` | `qmsg_types` library | Types shared by the orchestrator and providers |
| `providers/echo` | `.wasm` component | Example provider |

## Providers

A provider connects qmsg to one messaging platform. Providers are Rust crates
built against `qmsg-sdk` and compiled to `wasm32-wasip2` components. Each
provider owns its network connections and has full TCP and UDP access.

Each provider runs on its own thread and talks to the orchestrator over a
WebSocket on localhost, sending postcard-encoded `qmsg-types` messages.
`wit/qmsg.wit` only exports the entry point and should not need to change.
Change the types instead. While qmsg is in development, rebuild the host and
providers together after changing the types; we keep `ABI_VERSION` unchanged.
Postcard can't decode a layout it doesn't know, and a provider only loads when
its version matches the orchestrator's. Each
side refuses to send a message over 64 MiB rather than drop the connection.

Providers report the organizations they are part of, such as Discord servers
or Slack workspaces, with `Context::directory`. An organization holds users
and channels, and says which user is the account itself. Platforms with users
and channels outside any container, such as Discord direct messages or Signal
groups, report them with no organization. Slack direct messages belong to
their workspace, so they go in its organization.

Each channel has a kind (text, voice, video, announcement, forum, thread,
category, direct, group, or a custom one), an optional parent channel and
member list, and the content it accepts with that platform's limits: the
largest text and files, the MIME types, and how many attachments and bytes a
message can have. A kind can have several rules, such as small WebP stickers
and larger images of any type, and text can be measured in characters, UTF-16
units or bytes. Providers report the limits that apply to the account, and
`Channel::check` tells either side exactly how a message breaks them.

A provider sends a whole organization when it joins, then upserts or removes
single users and channels as they change. The orchestrator passes these on as
`ProviderEvent::Directory`. Every event carries a `ProviderId { name, instance }`,
which also comes from `ProviderHandle::id()`. Each spawn gets a unique instance
number across all orchestrators in this process; these are not persistent ids.
`Directory` takes that full id for updates, lookups and `remove_provider` on
`Exited`. A delayed exit or update from an old instance cannot change a new
instance's state, even when consumers merge separate event channels.

`Exited` is the last event of its own instance. There is no order between
instances. A name becomes free after its provider stops and its connection
finishes draining, including secret operations; sending `Exited` can still
wait for channel space. A replacement never waits for that exit to be read.
Secrets stay keyed by name so the replacement can use them. `kill()` and
handle drop stop the provider and discard events still waiting for room.
Already queued events remain, and `Exited` follows when there is room.

### Functions and input helpers

Providers answer `Command::Functions` with `Reply::Functions` to declare the
functions available in a context: the whole provider, an organization, a
channel, or one message. `ProviderHandle::functions` reads this list. Include
all verification and completion functions referenced by inputs in that list.
Ids are unique within the provider. Fetch the list again when context,
permissions or the provider's status change; discovery is a snapshot, not
permission to run an action.

A `Function` has an id, display label, description, kind, and named inputs.
Inputs declare their type, whether they are required, a display hint, and
optional ids of verification and completion functions. Hints are `Plain`,
`Secret` (hide it, such as a password), `Multiline`, or `Choices` (only these
values). `Function::check` checks required, unknown, and wrongly typed inputs,
and choices, locally. Providers must check again when called, including
permissions, platform limits, and whether ids still exist. Verification is a
preview of validity; it does not replace checks on execution.

Value types say what a function takes and returns. `ValueType::Record` lists
its fields, each with an id, label, type and whether it is required, so
clients can show typed results and forms. An empty field list takes any
record. Values support text, booleans, integers, lists, records, bytes,
channels, messages and message content (including blobs). `Value::Null` is an
explicit result for actions with no return value.

The orchestrator checks the provider's answers. `ProviderHandle::functions`
refuses declarations with repeated ids, helper references that don't resolve
to a helper of a fitting type, choices of the wrong type, and standard actions
that break their contract. `call_function`, `verify` and `complete` take the
function's declaration: they check inputs before asking the provider, and
refuse results and suggestions of the wrong type as `RequestError::BadReply`.

`FunctionKind::Action` declares the action and its result type. Standard
actions have a fixed contract, so one client form works for every provider.
`Function::action` builds one with the contract's inputs; providers may add
optional inputs of their own.

| Action | Context | Inputs | Result |
| --- | --- | --- | --- |
| `SendMessage` | channel | `content`, optional `reply_to` and `nonce` | message id |
| `EditMessage` | message | `content` | null |
| `DeleteMessage` | message | none | null |
| `OpenChannel` | provider or organization | `members`, a list of text | channel |
| `AddReaction`, `RemoveReaction` | message | `reaction` | null |
| `MarkRead`, `MarkUnread` | message | none | null |
| `Typing` | channel | none | null |
| `Login` | provider or organization | the provider's own | null |
| `Logout` | provider or organization | none | null |

`ActionKind::Custom` actions describe their own inputs and result, and need no
new protocol variants. For example, a provider can offer these functions for
a message the account sent:

```rust
use qmsg_types::*;

let edit = Function::action("edit", "Edit message", ActionKind::EditMessage);
let delete = Function::action("delete", "Delete message", ActionKind::DeleteMessage);
```

Use `FunctionKind::Lookup(result_type)` for general queries outside actions,
such as finding users. `ProviderHandle::call_function` sends `Command::Call`
with the function id, context, and an argument map. The provider answers with
`Reply::Value`.

`FunctionKind::Verification(type)` checks a value of that type, and
`FunctionKind::Completion(type)` suggests values of that type. For a list
input, a helper can work on one item. `ProviderHandle::verify` sends
`Command::Verify`. Its `VerificationRequest` carries the helper's function id,
context, other input values, the value to check, and an optional
`FunctionInputRef` identifying the parent function and input. The provider
answers with `Reply::Verified(Verification::Valid)` or `Verification::Invalid`
with one or more input issues. Each issue has an input id (or `None` for the
whole form), a stable code, and a message to display. A value of the wrong
type is invalid without asking the provider.

`ProviderHandle::complete` sends `Command::Complete`. `CompletionRequest`
carries the same context and input reference, plus query text, a page limit,
and an optional opaque cursor. The provider answers with `Reply::Completed`:
items have a display label, optional description, and the typed value to use
as input. An empty page means no matches. Pages must respect the limit. Pass
`next_cursor` with the same context, arguments, input and query to get
another page.

Both helpers accept partial input sets while a user types. `call.arguments`
holds the other inputs of the parent function when `input` is present, or the
helper's own named inputs for a standalone request. Set `input` to `None` to
use helpers independently of actions. Lookups, verification and completion
must not perform actions or change platform state.

Request failure differs from invalid input or no matches. Providers return
`CommandError::InvalidInput`, `UnknownFunction`, `Forbidden`, `RateLimited`,
`LoginRequired`, or another existing error. `CommandError::Provider` adds a
stable platform error code, display message, and optional typed details, such
as partial progress. Calls use the same timeouts, event-before-answer order,
and delivery rules as other commands: actions emit their message events, such
as `Edited`, before answering.

### Provider status and login

Providers report where they are with `Context::status`. The orchestrator
passes it on as `ProviderEvent::Status`, and `ProviderHandle::status` returns
the last one reported. A provider starts as `Connecting`, then reports:

- `LoginRequired(step)` when the account must log in,
- `Syncing` before it sends its directory,
- `Ready` once the directory is sent, along with what it missed,
- `Disconnected { reason }` when it lost its connection and is trying again,
- `Failed(reason)` when it can't go on without help.

Commands before `Ready` can fail, such as with `CommandError::LoginRequired`.

Logging in reuses functions and secrets. Each `LoginStep` has a message to
show, and any of: the id of an `ActionKind::Login` function to fill in and
call in the provider context, a link to open in a browser, and text to show as
a QR code. The login function's inputs say what to ask, such as a password
with the `Secret` hint, or a code. A wrong answer is
`CommandError::InvalidInput` on that input. The provider reports the next
status, another `LoginRequired` or `Syncing`, before it answers the call, so
the client never waits for a step it has already been told about. A QR code
that changes is reported again. Providers keep what lets them log in again,
such as a session token, with `secret_set`, so a restart doesn't ask. A
`Logout` function deletes it, reports `DirectoryUpdate::Reset`, and starts
over.

### History and sync

`ProviderHandle::history` sends `Command::History` for a channel and answers
with a `HistoryPage`: at most `limit` messages, oldest first, all in that
channel. The first request has no cursor and returns the newest page; pass
`next_cursor` for older ones. `None` means the start of the channel.
`ProviderHandle::get_message` reads one message, such as the one an edit or a
reply refers to, or fails with `CommandError::UnknownMessage`.

These rules keep clients in step:

- **Duplicates.** Events can repeat, and history can overlap live messages.
  Keep messages by channel and id: a `Received` message with a known id
  replaces the old one.
- **Sync.** Each time a provider connects, it reports `Syncing`, then
  `DirectoryUpdate::Reset` and a full snapshot of its directory, then `Ready`.
  A reset forgets everything that provider instance reported before.
- **Missed events.** After a reconnect, a provider replays what it missed as
  events, if the platform can. If it can't, it reports `MessageEvent::Gap` for
  each channel that may have missed events, before `Ready`; reload that
  channel's newest history page.
- **New instances.** A provider that restarts is a new instance. Clients that
  kept messages from an earlier one should reload history.

### Message features

Messages carry when they were last edited, their reactions (a key, a count,
and whether the account is one of them), and the client's nonce for messages
the account sent. `Content::Formatted` holds text with styled ranges: bold,
italic, underline, strikethrough, code, code blocks, quotes, spoilers, links,
and mentions of users, channels, roles or everyone. Ranges are in UTF-8 bytes;
the text reads well without them. Formatted text counts toward the channel's
text limit, and channels say whether they take it with `ContentKind::Formatted`.
Anything else a platform has stays `Content::Custom`.

Providers also emit:

- `Edited`, with the time of the edit, and `Deleted`,
- `Reacted`, when a user adds or removes a reaction,
- `Read`, when a user, including the account, has read a channel up to a
  message,
- `Typing`, which lasts until the user sends a message or for
  `TYPING_TIMEOUT_MS` unless repeated.

Presence is part of the directory: `DirectoryUpdate::Presence` says whether a
user is online, idle, busy or offline. The standard actions above send
reactions, read markers and typing.

### Responsiveness and send tracking

A provider runs on one thread, but must hear its platform while it answers
the orchestrator. `Context::wait` sleeps until a command arrives, a blob read
or name lookup is answered, or one of the provider's own sockets can be read
or written. Providers use non-blocking sockets, look names up with
`Context::start_lookup` and `take_lookup`, connect with
`qmsg_sdk::start_connect`, read the orchestrator's files with
`Context::start_read` and `take_read`, and
keep slow work, such as an upload or a send waiting for the platform's
answer, as state to finish later. Then autocomplete, other commands and
`Shutdown` are answered while it runs. See the `qmsg-sdk` docs for the loop.
The orchestrator reads blobs for a provider beside its other messages, so a
slow source holds up only the reads waiting on it. At most `MAX_BLOB_READS`
run at once; more fail at once without reading, so stalled sources never
stop the provider's other messages from being read.

Name lookups run in the orchestrator, since WASI can't wait for a lookup and
for sockets at once, so one in the provider would block it. They are
answered like blob reads, at most `MAX_LOOKUPS` per provider at once.
`Orchestrator::set_resolver` replaces the OS resolver, such as to use a
proxy. `start_connect` doesn't wait on WASI, where providers run, or on
Linux; elsewhere it fails as unsupported rather than wait.

Blob sources and lookups can block, so each runs on a blocking thread. An
orchestrator runs at most `MAX_BLOCKING` of them at once, for all its
providers, and more fail at once. A provider waits `BLOCKING_TIMEOUT` for
one, then gets a failure. A blocking thread can't be stopped, so one stuck
in a source or resolver keeps its place, even after its provider has
disconnected or exited, until it returns or panics. Stuck work never piles
up past `MAX_BLOCKING` threads, but while that many are stuck every blob
read and lookup fails.

A request that times out or is dropped is cancelled: the orchestrator sends
`Command::Cancel`. The SDK skips a cancelled command that the provider hasn't
taken yet and answers it with `CommandError::Cancelled`, first reporting a
skipped send with a nonce, by `Send` or a declared `SendMessage` function, as
`MessageEvent::NotSent`. A provider can check `Context::is_cancelled` before
a step it can't undo. Once that step is done, it finishes as usual:
cancelling never makes a command that already ran safe to retry.

Each provider can have `MAX_REQUESTS` requests, and `MAX_QUEUED` bytes of
them, unanswered. A request given up on still counts until the provider
answers or skips it, since it may still be queued or running, so a stalled
provider can't make the orchestrator queue more. Past the limit, requests
fail with `RequestError::Busy` and nothing is sent. Cancelling, releasing
blobs and `shutdown` don't count, so they always get through.

To settle a send whose answer never came, pass a nonce unique to it to
`send_message`. Later, a `Message` with that nonce means it was sent, and
`MessageEvent::NotSent` with that nonce means it never reached the platform
and is safe to send again. Without either, delivery stays unknown.

Each blob id a provider sends belongs to the one event or reply it came in.
A provider that returns a file again, such as in history, shares it under a
new id. So the orchestrator can release the blobs in an answer no one reads,
such as one that comes after its request was given up or that doesn't fit
the request, without taking a file from anyone else.

### The echo provider

Echo demonstrates all of this against its line-based TCP server, which
answers each line with `echo: <line>`; a file is a line
`file <name>: <size> bytes` followed by its bytes. Its
`users` lookup returns records with `id` and `name` fields; `open-channel`
follows the standard contract and uses `verify-user` and paged
`complete-user` on each member. The helpers also work as standalone requests.
In a channel it offers a standard `send`; on a message it offers edit and
delete (for the account's own), reactions with `complete-reaction`, and read
markers. The server has no edits, reactions or read markers, so these change
only echo's in-memory history, which also answers history requests.

Echo never blocks on the server or the orchestrator. It connects, reads the
files it is given, and writes to the server without waiting, so server lines
that aren't answers, such as its greeting, are emitted as they arrive, and
commands are answered while it connects or reconnects, while an upload waits
for a slow file or a server that stopped reading, and while a send waits for
the server's answers. Sends are written one at a time, after a last check that
they weren't cancelled; one cancelled or failing before that is reported as
`NotSent`. With a `password` setting, echo asks for the password and then the
`code` setting before it syncs, and keeps the session as a secret until
`logout`. When the server goes away, echo fails the sends still waiting,
reconnects, resets its directory and reports a `Gap`. A lookup of the
server's name that fails or times out is retried like a failed connection.
Closing a socket, even after shutting it down, waits for the write wasmtime
has already taken, which a server that stopped reading never finishes; the
runtime has no way to call it off. So echo leaves such a connection open
until it exits rather than block. After four, it fails, and exiting closes
them all, since the provider's store and runtime go with it.

Messages have an id, a timestamp and an optional message they reply to. Their
content is a list of parts, so one message can carry text, images, video,
audio, files and custom content together. Providers emit new messages, edits
and deletions, including the account's own messages.

The orchestrator commands a provider through `ProviderHandle`, whose methods
wait for the answer: `send_message` returns the new message's id,
`open_channel` opens or finds a conversation with some users, including ones
the provider hasn't reported, such as an email address, and `read_blob` reads
a file the provider sent. Failures say why, such as a limit the content
breaks, a rate limit or a missing permission. Requests give up after a minute
by default. Drain events on a separate task from requests: an answer may wait
behind events in a full channel. Keep timeouts enabled to bound the wait if
the consumer stalls. A timeout, exit before answering or kill leaves delivery
unknown; the server may have processed the command, so do not retry blindly.
Use a nonce to find out later, as described above.

Echo queues the sent message, with its nonce, and all server answers before
answering success. If the server goes away first, it queues the answers it
has received (without a `reply_to`, since the whole message was not sent),
then answers with a `send_incomplete` error whose details hold
`confirmed_lines`, `total_lines` and `delivery_unknown`: the other lines may
have reached the server. Either answer can wait for event queue space. After
receiving an answer, callers can kill or drop echo without losing those queued
events. A timeout is not an answer; killing before an answer can lose events.
This is an in-memory ordering guarantee, not durable or exactly-once delivery.

Files of any size, or that only the provider can download, travel as blobs:
the side that sends one keeps it, and the other reads it in pieces of up to
4 MiB. The orchestrator offers a file to one provider with
`ProviderHandle::add_blob`, until it drops the `Blob`. Providers read it with
`Context::start_read`, or `Context::blob_reader` where waiting is fine, and
offer their own with `Context::share`, or by answering `Command::ReadBlob`
themselves. A provider keeps its blobs until the orchestrator releases them
with `ProviderHandle::release_blob`.

Providers keep secrets, such as tokens, with `secret_get`, `secret_set` and
`secret_delete` in the SDK. The orchestrator stores them in SQLite at
`<data_dir>/qmsg.db` and encrypts them with XChaCha20-Poly1305 under a master
key held in the OS keychain: Keychain on macOS, Credential Manager on Windows
and Secret Service on Linux. Each data directory has its own key, and only one
orchestrator can use a data directory at a time. Without a keychain, secrets
are stored as plain text. `data_dir` defaults to `~/.qmsg`; the example config
sets it to `.qmsg` in the current directory.

The example provider connects to a line-based TCP server, so start one first.
Each line you type into it reaches the provider as a message from the server.
Nothing answers the provider's lines, so its sends stay waiting:

```sh
nc -lk 127.0.0.1 7000
```

Then build the provider and run the orchestrator:

```sh
rustup target add wasm32-wasip2
cargo build -p qmsg-provider-echo --target wasm32-wasip2
cp orchestrator.example.toml orchestrator.toml
cargo run --bin qmsg-orchestrator
```

## Development

Run the main app:

```sh
cargo run --bin qmsg
```

Build or test all crates:

```sh
cargo build
cargo test
```

The orchestrator's tests build the echo provider themselves, so they need the
`wasm32-wasip2` target installed.
