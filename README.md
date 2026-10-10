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
Ids are unique within the provider. Fetch the list again when context or
permissions change; discovery is a snapshot, not permission to run an action.

A `Function` has an id, display label, description, kind, and named inputs.
Inputs declare their type, whether they are required, and optional ids of
verification and completion functions. `Function::check` checks required,
unknown, and wrongly typed inputs locally. Providers must check again when
called, including permissions, platform limits, and whether ids still exist.
Verification is a preview of validity; it does not replace checks on execution.

`FunctionKind::Action` declares the action and its result type. `ActionKind`
includes send, edit, delete, open channel, and custom actions. For example,
a provider can offer these functions for a message it owns:

```rust
use qmsg_types::*;

let edit = Function {
    id: "edit".into(), label: "Edit message".into(),
    description: "Replace this message's content".into(),
    kind: FunctionKind::Action {
        action: ActionKind::EditMessage, result: ValueType::Null,
    },
    inputs: vec![FunctionInput {
        id: "content".into(), label: "Content".into(),
        description: "New message content".into(),
        value_type: ValueType::Content, required: true,
        verification: Some("verify-content".into()), completion: None,
    }],
};
let delete = Function {
    id: "delete".into(), label: "Delete message".into(),
    description: "Delete this message".into(),
    kind: FunctionKind::Action {
        action: ActionKind::DeleteMessage, result: ValueType::Null,
    },
    inputs: vec![],
};
```

Use `FunctionKind::Lookup(result_type)` for general queries outside actions,
such as finding users. `ProviderHandle::call_function` sends `Command::Call`
with the function id, context, and an argument map. The provider answers with
`Reply::Value`. Values support text, booleans, integers, lists, records, bytes,
channels, messages and message content (including blobs). `Value::Null` is an
explicit result for actions with no return value. `ValueType::Record` describes
the outer type; the provider describes and checks its fields. Custom actions
and records need no new protocol variants.

`ProviderHandle::verify` sends `Command::Verify` to a function declared as
`FunctionKind::Verification`. Its `VerificationRequest` carries the helper's
function id, context, other input values, the value to check, and an optional
`FunctionInputRef` identifying the parent function and input. The provider
answers with `Reply::Verified(Verification::Valid)` or `Verification::Invalid`
with one or more input issues. Each issue has an input id (or `None` for the
whole form), a stable code, and a message to display.

`ProviderHandle::complete` sends `Command::Complete` to a function declared as
`FunctionKind::Completion`. `CompletionRequest` carries the same context and
input reference, plus query text, a page limit, and an optional opaque cursor.
The provider answers with `Reply::Completed`: items have a display label,
optional description, and the typed value to use as input. An empty page means
no matches. Pages must respect the limit. Pass `next_cursor` with the same
context, arguments, input and query to get another page.

Both helpers accept partial input sets while a user types. `call.arguments`
holds the other inputs of the parent function when `input` is present, or the
helper's own named inputs for a standalone request. Set `input` to `None` to
use helpers independently of actions. Lookups, verification and completion
must not perform actions or change platform state.

Request failure differs from invalid input or no matches. Providers return
`CommandError::InvalidInput`, `UnknownFunction`, `Forbidden`, `RateLimited`, or
another existing error. `CommandError::Provider` adds a stable platform error
code, display message, and optional typed details, such as partial progress.
Calls use the same timeouts, event-before-answer order, and delivery rules as
existing commands. Edit/delete handlers emit the corresponding message event
before answering. The declarations above describe how providers can expose
those actions; echo's TCP server does not support them.

Echo demonstrates a `users` lookup, an `open-channel` action, `verify-user`,
and paged `complete-user`. The helpers work both on the `user` input and as
standalone requests. Incomplete sends now return the `send_incomplete` error
code with `confirmed_lines`, `total_lines`, and `last_line_delivery_unknown`
details, so clients do not have to parse a sentence to find out what happened.

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

Echo queues the sent message and all server replies before answering success.
On partial failure, it queues the replies it has received (without a `reply_to`,
since the whole message was not sent), then answers with how many lines the
server confirmed, and exits. The last failed line may also have reached the
server without a reply. Either answer can wait for event queue space. After
receiving an answer, callers can kill or drop echo without losing those queued
replies. A timeout is not an answer; killing before an answer can lose events.
This is an in-memory ordering guarantee, not durable or exactly-once delivery.

Files of any size, or that only the provider can download, travel as blobs:
the side that sends one keeps it, and the other reads it in pieces of up to
4 MiB. The orchestrator offers a file to one provider with
`ProviderHandle::add_blob`, until it drops the `Blob`. Providers read it with
`Context::blob_reader`, and offer their own with `Context::share`, or by
answering `Command::ReadBlob` themselves. A provider keeps its blobs until
the orchestrator releases them with `ProviderHandle::release_blob`.

Providers keep secrets, such as tokens, with `secret_get`, `secret_set` and
`secret_delete` in the SDK. The orchestrator stores them in SQLite at
`<data_dir>/qmsg.db` and encrypts them with XChaCha20-Poly1305 under a master
key held in the OS keychain: Keychain on macOS, Credential Manager on Windows
and Secret Service on Linux. Each data directory has its own key, and only one
orchestrator can use a data directory at a time. Without a keychain, secrets
are stored as plain text. `data_dir` defaults to `~/.qmsg`; the example config
sets it to `.qmsg` in the current directory.

The example provider connects to a line-based TCP server, so start one first.
Type a line into it to send the provider its greeting:

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
