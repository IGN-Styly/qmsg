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
Change the types instead. Any change to them, including a new enum variant,
needs an `ABI_VERSION` bump: postcard can't decode a layout it doesn't know,
and a provider only loads when its version matches the orchestrator's. Each
side refuses to send a message over 64 MiB rather than drop the connection.

Providers report the organizations they are part of, such as Discord servers
or Slack workspaces, with `Context::directory`. An organization holds users
and channels, and says which user is the account itself. Platforms with users
and channels outside any container, such as Discord direct messages or Signal
groups, report them with no organization. Slack direct messages belong to
their workspace, so they go in its organization.

Each channel has a kind (text, voice, video, announcement, forum, thread,
category, direct, group, or a custom one), an optional parent channel and
member list, and the content it accepts. A provider sends a whole organization
when it joins, then upserts or removes single users and channels as they
change. The orchestrator passes these on as `ProviderEvent::Directory`, and
`Directory` keeps the current state for each provider.

Messages have an id, a timestamp and an optional message they reply to. Their
content is a list of parts, so one message can carry text, images, video,
audio, files and custom content together. Providers emit new messages, edits
and deletions, including the account's own messages. Each `Command::Send`
carries a request number, and the provider answers it with the new message's
id or an error.

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
