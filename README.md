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
Change the types instead, appending new enum variants at the end, and bump
`ABI_VERSION` for breaking changes.

Build the example provider and run it:

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
