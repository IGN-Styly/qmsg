# qmsg
simple messaging for everything and everyone.

This Cargo workspace contains three crates:

| Crate | Output | Purpose |
| --- | --- | --- |
| `crates/qmsg` | `qmsg` binary | Main app |
| `crates/qmsg-orchestrator` | `qmsg-orchestrator` binary | Orchestrator |
| `crates/qmsg-sdk` | `qmsg_sdk` library | SDK |

Run the main app:

```sh
cargo run
```

Run the orchestrator:

```sh
cargo run -p qmsg-orchestrator
```

Build or test all crates:

```sh
cargo build --workspace
cargo test --workspace
```

The orchestrator and SDK are empty starting points. Add new crates under
`crates/` and list them in the root `Cargo.toml` workspace members. Shared
package settings live in `[workspace.package]`.
