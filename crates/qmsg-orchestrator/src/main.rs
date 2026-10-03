use std::path::PathBuf;

use anyhow::Context as _;
use qmsg_orchestrator::{Orchestrator, ProviderEvent, ProviderSpec};
use qmsg_types::Command;
use serde::Deserialize;
use tokio::sync::mpsc;

#[derive(Deserialize)]
struct Config {
    #[serde(default, rename = "provider")]
    providers: Vec<ProviderSpec>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let path = PathBuf::from(
        std::env::args()
            .nth(1)
            .unwrap_or_else(|| "orchestrator.toml".into()),
    );
    let config: Config = toml::from_str(
        &tokio::fs::read_to_string(&path)
            .await
            .with_context(|| format!("reading {}", path.display()))?,
    )?;

    let orchestrator = Orchestrator::new().await?;
    let (events_tx, mut events) = mpsc::unbounded_channel();
    let mut handles = Vec::new();
    for spec in config.providers {
        handles.push(orchestrator.spawn(spec, events_tx.clone())?);
    }
    drop(events_tx);

    let mut running = handles.len();
    while running > 0 {
        tokio::select! {
            Some(event) = events.recv() => match event {
                ProviderEvent::Message { provider, message } => {
                    tracing::info!(provider, chat = message.chat, author = message.author, "{}", message.body);
                }
                ProviderEvent::Exited { provider, result } => {
                    running -= 1;
                    match result {
                        Ok(()) => tracing::info!(provider, "exited"),
                        Err(e) => tracing::error!(provider, "exited: {e}"),
                    }
                }
            },
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("shutting down");
                for handle in &handles {
                    let _ = handle.send(Command::Shutdown);
                }
            }
        }
    }
    Ok(())
}
