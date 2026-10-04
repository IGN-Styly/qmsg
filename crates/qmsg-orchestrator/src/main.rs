use std::path::PathBuf;

use anyhow::Context as _;
use qmsg_orchestrator::{Directory, Encryption, Orchestrator, ProviderEvent, ProviderSpec};
use qmsg_types::{Content, DirectoryUpdate, MessageEvent};
use serde::Deserialize;
use tokio::sync::mpsc;

#[derive(Deserialize)]
struct Config {
    /// Where secrets are kept. Defaults to `~/.qmsg`; relative paths are from
    /// the current directory.
    data_dir: Option<PathBuf>,
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

    let data_dir = match config.data_dir {
        Some(dir) => dir,
        None => std::env::home_dir()
            .context("no home directory, set `data_dir` in the config")?
            .join(".qmsg"),
    };
    let orchestrator = Orchestrator::new(&data_dir, Encryption::Keychain).await?;
    let (events_tx, mut events) = mpsc::channel(256);
    let mut handles = Vec::new();
    for spec in config.providers {
        handles.push(orchestrator.spawn(spec, events_tx.clone())?);
    }
    drop(events_tx);

    let mut directory = Directory::new();
    let mut running = handles.len();
    let mut shutting_down = false;
    while running > 0 {
        tokio::select! {
            Some(event) = events.recv() => match event {
                ProviderEvent::Message { provider, event } => match event {
                    MessageEvent::Received(message) => {
                        let author = directory
                            .author(&provider, &message)
                            .map_or(message.author.as_str(), |u| u.name.as_str());
                        tracing::info!(
                            provider,
                            organization = message.channel.organization.as_deref().unwrap_or("-"),
                            channel = message.channel.channel,
                            id = message.id,
                            author,
                            "{}",
                            describe(&message.content),
                        );
                    }
                    MessageEvent::Edited { channel, id, content } => {
                        tracing::info!(provider, channel = channel.channel, id, "edited: {}", describe(&content));
                    }
                    MessageEvent::Deleted { channel, id } => {
                        tracing::info!(provider, channel = channel.channel, id, "deleted");
                    }
                },
                ProviderEvent::Directory { provider, update } => {
                    if let DirectoryUpdate::OrganizationUpserted(organization) = &update {
                        tracing::info!(
                            provider,
                            organization = organization.id,
                            users = organization.users.len(),
                            channels = organization.channels.len(),
                            "joined {}",
                            organization.name,
                        );
                    } else {
                        tracing::debug!(provider, "{update:?}");
                    }
                    if let Err(e) = directory.apply(&provider, update) {
                        tracing::warn!(provider, "ignored directory update: {e}");
                    }
                }
                ProviderEvent::Exited { provider, result } => {
                    running -= 1;
                    directory.remove_provider(&provider);
                    match result {
                        Ok(()) => tracing::info!(provider, "exited"),
                        Err(e) => tracing::error!(provider, "exited: {e}"),
                    }
                }
            },
            _ = tokio::signal::ctrl_c() => {
                if shutting_down {
                    // Killed providers still send `Exited`, right away.
                    tracing::warn!("killing providers");
                    for handle in &handles {
                        handle.kill();
                    }
                    continue;
                }
                tracing::info!("shutting down, press Ctrl-C again to kill");
                shutting_down = true;
                for handle in &handles {
                    let _ = handle.shutdown();
                }
            }
        }
    }
    Ok(())
}

/// Message content as one line, with attachments in brackets.
fn describe(content: &[Content]) -> String {
    let parts: Vec<String> = content
        .iter()
        .map(|part| match part {
            Content::Text(text) => text.clone(),
            Content::Image(_) => "[image]".into(),
            Content::Video(_) => "[video]".into(),
            Content::Audio(_) => "[audio]".into(),
            Content::File(_) => "[file]".into(),
            Content::Custom { kind, .. } => format!("[{kind}]"),
        })
        .collect();
    parts.join(" ")
}
