//! Providers' secret storage, kept in SQLite.
//!
//! Values are encrypted with XChaCha20-Poly1305 under a master key that lives
//! in the OS keychain: Keychain on macOS, Credential Manager on Windows and
//! Secret Service on Linux. Without a keychain, values are stored as plain
//! text. Each row records whether it is encrypted, so rows written either way
//! stay readable once a keychain is available, as long as the key is unchanged.

use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::Context as _;
use chacha20poly1305::aead::{Aead, Generate, Payload};
use chacha20poly1305::{Key, KeyInit, XChaCha20Poly1305, XNonce};
use keyring_core::api::CredentialStore;
use rusqlite::{Connection, OptionalExtension, params};

/// Total key and value bytes a provider may keep in secret storage.
const LIMIT: usize = 1024 * 1024;
/// Identifies the master key in the OS keychain.
const KEYCHAIN_SERVICE: &str = "qmsg";
const KEYCHAIN_USER: &str = "master-key";

/// How secret values are protected at rest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Encryption {
    /// Encrypt with a master key from the OS keychain, falling back to plain
    /// text when there is no keychain.
    Keychain,
    /// Store values as plain text.
    Plaintext,
}

pub(crate) struct Secrets {
    db: Mutex<Connection>,
    /// `None` stores values as plain text.
    cipher: Option<XChaCha20Poly1305>,
}

impl Secrets {
    /// Opens `qmsg.db` in `dir`, creating both if needed.
    pub(crate) fn open(dir: &Path, encryption: Encryption) -> anyhow::Result<Self> {
        create_dir(dir).with_context(|| format!("creating {}", dir.display()))?;
        let path = dir.join("qmsg.db");
        restrict(&path).with_context(|| format!("securing {}", path.display()))?;
        let db = Connection::open(&path).with_context(|| format!("opening {}", path.display()))?;
        let mut secrets = Self::new(db, None)?;
        if encryption == Encryption::Keychain {
            match master_key() {
                Ok(key) => secrets.cipher = Some(XChaCha20Poly1305::new(&key)),
                // Encrypted rows mean a keychain worked before, so this is a
                // fault, not a machine without one. Falling back would store
                // new secrets as plain text where they used to be encrypted.
                Err(e) if secrets.has_encrypted()? => {
                    return Err(
                        e.context("OS keychain unavailable, but stored secrets are encrypted")
                    );
                }
                Err(e) => tracing::warn!("no OS keychain, storing secrets as plain text: {e:#}"),
            }
        }
        Ok(secrets)
    }

    fn has_encrypted(&self) -> anyhow::Result<bool> {
        Ok(self.db.lock().unwrap().query_row(
            "SELECT EXISTS (SELECT 1 FROM secrets WHERE nonce IS NOT NULL)",
            [],
            |row| row.get(0),
        )?)
    }

    fn new(db: Connection, cipher: Option<XChaCha20Poly1305>) -> anyhow::Result<Self> {
        db.execute_batch(
            "CREATE TABLE IF NOT EXISTS secrets (
                provider TEXT NOT NULL,
                key TEXT NOT NULL,
                -- NULL when `value` is plain text.
                nonce BLOB,
                value BLOB NOT NULL,
                PRIMARY KEY (provider, key)
            ) WITHOUT ROWID",
        )?;
        Ok(Self {
            db: Mutex::new(db),
            cipher,
        })
    }

    pub(crate) fn get(&self, provider: &str, key: &str) -> Result<Option<Vec<u8>>, String> {
        let row: Option<(Option<Vec<u8>>, Vec<u8>)> = self
            .db
            .lock()
            .unwrap()
            .query_row(
                "SELECT nonce, value FROM secrets WHERE provider = ?1 AND key = ?2",
                params![provider, key],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|e| format!("reading secret: {e}"))?;
        let Some((nonce, value)) = row else {
            return Ok(None);
        };
        let Some(nonce) = nonce else {
            return Ok(Some(value));
        };
        let cipher = self
            .cipher
            .as_ref()
            .ok_or("secret is encrypted, but the OS keychain is unavailable")?;
        let nonce = XNonce::try_from(nonce.as_slice()).map_err(|_| "secret has a bad nonce")?;
        cipher
            .decrypt(
                &nonce,
                Payload {
                    msg: &value,
                    aad: &aad(provider, key),
                },
            )
            .map(Some)
            .map_err(|_| "secret can't be decrypted with the keychain's master key".into())
    }

    pub(crate) fn set(&self, provider: &str, key: &str, value: &[u8]) -> Result<(), String> {
        let too_big = || format!("secret storage is limited to {LIMIT} bytes");
        if key.len() + value.len() > LIMIT {
            return Err(too_big());
        }
        let (nonce, value) = match &self.cipher {
            Some(cipher) => {
                let nonce = XNonce::generate();
                let value = cipher
                    .encrypt(
                        &nonce,
                        Payload {
                            msg: value,
                            aad: &aad(provider, key),
                        },
                    )
                    .map_err(|_| "encrypting secret failed")?;
                (Some(nonce.to_vec()), value)
            }
            None => (None, value.to_vec()),
        };

        let mut db = self.db.lock().unwrap();
        let tx = db
            .transaction()
            .map_err(|e| format!("writing secret: {e}"))?;
        let others: i64 = tx
            .query_row(
                "SELECT COALESCE(SUM(length(CAST(key AS BLOB)) + length(value)), 0)
                FROM secrets WHERE provider = ?1 AND key != ?2",
                params![provider, key],
                |row| row.get(0),
            )
            .map_err(|e| format!("writing secret: {e}"))?;
        if others as usize + key.len() + value.len() > LIMIT {
            return Err(too_big());
        }
        tx.execute(
            "INSERT OR REPLACE INTO secrets (provider, key, nonce, value) VALUES (?1, ?2, ?3, ?4)",
            params![provider, key, nonce, value],
        )
        .and_then(|_| tx.commit())
        .map_err(|e| format!("writing secret: {e}"))
    }

    pub(crate) fn delete(&self, provider: &str, key: &str) -> Result<(), String> {
        self.db
            .lock()
            .unwrap()
            .execute(
                "DELETE FROM secrets WHERE provider = ?1 AND key = ?2",
                params![provider, key],
            )
            .map(|_| ())
            .map_err(|e| format!("deleting secret: {e}"))
    }
}

/// Binds a ciphertext to its row, so it can't be moved to another key or
/// provider.
fn aad(provider: &str, key: &str) -> Vec<u8> {
    qmsg_types::encode(&(provider, key)).expect("strings always encode")
}

/// Loads the master key from the OS keychain, creating it on first use.
fn master_key() -> anyhow::Result<Key> {
    let entry = keychain()?.build(KEYCHAIN_SERVICE, KEYCHAIN_USER, None)?;
    match entry.get_secret() {
        Ok(bytes) => Key::try_from(bytes.as_slice())
            .map_err(|_| anyhow::anyhow!("the keychain's master key has the wrong length")),
        Err(keyring_core::Error::NoEntry) => {
            let key = Key::generate();
            entry.set_secret(&key)?;
            tracing::info!("created a master key in the OS keychain");
            Ok(key)
        }
        Err(e) => Err(e.into()),
    }
}

#[cfg(target_os = "macos")]
fn keychain() -> anyhow::Result<Arc<CredentialStore>> {
    Ok(apple_native_keyring_store::keychain::Store::new()?)
}

#[cfg(windows)]
fn keychain() -> anyhow::Result<Arc<CredentialStore>> {
    Ok(windows_native_keyring_store::Store::new()?)
}

#[cfg(target_os = "linux")]
fn keychain() -> anyhow::Result<Arc<CredentialStore>> {
    Ok(zbus_secret_service_keyring_store::Store::new()?)
}

#[cfg(not(any(target_os = "macos", windows, target_os = "linux")))]
fn keychain() -> anyhow::Result<Arc<CredentialStore>> {
    anyhow::bail!("no supported keychain on this platform")
}

/// Creates the data directory. On Unix only its owner can enter it, since
/// secrets may be stored as plain text.
fn create_dir(dir: &Path) -> std::io::Result<()> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
    builder.create(dir)
}

/// Makes the database readable only by its owner on Unix, since secrets may
/// be stored as plain text.
fn restrict(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        // Otherwise the permissions of whatever it points to would change.
        if std::fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_symlink()) {
            return Err(std::io::Error::other("is a symlink"));
        }
        // Created with the right mode, so it is never briefly readable.
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .open(path)?
            .set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encrypted() -> Secrets {
        let cipher = XChaCha20Poly1305::new(&Key::generate());
        Secrets::new(Connection::open_in_memory().unwrap(), Some(cipher)).unwrap()
    }

    fn raw_value(secrets: &Secrets, provider: &str, key: &str) -> Vec<u8> {
        secrets
            .db
            .lock()
            .unwrap()
            .query_row(
                "SELECT value FROM secrets WHERE provider = ?1 AND key = ?2",
                params![provider, key],
                |row| row.get(0),
            )
            .unwrap()
    }

    #[test]
    fn round_trips_encrypted() {
        let secrets = encrypted();
        assert_eq!(secrets.get("a", "token"), Ok(None));
        secrets.set("a", "token", b"hunter2").unwrap();
        assert_eq!(secrets.get("a", "token"), Ok(Some(b"hunter2".to_vec())));
        assert_ne!(raw_value(&secrets, "a", "token"), b"hunter2");

        secrets.set("a", "token", b"changed").unwrap();
        assert_eq!(secrets.get("a", "token"), Ok(Some(b"changed".to_vec())));
        secrets.delete("a", "token").unwrap();
        assert_eq!(secrets.get("a", "token"), Ok(None));
    }

    #[test]
    fn plaintext_is_stored_as_is() {
        let secrets = Secrets::new(Connection::open_in_memory().unwrap(), None).unwrap();
        secrets.set("a", "token", b"hunter2").unwrap();
        assert_eq!(raw_value(&secrets, "a", "token"), b"hunter2");
        assert_eq!(secrets.get("a", "token"), Ok(Some(b"hunter2".to_vec())));
    }

    #[test]
    fn providers_are_separate() {
        let secrets = encrypted();
        secrets.set("a", "token", b"for a").unwrap();
        assert_eq!(secrets.get("b", "token"), Ok(None));
    }

    #[test]
    fn moved_ciphertext_does_not_decrypt() {
        let secrets = encrypted();
        secrets.set("a", "token", b"for a").unwrap();
        secrets
            .db
            .lock()
            .unwrap()
            .execute(
                "INSERT INTO secrets SELECT 'b', key, nonce, value FROM secrets WHERE provider = 'a'",
                [],
            )
            .unwrap();
        assert!(secrets.get("b", "token").is_err());
    }

    #[test]
    fn encrypted_rows_need_the_key() {
        let dir = tempfile::tempdir().unwrap();
        let cipher = XChaCha20Poly1305::new(&Key::generate());
        let db = Connection::open(dir.path().join("qmsg.db")).unwrap();
        Secrets::new(db, Some(cipher))
            .unwrap()
            .set("a", "token", b"hunter2")
            .unwrap();

        let secrets = Secrets::open(dir.path(), Encryption::Plaintext).unwrap();
        assert!(secrets.get("a", "token").is_err());
    }

    #[test]
    fn detects_encrypted_rows() {
        let secrets = encrypted();
        assert!(
            !Secrets::new(Connection::open_in_memory().unwrap(), None)
                .unwrap()
                .has_encrypted()
                .unwrap()
        );
        secrets.set("a", "token", b"hunter2").unwrap();
        assert!(secrets.has_encrypted().unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn database_is_private() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("qmsg.db");
        std::fs::write(&path, b"").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        Secrets::open(dir.path(), Encryption::Plaintext).unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    #[cfg(unix)]
    #[test]
    fn database_symlink_is_refused() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let other = dir.path().join("other");
        std::fs::write(&other, b"").unwrap();
        std::fs::set_permissions(&other, std::fs::Permissions::from_mode(0o644)).unwrap();
        std::os::unix::fs::symlink(&other, dir.path().join("qmsg.db")).unwrap();
        assert!(Secrets::open(dir.path(), Encryption::Plaintext).is_err());
        let mode = std::fs::metadata(&other).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o644);
    }

    #[test]
    fn storage_is_limited_per_provider() {
        let secrets = encrypted();
        let half = vec![0; LIMIT / 2];
        secrets.set("a", "one", &half).unwrap();
        // Replacing a value doesn't count the old one.
        secrets.set("a", "one", &half).unwrap();
        assert!(secrets.set("a", "two", &half).is_err());
        assert!(secrets.set("c", "big", &vec![0; LIMIT + 1]).is_err());
        secrets.set("b", "two", &half).unwrap();
    }
}
