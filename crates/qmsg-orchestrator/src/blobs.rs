//! Files the orchestrator offers to providers as [`MediaSource::Blob`]s.
//!
//! Providers read them in pieces of at most [`MAX_READ`] bytes, so a blob can
//! be any size. Blob ids are random, and only the provider sent one can read
//! it.

use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::{Arc, Mutex, Weak};

use qmsg_types::{MAX_READ, Media, MediaSource};

/// Something a [`Blob`] reads from.
pub trait BlobSource: Send + Sync + 'static {
    fn size(&self) -> u64;

    /// Reads up to `len` bytes starting at `offset`. Fewer only at the end.
    fn read_at(&self, offset: u64, len: usize) -> io::Result<Vec<u8>>;
}

impl BlobSource for Vec<u8> {
    fn size(&self) -> u64 {
        self.len() as u64
    }

    fn read_at(&self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        let start = usize::try_from(offset).map_or(self.len(), |o| o.min(self.len()));
        let end = start + len.min(self.len() - start);
        Ok(self[start..end].to_vec())
    }
}

/// A file on disk, read as it is when each piece is read.
pub struct FileBlob {
    file: Mutex<File>,
    size: u64,
}

impl FileBlob {
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = File::open(path)?;
        let size = file.metadata()?.len();
        Ok(Self {
            file: Mutex::new(file),
            size,
        })
    }
}

impl BlobSource for FileBlob {
    fn size(&self) -> u64 {
        self.size
    }

    fn read_at(&self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(offset))?;
        let mut buf = Vec::with_capacity(len);
        file.by_ref().take(len as u64).read_to_end(&mut buf)?;
        Ok(buf)
    }
}

pub(crate) type Blobs = Mutex<HashMap<String, Arc<dyn BlobSource>>>;

/// A blob providers can read. Dropping it stops them.
pub struct Blob {
    id: String,
    size: u64,
    blobs: Weak<Blobs>,
}

impl Blob {
    pub(crate) fn new(blobs: &Arc<Blobs>, id: String, source: impl BlobSource) -> Self {
        let size = source.size();
        blobs.lock().unwrap().insert(id.clone(), Arc::new(source));
        Self {
            id,
            size,
            blobs: Arc::downgrade(blobs),
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    /// Media that sends this blob.
    pub fn media(&self, name: Option<String>, mime: Option<String>) -> Media {
        Media {
            name,
            mime,
            size: Some(self.size),
            source: MediaSource::Blob(self.id.clone()),
        }
    }
}

impl Drop for Blob {
    fn drop(&mut self) {
        if let Some(blobs) = self.blobs.upgrade() {
            blobs.lock().unwrap().remove(&self.id);
        }
    }
}

/// Reads part of a blob for a provider, off the async threads since sources
/// can block.
pub(crate) async fn read(
    blobs: &Weak<Blobs>,
    id: &str,
    offset: u64,
    len: u32,
) -> Result<Vec<u8>, String> {
    let blobs = blobs.upgrade().ok_or("the orchestrator has shut down")?;
    let source = blobs
        .lock()
        .unwrap()
        .get(id)
        .cloned()
        .ok_or_else(|| format!("unknown blob `{id}`"))?;
    let len = len.min(MAX_READ) as usize;
    tokio::task::spawn_blocking(move || source.read_at(offset, len))
        .await
        .expect("blob source panicked")
        .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    #[test]
    fn reads_pieces_of_bytes_and_files() {
        let bytes: Vec<u8> = (0..=255).collect();
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(&bytes).unwrap();
        let sources: [Box<dyn BlobSource>; 2] = [
            Box::new(bytes.clone()),
            Box::new(FileBlob::open(file.path()).unwrap()),
        ];
        for source in sources {
            assert_eq!(source.size(), 256);
            assert_eq!(source.read_at(10, 3).unwrap(), [10, 11, 12]);
            assert_eq!(source.read_at(250, 100).unwrap(), &bytes[250..]);
            assert!(source.read_at(300, 10).unwrap().is_empty());
        }
    }

    #[test]
    fn dropping_a_blob_removes_it() {
        let blobs = Arc::new(Blobs::default());
        let blob = Blob::new(&blobs, "id".into(), vec![1, 2, 3]);
        assert_eq!(blob.media(None, None).size, Some(3));
        assert!(blobs.lock().unwrap().contains_key("id"));
        drop(blob);
        assert!(blobs.lock().unwrap().is_empty());
    }
}
