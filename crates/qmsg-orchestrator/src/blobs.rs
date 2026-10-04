//! Files the orchestrator offers to providers as [`MediaSource::Blob`]s.
//!
//! Providers read them in pieces of at most [`MAX_READ`] bytes, so a blob can
//! be any size. Each blob belongs to one provider, which alone can read it.

use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex, Weak};

use qmsg_types::{MAX_READ, Media, MediaSource};

/// Something a [`Blob`] reads from.
pub trait BlobSource: Send + Sync + 'static {
    fn size(&self) -> u64;

    /// Reads up to `len` bytes starting at `offset`. Fewer only at the end.
    fn read_at(&self, offset: u64, len: usize) -> io::Result<Vec<u8>>;
}

impl<T: BlobSource + ?Sized> BlobSource for Arc<T> {
    fn size(&self) -> u64 {
        (**self).size()
    }

    fn read_at(&self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        (**self).read_at(offset, len)
    }
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

/// A file on disk, read as it is when each piece is read. Reads don't wait
/// for each other.
pub struct FileBlob {
    file: File,
    size: u64,
}

impl FileBlob {
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = File::open(path)?;
        let size = file.metadata()?.len();
        Ok(Self { file, size })
    }
}

impl BlobSource for FileBlob {
    fn size(&self) -> u64 {
        self.size
    }

    fn read_at(&self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        let mut buf = vec![0; len];
        let mut filled = 0;
        while filled < len {
            let Some(at) = offset.checked_add(filled as u64) else {
                break;
            };
            match read_at(&self.file, &mut buf[filled..], at) {
                Ok(0) => break,
                Ok(n) => filled += n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        buf.truncate(filled);
        Ok(buf)
    }
}

#[cfg(unix)]
fn read_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    std::os::unix::fs::FileExt::read_at(file, buf, offset)
}

#[cfg(windows)]
fn read_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    std::os::windows::fs::FileExt::seek_read(file, buf, offset)
}

/// Blob sources by id, with the token of the session they belong to.
pub(crate) type Blobs = Mutex<HashMap<String, (String, Arc<dyn BlobSource>)>>;

/// A blob a provider can read. Dropping it stops it.
pub struct Blob {
    id: String,
    size: u64,
    blobs: Weak<Blobs>,
}

impl Blob {
    pub(crate) fn new(
        blobs: &Arc<Blobs>,
        id: String,
        owner: String,
        source: impl BlobSource,
    ) -> Self {
        let size = source.size();
        blobs
            .lock()
            .unwrap()
            .insert(id.clone(), (owner, Arc::new(source)));
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

/// Reads part of a blob for the provider with session token `owner`, off the
/// async threads since sources can block.
pub(crate) async fn read(
    blobs: &Weak<Blobs>,
    owner: &str,
    id: &str,
    offset: u64,
    len: u32,
) -> Result<Vec<u8>, String> {
    let blobs = blobs.upgrade().ok_or("the orchestrator has shut down")?;
    let source = match blobs.lock().unwrap().get(id) {
        Some((o, source)) if o == owner => source.clone(),
        // Not repeating the id keeps the reply small.
        _ => return Err("unknown blob".into()),
    };
    let len = len.min(MAX_READ) as usize;
    match tokio::task::spawn_blocking(move || source.read_at(offset, len)).await {
        Ok(result) => result.map_err(|e| e.to_string()),
        Err(_) => Err("reading the blob failed".into()),
    }
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
            assert!(source.read_at(u64::MAX, 10).unwrap_or_default().is_empty());
        }
    }

    #[test]
    fn dropping_a_blob_removes_it() {
        let blobs = Arc::new(Blobs::default());
        let blob = Blob::new(&blobs, "id".into(), "owner".into(), vec![1, 2, 3]);
        assert_eq!(blob.media(None, None).size, Some(3));
        assert!(blobs.lock().unwrap().contains_key("id"));
        drop(blob);
        assert!(blobs.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn only_the_owner_reads_a_blob() {
        let blobs = Arc::new(Blobs::default());
        let _blob = Blob::new(&blobs, "id".into(), "a".into(), vec![1, 2, 3]);
        let weak = Arc::downgrade(&blobs);
        assert_eq!(read(&weak, "a", "id", 1, 10).await, Ok(vec![2, 3]));
        assert_eq!(
            read(&weak, "b", "id", 0, 10).await,
            Err("unknown blob".into())
        );
    }

    #[tokio::test]
    async fn a_panicking_source_fails_the_read() {
        struct Panics;
        impl BlobSource for Panics {
            fn size(&self) -> u64 {
                1
            }
            fn read_at(&self, _: u64, _: usize) -> io::Result<Vec<u8>> {
                panic!("broken source");
            }
        }
        let blobs = Arc::new(Blobs::default());
        let _blob = Blob::new(&blobs, "id".into(), "a".into(), Panics);
        let result = read(&Arc::downgrade(&blobs), "a", "id", 0, 1).await;
        assert!(result.is_err());
    }
}
