//! Durable offline dataset output, independent of the asynchronous HTTP client.
pub use pragma_behavior_contract::recording::{RecordingConfig, RecordingManifest};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
};
use uuid::Uuid;

pub struct DatasetRecorder {
    directory: PathBuf,
    file: File,
    manifest: RecordingManifest,
    hash: Sha256,
    failed: bool,
}
fn invalid(message: impl ToString) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.to_string())
}
fn private_file(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}
impl DatasetRecorder {
    pub fn create(directory: impl AsRef<Path>, config: RecordingConfig) -> io::Result<Self> {
        config.validate().map_err(invalid)?;
        let directory = directory.as_ref().to_owned();
        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(&directory)?;
        let file = private_file(&directory.join("records.jsonl"))?;
        let manifest = RecordingManifest {
            format_version: 1,
            recording_id: Uuid::new_v4(),
            pipeline_version_id: config.pipeline_version_id,
            schema_hash: config.schema_hash.clone(),
            config,
            row_count: 0,
            byte_count: 0,
            content_sha256: hex::encode(Sha256::digest([])),
            finalized: false,
        };
        let recorder = Self {
            directory,
            file,
            manifest,
            hash: Sha256::new(),
            failed: false,
        };
        recorder.save_manifest()?;
        Ok(recorder)
    }
    pub fn record(&mut self, event: Value) -> io::Result<()> {
        if self.failed {
            return Err(invalid("recorder failed; start another recording"));
        }
        let event = self.manifest.config.sanitize(event).map_err(invalid)?;
        let mut data = serde_json::to_vec(&event).map_err(invalid)?;
        data.push(b'\n');
        if data.len() > 16384
            || self.manifest.row_count >= self.manifest.config.max_records
            || self.manifest.byte_count + data.len() > self.manifest.config.max_bytes
        {
            return Err(invalid(
                "recording limit reached; close and start another directory",
            ));
        }
        if let Err(error) = self
            .file
            .write_all(&data)
            .and_then(|_| self.file.sync_all())
        {
            self.failed = true;
            return Err(error);
        }
        self.hash.update(&data);
        self.manifest.row_count += 1;
        self.manifest.byte_count += data.len();
        Ok(())
    }
    fn save_manifest(&self) -> io::Result<()> {
        let temporary = self.directory.join(".manifest.tmp");
        let mut file = private_file(&temporary)?;
        file.write_all(&serde_json::to_vec_pretty(&self.manifest).map_err(invalid)?)?;
        file.sync_all()?;
        fs::rename(temporary, self.directory.join("manifest.json"))?;
        #[cfg(unix)]
        File::open(&self.directory)?.sync_all()?;
        Ok(())
    }
    /// Explicit finalization is required. Dropping an unfinished recorder never claims completeness.
    pub fn close(mut self) -> io::Result<RecordingManifest> {
        if self.failed {
            return Err(invalid(
                "recording write failed; manifest remains unfinished",
            ));
        }
        self.manifest.content_sha256 = hex::encode(self.hash.clone().finalize());
        self.manifest.finalized = true;
        self.save_manifest()?;
        Ok(self.manifest)
    }
}
