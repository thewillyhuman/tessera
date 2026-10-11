// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Spilling to local disk (QE-11): a blocking operator whose memory
//! grant is exhausted writes what it buffers as an Arrow IPC file under
//! the pool's spill directory and reads it back on finish, so that a
//! large query runs slower rather than failing. A spill file is removed
//! when it is dropped.

use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;

use crate::ExecError;

/// Distinguishes the files one process writes within a nanosecond.
static COUNTER: AtomicU64 = AtomicU64::new(0);

/// One run of rows on local disk, removed when dropped.
#[derive(Debug)]
pub struct SpillFile {
    path: PathBuf,
    bytes: u64,
}

impl SpillFile {
    /// Writes `batches`, all of `schema`, as one file under `dir`.
    ///
    /// # Errors
    ///
    /// If the file cannot be written.
    pub fn write(
        dir: &Path,
        schema: &SchemaRef,
        batches: &[RecordBatch],
    ) -> Result<Self, ExecError> {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let path = dir.join(format!(
            "{}-{nanos}-{}.arrow",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        let file = File::create(&path).map_err(ExecError::Spill)?;
        // The guard exists before the first write, so a failed write
        // still removes the file.
        let mut spill = SpillFile { path, bytes: 0 };
        let mut writer = StreamWriter::try_new(BufWriter::new(file), schema)?;
        for batch in batches {
            writer.write(batch)?;
        }
        writer.finish()?;
        spill.bytes = writer
            .into_inner()?
            .into_inner()
            .map_err(|e| ExecError::Spill(e.into_error()))?
            .metadata()
            .map_err(ExecError::Spill)?
            .len();
        Ok(spill)
    }

    /// The file's size.
    #[must_use]
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// The batches as written, one at a time.
    ///
    /// # Errors
    ///
    /// If the file cannot be opened or is not what was written.
    pub fn read(&self) -> Result<SpillReader, ExecError> {
        let file = File::open(&self.path).map_err(ExecError::Spill)?;
        let reader = StreamReader::try_new(BufReader::new(file), None)?;
        Ok(SpillReader { reader })
    }
}

impl Drop for SpillFile {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_file(&self.path) {
            tracing::warn!(path = %self.path.display(), error = %error, "spill file not removed");
        }
    }
}

/// The batches of a spill file, read as they are asked for.
pub struct SpillReader {
    reader: StreamReader<BufReader<File>>,
}

impl Iterator for SpillReader {
    type Item = Result<RecordBatch, ExecError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.reader
            .next()
            .map(|batch| batch.map_err(ExecError::Arrow))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::testing::{rows, schema, timestamps};

    #[test]
    fn a_spill_file_round_trips_and_goes_when_dropped() {
        let dir = std::env::temp_dir().join(format!("tessera-spill-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = SpillFile::write(&dir, &schema(), &[rows(&[3, 1]), rows(&[2])]).unwrap();
        assert!(file.bytes() > 0);
        let path = file.path.clone();
        assert!(path.exists());
        let back: Vec<RecordBatch> = file.read().unwrap().map(Result::unwrap).collect();
        assert_eq!(back.len(), 2);
        assert_eq!(timestamps(&back), vec![3, 1, 2]);
        drop(file);
        assert!(!path.exists(), "removed on drop");
        std::fs::remove_dir(&dir).unwrap();
    }
}
