//! Control file serialization, deserialization and persistence.

use anyhow::{bail, ensure, Context, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use camino::{Utf8Path, Utf8PathBuf};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use utils::crashsafe::durable_rename;

use std::io::Read;
use std::ops::Deref;
use std::path::Path;
use std::time::Instant;

use crate::control_file_upgrade::downgrade_v9_to_v8;
use crate::metrics::PERSIST_CONTROL_FILE_SECONDS;
use crate::state::{EvictionState, TimelinePersistentState};
use crate::{control_file_upgrade::upgrade_control_file, timeline::get_timeline_dir};
use utils::{bin_ser::LeSer, id::TenantTimelineId};

use crate::SafeKeeperConf;

pub const SK_MAGIC: u32 = 0xcafeceefu32;
pub const SK_FORMAT_VERSION: u32 = 9;

// contains persistent metadata for safekeeper
pub const CONTROL_FILE_NAME: &str = "safekeeper.control";
// needed to atomically update the state using `rename`
const CONTROL_FILE_NAME_PARTIAL: &str = "safekeeper.control.partial";
pub const CHECKSUM_SIZE: usize = std::mem::size_of::<u32>();

/// Storage should keep actual state inside of it. It should implement Deref
/// trait to access state fields and have persist method for updating that state.
#[async_trait::async_trait]
pub trait Storage: Deref<Target = TimelinePersistentState> {
    /// Persist safekeeper state on disk and update internal state.
    async fn persist(&mut self, s: &TimelinePersistentState) -> Result<()>;

    /// Timestamp of last persist.
    fn last_persist_at(&self) -> Instant;
}

#[derive(Debug)]
pub struct FileStorage {
    // save timeline dir to avoid reconstructing it every time
    timeline_dir: Utf8PathBuf,
    no_sync: bool,

    /// Last state persisted to disk.
    state: TimelinePersistentState,
    /// Not preserved across restarts.
    last_persist_at: Instant,
}

impl FileStorage {
    /// Initialize storage by loading state from disk.
    pub fn restore_new(ttid: &TenantTimelineId, conf: &SafeKeeperConf) -> Result<FileStorage> {
        let timeline_dir = get_timeline_dir(conf, ttid);
        let state = Self::load_control_file_from_dir(&timeline_dir)?;

        Ok(FileStorage {
            timeline_dir,
            no_sync: conf.no_sync,
            state,
            last_persist_at: Instant::now(),
        })
    }

    /// Create file storage for a new timeline, but don't persist it yet.
    pub fn create_new(
        timeline_dir: Utf8PathBuf,
        conf: &SafeKeeperConf,
        state: TimelinePersistentState,
    ) -> Result<FileStorage> {
        // we don't support creating new timelines in offloaded state
        assert!(matches!(state.eviction_state, EvictionState::Present));

        let store = FileStorage {
            timeline_dir,
            no_sync: conf.no_sync,
            state,
            last_persist_at: Instant::now(),
        };

        Ok(store)
    }

    /// Check the magic/version in the on-disk data and deserialize it, if possible.
    fn deser_sk_state(buf: &mut &[u8]) -> Result<TimelinePersistentState> {
        // Read the version independent part
        let magic = ReadBytesExt::read_u32::<LittleEndian>(buf)?;
        if magic != SK_MAGIC {
            bail!(
                "bad control file magic: {:X}, expected {:X}",
                magic,
                SK_MAGIC
            );
        }
        let version = ReadBytesExt::read_u32::<LittleEndian>(buf)?;
        if version == SK_FORMAT_VERSION {
            let res = TimelinePersistentState::des(buf)?;
            return Ok(res);
        }
        // try to upgrade
        upgrade_control_file(buf, version)
    }

    /// Load control file from given directory.
    fn load_control_file_from_dir(timeline_dir: &Utf8Path) -> Result<TimelinePersistentState> {
        let path = timeline_dir.join(CONTROL_FILE_NAME);
        Self::load_control_file(path)
    }

    /// Read in the control file.
    pub fn load_control_file<P: AsRef<Path>>(
        control_file_path: P,
    ) -> Result<TimelinePersistentState> {
        let mut control_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&control_file_path)
            .with_context(|| {
                format!(
                    "failed to open control file at {}",
                    control_file_path.as_ref().display(),
                )
            })?;

        let mut buf = Vec::new();
        control_file
            .read_to_end(&mut buf)
            .context("failed to read control file")?;

        let calculated_checksum = crc32c::crc32c(&buf[..buf.len() - CHECKSUM_SIZE]);

        let expected_checksum_bytes: &[u8; CHECKSUM_SIZE] =
            buf[buf.len() - CHECKSUM_SIZE..].try_into()?;
        let expected_checksum = u32::from_le_bytes(*expected_checksum_bytes);

        ensure!(
            calculated_checksum == expected_checksum,
            format!(
                "safekeeper control file checksum mismatch: expected {} got {}",
                expected_checksum, calculated_checksum
            )
        );

        let state = FileStorage::deser_sk_state(&mut &buf[..buf.len() - CHECKSUM_SIZE])
            .with_context(|| {
                format!(
                    "while reading control file {}",
                    control_file_path.as_ref().display(),
                )
            })?;
        Ok(state)
    }
}

impl Deref for FileStorage {
    type Target = TimelinePersistentState;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

#[async_trait::async_trait]
impl Storage for FileStorage {
    /// Persists state durably to the underlying storage.
    ///
    /// For a description, see <https://lwn.net/Articles/457667/>.
    async fn persist(&mut self, s: &TimelinePersistentState) -> Result<()> {
        let _timer = PERSIST_CONTROL_FILE_SECONDS.start_timer();

        // write data to safekeeper.control.partial
        let control_partial_path = self.timeline_dir.join(CONTROL_FILE_NAME_PARTIAL);
        let mut control_partial = File::create(&control_partial_path).await.with_context(|| {
            format!(
                "failed to create partial control file at: {}",
                &control_partial_path
            )
        })?;
        let mut buf: Vec<u8> = Vec::new();
        WriteBytesExt::write_u32::<LittleEndian>(&mut buf, SK_MAGIC)?;

        if s.eviction_state == EvictionState::Present {
            // temp hack for forward compatibility
            const PREV_FORMAT_VERSION: u32 = 8;
            let prev = downgrade_v9_to_v8(s);
            WriteBytesExt::write_u32::<LittleEndian>(&mut buf, PREV_FORMAT_VERSION)?;
            prev.ser_into(&mut buf)?;
        } else {
            // otherwise, we write the current format version
            WriteBytesExt::write_u32::<LittleEndian>(&mut buf, SK_FORMAT_VERSION)?;
            s.ser_into(&mut buf)?;
        }

        // calculate checksum before resize
        let checksum = crc32c::crc32c(&buf);
        buf.extend_from_slice(&checksum.to_le_bytes());

        control_partial.write_all(&buf).await.with_context(|| {
            format!(
                "failed to write safekeeper state into control file at: {}",
                control_partial_path
            )
        })?;
        control_partial.flush().await.with_context(|| {
            format!(
                "failed to flush safekeeper state into control file at: {}",
                control_partial_path
            )
        })?;

        let control_path = self.timeline_dir.join(CONTROL_FILE_NAME);
        durable_rename(&control_partial_path, &control_path, !self.no_sync).await?;

        // update internal state
        self.state = s.clone();
        Ok(())
    }

    fn last_persist_at(&self) -> Instant {
        self.last_persist_at
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tokio::fs;
    use utils::lsn::Lsn;

    fn stub_conf() -> SafeKeeperConf {
        let workdir = camino_tempfile::tempdir().unwrap().into_path();
        SafeKeeperConf {
            workdir,
            ..SafeKeeperConf::dummy()
        }
    }

    async fn load_from_control_file(
        conf: &SafeKeeperConf,
        ttid: &TenantTimelineId,
    ) -> Result<(FileStorage, TimelinePersistentState)> {
        let timeline_dir = get_timeline_dir(conf, ttid);
        fs::create_dir_all(&timeline_dir)
            .await
            .expect("failed to create timeline dir");
        Ok((
            FileStorage::restore_new(ttid, conf)?,
            FileStorage::load_control_file_from_dir(&timeline_dir)?,
        ))
    }

    async fn create(
        conf: &SafeKeeperConf,
        ttid: &TenantTimelineId,
    ) -> Result<(FileStorage, TimelinePersistentState)> {
        let timeline_dir = get_timeline_dir(conf, ttid);
        fs::create_dir_all(&timeline_dir)
            .await
            .expect("failed to create timeline dir");
        let state = TimelinePersistentState::empty();
        let storage = FileStorage::create_new(timeline_dir, conf, state.clone())?;
        Ok((storage, state))
    }

    #[tokio::test]
    async fn test_read_write_safekeeper_state() {
        let conf = stub_conf();
        let ttid = TenantTimelineId::generate();
        {
            let (mut storage, mut state) =
                create(&conf, &ttid).await.expect("failed to create state");
            // change something
            state.commit_lsn = Lsn(42);
            storage
                .persist(&state)
                .await
                .expect("failed to persist state");
        }

        let (_, state) = load_from_control_file(&conf, &ttid)
            .await
            .expect("failed to read state");
        assert_eq!(state.commit_lsn, Lsn(42));
    }

    #[tokio::test]
    async fn test_safekeeper_state_checksum_mismatch() {
        let conf = stub_conf();
        let ttid = TenantTimelineId::generate();
        {
            let (mut storage, mut state) =
                create(&conf, &ttid).await.expect("failed to read state");

            // change something
            state.commit_lsn = Lsn(42);
            storage
                .persist(&state)
                .await
                .expect("failed to persist state");
        }
        let control_path = get_timeline_dir(&conf, &ttid).join(CONTROL_FILE_NAME);
        let mut data = fs::read(&control_path).await.unwrap();
        data[0] += 1; // change the first byte of the file to fail checksum validation
        fs::write(&control_path, &data)
            .await
            .expect("failed to write control file");

        match load_from_control_file(&conf, &ttid).await {
            Err(err) => assert!(err
                .to_string()
                .contains("safekeeper control file checksum mismatch")),
            Ok(_) => panic!("expected error"),
        }
    }
}
