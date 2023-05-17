//! Wrapper around nix::sys::statvfs::Statvfs that allows for mocking.

use std::path::Path;

pub enum Statvfs {
    Real(nix::sys::statvfs::Statvfs),
    Mock(mock::Statvfs),
}

// NB: on macOS, the block count type of struct statvfs is u32.
// The workaround seems to be to use the non-standard statfs64 call.
// Sincce it should only be a problem on > 2TiB disks, let's ignore
// the problem for now and upcast to u64.
impl Statvfs {
    pub fn get(tenants_dir: &Path, mocked: Option<&mock::Behavior>) -> nix::Result<Self> {
        if let Some(mocked) = mocked {
            Ok(Statvfs::Mock(mock::get(tenants_dir, mocked)?))
        } else {
            Ok(Statvfs::Real(nix::sys::statvfs::statvfs(tenants_dir)?))
        }
    }

    // NB: allow() because the block count type is u32 on macOS.
    #[allow(clippy::useless_conversion)]
    pub fn blocks(&self) -> u64 {
        match self {
            Statvfs::Real(stat) => u64::try_from(stat.blocks()).unwrap(),
            Statvfs::Mock(stat) => stat.blocks,
        }
    }

    // NB: allow() because the block count type is u32 on macOS.
    #[allow(clippy::useless_conversion)]
    pub fn blocks_available(&self) -> u64 {
        match self {
            Statvfs::Real(stat) => u64::try_from(stat.blocks_available()).unwrap(),
            Statvfs::Mock(stat) => stat.blocks_available,
        }
    }

    pub fn fragment_size(&self) -> u64 {
        match self {
            Statvfs::Real(stat) => stat.fragment_size(),
            Statvfs::Mock(stat) => stat.fragment_size,
        }
    }

    pub fn block_size(&self) -> u64 {
        match self {
            Statvfs::Real(stat) => stat.block_size(),
            Statvfs::Mock(stat) => stat.block_size,
        }
    }
}

pub mod mock {
    use anyhow::Context;
    use regex::Regex;
    use std::path::Path;
    use tracing::log::info;

    #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    #[serde(tag = "type")]
    pub enum Behavior {
        Success {
            blocksize: u64,
            total_blocks: u64,
            name_filter: Option<utils::serde_regex::Regex>,
        },
        Failure {
            mocked_error: MockedError,
        },
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    #[allow(clippy::upper_case_acronyms)]
    pub enum MockedError {
        EIO,
    }

    impl From<MockedError> for nix::Error {
        fn from(e: MockedError) -> Self {
            match e {
                MockedError::EIO => nix::Error::EIO,
            }
        }
    }

    pub fn get(tenants_dir: &Path, behavior: &Behavior) -> nix::Result<Statvfs> {
        info!("running mocked statvfs");

        match behavior {
            Behavior::Success {
                blocksize,
                total_blocks,
                ref name_filter,
            } => {
                let used_bytes = walk_dir_disk_usage(tenants_dir, name_filter.as_deref()).unwrap();

                // round it up to the nearest block multiple
                let used_blocks = (used_bytes + (blocksize - 1)) / blocksize;

                if used_blocks > *total_blocks {
                    panic!(
                        "mocking error: used_blocks > total_blocks: {used_blocks} > {total_blocks}"
                    );
                }

                let avail_blocks = total_blocks - used_blocks;

                Ok(Statvfs {
                    blocks: *total_blocks,
                    blocks_available: avail_blocks,
                    fragment_size: *blocksize,
                    block_size: *blocksize,
                })
            }
            Behavior::Failure { mocked_error } => Err((*mocked_error).into()),
        }
    }

    fn walk_dir_disk_usage(path: &Path, name_filter: Option<&Regex>) -> anyhow::Result<u64> {
        let mut total = 0;
        for entry in walkdir::WalkDir::new(path) {
            let entry = entry?;
            if !entry.file_type().is_file() {
                continue;
            }
            if !name_filter
                .as_ref()
                .map(|filter| filter.is_match(entry.file_name().to_str().unwrap()))
                .unwrap_or(true)
            {
                continue;
            }
            total += entry
                .metadata()
                .with_context(|| format!("get metadata of {:?}", entry.path()))?
                .len();
        }
        Ok(total)
    }

    pub struct Statvfs {
        pub blocks: u64,
        pub blocks_available: u64,
        pub fragment_size: u64,
        pub block_size: u64,
    }
}
