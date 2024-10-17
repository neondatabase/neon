//! Wrapper around nix::sys::statvfs::Statvfs that allows for mocking.

use camino::Utf8Path;

pub enum Statvfs {
    Real(nix::sys::statvfs::Statvfs),
    Mock(mock::Statvfs),
}

// NB: on macOS, the block count type of struct statvfs is u32.
// The workaround seems to be to use the non-standard statfs64 call.
// Sincce it should only be a problem on > 2TiB disks, let's ignore
// the problem for now and upcast to u64.
impl Statvfs {
    pub fn get(tenants_dir: &Utf8Path, mocked: Option<&mock::Behavior>) -> nix::Result<Self> {
        if let Some(mocked) = mocked {
            Ok(Statvfs::Mock(mock::get(tenants_dir, mocked)?))
        } else {
            Ok(Statvfs::Real(nix::sys::statvfs::statvfs(
                tenants_dir.as_std_path(),
            )?))
        }
    }

    // NB: allow() because the block count type is u32 on macOS.
    #[allow(clippy::useless_conversion, clippy::unnecessary_fallible_conversions)]
    pub fn blocks(&self) -> u64 {
        match self {
            Statvfs::Real(stat) => u64::try_from(stat.blocks()).unwrap(),
            Statvfs::Mock(stat) => stat.blocks,
        }
    }

    // NB: allow() because the block count type is u32 on macOS.
    #[allow(clippy::useless_conversion, clippy::unnecessary_fallible_conversions)]
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

    /// Get the available and total bytes on the filesystem.
    pub fn get_avail_total_bytes(&self) -> (u64, u64) {
        // https://unix.stackexchange.com/a/703650
        let blocksize = if self.fragment_size() > 0 {
            self.fragment_size()
        } else {
            self.block_size()
        };

        // use blocks_available (b_avail) since, pageserver runs as unprivileged user
        let avail_bytes = self.blocks_available() * blocksize;
        let total_bytes = self.blocks() * blocksize;

        (avail_bytes, total_bytes)
    }
}

pub mod mock {
    use camino::Utf8Path;
    use regex::Regex;
    use tracing::log::info;

    pub use pageserver_api::config::statvfs::mock::Behavior;

    pub fn get(tenants_dir: &Utf8Path, behavior: &Behavior) -> nix::Result<Statvfs> {
        info!("running mocked statvfs");

        match behavior {
            Behavior::Success {
                blocksize,
                total_blocks,
                ref name_filter,
            } => {
                let used_bytes = walk_dir_disk_usage(tenants_dir, name_filter.as_deref()).unwrap();

                // round it up to the nearest block multiple
                let used_blocks = used_bytes.div_ceil(*blocksize);

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
            #[cfg(feature = "testing")]
            Behavior::Failure { mocked_error } => Err((*mocked_error).into()),
        }
    }

    fn walk_dir_disk_usage(path: &Utf8Path, name_filter: Option<&Regex>) -> anyhow::Result<u64> {
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
            let m = match entry.metadata() {
                Ok(m) => m,
                Err(e) if is_not_found(&e) => {
                    // some temp file which got removed right as we are walking
                    continue;
                }
                Err(e) => {
                    return Err(anyhow::Error::new(e)
                        .context(format!("get metadata of {:?}", entry.path())))
                }
            };
            total += m.len();
        }
        Ok(total)
    }

    fn is_not_found(e: &walkdir::Error) -> bool {
        let Some(io_error) = e.io_error() else {
            return false;
        };
        let kind = io_error.kind();
        matches!(kind, std::io::ErrorKind::NotFound)
    }

    pub struct Statvfs {
        pub blocks: u64,
        pub blocks_available: u64,
        pub fragment_size: u64,
        pub block_size: u64,
    }
}
