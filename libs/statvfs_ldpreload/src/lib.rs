use std::path::PathBuf;

use anyhow::Context;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type")]
enum UsedBytesSource {
    Fixed {
        value: u64,
    },
    WalkDir {
        path: PathBuf,
        // only count files whose names match this regex
        name_filter: Option<String>,
    },
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[allow(clippy::upper_case_acronyms)]
enum MockedError {
    EIO,
}

impl From<MockedError> for libc::c_int {
    fn from(e: MockedError) -> Self {
        match e {
            MockedError::EIO => libc::EIO,
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type")]
enum Mock {
    Success {
        blocksize: u64,
        total_blocks: u64,
        used: UsedBytesSource,
    },
    Failure {
        mocked_error: MockedError,
    },
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Config {
    magic: String,
    mock: Mock,
}

static INVOCATION_NUMBER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

#[derive(serde::Serialize)]
struct Status<'a> {
    config: &'a Config,
    invocation_number: usize,
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn fstatvfs(_fd: libc::c_int, buf: *mut libc::statvfs64) -> libc::c_int {
    use std::mem::MaybeUninit;

    // the intended behavior for this mock is provided in an environment variable
    let config = std::env::var("NEON_STATVFS_LDPRELOAD_CONFIG").unwrap_or_else(|_| {
        panic!("NEON_STATVFS_LDPRELOAD_CONFIG not set");
    });
    let config: Config = serde_json::from_str(&config).unwrap();

    // print a message to stderr, so that the test can ensure LD_PRELOAD is working
    let invocation_number = INVOCATION_NUMBER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let status = Status {
        config: &config,
        invocation_number,
    };
    eprintln!(
        "statvfs_ldpreload status: {}",
        serde_json::to_string(&status).unwrap()
    );

    // mock the statvfs call
    match config.mock {
        Mock::Success {
            blocksize,
            total_blocks,
            used,
        } => {
            let used_bytes = used.get().unwrap();

            // round it up to the nearest block multiple
            let used_blocks = (used_bytes + (blocksize - 1)) / blocksize;

            if used_blocks > total_blocks {
                panic!("mocking error: used_blocks > total_blocks: {used_blocks} > {total_blocks}");
            }

            let avail_blocks = total_blocks - used_blocks;

            // SAFETY: for the purposes of mocking, zeroed values for the fields which we
            // don't set below are fine.
            let mut ret = unsafe { MaybeUninit::<libc::statvfs64>::zeroed().assume_init() };
            ret.f_bsize = blocksize;
            ret.f_frsize = blocksize;
            ret.f_blocks = total_blocks;
            ret.f_bfree = avail_blocks;
            ret.f_bavail = avail_blocks;

            // SAFETY: the cfg! for this function ensures that the buffer has size of libc::statvfs64
            unsafe {
                buf.write(ret);
            }
            0
        }
        Mock::Failure { mocked_error } => {
            // SAFETY: we mock the libc, we're allowed to set errno
            unsafe { libc::__errno_location().write(mocked_error.into()) };
            -1
        }
    }
}

impl UsedBytesSource {
    fn get(&self) -> anyhow::Result<u64> {
        match self {
            UsedBytesSource::Fixed { value } => Ok(*value),
            UsedBytesSource::WalkDir { path, name_filter } => {
                let mut total = 0;
                let filter_compiled = name_filter.as_ref().map(|n| regex::Regex::new(n).unwrap());
                for entry in walkdir::WalkDir::new(path) {
                    let entry = entry?;
                    if !entry.file_type().is_file() {
                        continue;
                    }
                    if !filter_compiled
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
        }
    }
}
