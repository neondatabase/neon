use std::path::PathBuf;

use anyhow::Context;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
enum AvailBytesSource {
    Fixed(u64),
    WalkDir(PathBuf),
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
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
        avail: AvailBytesSource,
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
            avail,
        } => {
            let avail_bytes = avail.get().unwrap();

            // round it up to the nearest block multiple
            let avail_blocks = (avail_bytes + (blocksize - 1)) / blocksize;

            if avail_blocks > total_blocks {
                panic!(
                    "mocking error: avail_blocks > total_blocks: {avail_blocks} > {total_blocks}"
                );
            }

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
            return 0;
        }
        Mock::Failure { mocked_error } => {
            // SAFETY: we mock the libc, we're allowed to set errno
            unsafe { libc::__errno_location().write(mocked_error.into()) };
            return -1;
        }
    }
}

impl AvailBytesSource {
    fn get(&self) -> anyhow::Result<u64> {
        match self {
            AvailBytesSource::Fixed(n) => Ok(*n),
            AvailBytesSource::WalkDir(path) => {
                let mut total = 0;
                for entry in walkdir::WalkDir::new(path) {
                    let entry = entry?;
                    if entry.file_type().is_file() {
                        total += entry
                            .metadata()
                            .with_context(|| format!("get metadata of {:?}", entry.path()))?
                            .len();
                    }
                }
                Ok(total)
            }
        }
    }
}
