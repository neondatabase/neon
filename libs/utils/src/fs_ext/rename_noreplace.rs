use nix::NixPath;

/// Rename a file without replacing an existing file.
///
/// This is a wrapper around platform-specific APIs.
pub fn rename_noreplace<P1: ?Sized + NixPath, P2: ?Sized + NixPath>(
    src: &P1,
    dst: &P2,
) -> nix::Result<()> {
    {
        #[cfg(target_os = "linux")]
        {
            nix::fcntl::renameat2(
                None,
                src,
                None,
                dst,
                nix::fcntl::RenameFlags::RENAME_NOREPLACE,
            )
        }
        #[cfg(target_os = "macos")]
        {
            let res = src.with_nix_path(|src| {
                dst.with_nix_path(|dst|
                    // SAFETY: `src` and `dst` are valid C strings as per the NixPath trait and they outlive the call to renamex_np.
                    unsafe {
                        nix::libc::renamex_np(src.as_ptr(), dst.as_ptr(), nix::libc::RENAME_EXCL)
                })
            })??;
            nix::errno::Errno::result(res).map(drop)
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            std::compile_error!("OS does not support no-replace renames");
        }
    }
}

#[cfg(test)]
mod test {
    use std::{fs, path::PathBuf};

    use super::*;

    fn testdir() -> camino_tempfile::Utf8TempDir {
        match crate::env::var("NEON_UTILS_RENAME_NOREPLACE_TESTDIR") {
            Some(path) => {
                let path: camino::Utf8PathBuf = path;
                camino_tempfile::tempdir_in(path).unwrap()
            }
            None => camino_tempfile::tempdir().unwrap(),
        }
    }

    #[test]
    fn test_absolute_paths() {
        let testdir = testdir();
        println!("testdir: {}", testdir.path());

        let src = testdir.path().join("src");
        let dst = testdir.path().join("dst");

        fs::write(&src, b"").unwrap();
        fs::write(&dst, b"").unwrap();

        let src = src.canonicalize().unwrap();
        assert!(src.is_absolute());
        let dst = dst.canonicalize().unwrap();
        assert!(dst.is_absolute());

        let result = rename_noreplace(&src, &dst);
        assert_eq!(result.unwrap_err(), nix::Error::EEXIST);
    }

    #[test]
    fn test_relative_paths() {
        let testdir = testdir();
        println!("testdir: {}", testdir.path());

        // this is fine because we run in nextest => process per test
        std::env::set_current_dir(testdir.path()).unwrap();

        let src = PathBuf::from("src");
        let dst = PathBuf::from("dst");

        fs::write(&src, b"").unwrap();
        fs::write(&dst, b"").unwrap();

        let result = rename_noreplace(&src, &dst);
        assert_eq!(result.unwrap_err(), nix::Error::EEXIST);
    }

    #[test]
    fn test_works_when_not_exists() {
        let testdir = testdir();
        println!("testdir: {}", testdir.path());

        let src = testdir.path().join("src");
        let dst = testdir.path().join("dst");

        fs::write(&src, b"content").unwrap();

        rename_noreplace(src.as_std_path(), dst.as_std_path()).unwrap();
        assert_eq!(
            "content",
            String::from_utf8(std::fs::read(&dst).unwrap()).unwrap()
        );
    }
}
