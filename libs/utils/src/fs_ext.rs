/// Extensions to `std::fs` types.
use std::{fs, io, os::unix::process::CommandExt, path::Path, process::Command};

pub trait PathExt {
    /// Returns an error if `self` is not a directory.
    fn is_empty_dir(&self) -> io::Result<bool>;
}

impl<P> PathExt for P
where
    P: AsRef<Path>,
{
    fn is_empty_dir(&self) -> io::Result<bool> {
        Ok(fs::read_dir(self)?.into_iter().next().is_none())
    }
}

///
/// Command with ability not to give all file descriptors to child process
///
pub trait CloseFileDescriptors: CommandExt {
    ///
    /// Close file descriptors (other than stdin, stdout, stderr) in child process
    ///
    fn close_fds(&mut self) -> &mut Command;
}

impl<C: CommandExt> CloseFileDescriptors for C {
    fn close_fds(&mut self) -> &mut Command {
        unsafe {
            self.pre_exec(move || {
                // SAFETY: Code executed inside pre_exec should have async-signal-safety,
                // which means it should be safe to execute inside a signal handler.
                // The precise meaning depends on platform. See `man signal-safety`
                // for the linux definition.
                //
                // The set_fds_cloexec_threadsafe function is documented to be
                // async-signal-safe.
                //
                // Aside from this function, the rest of the code is re-entrant and
                // doesn't make any syscalls. We're just passing constants.
                //
                // NOTE: It's easy to indirectly cause a malloc or lock a mutex,
                // which is not async-signal-safe. Be careful.
                close_fds::set_fds_cloexec_threadsafe(3, &[]);
                Ok(())
            })
        }
    }
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    #[test]
    fn is_empty_dir() {
        use super::PathExt;

        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();

        // test positive case
        assert!(
            dir_path.is_empty_dir().expect("test failure"),
            "new tempdir should be empty"
        );

        // invoke on a file to ensure it returns an error
        let file_path: PathBuf = dir_path.join("testfile");
        let f = std::fs::File::create(&file_path).unwrap();
        drop(f);
        assert!(file_path.is_empty_dir().is_err());

        // do it again on a path, we know to be nonexistent
        std::fs::remove_file(&file_path).unwrap();
        assert!(file_path.is_empty_dir().is_err());
    }
}
