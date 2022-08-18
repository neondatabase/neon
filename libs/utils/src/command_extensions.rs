use std::path::PathBuf;
use std::{os::unix::prelude::CommandExt, process::Command};
use std::fs::File;


pub trait NeonCommandExtensions: CommandExt {
    fn capture_to_files(&mut self, path: PathBuf, name: &str) -> &mut Command;
}

impl NeonCommandExtensions for Command {
    fn capture_to_files(&mut self, path: PathBuf, name: &str) -> &mut Command {
        let out_file = File::create(path.join(format!("{}.out", name)))
            .expect("can't make file");
        let err_file = File::create(path.join(format!("{}.out", name)))
            .expect("can't make file");

        // TODO touch files?

        self.stdout(out_file).stderr(err_file)
    }
}
