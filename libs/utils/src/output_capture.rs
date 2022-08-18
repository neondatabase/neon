use std::{os::unix::prelude::CommandExt, process::Command};



/// Command with ability to capture stdout and stderr to files
trait CaptureOutputToFile: CommandExt {
    fn capture_to_file(&mut self) -> &mut Command;
}

impl<C: CommandExt> CaptureOutputToFile
