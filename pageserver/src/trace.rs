use crate::page_service::PagestreamFeMessage;
use std::{fs::File, io::Write, path::PathBuf};

pub struct Tracer {
    output: File,
}

impl Drop for Tracer {
    fn drop(&mut self) {
        self.flush()
    }
}

impl Tracer {
    pub fn new(path: PathBuf) -> Self {
        Tracer {
            output: File::create(path).expect("failed to create trace file"),
        }
    }

    pub fn trace(&mut self, _msg: &PagestreamFeMessage) {
        // TODO(now) implement
    }

    pub fn flush(&mut self) {
        self.output.flush().expect("failed to flush trace file");
    }
}
