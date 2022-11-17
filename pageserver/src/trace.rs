use bytes::Bytes;
use std::{
    fs::{create_dir_all, File},
    io::{BufWriter, Write},
    path::PathBuf,
};

pub struct Tracer {
    writer: BufWriter<File>,
}

impl Drop for Tracer {
    fn drop(&mut self) {
        self.flush()
    }
}

impl Tracer {
    pub fn new(path: PathBuf) -> Self {
        let parent = path.parent().expect("failed to parse parent path");
        create_dir_all(parent).expect("failed to create trace dir");

        let file = File::create(path).expect("failed to create trace file");
        Tracer {
            writer: BufWriter::new(file),
        }
    }

    pub fn trace(&mut self, msg: &Bytes) {
        self.writer.write_all(msg).expect("failed to write trace");
    }

    pub fn flush(&mut self) {
        self.writer.flush().expect("failed to flush trace file");
    }
}
