use std::path::Path;

#[derive(Debug, Clone)]
pub struct OpenOptions(std::fs::OpenOptions);

impl Default for OpenOptions {
    fn default() -> Self {
        Self(std::fs::OpenOptions::new())
    }
}

impl OpenOptions {
    pub fn new() -> OpenOptions {
        Self::default()
    }

    pub fn read(&mut self, read: bool) -> &mut OpenOptions {
        let _ = self.0.read(read);
        self
    }

    pub fn write(&mut self, write: bool) -> &mut OpenOptions {
        let _ = self.0.write(write);
        self
    }

    pub fn create(&mut self, create: bool) -> &mut OpenOptions {
        let _ = self.0.create(create);
        self
    }

    pub fn create_new(&mut self, create_new: bool) -> &mut OpenOptions {
        let _ = self.0.create_new(create_new);
        self
    }

    pub fn truncate(&mut self, truncate: bool) -> &mut OpenOptions {
        let _ = self.0.truncate(truncate);
        self
    }

    pub(in crate::virtual_file) async fn open(
        &self,
        path: &Path,
    ) -> std::io::Result<std::fs::File> {
        self.0.open(path)
    }
}

impl std::os::unix::prelude::OpenOptionsExt for OpenOptions {
    fn mode(&mut self, mode: u32) -> &mut OpenOptions {
        let _ = self.0.mode(mode);
        self
    }

    fn custom_flags(&mut self, flags: i32) -> &mut OpenOptions {
        let _ = self.0.custom_flags(flags);
        self
    }
}
