use std::fs::File;
use std::io::Seek;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use lazy_static::lazy_static;

const INVALID_TAG: u64 = u64::MAX;

struct OpenFiles {
    next: usize,
    files: Vec<OpenFile>,
}

lazy_static! {
    static ref OPEN_FILES: Mutex<OpenFiles> = Mutex::new(OpenFiles {
        next: 0,
        files: Vec::new(),
    });
}

struct OpenFile {
    tag: u64,
    file: Option<File>,
}

pub struct VirtualFile {
    vfd: usize,
    tag: u64,

    path: PathBuf,
}

impl VirtualFile {

    pub fn new(path: &Path) -> VirtualFile {
        VirtualFile {
            vfd: 0,
            tag: INVALID_TAG,
            path: path.to_path_buf(),
        }
    }

    pub fn open(&mut self) -> std::io::Result<File> {

        let mut l = OPEN_FILES.lock().unwrap();

        if self.vfd < l.files.len() && l.files[self.vfd].tag == self.tag {

            if let Some(mut file) = l.files[self.vfd].file.take() {
                // return cached File
                eprintln!("reusing {} from {}/{}", self.path.display(), self.vfd, self.tag);
                file.rewind()?;
                return Ok(file);
            }
        }
        eprintln!("opening {}", self.path.display());

        File::open(&self.path)
    }

    pub fn cache(&mut self, file: File) {

        let mut l = OPEN_FILES.lock().unwrap();

        let next = if l.next >= l.files.len() {
            if l.files.len() < 100 {
                l.files.push(OpenFile {
                    tag: 0,
                    file: None
                });
                l.files.len() - 1
            } else {
                // wrap around
                0
            }
        } else {
            l.next
        };
        l.next = next + 1;

        l.files[next].file.replace(file);
        l.files[next].tag += 1;

        self.vfd = next;
        self.tag = l.files[next].tag;

        eprintln!("caching {} at {}/{}", self.path.display(), self.vfd, self.tag);
        
        drop(l);
    }
}

impl Drop for VirtualFile {
    fn drop(&mut self) {

        // Close file if it's still open
        
        if self.tag != INVALID_TAG {
            let mut l = OPEN_FILES.lock().unwrap();

            if self.vfd < l.files.len() && l.files[self.vfd].tag == self.tag {
                l.files[self.vfd].file.take();
            }
        }
    }
}



#[cfg(test)]
mod tests {
    use crate::PageServerConf;
    use super::*;
    use std::io::Read;

    #[test]
    fn test_vfd() -> anyhow::Result<()> {

        let mut vfiles = Vec::new();

        let test_dir = PageServerConf::test_repo_dir("test_vfd");
        let _ = std::fs::remove_dir_all(&test_dir);
        std::fs::create_dir_all(&test_dir)?;
        
        for i in 0..2000 {
            let path = test_dir.join(format!("vfd_test{}", i));
            let content = format!("foobar{}", i);

            std::fs::write(&path, &content)?;

            let vfile = VirtualFile::new(&path);

            vfiles.push((vfile, path, content));
        }

        for i in 0..vfiles.len() {
            let (ref mut vfile, _path, expected_content) = &mut vfiles[i];
            let mut s = String::new();

            let mut file = vfile.open()?;
            file.read_to_string(&mut s)?;

            assert!(&s == expected_content);

            vfile.cache(file);

            s.clear();
            let (ref mut vfile, _path, expected_content) = &mut vfiles[0];
            let mut file = vfile.open()?;
            file.read_to_string(&mut s)?;

            assert!(&s == expected_content);

            vfile.cache(file);
        }            

        Ok(())
    }
}
