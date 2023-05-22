use std::sync::Arc;

use anyhow::Result;

use super::sync::{Mutex, Park};

pub trait Storage<T> {
    fn flush_pos(&self) -> u32;
    fn flush(&mut self) -> Result<()>;
    fn write(&mut self, t: T);
}

#[derive(Clone)]
pub struct SharedStorage<T> {
    pub state: Arc<Mutex<InMemoryStorage<T>>>,
}

impl<T> SharedStorage<T> {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(InMemoryStorage::new())),
        }
    }
}

impl<T> Storage<T> for SharedStorage<T> {
    fn flush_pos(&self) -> u32 {
        self.state.lock().flush_pos
    }

    fn flush(&mut self) -> Result<()> {
        Park::yield_thread();
        self.state.lock().flush()
    }

    fn write(&mut self, t: T) {
        Park::yield_thread();
        self.state.lock().write(t);
    }
}

pub struct InMemoryStorage<T> {
    pub data: Vec<T>,
    pub flush_pos: u32,
}

impl<T> InMemoryStorage<T> {
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
            flush_pos: 0,
        }
    }

    pub fn flush(&mut self) -> Result<()> {
        self.flush_pos = self.data.len() as u32;
        Ok(())
    }

    pub fn write(&mut self, t: T) {
        self.data.push(t);
    }
}
