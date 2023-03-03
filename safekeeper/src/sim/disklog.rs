use std::collections::HashMap;

pub struct DiskLog {
    pub 
}

impl KV {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    pub fn get(&self, key: &str) -> u32 {
        self.map.get(key).copied().unwrap_or(0)
    }

    pub fn set(&mut self, key: &str, value: u32) {
        self.map.insert(key.to_string(), value);
    }
}