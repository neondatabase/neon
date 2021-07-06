/// A single 8KB page.
pub struct Page(pub Box<[u8; 8192]>);

impl Default for Page {
    fn default() -> Self {
        Page(Box::new([0u8; 8192]))
    }
}

impl From<[u8; 8192]> for Page {
    fn from(array: [u8; 8192]) -> Self {
        Page(Box::new(array))
    }
}

impl From<Box<[u8; 8192]>> for Page {
    fn from(heap_array: Box<[u8; 8192]>) -> Self {
        Page(heap_array)
    }
}

impl AsRef<[u8; 8192]> for Page {
    fn as_ref(&self) -> &[u8; 8192] {
        self.0.as_ref()
    }
}

impl AsMut<[u8; 8192]> for Page {
    fn as_mut(&mut self) -> &mut [u8; 8192] {
        self.0.as_mut()
    }
}
