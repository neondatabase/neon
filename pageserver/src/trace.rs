pub struct Tracer {}

impl Drop for Tracer {
    fn drop(&mut self) {
        self.flush()
    }
}

impl Tracer {
    pub fn new() -> Self {
        Tracer {}
    }

    pub fn trace(&mut self) {
        // TODO(now) implement
    }

    pub fn flush(&mut self) {
        // TODO(now) implement
    }
}
