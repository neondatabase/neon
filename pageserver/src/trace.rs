use crate::page_service::PagestreamFeMessage;

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

    pub fn trace(&mut self, _msg: &PagestreamFeMessage) {
        // TODO(now) implement
    }

    pub fn flush(&mut self) {
        // TODO(now) implement
    }
}
