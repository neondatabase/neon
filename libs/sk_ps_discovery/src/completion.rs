pub struct Waiter {
    inner: Arc<Inner>,
}

struct Inner {
    next: Box<Inner>,
}

impl Waiter {
    pub fn
}
