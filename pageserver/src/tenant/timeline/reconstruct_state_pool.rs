use std::cell::RefCell;

use crate::tenant::storage_layer::ValueReconstructState;

struct Content(ValueReconstructState);

impl Content {
    fn empty() -> Self {
        Content(ValueReconstructState {
            records: smallvec::SmallVec::new(),
            img: None,
            scratch: smallvec::SmallVec::new(),
        })
    }
    fn reset(&mut self) {
        let inner = &mut self.0;
        inner.records.clear();
        inner.img = None;
        inner.scratch.clear();
    }
}

pub struct Pooled(Option<Box<Content>>);

// Thread-local list of re-usable buffers.
thread_local! {
    static POOL: RefCell<Vec<Box<Content>>> = RefCell::new(Vec::new());
}

pub(crate) fn get() -> Pooled {
    let maybe = POOL.with(|rc| rc.borrow_mut().pop());
    match maybe {
        Some(buf) => Pooled(Some(buf)),
        None => Pooled(Some(Box::new(Content::empty()))),
    }
}

impl Drop for Pooled {
    fn drop(&mut self) {
        let mut content = self.0.take().unwrap();
        content.reset();
        POOL.with(|rc| rc.borrow_mut().push(content))
    }
}

impl std::ops::Deref for Pooled {
    type Target = super::ValueReconstructState;

    fn deref(&self) -> &Self::Target {
        &self.0.as_ref().unwrap().as_ref().0
    }
}

impl std::ops::DerefMut for Pooled {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0.as_mut().unwrap().as_mut().0
    }
}
