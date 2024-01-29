use std::{
    cell::RefCell,
    sync::{Arc, Mutex, Weak},
};

use super::PinnedSlotsPermit;

// Thread-local list of re-usable buffers.
thread_local! {
    static POOL: RefCell<Vec<Arc<Mutex<Option<super::PinnedSlotsPermit>>>>> = RefCell::new(Vec::new());
}

pub(crate) struct Pooled {
    // Always Some() except when dropping
    strong: Option<Arc<Mutex<Option<super::PinnedSlotsPermit>>>>,
}

pub(crate) fn get(permit: PinnedSlotsPermit) -> Pooled {
    let maybe = POOL.with(|rc| rc.borrow_mut().pop());
    match maybe {
        Some(arc) => {
            let mut inner = arc.lock().unwrap();
            let prev = inner.replace(permit);
            assert!(prev.is_none(), "we set it to None() on Pooled::drop()");
            drop(inner);
            Pooled { strong: Some(arc) }
        }
        None => Pooled {
            strong: Some(Arc::new(Mutex::new(Some(permit)))),
        },
    }
}

impl Pooled {
    pub(crate) fn downgrade(this: &Self) -> PooledWeak {
        PooledWeak {
            weak: Arc::downgrade(this.strong.as_ref().unwrap()),
        }
    }
}

pub(crate) struct PooledWeak {
    weak: Weak<Mutex<Option<super::PinnedSlotsPermit>>>,
}

impl PooledWeak {
    pub(crate) fn new() -> Self {
        PooledWeak { weak: Weak::new() }
    }
    pub(crate) fn upgrade(&self) -> Option<Pooled> {
        let arc = self.weak.upgrade()?;
        todo!("this is broken here, another clone of the now-ugpraded Pooled can be returned");
        Some(Pooled { strong: arc })
    }
}

impl Drop for Pooled {
    fn drop(&mut self) {
        let arc = self.strong.take().unwrap();
        let mut inner = arc.lock().unwrap();
        let permit: super::PinnedSlotsPermit = inner
            .take()
            .expect("we handed it out as Some(), should get it back as Some()");
        drop(permit);
        assert!(inner.is_none());
        drop(inner);
        POOL.with(|rc| rc.borrow_mut().push(arc))
    }
}
