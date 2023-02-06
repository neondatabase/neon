/// Provides Send wrappers of Rc and RefMut.
use std::{
    borrow::Borrow,
    cell::{Ref, RefCell, RefMut},
    ops::{Deref, DerefMut},
    rc::Rc,
};

/// Rc wrapper which is Send.
/// This is useful to allow transferring a group of Rcs pointing to the same
/// object between threads, e.g. in self referential struct.
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SendRc<T>
where
    T: ?Sized,
{
    rc: Rc<T>,
}

// SAFETY: Passing Rc(s)<T: Send> between threads is fine as long as there is no
// concurrent access to the object they point to, so you must move all such Rcs
// together. This appears to be impossible to express in rust type system and
// SendRc doesn't provide any additional protection -- but unlike sendable
// crate, neither it requires any additional actions before/after move. Ensuring
// that sending conforms to the above is the responsibility of the type user.
unsafe impl<T: ?Sized + Send> Send for SendRc<T> {}

impl<T> SendRc<T> {
    /// Constructs a new SendRc<T>
    pub fn new(value: T) -> SendRc<T> {
        SendRc { rc: Rc::new(value) }
    }
}

// https://stegosaurusdormant.com/understanding-derive-clone/ explains in detail
// why derive Clone doesn't work here.
impl<T> Clone for SendRc<T> {
    fn clone(&self) -> Self {
        SendRc {
            rc: self.rc.clone(),
        }
    }
}

// Deref into inner rc.
impl<T> Deref for SendRc<T> {
    type Target = Rc<T>;

    fn deref(&self) -> &Self::Target {
        &self.rc
    }
}

/// Extends RefCell with borrow[_mut] variants which return Sendable Ref[Mut]
/// wrappers.
pub trait RefCellSend<T: ?Sized> {
    fn borrow_mut_send(&self) -> RefMutSend<'_, T>;
}

impl<T: Sized> RefCellSend<T> for RefCell<T> {
    fn borrow_mut_send(&self) -> RefMutSend<'_, T> {
        RefMutSend {
            ref_mut: self.borrow_mut(),
        }
    }
}

/// RefMut wrapper which is Send. See impl Send for safety. Allows to move a
/// RefMut along with RefCell it originates from between threads, e.g. have Send
/// Future containing RefMut.
#[derive(Debug)]
pub struct RefMutSend<'b, T>
where
    T: 'b + ?Sized,
{
    ref_mut: RefMut<'b, T>,
}

// SAFETY: Similar to SendRc, this is safe as long as RefMut stays in the same
// thread with original RefCell, so they should be passed together.
// Actually, since this is a referential type violating this is not
// straightforward; examples of unsafe usage could be
// - Passing a RefMut to different thread without source RefCell. Seems only
//   possible with std::thread::scope.
// - Somehow multiple threads get access to single RefCell concurrently,
//   violating its !Sync requirement. Improper usage of SendRc can do that.
unsafe impl<'b, T: ?Sized + Send> Send for RefMutSend<'b, T> {}

impl<'b, T> RefMutSend<'b, T> {
    /// Constructs a new RefMutSend<T>
    pub fn new(ref_mut: RefMut<'b, T>) -> RefMutSend<'b, T> {
        RefMutSend { ref_mut }
    }
}

// Deref into inner RefMut.
impl<'b, T> Deref for RefMutSend<'b, T>
where
    T: 'b + ?Sized,
{
    type Target = RefMut<'b, T>;

    fn deref<'a>(&'a self) -> &'a RefMut<'b, T> {
        &self.ref_mut
    }
}

// DerefMut into inner RefMut.
impl<'b, T> DerefMut for RefMutSend<'b, T>
where
    T: 'b + ?Sized,
{
    fn deref_mut<'a>(&'a mut self) -> &'a mut RefMut<'b, T> {
        &mut self.ref_mut
    }
}
