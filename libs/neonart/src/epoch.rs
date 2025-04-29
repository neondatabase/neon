//! This is similar to crossbeam_epoch crate, but works in shared memory
//!
//! FIXME: not implemented yet. (We haven't implemented removing any nodes from the ART
//! tree, which is why we get away without this now)

pub(crate) struct EpochPin {}

pub(crate) fn pin_epoch() -> EpochPin {
    EpochPin {}
}

/*
struct CollectorGlobal {
    epoch: AtomicU64,

    participants: CachePadded<AtomicU64>, // make it an array
}


struct CollectorQueue {

}
*/
