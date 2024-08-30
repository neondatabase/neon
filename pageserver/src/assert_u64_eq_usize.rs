//! `u64`` and `usize`` aren't guaranteed to be identical in Rust, but life is much simpler if that's the case.

pub(crate) const _ASSERT_U64_EQ_USIZE: () = {
    if std::mem::size_of::<usize>() != std::mem::size_of::<u64>() {
        panic!("the traits defined in this module assume that usize and u64 can be converted to each other without loss of information");
    }
};

pub(crate) trait U64IsUsize {
    fn into_usize(self) -> usize;
}

impl U64IsUsize for u64 {
    #[inline(always)]
    fn into_usize(self) -> usize {
        #[allow(clippy::let_unit_value)]
        let _ = _ASSERT_U64_EQ_USIZE;
        self as usize
    }
}

pub(crate) trait UsizeIsU64 {
    fn into_u64(self) -> u64;
}

impl UsizeIsU64 for usize {
    #[inline(always)]
    fn into_u64(self) -> u64 {
        #[allow(clippy::let_unit_value)]
        let _ = _ASSERT_U64_EQ_USIZE;
        self as u64
    }
}

pub const fn u64_to_usize(x: u64) -> usize {
    #[allow(clippy::let_unit_value)]
    let _ = _ASSERT_U64_EQ_USIZE;
    x as usize
}
