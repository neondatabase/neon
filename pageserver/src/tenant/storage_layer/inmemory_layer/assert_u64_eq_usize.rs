pub(crate) const _ASSERT_U64_EQ_USIZE: () = {
    if std::mem::size_of::<usize>() != std::mem::size_of::<u64>() {
        panic!("the traits defined in this module assume that usize and u64 can be converted to each other without loss of information");
    }
};

pub(crate) trait U64IsUsize {
    fn as_usize(self) -> usize;
}

impl U64IsUsize for u64 {
    #[inline(always)]
    fn as_usize(self) -> usize {
        let _ = _ASSERT_U64_EQ_USIZE;
        self as usize
    }
}

pub(crate) trait UsizeIsU64 {
    fn as_u64(self) -> u64;
}

impl UsizeIsU64 for usize {
    #[inline(always)]
    fn as_u64(self) -> u64 {
        let _ = _ASSERT_U64_EQ_USIZE;
        self as u64
    }
}
