pub trait Alignment: std::marker::Unpin + 'static {
    /// Returns the required alignments.
    fn align(&self) -> usize;
}

/// Alignment at compile time.
#[derive(Debug, Clone, Copy)]
pub struct ConstAlign<const A: usize>;

impl<const A: usize> Alignment for ConstAlign<A> {
    fn align(&self) -> usize {
        A
    }
}

/// Alignment at run time.
#[derive(Debug, Clone, Copy)]
pub struct RuntimeAlign {
    align: usize,
}

impl Alignment for RuntimeAlign {
    fn align(&self) -> usize {
        self.align
    }
}
