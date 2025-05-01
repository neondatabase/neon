use std::mem::MaybeUninit;

pub struct StaticAllocator<'t> {
    area: &'t mut [MaybeUninit<u8>],
}

impl<'t> StaticAllocator<'t> {
    pub fn new(_area: &'t mut [MaybeUninit<u8>]) -> StaticAllocator<'t> {
        todo!()
    }

    /*
        pub fn alloc<T>(&mut self, _init: T) -> &'t T {
            todo!()
    }
        */

    pub fn alloc_uninit<T>(&mut self) -> &'t mut MaybeUninit<T> {
        todo!()
    }

    pub fn remaining(self) -> &'t mut [MaybeUninit<u8>] {
        self.area
    }

    pub fn align(&mut self, _alignment: usize) {
        todo!()
    }

    /*

        pub fn static_alloc<'a, T: Sized>(&'a self, value: T) -> AllocatedBox<'a, T> {
            let sz = std::mem::size_of::<T>();

            // pad all allocations to MAXALIGN boundaries
            assert!(std::mem::align_of::<T>() <= MAXALIGN);
            let sz = sz.next_multiple_of(MAXALIGN);

            let offset = self.allocated.fetch_add(sz, Ordering::Relaxed);

            if offset + sz > self.size {
                panic!("out of memory");
            }

            let inner = unsafe {
                let inner = self.area.offset(offset as isize).cast::<T>();
                *inner = value;
                NonNull::new_unchecked(inner)
            };

            AllocatedBox {
                inner,
                _phantom: PhantomData,
            }
    }
        */
}
