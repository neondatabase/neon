// Copyright 2023 Folke Behrens
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use core::{
    alloc::{GlobalAlloc, Layout},
    cell::Cell,
};

/// A global allocator wrapper that does what the oom=panic setting
/// hopefully soon will do.
pub struct Allocator<T: GlobalAlloc>(pub T);

thread_local! {
    static PANICKING: Cell<bool> = const { Cell::new(false) };
}

#[allow(clippy::panic)]
#[inline]
fn panic_alloc() -> ! {
    PANICKING.with(|v| v.set(true));
    panic!("memory allocation failed");
}

#[allow(unsafe_code)]
unsafe impl<T: GlobalAlloc> GlobalAlloc for Allocator<T> {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = self.0.alloc(layout);
        if ptr.is_null() && !PANICKING.with(Cell::get) {
            panic_alloc();
        }
        ptr
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = self.0.alloc_zeroed(layout);
        if ptr.is_null() && !PANICKING.with(Cell::get) {
            panic_alloc();
        }
        ptr
    }

    #[inline]
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let ptr = self.0.realloc(ptr, layout, new_size);
        if ptr.is_null() && !PANICKING.with(Cell::get) && new_size > layout.size() {
            panic_alloc();
        }
        ptr
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        self.0.dealloc(ptr, layout);
    }
}
