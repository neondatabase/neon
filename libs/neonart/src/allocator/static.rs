use std::mem::MaybeUninit;

pub fn alloc_from_slice<T>(
    area: &mut [MaybeUninit<u8>],
) -> (&mut MaybeUninit<T>, &mut [MaybeUninit<u8>]) {
    let layout = std::alloc::Layout::new::<T>();

    let area_start = area.as_mut_ptr();

    // pad to satisfy alignment requirements
    let padding = area_start.align_offset(layout.align());
    if padding + layout.size() > area.len() {
        panic!("out of memory");
    }
    let area = &mut area[padding..];
    let (result_area, remain) = area.split_at_mut(layout.size());

    let result_ptr: *mut MaybeUninit<T> = result_area.as_mut_ptr().cast();
    let result = unsafe { result_ptr.as_mut().unwrap() };

    (result, remain)
}

pub fn alloc_array_from_slice<T>(
    area: &mut [MaybeUninit<u8>],
    len: usize,
) -> (&mut [MaybeUninit<T>], &mut [MaybeUninit<u8>]) {
    let layout = std::alloc::Layout::new::<T>();

    let area_start = area.as_mut_ptr();

    // pad to satisfy alignment requirements
    let padding = area_start.align_offset(layout.align());
    if padding + layout.size() * len > area.len() {
        panic!("out of memory");
    }
    let area = &mut area[padding..];
    let (result_area, remain) = area.split_at_mut(layout.size() * len);

    let result_ptr: *mut MaybeUninit<T> = result_area.as_mut_ptr().cast();
    let result = unsafe { std::slice::from_raw_parts_mut(result_ptr.as_mut().unwrap(), len) };

    (result, remain)
}
