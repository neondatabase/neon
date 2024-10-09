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

#![allow(unsafe_code, clippy::expect_used)]

use lazy_static::lazy_static;
use std::{ffi, fmt, fs, io, mem, ptr};
use tikv_jemalloc_ctl::{raw, stats_print, Error as MallctlError};
use tikv_jemalloc_sys::mallctlbymib;

lazy_static! {
    static ref MIB_OPT_PROF: [usize; 2] = {
        let mut mib = [0; 2];
        raw::name_to_mib(b"opt.prof\0", &mut mib).expect("mib");
        mib
    };
    static ref MIB_PROF_ACTIVE: [usize; 2] = {
        let mut mib = [0; 2];
        raw::name_to_mib(b"prof.active\0", &mut mib).expect("mib");
        mib
    };
    static ref MIB_PROF_DUMP: [usize; 2] = {
        let mut mib = [0; 2];
        raw::name_to_mib(b"prof.dump\0", &mut mib).expect("mib");
        mib
    };
    static ref MIB_PROF_RESET: [usize; 2] = {
        let mut mib = [0; 2];
        raw::name_to_mib(b"prof.reset\0", &mut mib).expect("mib");
        mib
    };
    static ref MIB_PROF_LG_SAMPLE: [usize; 2] = {
        let mut mib = [0; 2];
        raw::name_to_mib(b"prof.lg_sample\0", &mut mib).expect("mib");
        mib
    };
    static ref MIB_PROF_THREAD_ACTIVE_INIT: [usize; 2] = {
        let mut mib = [0; 2];
        raw::name_to_mib(b"prof.thread_active_init\0", &mut mib).expect("mib");
        mib
    };
    static ref MIB_THREAD_PROF_ACTIVE: [usize; 3] = {
        let mut mib = [0; 3];
        raw::name_to_mib(b"thread.prof.active\0", &mut mib).expect("mib");
        mib
    };
}

/// Reads `opt.prof`.
#[inline]
pub fn enabled() -> Result<bool, Error> {
    // SAFETY: use correct type (bool) for this mallctl command.
    unsafe { raw::read_mib(&*MIB_OPT_PROF).map_err(Into::into) }
}

// TODO: consider a guard pattern instead of checking for each command.
fn if_enabled<F, T>(f: F) -> Result<T, Error>
where
    F: FnOnce() -> Result<T, Error>,
{
    if enabled()? {
        f()
    } else {
        Err(Error::ProfilingDisabled)
    }
}

/// Writes `prof.active`.
#[inline]
pub fn set_active(value: bool) -> Result<(), Error> {
    if_enabled(move || {
        // SAFETY: use correct type (bool) for this mallctl command.
        unsafe { raw::write_mib(&*MIB_PROF_ACTIVE, value).map_err(Into::into) }
    })
}

/// Reads `prof.active`.
#[inline]
pub fn active() -> Result<bool, Error> {
    if_enabled(move || {
        // SAFETY: use correct type (bool) for this mallctl command.
        unsafe { raw::read_mib(&*MIB_PROF_ACTIVE).map_err(Into::into) }
    })
}

/// Writes `prof.thread_active_init`.
#[inline]
pub fn set_thread_active_init(value: bool) -> Result<(), Error> {
    if_enabled(move || {
        // SAFETY: use correct param type (bool) for this mallctl command.
        unsafe { raw::write_mib(&*MIB_PROF_THREAD_ACTIVE_INIT, value).map_err(Into::into) }
    })
}

/// Reads `prof.thread_active_init`.
#[inline]
pub fn thread_active_init() -> Result<bool, Error> {
    if_enabled(move || {
        // SAFETY: use correct return type (bool) for this mallctl command.
        unsafe { raw::read_mib(&*MIB_PROF_THREAD_ACTIVE_INIT).map_err(Into::into) }
    })
}

/// Writes `thread.prof.active`.
#[inline]
pub fn set_thread_active(value: bool) -> Result<(), Error> {
    if_enabled(move || {
        // SAFETY: use correct param type (bool) for this mallctl command.
        unsafe { raw::write_mib(&*MIB_THREAD_PROF_ACTIVE, value).map_err(Into::into) }
    })
}

/// Reads `thread.prof.active`.
#[inline]
pub fn thread_active() -> Result<bool, Error> {
    if_enabled(move || {
        // SAFETY: use correct return type (bool) for this mallctl command.
        unsafe { raw::read_mib(&*MIB_THREAD_PROF_ACTIVE).map_err(Into::into) }
    })
}

/// Writes `prof.reset`. Optionally settings a new sample interval.
#[allow(clippy::option_if_let_else, clippy::borrow_as_ptr)]
#[inline]
pub fn reset(sample: Option<usize>) -> Result<(), Error> {
    if_enabled(move || {
        let value = match sample {
            Some(mut sample) => &mut sample as *mut _,
            None => ptr::null_mut(),
        };
        // SAFETY: use correct param type (*size_t) for this mallctl command.
        unsafe { write_mib_ptr(&*MIB_PROF_RESET, value).map_err(Into::into) }
    })
}

/// Reads `prof.lg_sample`.
#[inline]
pub fn sample_interval() -> Result<usize, Error> {
    if_enabled(move || {
        // SAFETY: use correct return type (size_t) for this mallctl command.
        unsafe { raw::read_mib(&*MIB_PROF_LG_SAMPLE).map_err(Into::into) }
    })
}

/// Writes `prof.dump` causing a profile dump into a file.
/// If a path is given, use the file as dump target and return its content.
/// If not, jemalloc dumps the profile to a file based on name pattern.
#[inline]
pub fn dump(path: Option<&str>) -> Result<Option<Vec<u8>>, Error> {
    if_enabled(move || {
        let ptr = match path {
            Some(s) => ffi::CString::new(s)?.into_bytes_with_nul().as_ptr(),
            None => ptr::null(),
        };

        // SAFETY: use correct param type (*char+\0) for this mallctl command.
        unsafe {
            raw::write_mib(&*MIB_PROF_DUMP, ptr)?;
        }

        match path {
            Some(path) => {
                let mut f = fs::File::open(path)?;
                let mut buf = Vec::new();
                io::copy(&mut f, &mut buf)?;
                Ok(Some(buf))
            }
            None => Ok(None),
        }
    })
}

pub fn stats() -> Result<Vec<u8>, Error> {
    let mut output = Vec::with_capacity(4096);
    let mut options = stats_print::Options::default();
    options.skip_per_arena = true;
    stats_print::stats_print(&mut output, options)?;
    Ok(output)
}

// Direct call to mallctl to allow passing null ptr if parameter is optional.
unsafe fn write_mib_ptr<T>(mib: &[usize], value: *mut T) -> Result<(), Error> {
    match mallctlbymib(
        mib.as_ptr(),
        mib.len(),
        ptr::null_mut(),
        ptr::null_mut(),
        value.cast(),
        mem::size_of::<T>(),
    ) {
        0 => Ok(()),
        c => Err(Error::MallctlCode(c)),
    }
}

#[derive(thiserror::Error, fmt::Debug)]
pub enum Error {
    #[error("mallctl: profiling disabled")]
    ProfilingDisabled,

    #[error("mallctl error: {0}")]
    Mallctl(#[from] MallctlError),

    #[error("mallctl error code: {0}")]
    MallctlCode(ffi::c_int),

    #[error("NUL byte found error: {0}")]
    Nul(#[from] ffi::NulError),

    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore]
    fn test_prof_active() {
        // _RJEM_MALLOC_CONF=prof:true,prof_active:false
        assert!(enabled().expect("get_prof_enabled"));

        assert!(!active().expect("get_prof_active"));
        set_active(true).expect("set_prof_active");
        assert!(active().expect("get_prof_active"));
        set_active(false).expect("set_prof_active");
        assert!(!active().expect("get_prof_active"));
    }

    #[test]
    #[ignore]
    fn test_prof_reset() {
        // _RJEM_MALLOC_CONF=prof:true,prof_active:false,lg_prof_sample:10
        assert!(enabled().expect("get_prof_enabled"));
        assert_eq!(10, sample_interval().expect("get_prof_lg_sample"));

        reset(None).expect("prof_reset");
        assert_eq!(10, sample_interval().expect("get_prof_lg_sample"));
        reset(Some(8)).expect("prof_reset");
        assert_eq!(8, sample_interval().expect("get_prof_lg_sample"));
        reset(None).expect("prof_reset");
        assert_eq!(8, sample_interval().expect("get_prof_lg_sample"));
    }
}
