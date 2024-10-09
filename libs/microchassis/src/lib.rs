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

#![deny(unsafe_code, rust_2018_idioms)]
#![warn(
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    clippy::cargo,
    clippy::panic,
    clippy::unseparated_literal_suffix,
    clippy::unwrap_used,
    // clippy::expect_used, // TODO: revisit
    clippy::unwrap_in_result,
)]
#![allow(
    dead_code, // TODO: remove
    clippy::cargo_common_metadata, // TODO: revisit
    clippy::missing_const_for_fn,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::module_name_repetitions,
    clippy::unnecessary_wraps,
    clippy::use_self,
    clippy::unwrap_in_result, // TODO: revisit
    clippy::multiple_crate_versions,
    clippy::needless_pass_by_value,
    clippy::implicit_hasher,
)]
#![cfg_attr(not(feature = "std"), no_std)]

pub mod error;
#[cfg(feature = "jemalloc-profiling")]
pub mod oompanic;
pub mod profiling;

#[cfg(all(feature = "set-jemalloc-global", feature = "oompanic-allocator"))]
#[global_allocator]
static ALLOC: crate::oompanic::Allocator<tikv_jemallocator::Jemalloc> =
    crate::oompanic::Allocator(tikv_jemallocator::Jemalloc);

#[cfg(all(feature = "set-jemalloc-global", not(feature = "oompanic-allocator")))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
