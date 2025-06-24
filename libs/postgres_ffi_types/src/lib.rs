//! This package contains some PostgreSQL constants and datatypes that are the same in all versions
//! of PostgreSQL and unlikely to change in the future either. These could be derived from the
//! PostgreSQL headers with 'bindgen', but in order to avoid proliferating the dependency to bindgen
//! and the PostgreSQL C headers to all services, we prefer to have this small stand-alone crate for
//! them instead.
//!
//! Be mindful in what you add here, as these types are deeply ingrained in the APIs.

pub mod constants;
pub mod forknum;

pub type Oid = u32;
pub type RepOriginId = u16;
