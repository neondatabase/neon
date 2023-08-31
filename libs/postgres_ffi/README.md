This module contains utilities for working with PostgreSQL file
formats. It's a collection of structs that are auto-generated from the
PostgreSQL header files using bindgen, and Rust functions to read and
manipulate them.

There are also a bunch of constants in `pg_constants.rs` that are copied
from various PostgreSQL headers, rather than auto-generated. They mostly
should be auto-generated too, but that's a TODO.

The PostgreSQL on-disk file format is not portable across different
CPU architectures and operating systems. It is also subject to change
in each major PostgreSQL version. Currently, this module supports
PostgreSQL v14, v15 and v16: bindings and code that depends on them are
version-specific.
This code is organized in modules `postgres_ffi::v14`, `postgres_ffi::v15` and
`postgres_ffi::v16`. Version independent code is explicitly exported into
shared `postgres_ffi`.


TODO: Currently, there is also some code that deals with WAL records
in pageserver/src/waldecoder.rs.  That should be moved into this
module. The rest of the codebase should not have intimate knowledge of
PostgreSQL file formats or WAL layout, that knowledge should be
encapsulated in this module.
