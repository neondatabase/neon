This module contains utilities for working with PostgreSQL file
formats. It's a collection of structs that are auto-generated from the
PostgreSQL header files using bindgen, and Rust functions to read and
manipulate them.

There are also a bunch of constants in `pg_constants.rs` that are copied
from various PostgreSQL headers, rather than auto-generated. They mostly
should be auto-generated too, but that's a TODO.

The PostgreSQL on-disk file format is not portable across different
CPU architectures and operating systems. It is also subject to change
in each major PostgreSQL version. Currently, this module is based on
PostgreSQL v14, but in the future we will probably need a separate
copy for each PostgreSQL version.

TODO: Currently, there is also some code that deals with WAL records
in pageserver/src/waldecoder.rs.  That should be moved into this
module. The rest of the codebase should not have intimate knowledge of
PostgreSQL file formats or WAL layout, that knowledge should be
encapsulated in this module.
