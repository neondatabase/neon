This package will evolve into a "compute-pageserver communicator"
process and machinery. For now, it's just a dummy that doesn't do
anything interesting, but it allows us to test the compilation and
linking of Rust code into the Postgres extensions.

At compilation time, pgxn/neon/communicator/ produces a static
library, libcommunicator.a. It is linked to the neon.so extension
library.
