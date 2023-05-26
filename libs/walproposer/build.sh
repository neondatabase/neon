#!/bin/bash

# cc -c -o walproposer.o \
#  -static \
#  -I /Users/arthur/zen/zenith/pg_install/v15/include/postgresql/server \
#  /Users/arthur/zen/zenith/pgxn/neon/walproposer.c

# cc -shared \
#     -I /Users/arthur/zen/zenith/pg_install/v15/include/postgresql/server \
#     -I /Users/arthur/zen/zenith/pgxn/neon \
#     -L /Users/arthur/zen/zenith/pg_install/build/v15/src/common \
#     -L /Users/arthur/zen/zenith/pg_install/build/v15/src/port \
#     -lpgcommon -lpgport -lz -lreadline -lm \
#     -o walproposer.so walproposer.o

clang -c -o walproposer.o walproposer.c -ferror-limit=1
ar rcs libwalproposer.a walproposer.o