#!/bin/bash
set -e

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

# clang -c -o walproposer.o walproposer.c -ferror-limit=1
# ar rcs libwalproposer.a walproposer.o

make -C ../../ neon-pg-ext-v15 -s
make -C ../../pg_install/build/v15/src/backend postgres-lib -s
cp ../../pg_install/build/v15/src/backend/libpostgres.a .
cp ../../pg_install/build/v15/src/common/libpgcommon_srv.a .
cp ../../pg_install/build/v15/src/port/libpgport_srv.a .
cp ../../pg_install/build/v15/src/interfaces/libpq/libpq.a .

# -lseccomp -lssl -lcrypto -lz -lpthread -lrt -ldl -lm

clang -c -o test.o test.c -ferror-limit=1 -I ../../pg_install/v15/include/postgresql/server
rm -rf libwalproposer.a
ar rcs libwalproposer.a test.o

rm -rf libwalproposer2.a
ar rcs libwalproposer2.a ../../pg_install/build/neon-v15/*.o
