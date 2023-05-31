#!/bin/bash
set -e

# TODO: rewrite to Makefile

# make -C ../../ neon-pg-ext-v15 -s
# make -C ../../pg_install/build/v15/src/backend postgres-lib -s
cp ../../pg_install/build/v15/src/backend/libpostgres.a .
cp ../../pg_install/build/v15/src/common/libpgcommon_srv.a .
cp ../../pg_install/build/v15/src/port/libpgport_srv.a .

clang -g -c -o test.o test.c -ferror-limit=1 -I ../../pg_install/v15/include/postgresql/server
rm -rf libwalproposer.a
ar rcs libwalproposer.a test.o

rm -rf libwalproposer2.a
ar rcs libwalproposer2.a ../../pg_install/build/neon-v15/*.o
