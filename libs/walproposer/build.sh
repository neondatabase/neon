#!/bin/bash
set -e

# TODO: rewrite to Makefile

make -C ../../ neon-pg-ext-walproposer -s
# make -C ../../pg_install/build/v15/src/backend postgres-lib -s
cp ../../pg_install/build/v15/src/backend/libpostgres.a .
cp ../../pg_install/build/v15/src/common/libpgcommon_srv.a .
cp ../../pg_install/build/v15/src/port/libpgport_srv.a .

clang -g -c libpqwalproposer.c test.c -ferror-limit=1 -I ../../pg_install/v15/include/postgresql/server -I ../../pgxn/neon
rm -rf libsim.a
ar rcs libsim.a test.o libpqwalproposer.o

rm -rf libwalproposer.a

PGXN_DIR=../../pg_install/build/neon-v15/
ar rcs libwalproposer.a $PGXN_DIR/walproposer.o $PGXN_DIR/walproposer_utils.o $PGXN_DIR/neon.o 
