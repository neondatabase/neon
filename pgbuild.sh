#!/bin/sh
#
#   Purpose of this script is to build and install postgres in a local directory
# so that zenith intergation tests would find pg binaries and support files.
#
# ./pgbuild.sh would do following:
#
#   1) run out-of-source build of postgres in REPO_ROOT/tmp_install/build directory (I'm reusing
#  tmp_install path here since it is already present in .gitignore)
#
#   2) installs postgres to REPO_ROOT/tmp_install/
#
REPO_ROOT=$(dirname "$0")
REPO_ROOT="`( cd \"$REPO_ROOT\" && pwd )`"

# configure
mkdir -p $REPO_ROOT/tmp_install/build
cd $REPO_ROOT/tmp_install/build
../../vendor/postgres/configure CFLAGS='-O0' --enable-debug --enable-cassert \
    --enable-depend --with-libxml --prefix=/

# compile
make -j8
export DESTDIR=$REPO_ROOT/tmp_install
make install
