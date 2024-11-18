#!/bin/bash

# like restore_from_wal.sh, but takes existing initdb.tar.zst

set -euxo pipefail

PG_BIN=$1
WAL_PATH=$2
DATA_DIR=$3
PORT=$4
echo "port=$PORT" >> "$DATA_DIR"/postgresql.conf
echo "shared_preload_libraries='\$libdir/neon_rmgr.so'" >> "$DATA_DIR"/postgresql.conf
REDO_POS=0x$("$PG_BIN"/pg_controldata -D "$DATA_DIR" | grep -F "REDO location"| cut -c 42-)
declare -i WAL_SIZE=$REDO_POS+114
"$PG_BIN"/pg_ctl -D "$DATA_DIR" -l "$DATA_DIR/logfile.log" start
"$PG_BIN"/pg_ctl -D "$DATA_DIR" -l "$DATA_DIR/logfile.log" stop -m immediate
cp "$DATA_DIR"/pg_wal/000000010000000000000001 "$DATA_DIR"
cp "$WAL_PATH"/* "$DATA_DIR"/pg_wal/
for partial in "$DATA_DIR"/pg_wal/*.partial ; do mv "$partial" "${partial%.partial}" ; done
dd if="$DATA_DIR"/000000010000000000000001 of="$DATA_DIR"/pg_wal/000000010000000000000001 bs=$WAL_SIZE count=1 conv=notrunc
rm -f "$DATA_DIR"/000000010000000000000001
