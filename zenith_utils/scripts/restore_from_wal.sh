#!/bin/bash
PG_BIN=$1
WAL_PATH=$2
DATA_DIR=$3
PORT=$4
SYSID=`od -A n -j 24 -N 8 -t d8 $WAL_PATH/000000010000000000000002* | cut -c 3-`
rm -fr $DATA_DIR
env -i LD_LIBRARY_PATH=$PG_BIN/../lib $PG_BIN/initdb -E utf8 -D $DATA_DIR --sysid=$SYSID
echo port=$PORT >> $DATA_DIR/postgresql.conf
REDO_POS=0x`$PG_BIN/pg_controldata -D $DATA_DIR | fgrep "REDO location"| cut -c 42-`
declare -i WAL_SIZE=$REDO_POS+114
$PG_BIN/pg_ctl -D $DATA_DIR -l logfile start
$PG_BIN/pg_ctl -D $DATA_DIR -l logfile stop -m immediate
cp $DATA_DIR/pg_wal/000000010000000000000001 .
cp $WAL_PATH/* $DATA_DIR/pg_wal/
if [ -f $DATA_DIR/pg_wal/*.partial ]
then
	(cd $DATA_DIR/pg_wal ; for partial in \*.partial ; do mv $partial `basename $partial .partial` ; done)
fi
dd if=000000010000000000000001 of=$DATA_DIR/pg_wal/000000010000000000000001 bs=$WAL_SIZE count=1 conv=notrunc
rm -f 000000010000000000000001
