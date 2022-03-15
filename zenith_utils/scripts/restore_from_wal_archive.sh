PG_BIN=$1
WAL_PATH=$2
DATA_DIR=$3
PORT=$4
SYSID=`od -A n -j 24 -N 8 -t d8 $WAL_PATH/000000010000000000000002* | cut -c 3-`
rm -fr $DATA_DIR /tmp/pg_wals
mkdir /tmp/pg_wals
env -i $PG_BIN/initdb -E utf8 -U zenith_admin -D $DATA_DIR --sysid=$SYSID
echo port=$PORT >> $DATA_DIR/postgresql.conf
REDO_POS=0x`$PG_BIN/pg_controldata -D $DATA_DIR | fgrep "REDO location"| cut -c 42-`
declare -i WAL_SIZE=$REDO_POS+114
cp $WAL_PATH/* /tmp/pg_wals
if [ -f $DATA_DIR/pg_wal/*.partial ]
then
	(cd /tmp/pg_wals ; for partial in \*.partial ; do  mv $partial `basename $partial .partial` ; done)
fi
dd if=$DATA_DIR/pg_wal/000000010000000000000001 of=/tmp/pg_wals/000000010000000000000001 bs=$WAL_SIZE count=1 conv=notrunc
echo > $DATA_DIR/recovery.signal
rm -f $DATA_DIR/pg_wal/*
echo "restore_command = 'cp /tmp/pg_wals/%f %p'" >> $DATA_DIR/postgresql.conf
