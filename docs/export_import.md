1. Prepare minio and create bucket (i.e. `demo2` in examples below), set env

		export S3_ENDPOINT=http://127.0.0.1:9000
		export S3_REGION=us-east-1
		export S3_ACCESSKEY=minioadmin
		export S3_SECRET=minioadmin

#### Case 1:  import from fs + export to s3

1.  Prepare datadir to load data from:

		initdb -D $PGDATA
		pg_ctl start -D $PGDATA
		psql postgres -c "create table tbl as select * from pg_class"

2. Clean up previous installation, if any.

		pgrep 'pageserver|postgres|wal_acceptor' | xargs kill -9
		rm -rf .zenith/

3. Initialize pageserver from this pgdata

		./target/debug/zenith init --snapshot-path=fs:$PGDATA

4.  Start and ensure that we see the test table

		./target/debug/zenith start
		./target/debug/zenith pg create main
		./target/debug/zenith pg start main
		psql postgres -h 127.0.0.1 -p 55432 -U anastasia -c "\d"

5. Push pageserver to s3
		./target/debug/zenith export main --snapshot-path=s3:demo2



### Case 2. Restore from s3. 

1. Clean previous installation, if any.
		pgrep 'pageserver|postgres|wal_acceptor' | xargs kill -9
		rm -rf .zenith/

2.  Initialize pageserver from s3
		./target/debug/zenith init --snapshot-path=s3:demo2

3. Start and ensure that we see the test table

		./target/debug/zenith start
		./target/debug/zenith pg create main
		./target/debug/zenith pg start main
		psql postgres -h 127.0.0.1 -p 55432 -U anastasia -c "\d"