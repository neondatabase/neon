Create a new Zenith repository in the current directory:

    ~/git-sandbox/zenith (cli-v2)$ ./target/debug/cli init
    The files belonging to this database system will be owned by user "heikki".
    This user must also own the server process.
    
    The database cluster will be initialized with locale "en_GB.UTF-8".
    The default database encoding has accordingly been set to "UTF8".
    The default text search configuration will be set to "english".
    
    Data page checksums are disabled.
    
    creating directory tmp ... ok
    creating subdirectories ... ok
    selecting dynamic shared memory implementation ... posix
    selecting default max_connections ... 100
    selecting default shared_buffers ... 128MB
    selecting default time zone ... Europe/Helsinki
    creating configuration files ... ok
    running bootstrap script ... ok
    performing post-bootstrap initialization ... ok
    syncing data to disk ... ok
    
    initdb: warning: enabling "trust" authentication for local connections
    You can change this by editing pg_hba.conf or using the option -A, or
    --auth-local and --auth-host, the next time you run initdb.
    new zenith repository was created in .zenith

Initially, there is only one branch:

    ~/git-sandbox/zenith (cli-v2)$ ./target/debug/cli branch
      main

Start a local Postgres instance on the branch:

    ~/git-sandbox/zenith (cli-v2)$ ./target/debug/cli start main
    Creating data directory from snapshot at 0/15FFB08...
    waiting for server to start....2021-04-13 09:27:43.919 EEST [984664] LOG:  starting PostgreSQL 14devel on x86_64-pc-linux-gnu, compiled by gcc (Debian 10.2.1-6) 10.2.1 20210110, 64-bit
    2021-04-13 09:27:43.920 EEST [984664] LOG:  listening on IPv6 address "::1", port 5432
    2021-04-13 09:27:43.920 EEST [984664] LOG:  listening on IPv4 address "127.0.0.1", port 5432
    2021-04-13 09:27:43.927 EEST [984664] LOG:  listening on Unix socket "/tmp/.s.PGSQL.5432"
    2021-04-13 09:27:43.939 EEST [984665] LOG:  database system was interrupted; last known up at 2021-04-13 09:27:33 EEST
    2021-04-13 09:27:43.939 EEST [984665] LOG:  creating missing WAL directory "pg_wal/archive_status"
    2021-04-13 09:27:44.189 EEST [984665] LOG:  database system was not properly shut down; automatic recovery in progress
    2021-04-13 09:27:44.195 EEST [984665] LOG:  invalid record length at 0/15FFB80: wanted 24, got 0
    2021-04-13 09:27:44.195 EEST [984665] LOG:  redo is not required
    2021-04-13 09:27:44.225 EEST [984664] LOG:  database system is ready to accept connections
     done
    server started

Run some commands against it:

    ~/git-sandbox/zenith (cli-v2)$ psql postgres -c "create table foo (t text);" 
    CREATE TABLE
    ~/git-sandbox/zenith (cli-v2)$ psql postgres -c "insert into foo values ('inserted on the main branch');" 
    INSERT 0 1
    ~/git-sandbox/zenith (cli-v2)$ psql postgres -c "select * from foo" 
                  t              
    -----------------------------
     inserted on the main branch
    (1 row)

Create a new branch called 'experimental'. We create it from the
current end of the 'main' branch, but you could specify a different
LSN as the start point instead.

    ~/git-sandbox/zenith (cli-v2)$ ./target/debug/cli branch experimental main
    branching at end of WAL: 0/161F478
    
    ~/git-sandbox/zenith (cli-v2)$ ./target/debug/cli branch 
      experimental
      main

Start another Postgres instance off the 'experimental' branch:

    ~/git-sandbox/zenith (cli-v2)$ ./target/debug/cli start experimental -- -o -p5433
    Creating data directory from snapshot at 0/15FFB08...
    waiting for server to start....2021-04-13 09:28:41.874 EEST [984766] LOG:  starting PostgreSQL 14devel on x86_64-pc-linux-gnu, compiled by gcc (Debian 10.2.1-6) 10.2.1 20210110, 64-bit
    2021-04-13 09:28:41.875 EEST [984766] LOG:  listening on IPv6 address "::1", port 5433
    2021-04-13 09:28:41.875 EEST [984766] LOG:  listening on IPv4 address "127.0.0.1", port 5433
    2021-04-13 09:28:41.883 EEST [984766] LOG:  listening on Unix socket "/tmp/.s.PGSQL.5433"
    2021-04-13 09:28:41.896 EEST [984767] LOG:  database system was interrupted; last known up at 2021-04-13 09:27:33 EEST
    2021-04-13 09:28:42.265 EEST [984767] LOG:  database system was not properly shut down; automatic recovery in progress
    2021-04-13 09:28:42.269 EEST [984767] LOG:  redo starts at 0/15FFB80
    2021-04-13 09:28:42.272 EEST [984767] LOG:  invalid record length at 0/161F4B0: wanted 24, got 0
    2021-04-13 09:28:42.272 EEST [984767] LOG:  redo done at 0/161F478 system usage: CPU: user: 0.00 s, system: 0.00 s, elapsed: 0.00 s
    2021-04-13 09:28:42.321 EEST [984766] LOG:  database system is ready to accept connections
     done
    server started

Insert some a row on the 'experimental' branch:

    ~/git-sandbox/zenith (cli-v2)$ psql postgres -p5433 -c "select * from foo" 
                  t              
    -----------------------------
     inserted on the main branch
    (1 row)
    
    ~/git-sandbox/zenith (cli-v2)$ psql postgres -p5433 -c "insert into foo values ('inserted on experimental')" 
    INSERT 0 1
    ~/git-sandbox/zenith (cli-v2)$ psql postgres -p5433 -c "select * from foo" 
                  t              
    -----------------------------
     inserted on the main branch
     inserted on experimental
    (2 rows)
    
See that the other Postgres instance is still running on 'main' branch on port 5432:


    ~/git-sandbox/zenith (cli-v2)$ psql postgres -p5432 -c "select * from foo" 
                  t              
    -----------------------------
     inserted on the main branch
    (1 row)




Everything is stored in the .zenith directory:

    ~/git-sandbox/zenith (cli-v2)$ ls -l .zenith/
    total 12
    drwxr-xr-x 4 heikki heikki 4096 Apr 13 09:28 datadirs
    drwxr-xr-x 4 heikki heikki 4096 Apr 13 09:27 refs
    drwxr-xr-x 4 heikki heikki 4096 Apr 13 09:28 timelines

The 'datadirs' directory contains the datadirs of the running instances:

    ~/git-sandbox/zenith (cli-v2)$ ls -l .zenith/datadirs/
    total 8
    drwx------ 18 heikki heikki 4096 Apr 13 09:27 3c0c634c1674079b2c6d4edf7c91523e
    drwx------ 18 heikki heikki 4096 Apr 13 09:28 697e3c103d4b1763cd6e82e4ff361d76
    ~/git-sandbox/zenith (cli-v2)$ ls -l .zenith/datadirs/3c0c634c1674079b2c6d4edf7c91523e/
    total 124
    drwxr-xr-x 5 heikki heikki  4096 Apr 13 09:27 base
    drwxr-xr-x 2 heikki heikki  4096 Apr 13 09:27 global
    drwxr-xr-x 2 heikki heikki  4096 Apr 13 09:27 pg_commit_ts
    drwxr-xr-x 2 heikki heikki  4096 Apr 13 09:27 pg_dynshmem
    -rw------- 1 heikki heikki  4760 Apr 13 09:27 pg_hba.conf
    -rw------- 1 heikki heikki  1636 Apr 13 09:27 pg_ident.conf
    drwxr-xr-x 4 heikki heikki  4096 Apr 13 09:32 pg_logical
    drwxr-xr-x 4 heikki heikki  4096 Apr 13 09:27 pg_multixact
    drwxr-xr-x 2 heikki heikki  4096 Apr 13 09:27 pg_notify
    drwxr-xr-x 2 heikki heikki  4096 Apr 13 09:27 pg_replslot
    drwxr-xr-x 2 heikki heikki  4096 Apr 13 09:27 pg_serial
    drwxr-xr-x 2 heikki heikki  4096 Apr 13 09:27 pg_snapshots
    drwxr-xr-x 2 heikki heikki  4096 Apr 13 09:27 pg_stat
    drwxr-xr-x 2 heikki heikki  4096 Apr 13 09:34 pg_stat_tmp
    drwxr-xr-x 2 heikki heikki  4096 Apr 13 09:27 pg_subtrans
    drwxr-xr-x 2 heikki heikki  4096 Apr 13 09:27 pg_tblspc
    drwxr-xr-x 2 heikki heikki  4096 Apr 13 09:27 pg_twophase
    -rw------- 1 heikki heikki     3 Apr 13 09:27 PG_VERSION
    lrwxrwxrwx 1 heikki heikki    52 Apr 13 09:27 pg_wal -> ../../timelines/3c0c634c1674079b2c6d4edf7c91523e/wal
    drwxr-xr-x 2 heikki heikki  4096 Apr 13 09:27 pg_xact
    -rw------- 1 heikki heikki    88 Apr 13 09:27 postgresql.auto.conf
    -rw------- 1 heikki heikki 28688 Apr 13 09:27 postgresql.conf
    -rw------- 1 heikki heikki    96 Apr 13 09:27 postmaster.opts
    -rw------- 1 heikki heikki   149 Apr 13 09:27 postmaster.pid

Note how 'pg_wal' is just a symlink to the 'timelines' directory. The
datadir is ephemeral, you can delete it at any time, and it can be reconstructed
from the snapshots and WAL stored in the 'timelines' directory. So if you push/pull
the repository, the 'datadirs' are not included. (They are like git working trees)

    ~/git-sandbox/zenith (cli-v2)$ killall -9 postgres
    ~/git-sandbox/zenith (cli-v2)$ rm -rf .zenith/datadirs/*
    ~/git-sandbox/zenith (cli-v2)$ ./target/debug/cli start experimental -- -o -p5433
    Creating data directory from snapshot at 0/15FFB08...
    waiting for server to start....2021-04-13 09:37:05.476 EEST [985340] LOG:  starting PostgreSQL 14devel on x86_64-pc-linux-gnu, compiled by gcc (Debian 10.2.1-6) 10.2.1 20210110, 64-bit
    2021-04-13 09:37:05.477 EEST [985340] LOG:  listening on IPv6 address "::1", port 5433
    2021-04-13 09:37:05.477 EEST [985340] LOG:  listening on IPv4 address "127.0.0.1", port 5433
    2021-04-13 09:37:05.487 EEST [985340] LOG:  listening on Unix socket "/tmp/.s.PGSQL.5433"
    2021-04-13 09:37:05.498 EEST [985341] LOG:  database system was interrupted; last known up at 2021-04-13 09:27:33 EEST
    2021-04-13 09:37:05.808 EEST [985341] LOG:  database system was not properly shut down; automatic recovery in progress
    2021-04-13 09:37:05.813 EEST [985341] LOG:  redo starts at 0/15FFB80
    2021-04-13 09:37:05.815 EEST [985341] LOG:  invalid record length at 0/161F770: wanted 24, got 0
    2021-04-13 09:37:05.815 EEST [985341] LOG:  redo done at 0/161F738 system usage: CPU: user: 0.00 s, system: 0.00 s, elapsed: 0.00 s
    2021-04-13 09:37:05.866 EEST [985340] LOG:  database system is ready to accept connections
     done
    server started
    ~/git-sandbox/zenith (cli-v2)$ psql postgres -p5433 -c "select * from foo" 
                  t              
    -----------------------------
     inserted on the main branch
     inserted on experimental
    (2 rows)

