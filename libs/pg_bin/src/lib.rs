//! Utils for runnig postgres binaries as subprocesses.
use std::{fs::{File, remove_dir_all}, path::PathBuf, process::{Child, Command}, time::Duration};
use std::io::Write;

use utils::command_extensions::NeonCommandExtensions;


pub struct PgDatadir {
    path: PathBuf
}

impl PgDatadir {
    pub fn new_initdb(
        path: PathBuf,
        pg_prefix: &PathBuf,
        command_output_dir: &PathBuf,
        remove_if_exists: bool
    ) -> Self {
        if remove_if_exists {
            remove_dir_all(path.clone()).ok();
        }

        let status = Command::new(pg_prefix.join("initdb"))
            .arg("-D")
            .arg(path.clone())
            .capture_to_files(command_output_dir.clone(), "initdb")
            .status()
            .expect("failed to get status");
        assert!(status.success());

        Self {
            path
        }
    }

    pub fn load_existing(path: PathBuf) -> Self{
        Self {
            path
        }
    }

    pub fn path(&self) -> PathBuf {
        self.path.clone()
    }

    pub fn spawn_postgres(self, pg_prefix: PathBuf, command_output_dir: PathBuf) -> LocalPostgres {
        let port = 54729;

        // Write conf
        // TODO don't override existing conf
        // - instead infer port from conf
        let mut conf = File::create(self.path().join("postgresql.conf")).expect("failed to create file");
        writeln!(&mut conf, "port = {}", port).expect("failed to write conf");

        let process = Command::new(pg_prefix.join("postgres"))
            .env("PGDATA", self.path())
            .capture_to_files(command_output_dir, "pg")
            .spawn()
            .expect("postgres failed to spawn");

        // Wait until ready. TODO improve this
        std::thread::sleep(Duration::from_millis(300));

        LocalPostgres {
            datadir: self,
            port: 54729,
            process,
        }
    }
}

pub struct LocalPostgres {
    datadir: PgDatadir,
    port: u16,
    process: Child,
}

impl LocalPostgres {
    pub fn admin_conn_info(&self) -> tokio_postgres::Config {
        // I don't like this, but idk what else to do
        let whoami = Command::new("whoami").output().unwrap().stdout;
        let user = String::from_utf8_lossy(&whoami);
        let user = user.trim();

        let mut config = tokio_postgres::Config::new();
        config
            .host("127.0.0.1")
            .port(self.port)
            .dbname("postgres")
            .user(&user);
        config
    }

    pub fn stop(mut self) -> PgDatadir {
        self.process.kill().expect("failed to kill child");
        PgDatadir {
            path: self.datadir.path.clone()
        }
    }
}

impl Drop for LocalPostgres {
    fn drop(&mut self) {
        self.process.kill().expect("failed to kill child");
    }
}
