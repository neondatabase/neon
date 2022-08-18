//! Utils for runnig postgres binaries as subprocesses.
use std::{fs::{File, remove_dir_all}, path::PathBuf, process::{Child, Command, Stdio}, time::Duration};
use std::io::Write;


pub struct LocalPostgres {
    datadir: PathBuf,
    pg_prefix: PathBuf,
    port: u16,
    running: Option<Child>,
}

impl LocalPostgres {
    pub fn new(datadir: PathBuf, pg_prefix: PathBuf) -> Self {
        LocalPostgres {
            datadir,
            pg_prefix,
            port: 54729,
            running: None,
        }
    }

    // TODO is this the right interface? Why not start it?
    pub fn start(&mut self) {
        remove_dir_all(self.datadir.as_os_str()).ok();

        let status = Command::new(self.pg_prefix.join("initdb"))
            .arg("-D")
            .arg(self.datadir.as_os_str())
            .stdout(Stdio::null())  // TODO to file instead
            .stderr(Stdio::null())  // TODO to file instead
            .status()
            .expect("failed to get status");
        assert!(status.success());

        // Write conf
        let mut conf = File::create(self.datadir.join("postgresql.conf"))
            .expect("failed to create file");
        writeln!(&mut conf, "port = {}", self.port)
            .expect("failed to write conf");

        // TODO check if already running
        let out_file = File::create(self.datadir.join("pg.log")).expect("can't make file");
        let err_file = File::create(self.datadir.join("pg.err")).expect("can't make file");
        self.running.replace(Command::new(self.pg_prefix.join("postgres"))
            .env("PGDATA", self.datadir.as_os_str())
            .stdout(Stdio::from(out_file))
            .stderr(Stdio::from(err_file))
            .spawn()
            .expect("postgres failed to spawn"));

        std::thread::sleep(Duration::from_millis(300));
    }

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
}

impl Drop for LocalPostgres {
    fn drop(&mut self) {
        if let Some(mut child) = self.running.take() {
            child.kill().expect("failed to kill child");
        }
    }
}
