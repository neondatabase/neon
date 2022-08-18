use std::{fs::{File, remove_dir_all}, path::PathBuf, process::{Child, Command, Stdio}, time::Duration};
use std::io::Write;

pub trait PgProtocol {
    fn conn_info(&self) -> tokio_postgres::Config;
}

type Port = u16;

pub struct LocalPostgres {
    datadir: PathBuf,
    pg_prefix: PathBuf,
    port: Port,
    running: Option<Child>,
}

impl LocalPostgres {
    fn new(datadir: PathBuf, pg_prefix: PathBuf) -> Self {
        LocalPostgres {
            datadir,
            pg_prefix,
            port: 54729,
            running: None,
        }
    }

    fn start(&mut self) {
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
}

impl Drop for LocalPostgres {
    fn drop(&mut self) {
        if let Some(mut child) = self.running.take() {
            child.kill().expect("failed to kill child");
        }
    }
}

impl PgProtocol for LocalPostgres {
    fn conn_info(&self) -> tokio_postgres::Config {
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


#[cfg(test)]
mod tests {
    use tokio_postgres::NoTls;

    use super::*;

    #[tokio::test]
    async fn test_postgres_select_1() -> anyhow::Result<()> {
        // Test setup
        let pg_datadir = PathBuf::from("/home/bojan/tmp/t1/");
        let pg_prefix = PathBuf::from("/home/bojan/src/neondatabase/neon/tmp_install/bin/");

        // Get a postgres
        let mut postgres = LocalPostgres::new(pg_datadir, pg_prefix);
        postgres.start();
        let config = postgres.conn_info();

        if let Err(e) = config.connect(NoTls).await {
            eprintln!("error error {:?}", e);
        }

        // Get client, run connection
        let (client, connection) = config.connect(NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        // Run "select 1"
        let rows = client.query("SELECT 'hello';", &[]).await?;
        let value: &str = rows[0].get(0);
        assert_eq!(value, "hello");

        Ok(())
    }
}
