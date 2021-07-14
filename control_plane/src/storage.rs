use std::collections::HashMap;
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::Command;
use std::thread;
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;
use postgres::{Config, NoTls};

use crate::local_env::LocalEnv;
use crate::read_pidfile;
use pageserver::branches::BranchInfo;
use zenith_utils::connstring::connection_address;

//
// Control routines for pageserver.
//
// Used in CLI and tests.
//
pub struct PageServerNode {
    pub kill_on_exit: bool,
    pub connection_config: Option<Config>,
    pub env: LocalEnv,
}

impl PageServerNode {
    pub fn from_env(env: &LocalEnv) -> PageServerNode {
        PageServerNode {
            kill_on_exit: false,
            connection_config: None, // default
            env: env.clone(),
        }
    }

    fn default_config() -> Config {
        "postgresql://no_user@localhost:64000/no_db"
            .parse()
            .unwrap()
    }

    pub fn connection_config(&self) -> Config {
        match &self.connection_config {
            Some(config) => config.clone(),
            None => Self::default_config(),
        }
    }

    pub fn init(&self) -> Result<()> {
        let mut cmd = Command::new(self.env.pageserver_bin()?);
        let status = cmd
            .args(&[
                "--init",
                "-D",
                self.env.base_data_dir.to_str().unwrap(),
                "--postgres-distrib",
                self.env.pg_distrib_dir.to_str().unwrap(),
            ])
            .env_clear()
            .env("RUST_BACKTRACE", "1")
            .status()
            .expect("pageserver init failed");

        if status.success() {
            Ok(())
        } else {
            Err(anyhow!("pageserver init failed"))
        }
    }

    pub fn repo_path(&self) -> PathBuf {
        self.env.pageserver_data_dir()
    }

    pub fn pid_file(&self) -> PathBuf {
        self.repo_path().join("pageserver.pid")
    }

    pub fn start(&self) -> Result<()> {
        println!(
            "Starting pageserver at '{}' in {}",
            connection_address(&self.connection_config()),
            self.repo_path().display()
        );

        let mut cmd = Command::new(self.env.pageserver_bin()?);
        cmd.args(&["-D", self.repo_path().to_str().unwrap()])
            .arg("-d")
            .env_clear()
            .env("RUST_BACKTRACE", "1");

        if !cmd.status()?.success() {
            bail!(
                "Pageserver failed to start. See '{}' for details.",
                self.repo_path().join("pageserver.log").display()
            );
        }

        // It takes a while for the page server to start up. Wait until it is
        // open for business.
        for retries in 1..15 {
            let client = self.page_server_psql_client();
            if client.is_ok() {
                break;
            } else {
                println!("Pageserver not responding yet, retrying ({})...", retries);
                thread::sleep(Duration::from_secs(1));
            }
        }

        println!("Pageserver started");

        Ok(())
    }

    pub fn stop(&self) -> Result<()> {
        let pid = read_pidfile(&self.pid_file())?;
        let pid = Pid::from_raw(pid);
        if kill(pid, Signal::SIGTERM).is_err() {
            bail!("Failed to kill pageserver with pid {}", pid);
        }

        // wait for pageserver stop
        let address = connection_address(&self.connection_config());
        for _ in 0..5 {
            let stream = TcpStream::connect(&address);
            thread::sleep(Duration::from_secs(1));
            if let Err(_e) = stream {
                println!("Pageserver stopped");
                return Ok(());
            }
            println!("Stopping pageserver on {}", address);
        }

        bail!("Failed to stop pageserver with pid {}", pid);
    }

    pub fn page_server_psql(&self, sql: &str) -> Vec<postgres::SimpleQueryMessage> {
        let mut client = self.connection_config().connect(NoTls).unwrap();

        println!("Pageserver query: '{}'", sql);
        client.simple_query(sql).unwrap()
    }

    pub fn page_server_psql_client(&self) -> Result<postgres::Client, postgres::Error> {
        self.connection_config().connect(NoTls)
    }

    pub fn branches_list(&self) -> Result<Vec<BranchInfo>> {
        let mut client = self.page_server_psql_client()?;
        let query_result = client.simple_query("branch_list")?;
        let branches_json = query_result
            .first()
            .map(|msg| match msg {
                postgres::SimpleQueryMessage::Row(row) => row.get(0),
                _ => None,
            })
            .flatten()
            .ok_or_else(|| anyhow!("missing branches"))?;

        let res: Vec<BranchInfo> = serde_json::from_str(branches_json)?;
        Ok(res)
    }

    pub fn branch_create(&self, name: &str, startpoint: &str) -> Result<BranchInfo> {
        let mut client = self.page_server_psql_client()?;
        let query_result =
            client.simple_query(format!("branch_create {} {}", name, startpoint).as_str())?;

        let branch_json = query_result
            .first()
            .map(|msg| match msg {
                postgres::SimpleQueryMessage::Row(row) => row.get(0),
                _ => None,
            })
            .flatten()
            .ok_or_else(|| anyhow!("missing branch"))?;

        let res: BranchInfo = serde_json::from_str(branch_json).map_err(|e| {
            anyhow!(
                "failed to parse branch_create response: {}: {}",
                branch_json,
                e
            )
        })?;

        Ok(res)
    }

    // TODO: make this a separate request type and avoid loading all the branches
    pub fn branch_get_by_name(&self, name: &str) -> Result<BranchInfo> {
        let branch_infos = self.branches_list()?;
        let branche_by_name: Result<HashMap<String, BranchInfo>> = branch_infos
            .into_iter()
            .map(|branch_info| Ok((branch_info.name.clone(), branch_info)))
            .collect();
        let branche_by_name = branche_by_name?;

        let branch = branche_by_name
            .get(name)
            .ok_or_else(|| anyhow!("Branch {} not found", name))?;

        Ok(branch.clone())
    }
}

impl Drop for PageServerNode {
    fn drop(&mut self) {
        if self.kill_on_exit {
            let _ = self.stop();
        }
    }
}
