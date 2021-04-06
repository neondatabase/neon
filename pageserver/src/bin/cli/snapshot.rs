use anyhow::Result;
use clap::{App, AppSettings, Arg};

use crate::subcommand;

pub struct SnapshotCmd<'a> {
    pub clap_cmd: clap::App<'a, 'a>,
}

impl subcommand::SubCommand for SnapshotCmd<'_> {
    fn gen_clap_command(&self) -> clap::App {
        let c = self.clap_cmd.clone();
        c.about("Operations with zenith snapshots")
            .setting(AppSettings::SubcommandRequiredElseHelp)
            .subcommand(App::new("list"))
            .subcommand(App::new("create").arg(Arg::with_name("pgdata").required(true)))
            .subcommand(App::new("destroy"))
            .subcommand(App::new("start"))
            .subcommand(App::new("stop"))
            .subcommand(App::new("show"))
    }

    fn run(&self, args: clap::ArgMatches) -> Result<()> {
        println!("Run SnapshotCmd with args {:?}", args);
        Ok(())
    }
}
