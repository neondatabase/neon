use clap::{App, AppSettings};
use anyhow::{Result};

use crate::subcommand;

pub struct StorageCmd<'a> {
    pub clap_cmd: clap::App<'a, 'a>,
}

impl subcommand::SubCommand for StorageCmd<'_> {
    fn gen_clap_command(&self) -> clap::App {
        let c = self.clap_cmd.clone();
        c.about("Operations with zenith storage nodes")
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .subcommand(
            App::new("list")
        )
        .subcommand(
            App::new("attach")
        )
        .subcommand(
            App::new("detach")
        )
        .subcommand(
            App::new("show")
        )
    }

    fn run(&self, args: clap::ArgMatches) -> Result<()> {

        println!("Run StorageCmd with args {:?}", args);
        Ok(())
    }
}