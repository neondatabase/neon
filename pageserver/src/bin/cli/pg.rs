use clap::{App, AppSettings, Arg};
use anyhow::{Result};

use crate::subcommand;



pub struct PgCmd<'a> {
    pub clap_cmd: clap::App<'a, 'a>,
}

impl subcommand::SubCommand for PgCmd<'_> {
    fn gen_clap_command(&self) -> clap::App {
        let c = self.clap_cmd.clone();
        c.about("Operations with zenith compute nodes")
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .subcommand(
            App::new("list")
            .about("List existing compute nodes")
        )
        .subcommand(
            App::new("create")
            .about("Create (init) new data directory using given storage and start postgres")
            .arg(Arg::with_name("name")
                .short("n")
                .long("name")
                .takes_value(true)
                .help("Name of the compute node"))
            .arg(Arg::with_name("storage")
                .short("s")
                .long("storage")
                .takes_value(true)
                .help("Name of the storage node to use"))
            //TODO should it be just name of uploaded snapshot or some path?
            .arg(Arg::with_name("snapshot")
                .long("snapshot")
                .takes_value(true)
                .help("Name of the snapshot to use"))
            .arg(Arg::with_name("nostart")
                .long("no-start")
                .takes_value(false)
                .help("Don't start postgres on the created node"))
        )
        .subcommand(
            App::new("destroy")
            .about("Stop postgres and destroy node's data directory")
            .arg(Arg::with_name("name")
                .short("n")
                .long("name")
                .takes_value(true)
                .help("Name of the compute node"))
        )
        .subcommand(
            App::new("start")
            .about("Start postgres on the given node")
            .arg(Arg::with_name("name")
                .short("n")
                .long("name")
                .takes_value(true)
                .help("Name of the compute node"))
            .arg(Arg::with_name("replica")
                .long("replica")
                .takes_value(false)
                .help("Start the compute node as replica"))
        )
        .subcommand(
            App::new("stop")
            .about("Stop postgres on the given node")
            .arg(Arg::with_name("name")
                .short("n")
                .long("name")
                .takes_value(true)
                .help("Name of the compute node"))

        )
        .subcommand(
            App::new("show")
            .about("Show info about the given node")
            .arg(Arg::with_name("name")
                .short("n")
                .long("name")
                .takes_value(true)
                .help("Name of the compute node"))

        )
    }

    fn run(&self, args: clap::ArgMatches) -> Result<()> {

        println!("Run PgCmd with args {:?}", args);
        Ok(())
    }
}