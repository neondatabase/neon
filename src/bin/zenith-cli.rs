
use clap::{App, AppSettings, Arg};

fn main() {

    let matches = App::new("zenith")
        .about("Zenith CLI")
        .version("1.0")
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .subcommand(
            App::new("pg")
                .about("compute node postgres")
                .setting(AppSettings::SubcommandRequiredElseHelp)
                .subcommand(
                    App::new("list")
                )
                .subcommand(
                    App::new("create")
                    .arg(Arg::with_name("pgdata").required(true)),
                )
                .subcommand(
                    App::new("destroy")
                )
                .subcommand(
                    App::new("start")
                )
                .subcommand(
                    App::new("stop")
                )
                .subcommand(
                    App::new("show")
                ),
        )
        .subcommand(
            App::new("storage")
                .about("storage node postgres")
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
                ),
        )
        .get_matches();


    if let Some(subcommand) = matches.subcommand_name() {
        println!("'git {}' was used", subcommand);
    }

    match matches.subcommand() {
        ("pg", Some(pg_matches)) => {
            println!("pg subcommand");
            match pg_matches.subcommand()
            {
                ("list", _) => {
                    println!("list subcommand");
                }
                ("create", _) => {
                }
                ("destroy", _) => {
                }
                ("start", _) => {
                }
                ("stop", _) => {
                }
                ("show", _) => {
                }
                ("", None) => println!("No subcommand"),
                _ => unreachable!(),
            }
        }
        ("storage", Some(storage_matches)) => {
            println!("storage subcommand");
            match storage_matches.subcommand()
            {
                ("list", _) => {
                    println!("list subcommand");
                }
                ("attach", _) => {
                }
                ("detach", _) => {
                }
                ("show", _) => {
                }
                ("", None) => println!("No subcommand"),
                _ => unreachable!(),
            }
        }

        ("", None) => println!("No subcommand"),
        _ => unreachable!(),
    }
}
