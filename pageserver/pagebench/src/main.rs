use clap::Parser;

/// Component-level performance test for pageserver.
#[derive(clap::Parser)]
enum Args {
    Help,
}

fn main() {
    let args = Args::parse();
    match args {
        Args::Help => {
            eprintln!("use flag --help");
            anyhow::Ok(())
        }
    }
    .unwrap()
}
