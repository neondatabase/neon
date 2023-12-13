use clap::Parser;

pub(crate) mod util;

mod basebackup;

/// Component-level performance test for pageserver.
#[derive(clap::Parser)]
enum Args {
    Basebackup(basebackup::Args),
}

fn main() {
    let args = Args::parse();
    match args {
        Args::Basebackup(args) => basebackup::main(args),
    }
    .unwrap()
}
