use clap::{App, Arg};
use pageserver::page_service::PagestreamFeMessage;
use std::{
    fs::{read_dir, File},
    io::BufReader,
    path::PathBuf,
    str::FromStr,
};
use utils::{
    id::{TenantId, TimelineId},
    pq_proto::{BeMessage, FeMessage},
};

fn main() -> anyhow::Result<()> {
    // TODO upgrade to struct macro arg parsing
    let arg_matches = App::new("Pageserver trace visualization tool")
        .about("Makes a svg file that displays the read pattern")
        .arg(
            Arg::new("traces_dir")
                .takes_value(true)
                .help("Directory where the read traces are stored"),
        )
        .get_matches();

    let traces_dir = PathBuf::from(arg_matches.value_of("traces_dir").unwrap());
    for tenant_dir in read_dir(traces_dir)? {
        let entry = tenant_dir?;
        let path = entry.path();
        let _tenant_id = TenantId::from_str(path.file_name().unwrap().to_str().unwrap())?;

        for timeline_dir in read_dir(path)? {
            let entry = timeline_dir?;
            let path = entry.path();
            let _timeline_id = TimelineId::from_str(path.file_name().unwrap().to_str().unwrap())?;

            for trace_dir in read_dir(path)? {
                let entry = trace_dir?;
                let path = entry.path();
                let _conn_id = TimelineId::from_str(path.file_name().unwrap().to_str().unwrap())?;

                let file = File::open(path.clone())?;
                let mut reader = BufReader::new(file);
                while let Ok(msg) = PagestreamFeMessage::parse(&mut reader) {
                    println!("Parsed message {:?}", msg);
                    // TODO add to svg
                }
            }
        }
    }

    Ok(())
}
