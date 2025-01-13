use std::collections::HashMap;
use std::io::BufReader;

use camino::Utf8PathBuf;
use clap::Parser;
use itertools::Itertools as _;
use pageserver_api::key::{CompactKey, Key};
use pageserver_api::models::PageTraceEvent;
use pageserver_api::reltag::RelTag;

/// Parses a page trace (as emitted by the `page_trace` timeline API), and outputs stats.
#[derive(Parser)]
pub(crate) struct PageTraceCmd {
    /// Trace input file.
    path: Utf8PathBuf,
}

pub(crate) fn main(cmd: &PageTraceCmd) -> anyhow::Result<()> {
    let mut file = BufReader::new(std::fs::OpenOptions::new().read(true).open(&cmd.path)?);
    let mut events: Vec<PageTraceEvent> = Vec::new();
    loop {
        match bincode::deserialize_from(&mut file) {
            Ok(event) => events.push(event),
            Err(err) => {
                if let bincode::ErrorKind::Io(ref err) = *err {
                    if err.kind() == std::io::ErrorKind::UnexpectedEof {
                        break;
                    }
                }
                return Err(err.into());
            }
        }
    }

    let mut reads_by_relation: HashMap<RelTag, i64> = HashMap::new();
    let mut reads_by_key: HashMap<CompactKey, i64> = HashMap::new();

    for event in events {
        let key = Key::from_compact(event.key);
        let reltag = RelTag {
            spcnode: key.field2,
            dbnode: key.field3,
            relnode: key.field4,
            forknum: key.field5,
        };

        *reads_by_relation.entry(reltag).or_default() += 1;
        *reads_by_key.entry(event.key).or_default() += 1;
    }

    let multi_read_keys = reads_by_key
        .into_iter()
        .filter(|(_, count)| *count > 1)
        .sorted_by_key(|(key, count)| (-*count, *key))
        .collect_vec();

    println!("Multi-read keys: {}", multi_read_keys.len());
    for (key, count) in multi_read_keys {
        println!("  {key}: {count}");
    }

    let reads_by_relation = reads_by_relation
        .into_iter()
        .sorted_by_key(|(rel, count)| (-*count, *rel))
        .collect_vec();

    println!("Reads by relation:");
    for (reltag, count) in reads_by_relation {
        println!("  {reltag}: {count}");
    }

    Ok(())
}
