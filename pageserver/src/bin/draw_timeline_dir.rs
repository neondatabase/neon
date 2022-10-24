//! A tool for visualizing the arrangement of layerfiles within a timeline.
//!
//! It reads filenames from stding and prints a svg on stdout.
//!
//! Example use:
//! ```
//! $ cd test_output/test_pgbench\[neon-45-684\]/repo/tenants/$TENANT/timelines/$TIMELINE
//! $ ls | grep "__" | cargo run --release --bin draw_timeline_dir > out.svg
//! $ firefox out.svg
//! ```
//!
//! This API was chosen so that we can easily work with filenames extracted from ssh,
//! or from pageserver log files.
//!
//! TODO Consider shipping this as a grafana panel plugin:
//!      https://grafana.com/tutorials/build-a-panel-plugin/
use svg_fmt::*;
use anyhow::Result;
use std::{collections::{BTreeMap, BTreeSet}, ops::Range};
use utils::{lsn::Lsn, project_git_version};
use pageserver::repository::Key;
use std::io::{self, BufRead};

project_git_version!(GIT_VERSION);


fn analyze<T: Ord + Copy>(coords: Vec<T>) -> (usize, BTreeMap<T, usize>) {
    let set: BTreeSet<T> = coords.into_iter().collect();

    let mut map: BTreeMap<T, usize> = BTreeMap::new();
    for (i, e) in set.iter().enumerate() {
        map.insert(*e, i);
    }

    (set.len(), map)
}


fn parse_filename(name: &str) -> (Range<Key>, Range<Lsn>) {
    let split: Vec<&str> = name.split("__").collect();
    let keys: Vec<&str> = split[0].split("-").collect();
    let mut lsns: Vec<&str> = split[1].split("-").collect();
    if lsns.len() == 1 {
        lsns.push(lsns[0]);
    }

    let keys = Key::from_hex(keys[0]).unwrap()..Key::from_hex(keys[1]).unwrap();
    let lsns = Lsn::from_hex(lsns[0]).unwrap()..Lsn::from_hex(lsns[1]).unwrap();
    (keys, lsns)
}

fn main() -> Result<()> {
    // Parse layer filenames from stdin
    let mut ranges: Vec<(Range<Key>, Range<Lsn>)> = vec![];
    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        let range = parse_filename(&line.unwrap());
        ranges.push(range);
    }

    // Collect all coordinates
    let mut keys: Vec<Key> = vec![];
    let mut lsns: Vec<Lsn> = vec![];
    for (keyr, lsnr) in &ranges {
        keys.push(keyr.start);
        keys.push(keyr.end);
        lsns.push(lsnr.start);
        lsns.push(lsnr.end);
    }

    // Analyze
    let (key_max, key_map) = analyze(keys);
    let (lsn_max, lsn_map) = analyze(lsns);

    // Initialize stats
    let mut num_deltas = 0;
    let mut num_images = 0;

    // Draw
    let stretch = 3.0;  // Stretch out vertically for better visibility
    println!("{}", BeginSvg { w: key_max as f32, h: stretch * lsn_max as f32 });
    for (keyr, lsnr) in &ranges {
        let key_start = *key_map.get(&keyr.start).unwrap();
        let key_end = *key_map.get(&keyr.end).unwrap();
        let key_diff = key_end - key_start;

        if key_start >= key_end {
            panic!("AAA");
        }

        let lsn_start = *lsn_map.get(&lsnr.start).unwrap();
        let lsn_end = *lsn_map.get(&lsnr.end).unwrap();

        let mut lsn_diff = (lsn_end - lsn_start) as f32;
        let mut fill = Fill::None;
        let mut margin = 0.05 * lsn_diff;  // Height-dependent margin to avoid overlapping
        let mut lsn_offset = 0.0;

        if lsn_start == lsn_end {  // Image
            num_images += 1;
            lsn_diff = 0.3;
            lsn_offset = -lsn_diff / 2.0;
            margin = 0.05;
            fill = Fill::Color(rgb(0, 0, 0));
        } else if lsn_start < lsn_end {  // Delta
            num_deltas += 1;
        } else {
            panic!("AAA");
        }

        println!("    {}",
            rectangle(key_start as f32 + stretch * margin,
                      stretch * (lsn_max as f32 - (lsn_end as f32 - margin - lsn_offset)),
                      key_diff as f32 - stretch * 2.0 * margin,
                      stretch * (lsn_diff - 2.0 * margin))
                .fill(fill)
                .stroke(Stroke::Color(rgb(0, 0, 0), 0.1))
                .border_radius(0.4)
        );
    }
    println!("{}", EndSvg);

    eprintln!("num_images: {}", num_images);
    eprintln!("num_deltas: {}", num_deltas);

    Ok(())
}
