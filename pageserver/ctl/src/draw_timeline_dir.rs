//! A tool for visualizing the arrangement of layerfiles within a timeline.
//!
//! It reads filenames from stdin and prints a svg on stdout. The image is a plot in
//! page-lsn space, where every delta layer is a rectangle and every image layer is a
//! thick line. Legend:
//! - The x axis (left to right) represents page index.
//! - The y axis represents LSN, growing upwards.
//!
//! Coordinates in both axis are compressed for better readability.
//! (see <https://medium.com/algorithms-digest/coordinate-compression-2fff95326fb>)
//!
//! Example use:
//! ```bash
//! $ ls test_output/test_pgbench\[neon-45-684\]/repo/tenants/$TENANT/timelines/$TIMELINE | \
//! $   grep "__" | cargo run --release --bin pagectl draw-timeline-dir > out.svg
//! $ firefox out.svg
//! ```
//!
//! This API was chosen so that we can easily work with filenames extracted from ssh,
//! or from pageserver log files.
//!
//! TODO Consider shipping this as a grafana panel plugin:
//!      <https://grafana.com/tutorials/build-a-panel-plugin/>
use anyhow::Result;
use pageserver::repository::Key;
use pageserver::METADATA_FILE_NAME;
use std::cmp::Ordering;
use std::io::{self, BufRead};
use std::path::PathBuf;
use std::str::FromStr;
use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Range,
};
use svg_fmt::{rectangle, rgb, BeginSvg, EndSvg, Fill, Stroke};
use utils::{lsn::Lsn, project_git_version};

project_git_version!(GIT_VERSION);

// Map values to their compressed coordinate - the index the value
// would have in a sorted and deduplicated list of all values.
fn build_coordinate_compression_map<T: Ord + Copy>(coords: Vec<T>) -> BTreeMap<T, usize> {
    let set: BTreeSet<T> = coords.into_iter().collect();

    let mut map: BTreeMap<T, usize> = BTreeMap::new();
    for (i, e) in set.iter().enumerate() {
        map.insert(*e, i);
    }

    map
}

fn parse_filename(name: &str) -> (Range<Key>, Range<Lsn>) {
    let split: Vec<&str> = name.split("__").collect();
    let keys: Vec<&str> = split[0].split('-').collect();
    let mut lsns: Vec<&str> = split[1].split('-').collect();
    if lsns.len() == 1 {
        lsns.push(lsns[0]);
    }

    let keys = Key::from_hex(keys[0]).unwrap()..Key::from_hex(keys[1]).unwrap();
    let lsns = Lsn::from_hex(lsns[0]).unwrap()..Lsn::from_hex(lsns[1]).unwrap();
    (keys, lsns)
}

pub fn main() -> Result<()> {
    // Parse layer filenames from stdin
    let mut ranges: Vec<(Range<Key>, Range<Lsn>)> = vec![];
    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        let line = line.unwrap();
        let line = PathBuf::from_str(&line).unwrap();
        let filename = line.file_name().unwrap();
        let filename = filename.to_str().unwrap();
        if filename == METADATA_FILE_NAME {
            // Don't try and parse "metadata" like a key-lsn range
            continue;
        }
        let range = parse_filename(filename);
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
    let key_map = build_coordinate_compression_map(keys);
    let lsn_map = build_coordinate_compression_map(lsns);

    // Initialize stats
    let mut num_deltas = 0;
    let mut num_images = 0;

    // Draw
    let stretch = 3.0; // Stretch out vertically for better visibility
    println!(
        "{}",
        BeginSvg {
            w: key_map.len() as f32,
            h: stretch * lsn_map.len() as f32
        }
    );
    for (keyr, lsnr) in &ranges {
        let key_start = *key_map.get(&keyr.start).unwrap();
        let key_end = *key_map.get(&keyr.end).unwrap();
        let key_diff = key_end - key_start;
        let lsn_max = lsn_map.len();

        if key_start >= key_end {
            panic!("Invalid key range {}-{}", key_start, key_end);
        }

        let lsn_start = *lsn_map.get(&lsnr.start).unwrap();
        let lsn_end = *lsn_map.get(&lsnr.end).unwrap();

        let mut lsn_diff = (lsn_end - lsn_start) as f32;
        let mut fill = Fill::None;
        let mut ymargin = 0.05 * lsn_diff; // Height-dependent margin to disambiguate overlapping deltas
        let xmargin = 0.05; // Height-dependent margin to disambiguate overlapping deltas
        let mut lsn_offset = 0.0;

        // Fill in and thicken rectangle if it's an
        // image layer so that we can see it.
        match lsn_start.cmp(&lsn_end) {
            Ordering::Less => num_deltas += 1,
            Ordering::Equal => {
                num_images += 1;
                lsn_diff = 0.3;
                lsn_offset = -lsn_diff / 2.0;
                ymargin = 0.05;
                fill = Fill::Color(rgb(0, 0, 0));
            }
            Ordering::Greater => panic!("Invalid lsn range {}-{}", lsn_start, lsn_end),
        }

        println!(
            "    {}",
            rectangle(
                key_start as f32 + stretch * xmargin,
                stretch * (lsn_max as f32 - (lsn_end as f32 - ymargin - lsn_offset)),
                key_diff as f32 - stretch * 2.0 * xmargin,
                stretch * (lsn_diff - 2.0 * ymargin)
            )
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
