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
//! The plain text API was chosen so that we can easily work with filenames from various
//! sources; see the Usage section below for examples.
//!
//! # Usage
//!
//! ## Producing the SVG
//!
//! ```bash
//!
//! # local timeline dir
//! ls test_output/test_pgbench\[neon-45-684\]/repo/tenants/$TENANT/timelines/$TIMELINE | \
//!     grep "__" | cargo run --release --bin pagectl draw-timeline-dir > out.svg
//!
//! # Layer map dump from `/v1/tenant/$TENANT/timeline/$TIMELINE/layer`
//! (jq -r '.historic_layers[] | .layer_file_name' | cargo  run -p pagectl draw-timeline) < layer-map.json > out.svg
//!
//! # From an `index_part.json` in S3
//! (jq -r '.layer_metadata | keys[]' | cargo  run -p pagectl draw-timeline ) < index_part.json-00000016 > out.svg
//!
//! # enrich with lines for gc_cutoff and a child branch point
//! cat <(jq -r '.historic_layers[] | .layer_file_name' < layers.json) <(echo -e 'gc_cutoff:0000001CE3FE32C9\nbranch:0000001DE3FE32C9') | cargo run --bin pagectl draw-timeline >| out.svg
//! ```
//!
//! ## Viewing
//!
//! **Inkscape** is better than the built-in viewers in browsers.
//!
//! After selecting a layer file rectangle, use "Open XML Editor" (Ctrl|Cmd + Shift + X)
//! to see the layer file name in the comment field.
//!
//! ```bash
//!
//! # Linux
//! inkscape out.svg
//!
//! # macOS
//! /Applications/Inkscape.app/Contents/MacOS/inkscape out.svg
//!
//! ```
//!

use anyhow::{Context, Result};
use pageserver_api::key::Key;
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

    // The current format of the layer file name: 000000067F0000000400000B150100000000-000000067F0000000400000D350100000000__00000000014B7AC8-v1-00000001

    // Handle generation number `-00000001` part
    if lsns.last().expect("should").len() == 8 {
        lsns.pop();
    }

    // Handle version number `-v1` part
    if lsns.last().expect("should").starts_with('v') {
        lsns.pop();
    }

    if lsns.len() == 1 {
        lsns.push(lsns[0]);
    }

    let keys = Key::from_hex(keys[0]).unwrap()..Key::from_hex(keys[1]).unwrap();
    let lsns = Lsn::from_hex(lsns[0]).unwrap()..Lsn::from_hex(lsns[1]).unwrap();
    (keys, lsns)
}

#[derive(Clone, Copy)]
enum LineKind {
    GcCutoff,
    Branch,
}

impl From<LineKind> for Fill {
    fn from(value: LineKind) -> Self {
        match value {
            LineKind::GcCutoff => Fill::Color(rgb(255, 0, 0)),
            LineKind::Branch => Fill::Color(rgb(0, 255, 0)),
        }
    }
}

impl FromStr for LineKind {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> std::prelude::v1::Result<Self, Self::Err> {
        Ok(match s {
            "gc_cutoff" => LineKind::GcCutoff,
            "branch" => LineKind::Branch,
            _ => anyhow::bail!("unsupported linekind: {s}"),
        })
    }
}

pub fn main() -> Result<()> {
    // Parse layer filenames from stdin
    struct Layer {
        filename: String,
        key_range: Range<Key>,
        lsn_range: Range<Lsn>,
    }
    let mut files: Vec<Layer> = vec![];
    let stdin = io::stdin();

    let mut lines: Vec<(Lsn, LineKind)> = vec![];

    for (lineno, line) in stdin.lock().lines().enumerate() {
        let lineno = lineno + 1;

        let line = line.unwrap();
        if let Some((kind, lsn)) = line.split_once(':') {
            let (kind, lsn) = LineKind::from_str(kind)
                .context("parse kind")
                .and_then(|kind| {
                    if lsn.contains('/') {
                        Lsn::from_str(lsn)
                    } else {
                        Lsn::from_hex(lsn)
                    }
                    .map(|lsn| (kind, lsn))
                    .context("parse lsn")
                })
                .with_context(|| format!("parse {line:?} on {lineno}"))?;
            lines.push((lsn, kind));
            continue;
        }
        let line = PathBuf::from_str(&line).unwrap();
        let filename = line.file_name().unwrap();
        let filename = filename.to_str().unwrap();
        let (key_range, lsn_range) = parse_filename(filename);
        files.push(Layer {
            filename: filename.to_owned(),
            key_range,
            lsn_range,
        });
    }

    // Collect all coordinates
    let mut keys: Vec<Key> = Vec::with_capacity(files.len());
    let mut lsns: Vec<Lsn> = Vec::with_capacity(files.len() + lines.len());

    for Layer {
        key_range: keyr,
        lsn_range: lsnr,
        ..
    } in &files
    {
        keys.push(keyr.start);
        keys.push(keyr.end);
        lsns.push(lsnr.start);
        lsns.push(lsnr.end);
    }

    lsns.extend(lines.iter().map(|(lsn, _)| *lsn));

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
            w: (key_map.len() + 10) as f32,
            h: stretch * lsn_map.len() as f32
        }
    );

    let xmargin = 0.05; // Height-dependent margin to disambiguate overlapping deltas

    for Layer {
        filename,
        key_range: keyr,
        lsn_range: lsnr,
    } in &files
    {
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
                5.0 + key_start as f32 + stretch * xmargin,
                stretch * (lsn_max as f32 - (lsn_end as f32 - ymargin - lsn_offset)),
                key_diff as f32 - stretch * 2.0 * xmargin,
                stretch * (lsn_diff - 2.0 * ymargin)
            )
            .fill(fill)
            .stroke(Stroke::Color(rgb(0, 0, 0), 0.1))
            .border_radius(0.4)
            .comment(filename)
        );
    }

    for (lsn, kind) in lines {
        let lsn_start = *lsn_map.get(&lsn).unwrap();
        let lsn_end = lsn_start;
        let stretch = 2.0;
        let lsn_diff = 0.3;
        let lsn_offset = -lsn_diff / 2.0;
        let ymargin = 0.05;
        println!(
            "{}",
            rectangle(
                0.0f32 + stretch * xmargin,
                stretch * (lsn_map.len() as f32 - (lsn_end as f32 - ymargin - lsn_offset)),
                (key_map.len() + 10) as f32,
                stretch * (lsn_diff - 2.0 * ymargin)
            )
            .fill(kind)
        );
    }

    println!("{}", EndSvg);

    eprintln!("num_images: {}", num_images);
    eprintln!("num_deltas: {}", num_deltas);

    Ok(())
}
