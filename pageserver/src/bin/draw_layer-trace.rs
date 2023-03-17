use anyhow::Result;
use pageserver::repository::Key;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::io::{self, BufRead};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt::Write,
    ops::Range,
};
use svg_fmt::{rgb, BeginSvg, EndSvg, Fill, Stroke, Style};
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

#[derive(Serialize, Deserialize, PartialEq)]
enum  LayerTraceOp {
    #[serde(rename = "evict")]
    Evict,
    #[serde(rename = "flush")]
    Flush,
    #[serde(rename = "compact_create")]
    CompactCreate,
    #[serde(rename = "compact_delete")]
    CompactDelete,
    #[serde(rename = "image_create")]
    ImageCreate,
    #[serde(rename = "gc_delete")]
    GcDelete,
    #[serde(rename = "gc_start")]
    GcStart,
}

impl std::fmt::Display for LayerTraceOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        let op_str = match self {
            LayerTraceOp::Evict => "evict",
            LayerTraceOp::Flush => "flush",
            LayerTraceOp::CompactCreate => "compact_create",
            LayerTraceOp::CompactDelete => "compact_delete",
            LayerTraceOp::ImageCreate => "image_create",
            LayerTraceOp::GcDelete => "gc_delete",
            LayerTraceOp::GcStart => "gc_start",
        };
        f.write_str(op_str)
    }
}

#[serde_with::serde_as]
#[derive(Serialize, Deserialize)]
struct LayerTraceLine {
    time: u64,
    op: LayerTraceOp,
    #[serde(default)]
    filename: String,
    #[serde_as(as = "Option<serde_with::DisplayFromStr>")]
    cutoff: Option<Lsn>,
}

struct LayerTraceFile {
    filename: String,
    key_range: Range<Key>,
    lsn_range: Range<Lsn>,
}

impl LayerTraceFile {
    fn is_image(&self) -> bool {
        self.lsn_range.start == self.lsn_range.end
    }
}

struct LayerTraceEvent {
    time_rel: u64,
    op: LayerTraceOp,
    filename: String,
}

struct GcEvent {
    time_rel: u64,
    cutoff: Lsn,
}

fn main() -> Result<()> {
    // Parse trace lines from stdin
    let stdin = io::stdin();

    let mut files: HashMap<String, LayerTraceFile> = HashMap::new();
    let mut layer_events: Vec<LayerTraceEvent> = Vec::new();
    let mut gc_events: Vec<GcEvent> = Vec::new();
    let mut first_time: Option<u64> = None;
    for line in stdin.lock().lines() {
        let line = line.unwrap();
        let parsed_line: LayerTraceLine = serde_json::from_str(&line)?;

        let time_rel = if let Some(first_time) = first_time {
            parsed_line.time - first_time
        } else {
            first_time = Some(parsed_line.time);
            0
        };

        if parsed_line.op == LayerTraceOp::GcStart {
            gc_events.push(GcEvent {
                time_rel,
                cutoff: parsed_line.cutoff.unwrap(),
            });
        } else {
            layer_events.push(LayerTraceEvent {
                time_rel,
                filename: parsed_line.filename.clone(),
                op: parsed_line.op,
            });

            if !files.contains_key(&parsed_line.filename) {
                let (key_range, lsn_range) = parse_filename(&parsed_line.filename);
                files.insert(parsed_line.filename.clone(), LayerTraceFile {
                    filename: parsed_line.filename.clone(),
                    key_range,
                    lsn_range,
                });
            };
        }
    }
    let mut last_time_rel = layer_events.last().unwrap().time_rel;
    if let Some(last_gc) = gc_events.last() {
        last_time_rel = std::cmp::min(last_gc.time_rel, last_time_rel);
    }

    // Collect all coordinates
    let mut keys: Vec<Key> = vec![];
    let mut lsns: Vec<Lsn> = vec![];
    for f in files.values() {
        keys.push(f.key_range.start);
        keys.push(f.key_range.end);
        lsns.push(f.lsn_range.start);
        lsns.push(f.lsn_range.end);
    }
    for gc_event in &gc_events {
        lsns.push(gc_event.cutoff);
    }

    // Analyze
    let key_map = build_coordinate_compression_map(keys);
    let lsn_map = build_coordinate_compression_map(lsns);

    // Initialize stats
    let mut num_deltas = 0;
    let mut num_images = 0;

    let mut svg = String::new();

    // Draw
    let stretch = 3.0; // Stretch out vertically for better visibility
    writeln!(svg,
        "{}",
        BeginSvg {
            w: key_map.len() as f32,
            h: stretch * lsn_map.len() as f32
        }
    )?;
    let lsn_max = lsn_map.len();

    // Sort the files by LSN, but so that image layers go after all delta layers
    // The SVG is painted in the order the elements appear, and we want to draw
    // image layers on top of the delta layers if they overlap
    let mut files_sorted: Vec<LayerTraceFile> = files.into_values().collect();
    files_sorted.sort_by(|a, b| {
        if a.is_image() && !b.is_image() {
            Ordering::Greater
        } else if !a.is_image() && b.is_image() {
            Ordering::Less
        } else {
            a.lsn_range.end.cmp(&b.lsn_range.end)
        }
    });

    for f in files_sorted {
        let key_start = *key_map.get(&f.key_range.start).unwrap();
        let key_end = *key_map.get(&f.key_range.end).unwrap();
        let key_diff = key_end - key_start;

        if key_start >= key_end {
            panic!("Invalid key range {}-{}", key_start, key_end);
        }

        let lsn_start = *lsn_map.get(&f.lsn_range.start).unwrap();
        let lsn_end = *lsn_map.get(&f.lsn_range.end).unwrap();

        // Fill in and thicken rectangle if it's an
        // image layer so that we can see it.
        let mut style = Style::default();
        style.fill = Fill::Color(rgb(0x80, 0x80, 0x80));
        style.stroke = Stroke::Color(rgb(0, 0, 0), 0.5);

        let y_start = stretch * (lsn_max - lsn_start) as f32;
        let y_end = stretch * (lsn_max - lsn_end) as f32;

        let x_margin = 0.25;
        let y_margin = 0.5;

        match lsn_start.cmp(&lsn_end) {
            Ordering::Less => {
                num_deltas += 1;
                write!(svg,
                       r#"    <rect id="layer_{}" x="{}" y="{}" width="{}" height="{}" ry="{}" style="{}">"#,
                       f.filename,
                       key_start as f32 + x_margin,
                       y_end + y_margin,
                       key_diff as f32 - x_margin * 2.0,
                       y_start - y_end - y_margin * 2.0,
                       1.0, // border_radius,
                       style.to_string(),
                )?;
                write!(svg, "<title>{}<br>{} - {}</title>", f.filename, lsn_end, y_end)?;
                writeln!(svg, "</rect>")?;
            }
            Ordering::Equal => {
                num_images += 1;
                //lsn_diff = 0.3;
                //lsn_offset = -lsn_diff / 2.0;
                //margin = 0.05;
                style.fill = Fill::Color(rgb(0x80, 0, 0x80));
                style.stroke = Stroke::Color(rgb(0x80, 0, 0x80), 3.0);
                write!(svg,
                       r#"    <line id="layer_{}" x1="{}" y1="{}" x2="{}" y2="{}" style="{}">"#,
                       f.filename,
                       key_start as f32 + x_margin,
                       y_end,
                       key_end as f32 - x_margin,
                       y_end,
                       style.to_string(),
                )?;
                write!(svg, "<title>{}<br>{} - {}</title>", f.filename, lsn_end, y_end)?;
                writeln!(svg, "</line>")?;
            }
            Ordering::Greater => panic!("Invalid lsn range {}-{}", lsn_start, lsn_end),
        }
    }

    for (idx, gc) in gc_events.iter().enumerate() {
        let cutoff_lsn = *lsn_map.get(&gc.cutoff).unwrap();

        let mut style = Style::default();
        style.fill = Fill::None;
        style.stroke = Stroke::Color(rgb(0xff, 0, 0), 0.5);

        let y = stretch * (lsn_max as f32 - (cutoff_lsn as f32));
        writeln!(svg,
                 r#"    <line id="gc_{}" x1="{}" y1="{}" x2="{}" y2="{}" style="{}" />"#,
                 idx,
                 0,
                 y,
                 key_map.len() as f32,
                 y,
                 style.to_string(),
        )?;
    }

    writeln!(svg, "{}", EndSvg)?;

    let mut layer_events_str = String::new();
    let mut first = true;
    for e in layer_events {
        if !first {
            writeln!(layer_events_str, ",")?;
        }
        write!(layer_events_str,
                 r#"  {{"time_rel": {}, "filename": "{}", "op": "{}"}}"#,
                 e.time_rel, e.filename, e.op)?;
        first = false;
    }
    writeln!(layer_events_str)?;

    let mut gc_events_str = String::new();
    let mut first = true;
    for e in gc_events {
        if !first {
            writeln!(gc_events_str, ",")?;
        }
        write!(gc_events_str,
                 r#"  {{"time_rel": {}, "cutoff_lsn": "{}"}}"#,
                 e.time_rel, e.cutoff)?;
        first = false;
    }
    writeln!(gc_events_str)?;
    
    println!(r#"<!DOCTYPE html>
<html>
<head>
<style>
/* Keep the slider pinned at top */
.topbar {{
  display: block;
  overflow: hidden;
  background-color: lightgrey;
  position: fixed;
  top: 0;
  width: 100%;
/*  width: 500px; */
}}
.slidercontainer {{
  float: left;
  width: 50%;
  margin-right: 200px;
}}
.slider {{
  float: left;
  width: 100%;
}}
.legend {{
  width: 200px;
  float: right;
}}

/* Main content */
.main {{
  margin-top: 50px; /* Add a top margin to avoid content overlay */
}}
</style>
</head>

  <body>
    <script type="text/javascript">

      var layer_events = [{layer_events_str}]
      var gc_events = [{gc_events_str}]

      function redoLayerEvent(n, dir) {{
          var layer = document.getElementById("layer_" + layer_events[n].filename);
          switch (layer_events[n].op) {{
              case "evict":
                  break;
              case "flush":
                  layer.style.visibility = "visible";
                  break;
              case "compact_create":
                  layer.style.visibility = "visible";
                  break;
              case "image_create":
                  layer.style.visibility = "visible";
                  break;
              case "compact_delete":
                  layer.style.visibility = "hidden";
                  break;
              case "gc_delete":
                  layer.style.visibility = "hidden";
                  break;
              case "gc_start":
                  layer.style.visibility = "hidden";
                  break;
          }}
      }}
      function undoLayerEvent(n) {{
          var layer = document.getElementById("layer_" + layer_events[n].filename);
          switch (layer_events[n].op) {{
              case "evict":
                  break;
              case "flush":
                  layer.style.visibility = "hidden";
                  break;
              case "compact_create":
                  layer.style.visibility = "hidden";
                  break;
              case "image_create":
                  layer.style.visibility = "hidden";
                  break;
              case "compact_delete":
                  layer.style.visibility = "visible";
                  break;
              case "gc_delete":
                  layer.style.visibility = "visible";
                  break;
          }}
      }}

      function redoGcEvent(n) {{
          var prev_gc_bar = document.getElementById("gc_" + (n - 1));
          var new_gc_bar = document.getElementById("gc_" + n);

          prev_gc_bar.style.visibility = "hidden"
          new_gc_bar.style.visibility = "visible"
      }}
      function undoGcEvent(n) {{
          var prev_gc_bar = document.getElementById("gc_" + n);
          var new_gc_bar = document.getElementById("gc_" + (n - 1));

          prev_gc_bar.style.visibility = "hidden"
          new_gc_bar.style.visibility = "visible"
      }}

      var last_slider_pos = 0
      var last_layer_event = 0
      var last_gc_event = 0

      var moveSlider = function(slider) {{
	  var new_pos = slider.value;

          if (new_pos > last_slider_pos) {{
              while (last_layer_event < layer_events.length - 1) {{
                  if (layer_events[last_layer_event + 1].time_rel > new_pos) {{
                      break;
                  }}
                  last_layer_event += 1;
                  redoLayerEvent(last_layer_event)
              }}

              while (last_gc_event < gc_events.length - 1) {{
                  if (gc_events[last_gc_event + 1].time_rel > new_pos) {{
                      break;
                  }}
                  last_gc_event += 1;
                  redoGcEvent(last_gc_event)
              }}

          }}
          if (new_pos < last_slider_pos) {{
              while (last_layer_event > 0) {{
                  if (layer_events[last_layer_event - 1].time_rel < new_pos) {{
                      break;
                  }}
                  undoLayerEvent(last_layer_event)
                  last_layer_event -= 1;
              }}
              while (last_gc_event > 0) {{
                  if (gc_events[last_gc_event - 1].time_rel < new_pos) {{
                      break;
                  }}
                  undoGcEvent(last_gc_event)
                  last_gc_event -= 1;
              }}
          }}
          last_slider_pos = new_pos;
          document.getElementById("debug_pos").textContent=new_pos;
          document.getElementById("debug_layer_event").textContent=last_layer_event + " " + layer_events[last_layer_event].time_rel + " " + layer_events[last_layer_event].op;
          document.getElementById("debug_gc_event").textContent=last_gc_event + " " + gc_events[last_gc_event].time_rel;
      }}
    </script>

    <div class="topbar">
      <div class="slidercontainer">
        <label for="time-slider">TIME</label>:
        <input id="time-slider" class="slider" type="range" min="0" max="{last_time_rel}" value="0" oninput="moveSlider(this)"><br>

        pos: <span id="debug_pos"></span><br>
        event: <span id="debug_layer_event"></span><br>
        gc: <span id="debug_gc_event"></span><br>
      </div>

      <svg class="legend">
        <rect x=5 y=0 width=20 height=20 style="fill:rgb(128,128,128);stroke:rgb(0,0,0);stroke-width:0.5;fill-opacity:1;stroke-opacity:1;"/>
        <line x1=5 y1=30 x2=25 y2=30 style="fill:rgb(128,0,128);stroke:rgb(128,0,128);stroke-width:3;fill-opacity:1;stroke-opacity:1;"/>
        <line x1=0 y1=40 x2=30 y2=40 style="fill:none;stroke:rgb(255,0,0);stroke-width:0.5;fill-opacity:1;stroke-opacity:1;"/>
      </svg>
    </div>

    <div class="main">
{svg}
    </div>
  </body>
</html>
"#);

    eprintln!("num_images: {}", num_images);
    eprintln!("num_deltas: {}", num_deltas);

    Ok(())
}
