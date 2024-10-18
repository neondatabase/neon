use crate::{SegmentMethod, SegmentSizeResult, SizeResult, StorageModel};
use std::fmt::Write;

const SVG_WIDTH: f32 = 500.0;

/// Different branch kind for SVG drawing.
#[derive(PartialEq)]
pub enum SvgBranchKind {
    Timeline,
    Lease,
}

struct SvgDraw<'a> {
    storage: &'a StorageModel,
    branches: &'a [String],
    seg_to_branch: &'a [(usize, SvgBranchKind)],
    sizes: &'a [SegmentSizeResult],

    // layout
    xscale: f32,
    min_lsn: u64,
    seg_coordinates: Vec<(f32, f32)>,
}

fn draw_legend(result: &mut String) -> anyhow::Result<()> {
    writeln!(
        result,
        "<circle cx=\"10\" cy=\"10\" r=\"5\" stroke=\"red\"/>"
    )?;
    writeln!(result, "<text x=\"20\" y=\"15\">logical snapshot</text>")?;
    writeln!(
        result,
        "<line x1=\"5\" y1=\"30\" x2=\"15\" y2=\"30\" stroke-width=\"6\" stroke=\"black\" />"
    )?;
    writeln!(
        result,
        "<text x=\"20\" y=\"35\">WAL within retention period</text>"
    )?;
    writeln!(
        result,
        "<line x1=\"5\" y1=\"50\" x2=\"15\" y2=\"50\" stroke-width=\"3\" stroke=\"black\" />"
    )?;
    writeln!(
        result,
        "<text x=\"20\" y=\"55\">WAL retained to avoid copy</text>"
    )?;
    writeln!(
        result,
        "<line x1=\"5\" y1=\"70\" x2=\"15\" y2=\"70\" stroke-width=\"1\" stroke=\"gray\" />"
    )?;
    writeln!(result, "<text x=\"20\" y=\"75\">WAL not retained</text>")?;
    writeln!(
        result,
        "<line x1=\"10\" y1=\"85\" x2=\"10\" y2=\"95\" stroke-width=\"3\" stroke=\"blue\" />"
    )?;
    writeln!(result, "<text x=\"20\" y=\"95\">LSN lease</text>")?;
    Ok(())
}

pub fn draw_svg(
    storage: &StorageModel,
    branches: &[String],
    seg_to_branch: &[(usize, SvgBranchKind)],
    sizes: &SizeResult,
) -> anyhow::Result<String> {
    let mut draw = SvgDraw {
        storage,
        branches,
        seg_to_branch,
        sizes: &sizes.segments,

        xscale: 0.0,
        min_lsn: 0,
        seg_coordinates: Vec::new(),
    };

    let mut result = String::new();

    writeln!(result, "<svg xmlns=\"http://www.w3.org/2000/svg\" xmlns:xlink=\"http://www.w3.org/1999/xlink\" height=\"300\" width=\"500\">")?;

    draw.calculate_svg_layout();

    // Draw the tree
    for (seg_id, _seg) in storage.segments.iter().enumerate() {
        draw.draw_seg_phase1(seg_id, &mut result)?;
    }

    // Draw snapshots
    for (seg_id, _seg) in storage.segments.iter().enumerate() {
        draw.draw_seg_phase2(seg_id, &mut result)?;
    }

    draw_legend(&mut result)?;

    write!(result, "</svg>")?;

    Ok(result)
}

impl SvgDraw<'_> {
    fn calculate_svg_layout(&mut self) {
        // Find x scale
        let segments = &self.storage.segments;
        let min_lsn = segments.iter().map(|s| s.lsn).fold(u64::MAX, std::cmp::min);
        let max_lsn = segments.iter().map(|s| s.lsn).fold(0, std::cmp::max);

        // Start with 1 pixel = 1 byte. Double the scale until it fits into the image
        let mut xscale = 1.0;
        while (max_lsn - min_lsn) as f32 / xscale > SVG_WIDTH {
            xscale *= 2.0;
        }

        // Layout the timelines on Y dimension.
        // TODO
        let mut y = 120.0;
        let mut branch_y_coordinates = Vec::new();
        for _branch in self.branches {
            branch_y_coordinates.push(y);
            y += 40.0;
        }

        // Calculate coordinates for each point
        let seg_coordinates = std::iter::zip(segments, self.seg_to_branch)
            .map(|(seg, (branch_id, _))| {
                let x = (seg.lsn - min_lsn) as f32 / xscale;
                let y = branch_y_coordinates[*branch_id];
                (x, y)
            })
            .collect();

        self.xscale = xscale;
        self.min_lsn = min_lsn;
        self.seg_coordinates = seg_coordinates;
    }

    /// Draws lines between points
    fn draw_seg_phase1(&self, seg_id: usize, result: &mut String) -> anyhow::Result<()> {
        let seg = &self.storage.segments[seg_id];

        let wal_bytes = if let Some(parent_id) = seg.parent {
            seg.lsn - self.storage.segments[parent_id].lsn
        } else {
            0
        };

        let style = match self.sizes[seg_id].method {
            SegmentMethod::SnapshotHere => "stroke-width=\"1\" stroke=\"gray\"",
            SegmentMethod::Wal if seg.needed && wal_bytes > 0 => {
                "stroke-width=\"6\" stroke=\"black\""
            }
            SegmentMethod::Wal => "stroke-width=\"3\" stroke=\"black\"",
            SegmentMethod::Skipped => "stroke-width=\"1\" stroke=\"gray\"",
        };
        if let Some(parent_id) = seg.parent {
            let (x1, y1) = self.seg_coordinates[parent_id];
            let (x2, y2) = self.seg_coordinates[seg_id];

            writeln!(
                result,
                "<line x1=\"{x1}\" y1=\"{y1}\" x2=\"{x2}\" y2=\"{y2}\" {style}>",
            )?;
            writeln!(
                result,
                "  <title>{wal_bytes} bytes of WAL (seg {seg_id})</title>"
            )?;
            writeln!(result, "</line>")?;
        } else {
            // draw a little dash to mark the starting point of this branch
            let (x, y) = self.seg_coordinates[seg_id];
            let (x1, y1) = (x, y - 5.0);
            let (x2, y2) = (x, y + 5.0);

            writeln!(
                result,
                "<line x1=\"{x1}\" y1=\"{y1}\" x2=\"{x2}\" y2=\"{y2}\" {style}>",
            )?;
            writeln!(result, "  <title>(seg {seg_id})</title>")?;
            writeln!(result, "</line>")?;
        }

        Ok(())
    }

    /// Draw circles where snapshots are taken
    fn draw_seg_phase2(&self, seg_id: usize, result: &mut String) -> anyhow::Result<()> {
        let seg = &self.storage.segments[seg_id];

        // draw a snapshot point if it's needed
        let (coord_x, coord_y) = self.seg_coordinates[seg_id];

        let (_, kind) = &self.seg_to_branch[seg_id];
        if kind == &SvgBranchKind::Lease {
            let (x1, y1) = (coord_x, coord_y - 10.0);
            let (x2, y2) = (coord_x, coord_y + 10.0);

            let style = "stroke-width=\"3\" stroke=\"blue\"";

            writeln!(
                result,
                "<line x1=\"{x1}\" y1=\"{y1}\" x2=\"{x2}\" y2=\"{y2}\" {style}>",
            )?;
            writeln!(result, "  <title>leased lsn at {}</title>", seg.lsn)?;
            writeln!(result, "</line>")?;
        }

        if self.sizes[seg_id].method == SegmentMethod::SnapshotHere {
            writeln!(
                result,
                "<circle cx=\"{coord_x}\" cy=\"{coord_y}\" r=\"5\" stroke=\"red\">",
            )?;
            writeln!(
                result,
                "  <title>logical size {}</title>",
                seg.size.unwrap()
            )?;
            write!(result, "</circle>")?;
        }

        Ok(())
    }
}
