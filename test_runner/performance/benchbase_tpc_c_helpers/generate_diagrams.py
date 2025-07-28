#!/usr/bin/env python3
"""
Generate TPS and latency charts from BenchBase TPC-C results CSV files.

This script reads a CSV file containing BenchBase results and generates two charts:
1. TPS (requests per second) over time
2. P95 and P99 latencies over time

Both charts are combined in a single SVG file.
"""

import argparse
import sys
from pathlib import Path

import matplotlib.pyplot as plt  # type: ignore[import-not-found]
import pandas as pd  # type: ignore[import-untyped]


def load_results_csv(csv_file_path):
    """Load BenchBase results CSV file into a pandas DataFrame."""
    try:
        df = pd.read_csv(csv_file_path)

        # Validate required columns exist
        required_columns = [
            "Time (seconds)",
            "Throughput (requests/second)",
            "95th Percentile Latency (millisecond)",
            "99th Percentile Latency (millisecond)",
        ]

        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"Error: Missing required columns: {missing_columns}")
            sys.exit(1)

        return df

    except FileNotFoundError:
        print(f"Error: CSV file not found: {csv_file_path}")
        sys.exit(1)
    except pd.errors.EmptyDataError:
        print(f"Error: CSV file is empty: {csv_file_path}")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        sys.exit(1)


def generate_charts(df, input_filename, output_svg_path, title_suffix=None):
    """Generate combined TPS and latency charts and save as SVG."""

    # Get the filename without extension for chart titles
    file_label = Path(input_filename).stem

    # Build title ending with optional suffix
    if title_suffix:
        title_ending = f"{title_suffix} - {file_label}"
    else:
        title_ending = file_label

    # Create figure with two subplots
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))

    # Chart 1: Time vs TPS
    ax1.plot(
        df["Time (seconds)"],
        df["Throughput (requests/second)"],
        linewidth=1,
        color="blue",
        alpha=0.7,
    )
    ax1.set_xlabel("Time (seconds)")
    ax1.set_ylabel("TPS (Requests Per Second)")
    ax1.set_title(f"Benchbase TPC-C Like Throughput (TPS) - {title_ending}")
    ax1.grid(True, alpha=0.3)
    ax1.set_xlim(0, df["Time (seconds)"].max())

    # Chart 2: Time vs P95 and P99 Latencies
    ax2.plot(
        df["Time (seconds)"],
        df["95th Percentile Latency (millisecond)"],
        linewidth=1,
        color="orange",
        alpha=0.7,
        label="Latency P95",
    )
    ax2.plot(
        df["Time (seconds)"],
        df["99th Percentile Latency (millisecond)"],
        linewidth=1,
        color="red",
        alpha=0.7,
        label="Latency P99",
    )
    ax2.set_xlabel("Time (seconds)")
    ax2.set_ylabel("Latency (ms)")
    ax2.set_title(f"Benchbase TPC-C Like Latency - {title_ending}")
    ax2.grid(True, alpha=0.3)
    ax2.set_xlim(0, df["Time (seconds)"].max())
    ax2.legend()

    plt.tight_layout()

    # Save as SVG
    try:
        plt.savefig(output_svg_path, format="svg", dpi=300, bbox_inches="tight")
        print(f"Charts saved to: {output_svg_path}")
    except Exception as e:
        print(f"Error saving SVG file: {e}")
        sys.exit(1)


def main():
    """Main function to parse arguments and generate charts."""
    parser = argparse.ArgumentParser(
        description="Generate TPS and latency charts from BenchBase TPC-C results CSV"
    )
    parser.add_argument(
        "--input-csv", type=str, required=True, help="Path to the input CSV results file"
    )
    parser.add_argument(
        "--output-svg", type=str, required=True, help="Path for the output SVG chart file"
    )
    parser.add_argument(
        "--title-suffix",
        type=str,
        required=False,
        help="Optional suffix to add to chart titles (e.g., 'Warmup', 'Benchmark Phase')",
    )

    args = parser.parse_args()

    # Validate input file exists
    if not Path(args.input_csv).exists():
        print(f"Error: Input CSV file does not exist: {args.input_csv}")
        sys.exit(1)

    # Create output directory if it doesn't exist
    output_path = Path(args.output_svg)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Load data and generate charts
    df = load_results_csv(args.input_csv)
    generate_charts(df, args.input_csv, args.output_svg, args.title_suffix)

    print(f"Successfully generated charts from {len(df)} data points")


if __name__ == "__main__":
    main()
