#!/usr/bin/env python3

from __future__ import annotations

import argparse
import enum
import os
import subprocess
import sys


@enum.unique
class Color(enum.Enum):
    RED = "\033[0;31m"
    GREEN = "\033[0;33m"
    CYAN = "\033[0;36m"


NC = "\033[0m"  # No Color


def colorify(
    s: str,
    color: Color,
    no_color: bool = False,
):
    if no_color:
        return s
    return f"{color.value}{s}{NC}"


def rustfmt(fix_inplace: bool = False, no_color: bool = False) -> str:
    cmd = "rustfmt --edition=2021"
    if not fix_inplace:
        cmd += " --check"
    if no_color:
        cmd += " --color=never"
    return cmd


def ruff_check(fix_inplace: bool) -> str:
    cmd = "poetry run ruff check"
    if fix_inplace:
        cmd += " --fix"
    return cmd


def ruff_format(fix_inplace: bool) -> str:
    cmd = "poetry run ruff format"
    if not fix_inplace:
        cmd += " --diff --check"
    return cmd


def mypy() -> str:
    return "poetry run mypy"


def get_commit_files() -> list[str]:
    files = subprocess.check_output("git diff --cached --name-only --diff-filter=ACM".split())
    return files.decode().splitlines()


def check(name: str, suffix: str, cmd: str, changed_files: list[str], no_color: bool = False):
    print(f"Checking: {name} ", end="")
    applicable_files = list(filter(lambda fname: fname.strip().endswith(suffix), changed_files))
    if not applicable_files:
        print(colorify("[NOT APPLICABLE]", Color.CYAN, no_color))
        return

    cmd = f'{cmd} {" ".join(applicable_files)}'
    res = subprocess.run(cmd.split(), capture_output=True)
    if res.returncode != 0:
        print(colorify("[FAILED]", Color.RED, no_color))
        if name == "mypy":
            print("Please inspect the output below and fix type mismatches.")
        else:
            print("Please inspect the output below and run make fmt to fix automatically.")
        if suffix == ".py":
            print(
                "If the output is empty, ensure that you've installed Python tooling by\n"
                "running './scripts/pysync' in the current directory (no root needed)"
            )
        print()
        print(res.stdout.decode())
        sys.exit(1)

    print(colorify("[OK]", Color.GREEN, no_color))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--fix-inplace", action="store_true", help="apply fixes inplace")
    parser.add_argument(
        "--no-color",
        action="store_true",
        help="disable colored output",
        default=not sys.stdout.isatty() or os.getenv("TERM") == "dumb",
    )
    args = parser.parse_args()

    files = get_commit_files()
    # we use rustfmt here because cargo fmt does not accept list of files
    # it internally gathers project files and feeds them to rustfmt
    # so because we want to check only files included in the commit we use rustfmt directly
    check(
        name="rustfmt",
        suffix=".rs",
        cmd=rustfmt(fix_inplace=args.fix_inplace, no_color=args.no_color),
        changed_files=files,
        no_color=args.no_color,
    )
    check(
        name="ruff check",
        suffix=".py",
        cmd=ruff_check(fix_inplace=args.fix_inplace),
        changed_files=files,
        no_color=args.no_color,
    )
    check(
        name="ruff format",
        suffix=".py",
        cmd=ruff_format(fix_inplace=args.fix_inplace),
        changed_files=files,
        no_color=args.no_color,
    )
    check(
        name="mypy",
        suffix=".py",
        cmd=mypy(),
        changed_files=files,
        no_color=args.no_color,
    )
