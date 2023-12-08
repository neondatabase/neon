#!/usr/bin/env python3

from __future__ import annotations

import argparse
import enum
import os
import subprocess
import sys


@enum.unique
class Color(enum.StrEnum):
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


def cargo_fmt(fix_inplace: bool = False, no_color: bool = False) -> str:
    cmd = "cargo fmt"
    if not fix_inplace:
        cmd += " --check"
    if no_color:
        cmd += " -- --color=never"
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


def pgindent(fix_inplace: bool) -> str:
    if fix_inplace:
        return "make neon-pgindent"

    return "make -s -j neon-pgindent-check"


def is_applicable(fname: str, suffix: str | set[str]) -> bool:
    fname = fname.strip()
    if isinstance(suffix, str):
        suffix = {suffix}

    for s in suffix:
        if fname.endswith(s):
            return True

    return False


def check(
    name: str,
    suffix: str | set[str],
    cmd: str,
    changed_files: list[str],
    no_color: bool = False,
    append_files_to_cmd: bool = True,
):
    print(f"Checking: {name} ", end="")
    applicable_files = list(filter(lambda fname: is_applicable(fname, suffix), changed_files))
    if not applicable_files:
        print(colorify("[NOT APPLICABLE]", Color.CYAN, no_color))
        return

    if append_files_to_cmd:
        cmd = f"{cmd} {' '.join(applicable_files)}"

    res = subprocess.run(cmd.split(), capture_output=True)
    if res.returncode != 0:
        print(colorify("[FAILED]", Color.RED, no_color))
        if name == "mypy":
            print("Please inspect the output below and fix type mismatches.")
        elif name == "pgindent":
            print("pgindent does not print output.")
        else:
            print("Please inspect the output below and run make fmt to fix automatically.")
        if suffix == ".py":
            print(
                "If the output is empty, ensure that you've installed Python tooling by\n"
                + "running './scripts/pysync' in the current directory (no root needed)"
            )

        output = res.stdout.decode()
        if len(output) > 0:
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
    check(
        name="cargo fmt",
        suffix=".rs",
        cmd=cargo_fmt(fix_inplace=args.fix_inplace, no_color=args.no_color),
        changed_files=files,
        no_color=args.no_color,
        append_files_to_cmd=False,
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
    check(
        name="pgindent",
        suffix={"c", "h"},
        cmd=pgindent(fix_inplace=args.fix_inplace),
        changed_files=files,
    )
