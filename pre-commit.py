#!/usr/bin/env python3

from typing import List
import subprocess
import sys

RED = "\033[0;31m"
GREEN = "\033[0;33m"
CYAN = "\033[0;36m"
NC = "\033[0m"  # No Color


def fix_inplace() -> bool:
    if len(sys.argv) == 2 and sys.argv[1] == "--fix-inplace":
        return True
    return False


def get_commit_files() -> List[str]:
    files = subprocess.check_output(
        "git diff --cached --name-only --diff-filter=ACM".split()
    )
    return files.decode().splitlines()


def check(name: str, suffix: str, cmd: str, changed_files: List[str]):
    print(f"Checking: {name} ", end="")
    applicable_files = list(
        filter(lambda fname: fname.strip().endswith(suffix), changed_files)
    )
    if not applicable_files:
        print(f"{CYAN}[NOT APPLICABLE]{NC}")
        return

    cmd = f'{cmd} {" ".join(applicable_files)}'
    res = subprocess.run(cmd.split(), capture_output=True)
    if res.returncode != 0:
        print(f"{RED}[FAILED]{NC}")
        print("Please inspect the output below and run make fmt to fix automatically\n")
        print(res.stdout.decode())
        exit(1)
    print(f"{GREEN}[OK]{NC}")


if __name__ == "__main__":
    files = get_commit_files()
    rustfmt = "rustfmt --edition=2018"
    if not fix_inplace():
        rustfmt += " --check"
    check(
        name="rustfmt",
        suffix=".rs",
        cmd=rustfmt,
        changed_files=files,
    )
