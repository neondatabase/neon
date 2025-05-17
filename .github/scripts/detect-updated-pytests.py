import os
import re
import shutil
import subprocess
import sys

commit_sha = os.getenv("COMMIT_SHA")
base_sha = os.getenv("BASE_SHA")

cmd = ["git", "merge-base", base_sha, commit_sha]
print(f"Running: {' '.join(cmd)}...")
result = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
if result.returncode != 0 or not (baseline := result.stdout.strip()):
    print("Baseline commit for PR is not found, detection skipped.")
    sys.exit(0)
print(f"Baseline commit: {baseline}")

cmd = ["git", "diff", "--name-only", f"{baseline}..{commit_sha}", "test_runner/regress/"]
print(f"Running: {' '.join(cmd)}...")
result = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
if result.returncode != 0:
    print(f"Git diff returned code {result.returncode}\n{result.stdout}\nDetection skipped.")
    sys.exit(0)


def collect_tests(test_file_name):
    cmd = ["./scripts/pytest", "--collect-only", "-q", test_file_name]
    print(f"Running: {' '.join(cmd)}...")
    result = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if result.returncode != 0:
        print(
            f"pytest --collect-only returned code {result.returncode}\n{result.stdout}\nDetection skipped."
        )
        sys.exit(0)

    tests = []
    for test_item in result.stdout.split("\n"):
        if not test_item.startswith(test_file_name):
            break
        test_name = re.sub(r"(.*::)([^\[]+)(\[.*)", r"\2", test_item)
        if test_name not in tests:
            tests.append(test_name)
    return tests


all_new_tests = []
all_updated_tests = []
temp_test_file = "test_runner/regress/__temp__.py"
temp_file = None
for test_file in result.stdout.split("\n"):
    if not test_file:
        continue
    print(f"Test file modified: {test_file}.")

    # Get and compare two lists of items collected by pytest to detect new tests in the PR
    if temp_file:
        temp_file.close()
    temp_file = open(temp_test_file, "w")
    cmd = ["git", "show", f"{baseline}:{test_file}"]
    print(f"Running: {' '.join(cmd)}...")
    result = subprocess.run(cmd, text=True, stdout=temp_file)
    if result.returncode != 0:
        tests0 = []
    else:
        tests0 = collect_tests(temp_test_file)

    tests1 = collect_tests(test_file)

    new_tests = set(tests1).difference(tests0)
    for test_name in new_tests:
        all_new_tests.append(f"{test_file}::{test_name}")

    # Detect pre-existing test functions updated in the PR
    cmd = ["git", "diff", f"{baseline}..{commit_sha}", test_file]
    print(f"Running: {' '.join(cmd)}...")
    result = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if result.returncode != 0:
        print(f"Git diff returned code {result.returncode}\n{result.stdout}\nDetection skipped.")
        sys.exit(0)
    updated_funcs = []
    for diff_line in result.stdout.split("\n"):
        print(diff_line)
        # TODO: detect functions with added/modified parameters
        if not diff_line.startswith("@@"):
            continue

        # Extract names of functions with updated content relying on hunk header
        m = re.match(r"^(@@[0-9, +-]+@@ def )([^(]+)(.*)", diff_line)
        if not m:
            continue
        func_name = m.group(2)
        print(func_name)  ##

        # Ignore functions not collected by pytest
        if func_name not in tests1:
            continue
        if func_name not in updated_funcs:
            updated_funcs.append(func_name)

    for func_name in updated_funcs:
        print(f"Function modified: {func_name}.")
        # Extract changes within the function

        cmd = ["git", "log", f"{baseline}..{commit_sha}", "-L", f":{func_name}:{test_file}"]
        print(f"Running: {' '.join(cmd)}...")
        result = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        if result.returncode != 0:
            continue

        patch_contents = result.stdout

        # Revert changes to get the file with only this function updated
        # (applying the patch might fail if it contains a change for the next function declaraion)
        shutil.copy(test_file, temp_test_file)

        cmd = ["patch", "-R", "-p1", "--no-backup-if-mismatch", "-r", "/dev/null", temp_test_file]
        print(f"Running: {' '.join(cmd)}...")
        result = subprocess.run(
            cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, input=patch_contents
        )
        print(f"result: {result.returncode}; {result.stdout}")
        if result.returncode != 0:
            continue

        # Ignore whitespace-only changes
        cmd = ["diff", "-w", test_file, temp_test_file]
        print(f"Running: {' '.join(cmd)}...")
        result = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        if result.returncode == 0:
            continue
        all_updated_tests.append(f"{test_file}::{func_name}")

if temp_file:
    temp_file.close()
if os.path.exists(temp_test_file):
    os.remove(temp_test_file)

if github_output := os.getenv("GITHUB_OUTPUT"):
    with open(github_output, "a") as f:
        if all_new_tests or all_updated_tests:
            f.write("tests=")
            f.write(" ".join(all_new_tests + all_updated_tests))
            f.write("\n")
