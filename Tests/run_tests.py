# =========================================================
# run_tests.py
# =========================================================
# Run all pytest tests from a Databricks notebook cell.
# Paste this as a single %sh cell or a Python cell.
#
# USAGE IN DATABRICKS:
# Option A — %sh cell (runs in shell):
#   %sh
#   cd /dbfs/FileStore/tests && pip install pytest -q && pytest -v
#
# Option B — Python cell (shown below):
# =========================================================

import subprocess
import sys

# Install pytest if not already available
subprocess.run([sys.executable, "-m", "pip", "install", "pytest", "-q"],
               check=True)

# Copy test files to a local path accessible by pytest
# (files must be uploaded to DBFS or repo first)
import os

TEST_DIR = "/tmp/traffic_pipeline_tests"
os.makedirs(TEST_DIR, exist_ok=True)

# ── If using Databricks Repos, point to your repo path ───
TEST_DIR = "Workspace/Users/dvmb1412@gmail.com/Traffic-Data-Pipeline/tests"

# Run pytest with verbose output
result = subprocess.run(
    [
        sys.executable, "-m", "pytest",
        TEST_DIR,
        "-v",                          # verbose — show each test name
        "--tb=short",                  # short traceback on failure
        "--no-header",
        "-p", "no:cacheprovider",      # disable cache (not needed in Databricks)
        "--color=yes",
    ],
    capture_output=True,
    text=True
)

# Print output to notebook
print(result.stdout)
if result.stderr:
    print("STDERR:", result.stderr)

# Exit code 0 = all passed, non-zero = failures
print(f"\nExit code: {result.returncode}")
if result.returncode != 0:
    raise Exception("Pytest failed — see output above")
