#!/usr/bin/env python3
"""
Script to run coverage analysis on wtree3
Works on Windows without needing Makefile targets
"""

import subprocess
import sys
import os
from pathlib import Path

def run_command(cmd, cwd=None):
    """Run a command and print output"""
    print(f"\n{'='*60}")
    print(f"Running: {' '.join(cmd)}")
    print('='*60)

    result = subprocess.run(cmd, cwd=cwd, capture_output=False)
    if result.returncode != 0:
        print(f"ERROR: Command failed with code {result.returncode}")
        return False
    return True

def main():
    # Get project root
    project_root = Path(__file__).parent.absolute()
    build_dir = project_root / "build"

    print(f"Project root: {project_root}")
    print(f"Build directory: {build_dir}")

    # Step 1: Create build directory
    print("\n[1/5] Creating build directory...")
    build_dir.mkdir(exist_ok=True)

    # Step 2: Configure with CMake
    print("\n[2/5] Configuring with CMake...")
    if not run_command([
        "cmake",
        "-G", "MinGW Makefiles",
        "-DENABLE_COVERAGE=ON",
        ".."
    ], cwd=build_dir):
        return 1

    # Step 3: Build
    print("\n[3/5] Building...")
    if not run_command(["cmake", "--build", "."], cwd=build_dir):
        return 1

    # Step 4: Run tests
    print("\n[4/5] Running tests...")
    if not run_command(["ctest", "--verbose"], cwd=build_dir):
        print("WARNING: Some tests failed, but continuing with coverage...")

    # Step 5: Generate coverage
    print("\n[5/5] Generating coverage report...")

    # Find all gcda files
    gcda_files = list(build_dir.rglob("*.gcda"))
    print(f"Found {len(gcda_files)} coverage data files")

    if not gcda_files:
        print("ERROR: No coverage data found. Make sure tests ran successfully.")
        return 1

    # Run gcovr
    output_file = build_dir / "coverage.html"
    if not run_command([
        sys.executable, "-m", "gcovr",
        "--root", str(project_root),
        "--exclude", r".*external/.*",
        "--exclude", r".*tests/.*",
        "--exclude", r".*build.*/.*",
        "--exclude", r".*_deps/.*",
        "--gcov-ignore-parse-errors", "negative_hits.warn",  # Fix GCC gcov bug
        "--html", "--html-details",
        "-o", str(output_file),
        str(build_dir)
    ], cwd=project_root):
        return 1

    print(f"\n{'='*60}")
    print(f"SUCCESS! Coverage report generated:")
    print(f"  {output_file}")
    print(f"\nTo view in browser, run:")
    print(f"  start {output_file}")
    print('='*60)

    # Optionally open in browser
    if sys.platform == 'win32':
        subprocess.run(["start", str(output_file)], shell=True)

    return 0

if __name__ == "__main__":
    sys.exit(main())
