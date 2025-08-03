#!/usr/bin/env python3
"""
Test reconciliation with new repository structure
"""
import sys
from pathlib import Path

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

# Import and run reconcile directly
import subprocess

if __name__ == "__main__":
    # Test with GREYSON PO 4755 using subprocess
    result = subprocess.run([
        sys.executable, "-m", "reconcile",
        "--customer", "GREYSON",
        "--po", "4755"
    ], cwd="src", capture_output=True, text=True)
    
    print("STDOUT:")
    print(result.stdout)
    if result.stderr:
        print("STDERR:")
        print(result.stderr)
    print(f"Return code: {result.returncode}")
