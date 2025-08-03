#!/usr/bin/env python3
"""
Enhanced Matcher Runner - Wrapper script for easy access
Runs the enhanced database-driven matcher from proper src/ location
"""

import sys
import subprocess
from pathlib import Path

def main():
    """Run the enhanced matcher with all arguments passed through"""
    script_path = Path(__file__).parent.parent / "reconciliation" / "enhanced_db_matcher.py"
    
    # Pass all command line arguments to the actual script
    cmd = [sys.executable, str(script_path)] + sys.argv[1:]
    
    try:
        result = subprocess.run(cmd, check=True)
        return result.returncode
    except subprocess.CalledProcessError as e:
        return e.returncode

if __name__ == "__main__":
    exit(main())
