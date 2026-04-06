#!/usr/bin/env python3
"""
Entry point for CDS-CityFetch CLI tool.

This is a standalone entry point for PyInstaller that uses absolute imports
instead of relative imports to avoid packaging issues.
"""

import sys
from pathlib import Path

# Add the project directory to Python path so we can import cityfetch
project_dir = Path(__file__).parent
if str(project_dir) not in sys.path:
    sys.path.insert(0, str(project_dir))

from cityfetch.cli import main

if __name__ == "__main__":
    main()
