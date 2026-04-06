#!/usr/bin/env python3
"""
build.py
--------
Build script for creating standalone binaries with PyInstaller.

Usage:
    python build.py                    # Build for current platform
    python build.py --all              # Build for all platforms (requires cross-compilation setup)
    python build.py --clean            # Clean build directories first
"""

import argparse
import platform
import shutil
import subprocess
import sys
from pathlib import Path


def get_platform_name() -> str:
    """Get the current platform identifier."""
    system = platform.system().lower()
    if system == "darwin":
        return "macos"
    return system


def get_binary_name(platform_name: str | None = None) -> str:
    """Get the binary name for a platform."""
    if platform_name is None:
        platform_name = get_platform_name()
    
    suffix = ".exe" if platform_name == "windows" else ""
    return f"cityfetch-{platform_name}{suffix}"


def clean_build_dirs() -> None:
    """Remove build and dist directories."""
    dirs_to_remove = ["build", "dist"]
    for dir_name in dirs_to_remove:
        dir_path = Path(dir_name)
        if dir_path.exists():
            print(f"Removing {dir_name}/...")
            shutil.rmtree(dir_path)


def build_binary(platform_name: str | None = None) -> Path:
    """
    Build a standalone binary using PyInstaller.
    
    Args:
        platform_name: Target platform (or None for current platform)
        
    Returns:
        Path to the built binary
    """
    if platform_name is None:
        platform_name = get_platform_name()
    
    binary_name = get_binary_name(platform_name)
    print(f"Building {binary_name}...")
    
    # PyInstaller command
    cmd = [
        sys.executable, "-m", "PyInstaller",
        "--onefile",                           # Single executable
        "--name", binary_name,                # Binary name
        "--distpath", "dist",                 # Output directory
        "--workpath", "build",                # Build directory
        "--specpath", "build",                # Spec file directory
        "--clean",                            # Clean PyInstaller cache
        "--noconfirm",                        # Overwrite existing
        # Hidden imports
        "--hidden-import", "cityfetch.wikidata_service",
        "--hidden-import", "cityfetch.language_service",
        "--hidden-import", "cityfetch.fetcher",
        "--hidden-import", "cityfetch.outputs.sql_generator",
        "--hidden-import", "cityfetch.outputs.json_generator",
        "--hidden-import", "cityfetch.outputs.csv_generator",
        # Entry point - use standalone entry point with absolute imports
        "cli_entrypoint.py"
    ]
    
    # Add icon for Windows
    if platform_name == "windows":
        icon_path = Path("assets/icon.ico")
        if icon_path.exists():
            cmd.extend(["--icon", str(icon_path)])
    
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=False)
    
    if result.returncode != 0:
        print(f"Error: Build failed with return code {result.returncode}")
        sys.exit(1)
    
    output_path = Path("dist") / binary_name
    if not output_path.exists():
        print(f"Error: Expected output not found at {output_path}")
        sys.exit(1)
    
    # Get file size
    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"[OK] Built {binary_name} ({size_mb:.1f} MB)")
    print(f"  Location: {output_path.absolute()}")
    
    return output_path


def main() -> None:
    """Main entry point for the build script."""
    parser = argparse.ArgumentParser(
        description="Build standalone binaries for CDS-CityFetch"
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Clean build directories before building",
    )
    parser.add_argument(
        "--platform",
        choices=["linux", "windows", "macos"],
        help="Target platform (default: current platform)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Build for all platforms (requires proper cross-compilation setup)",
    )
    
    args = parser.parse_args()
    
    # Check if PyInstaller is installed
    try:
        import PyInstaller
    except ImportError:
        print("Error: PyInstaller is not installed.")
        print("Install it with: pip install pyinstaller")
        sys.exit(1)
    
    # Clean if requested
    if args.clean:
        clean_build_dirs()
    
    # Build
    if args.all:
        print("Building for all platforms...")
        for platform_name in ["linux", "windows", "macos"]:
            print(f"\n{'='*60}")
            print(f"Building for {platform_name}")
            print(f"{'='*60}")
            try:
                build_binary(platform_name)
            except Exception as e:
                print(f"Warning: Failed to build for {platform_name}: {e}")
                print("Note: Cross-compilation requires additional setup.")
    else:
        platform_name = args.platform or get_platform_name()
        build_binary(platform_name)
    
    print("\n" + "="*60)
    print("Build complete!")
    print("="*60)
    print("\nBinaries are in the 'dist/' directory.")
    print("\nNext steps:")
    print("  1. Test the binary: ./dist/cityfetch-<platform> --help")
    print("  2. Upload to GitHub releases")
    print("  3. Update package manager manifests")


if __name__ == "__main__":
    main()
