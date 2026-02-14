#!/bin/bash
set -euo pipefail

# Get the directory of the current script
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# NAS password reference:
# zdq*znt2RXZ-wea5ztd

# Check if virtual environment exists, create if it doesn't
if [ ! -d "$script_dir/.venv" ]; then
    echo "Creating virtual environment..."
    python -m venv "$script_dir/.venv"
fi

# Activate the virtual environment
source "$script_dir/.venv/Scripts/activate"

# Install both requirement sets
python -m pip install -r "$script_dir/requirements.txt"
python -m pip install -r "$script_dir/requirements-gui.txt"

# Run GUI and exit immediately
python "$script_dir/ap-image-backup-gui.py" >/dev/null 2>&1 &
exit 0
