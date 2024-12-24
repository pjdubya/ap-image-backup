#!/bin/bash

# Get the directory of the current script
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Check if virtual environment exists, create if it doesn't
if [ ! -d "$script_dir/.venv" ]; then
    echo "Creating virtual environment..."
    python -m venv "$script_dir/.venv"
fi

# Activate the virtual environment
source "$script_dir/.venv/Scripts/activate"

# Make sure all requirements are installed
pip install -r "$script_dir/requirements.txt"

# Get hostname and set appropriate path
hostname=$(hostname)
if [ "$hostname" = "Polaris" ]; then
    localpath="D:\\Pictures\\NINA"
    delete_source=False
else
    # Default path for Starlight and any other machine
    localpath="Pictures\\NINA"
    delete_source=True
fi

# Check for direction parameter
if [ "$1" = "capture" ]; then
    profile="IMAGE_CAPTURE_TO_NAS"
elif [ "$1" = "wip" ]; then
    profile="WIP_TO_NAS"
elif [ "$1" = "retrieve" ]; then
    profile="NAS_TO_PROCESSING"
else
    echo "Please specify direction: capture, wip, or retrieve"
    exit 1
fi

parameters="
--server nasbox
--username apBackup  
--password zdq*znt2RXZ-wea5ztd
--profile $profile
--localpath $localpath
--serverpath \\home\\NINA
--delete_source $delete_source
"

python "$script_dir/ap-image-backup.py" $parameters 
