#!/bin/bash

# Get the directory of the current script
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Activate the virtual environment
source "$script_dir/.venv/Scripts/activate"

# Make sure all requirements are installed
pip install -r "$script_dir/requirements.txt"

parameters="
--server nasbox
--username apBackup  
--password zdq*znt2RXZ-wea5ztd
--profile IMAGE_CAPTURE_TO_NAS
--localpath Pictures\\NINA
--serverpath \\home\\NINA
--delete_source False
"

python "$script_dir/ap-image-backup.py" $parameters 
