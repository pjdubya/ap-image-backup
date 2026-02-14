import argparse
import getpass
import logging
import os
import sys
import smbclient
from compare_engine import upsert_nas_index_entries
from datetime import datetime
from enum import Enum

class Profile(Enum):
    IMAGE_CAPTURE_TO_NAS = 1
    WIP_TO_NAS = 2
    NAS_TO_PROCESSING = 3

class CopyType(Enum):
    IMAGES = 1
    WIP = 2

# Ensure logs directory exists
logs_dir = 'logs'
os.makedirs(logs_dir, exist_ok=True)

# Configure the root logger
script_name = os.path.splitext(os.path.basename(__file__))[0]
current_date = datetime.now().strftime("%Y%m%d")
log_file = os.path.join(logs_dir, f"{script_name}.{current_date}.log")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename=log_file, filemode='a')  # Change filemode to 'a' for append

# Create a console handler and set its level to INFO
console = logging.StreamHandler()
console.setLevel(logging.INFO)

# Create a formatter and set the format for the console handler
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console.setFormatter(formatter)

# Add the console handler to the root logger
logging.getLogger('').addHandler(console)

# Set logger for smbclient and smbprotocol to WARNING to keep it from logging.infoing a ton of "receiving SMB2 Write Response" messages
smb_logger = logging.getLogger("smbclient")
smb_logger.setLevel(logging.WARNING)
smb_logger = logging.getLogger("smbprotocol")
smb_logger.setLevel(logging.WARNING)


def parse_profile(value):
    try:
        return Profile[value]
    except KeyError:
        raise argparse.ArgumentTypeError(f"Invalid profile: {value}")

def parse_bool(value):
    if value.lower() in ('true', 't', 'yes', 'y', '1'):
        return True
    elif value.lower() in ('false', 'f', 'no', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError(f"Invalid boolean value: {value}")

def copy_local_files_to_nas(source_dir, smb_server, smb_share, smb_username, smb_password, images_or_wip, delete_source):

    errors_detected = False
    nas_db_upserts: list[tuple[str, int, int]] = []

    # Register the SMB server session
    smbclient.register_session(smb_server, username=smb_username, password=smb_password)

    try:
        # Patterns for special handling
        wip_patterns = ["WBPP", "Processing"]
        never_delete_patterns = ["Processing", "ASI2600MC Pro Masters"]

        dirs_to_skip = []
        dirs_to_delete = []
        files_to_delete = []

        # Walk through the source directory
        for root, dirs, files in os.walk(source_dir):

            if images_or_wip == CopyType.IMAGES:
                # Exclude directories matching the patterns
                filtered_dirs = [d for d in dirs if not any(pattern in d for pattern in wip_patterns)]
            
                if (dirs != filtered_dirs):
                    dirs_to_skip.append(root)

                dirs[:] = filtered_dirs

                # Create corresponding directory structure on the SMB share
                for dir in dirs:
                    source_subdir = os.path.join(root, dir)
                    smb_subdir = os.path.join(smb_share, os.path.relpath(source_subdir, source_dir))
                    dirs_to_delete.append(source_subdir)
                    try:
                        if not smbclient.path.isdir(smb_subdir):
                            smbclient.mkdir(smb_subdir)
                    except Exception as e:
                        logging.error(f"Failed to create directory {smb_subdir}: {e}")
                        errors_detected = True

            elif images_or_wip == CopyType.WIP:
                if not any(word in component for component in root.split(os.path.sep) for word in wip_patterns):
                    logging.info(f"Skipping directory {root} as it does not have a WIP directory anscestor")
                    continue

                # in a WIP directory or subdirectory of one, so keep creating direcories and files while this is still true
                # only need to create directory for root and copy files; dirs will be created in next iteration
                # WIP directories will only exist on processing machine within a directory that is already on the NAS e.g. Orion\WBPP
                source_subdir = root
                smb_subdir = os.path.join(smb_share, os.path.relpath(source_subdir, source_dir))
                dirs_to_delete.append(source_subdir)
                try:
                    if not smbclient.path.isdir(smb_subdir):
                        smbclient.mkdir(smb_subdir)
                except Exception as e:
                    logging.error(f"Failed to create directory {smb_subdir}: {e}")
                    errors_detected = True
            else:
                raise ValueError("Invalid copy type")

            # Copy files to the SMB share
            for file in files:
                source_file_path = os.path.join(root, file)
                smb_file_path = os.path.join(smb_share, os.path.relpath(source_file_path, source_dir))
                files_to_delete.append(source_file_path)
                # Check if the file already exists on the SMB share with the same size and modification time
                try:
                    smb_file_stat = smbclient.stat(smb_file_path)
                    source_file_stat = os.stat(source_file_path)
                    if (source_file_stat.st_size, int(source_file_stat.st_mtime)) == (smb_file_stat.st_size, int(smb_file_stat.st_mtime)): 
                        logging.info(f"Skipping {source_file_path} as an identical file already exists on the SMB share")
                        continue
                except Exception as e:
                    logging.debug(f"Error checking file stats: {e}")

                # Copy the file if it doesn't exist on the SMB share or if it's different
                with open(source_file_path, "rb") as f:
                    try:
                        source_file_stat = os.stat(source_file_path)
                        original_mtime = int(source_file_stat.st_mtime)

                        with smbclient.open_file(smb_file_path, mode='wb') as smb_file:
                            smb_file.write(f.read())

                        # Set the modification time of the copied file on the SMB share
                        smbclient.utime(smb_file_path, (original_mtime, original_mtime))

                        rel_path = os.path.relpath(source_file_path, source_dir).replace("\\", "/")
                        nas_db_upserts.append((rel_path, int(source_file_stat.st_size), int(original_mtime)))

                        logging.info(f"Copied {source_file_path} to SMB share: {smb_file_path}")
                    except Exception as e:
                        logging.error(f"Failed to copy {source_file_path} to SMB share: {e}")
                        errors_detected = True

        if nas_db_upserts:
            try:
                upsert_nas_index_entries(
                    server=smb_server,
                    username=smb_username,
                    password=smb_password,
                    share_root=smb_share,
                    entries=nas_db_upserts,
                )
                logging.info("Updated NAS DB index entries for copied files")
            except Exception as e:
                logging.error(f"Failed to update NAS DB index entries: {e}")
                errors_detected = True

        if not delete_source:
            logging.info("Delete source flag is not set. Source files will not be deleted.")
            return

        if not errors_detected:
            logging.info(f"All files copied successfully to {smb_share}. Now deleting the source files...")

            files_to_delete.reverse()
            for file in files_to_delete:
                # Check if the file is in a directory that should never be deleted
                if any(pattern in file for pattern in never_delete_patterns):
                    logging.info(f"Skipping deletion of file {file} as it matches a pattern in never_delete_patterns")
                    continue

                try:
                    logging.info(f"Deleting file {file}")
                    os.remove(file)
                except Exception as e:
                    logging.error(f"Failed to delete {file}: {e}")

            dirs_to_delete.reverse()
            logging.info(rf"Pruning skipped directories from the list to delete: {dirs_to_skip}")
            dirs_to_delete = [dir for dir in dirs_to_delete if dir not in dirs_to_skip]
            dirs_to_delete = [dir for dir in dirs_to_delete if not any(pattern in dir for pattern in never_delete_patterns)]           
            
            for dir in dirs_to_delete:
                try:
                    logging.info(f"Deleting directory {dir}")
                    os.rmdir(dir)
                except Exception as e:
                    logging.error(f"Failed to delete {dir}: {e}")

        else:
            logging.info("Errors detected in copy operations. Source files will not be deleted.")

    finally:
        # Unregister the SMB server session
        smbclient.reset_connection_cache()

def report_error_and_exit(message):
    logging.error(message)
    sys.exit(1)

def get_nas_targets(smb_share):
    """Get list of available target directories on NAS"""
    try:
        targets = sorted([item.name for item in smbclient.scandir(smb_share) if item.is_dir()])
        return targets
    except Exception as e:
        logging.error(f"Error getting NAS targets: {e}")
        return []

def copy_dir_and_contents(smb_path, local_target_path):
    """Copy directory and its contents to target directory"""

    errors_detected = False

    # Walk through the NAS directory
    for root, dirs, files in smbclient.walk(smb_path):
        # Create corresponding local directory structure
        rel_path = os.path.relpath(root, smb_path)
        local_dir = os.path.join(local_target_path, rel_path)

        os.makedirs(local_dir, exist_ok=True)
        
        # Copy files to local
        for file in files:
            smb_file_path = os.path.join(root, file)
            local_file_path = os.path.join(local_dir, file)
            
            # Check if file already exists locally with same size and modification time
            try:
                smb_stat = smbclient.stat(smb_file_path)
                if os.path.exists(local_file_path):
                    local_stat = os.stat(local_file_path)
                    if (local_stat.st_size, int(local_stat.st_mtime)) == (smb_stat.st_size, int(smb_stat.st_mtime)):
                        logging.info(f"Skipping {file} as an identical file already exists locally")
                        continue
            except Exception as e:
                logging.error(f"Error checking file stats: {e}")
                errors_detected = True
                continue
            
            # Copy the file
            try:
                with smbclient.open_file(smb_file_path, mode='rb') as smb_file:
                    with open(local_file_path, 'wb') as local_file:
                        local_file.write(smb_file.read())
                
                # Set the modification time of the local file
                os.utime(local_file_path, (smb_stat.st_mtime, smb_stat.st_mtime))
                logging.info(f"Copied {smb_file_path} to local: {local_file_path}")
            except Exception as e:
                logging.error(f"Failed to copy {smb_file_path} to local: {e}")
                errors_detected = True

    return errors_detected

def copy_nas_files_to_local(source_dir, local_path, smb_server, smb_share, smb_username, smb_password):
    """Copy files from NAS to local processing machine"""
    errors_detected = False
    
    # Register the SMB server session
    smbclient.register_session(smb_server, username=smb_username, password=smb_password)
    
    try:
        # Get available targets
        targets = get_nas_targets(smb_share)
        if not targets:
            logging.error("No target directories found on NAS")
            return
        
        # Present targets to user
        print("\nAvailable targets:")
        for i, target in enumerate(targets, 1):
            print(f"{i}. {target}")
        
        while True:
            try:
                choice = int(input("\nSelect target number: ")) - 1
                if 0 <= choice < len(targets):
                    selected_target = targets[choice]
                    break
                print("Invalid selection. Please try again.")
            except ValueError:
                print("Please enter a number.")
        
        # Ask if user also wants to retrieve flats for the selected target's lights
        retrieve_flats = input("Also retrieve flats for the selected target's lights? (y/n): ").lower() == "y"

        smb_path = os.path.join(smb_share, selected_target)
        local_target_path = os.path.join(local_path, selected_target)

        logging.info(f"NAS source path: {smb_path}")
        logging.info(f"Local target path: {local_target_path}")
        
        # Create local target directory if it doesn't exist
        os.makedirs(local_target_path, exist_ok=True)
        
        errors_detected = copy_dir_and_contents(smb_path, local_target_path)

        if retrieve_flats:
            # Get list of lights top level directory folder names so we can retrieve the flats for them
            contents = smbclient.listdir(smb_path)
            top_level_dirs = [entry for entry in contents if smbclient.path.isdir(f"{smb_path}/{entry}")]
            for subdir in top_level_dirs:
                smb_path = os.path.join(smb_share, "_FlatWizard", subdir)
                local_target_path = os.path.join(local_path, "_FlatWizard", subdir)
                logging.info(f"Copying flats for {subdir} from {smb_path} to {local_target_path}")
                errors_detected = copy_dir_and_contents(smb_path, local_target_path)

        if errors_detected:
            logging.warning("Some errors occurred during the copy operation")
        else:
            logging.info("All files copied successfully")
            
    finally:
        # Unregister the SMB server session
        smbclient.reset_connection_cache()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", type=str, help="Hostname of server, e.g. myserver.local")
    parser.add_argument("--username", type=str, help="Username of user with SMB access")
    parser.add_argument("--password", type=str, help="Password of user with SMB access")
    parser.add_argument("--serverpath", type=str, help="Path to location on SMB share where files will be stored, e.g.. \\home\\myuser\\myfiles")
    parser.add_argument("--localpath", type=str, help="Path to location on local machine where files will be stored, releative to user's home directory, e.g.. Pictures\\Nina")
    parser.add_argument("--localpathabs", type=str, help="Path to location on local machine where files will be stored, using absolute format, e.g.. C:\\Users\\User\\Pictures\\Nina")
    parser.add_argument("--profile", type=parse_profile, default=Profile.IMAGE_CAPTURE_TO_NAS, help="Profile type")
    parser.add_argument("--delete_source", type=parse_bool, default=False, help="Delete source files after copying successfully")
    args = parser.parse_args()

    # Remove password from args before logging
    logging_args = args.__dict__.copy()
    if "password" in logging_args:
        logging_args["password"] = "*" * len(logging_args["password"])

    logging.info("\n***********\nStarting...\n***********")
    logging.info(f"Starting script {__file__} with args: {logging_args}")

    server = args.server
    username = args.username
    password = args.password
    server_path = args.serverpath
    local_path = args.localpath
    profile = args.profile
    delete_source = args.delete_source
    share_root = rf"\\{server}{server_path}"

    if not username:
        report_error_and_exit("Username is required")

    if not password:
        # Prompt for password if not provided as some users may not want to expose the password in shell/memory
        newpassword = getpass.getpass("Password of user with SMB access: ")
        password = str(newpassword)

    # Test the SMB connection
    if not server:
        report_error_and_exit("Server is required")
    try:
        smbclient.register_session(server, username, password)
    except Exception as e:
        report_error_and_exit("Authentication or connection error: " + str(e))

    smbclient.reset_connection_cache()

    if not server_path:
        report_error_and_exit("Server path is required")

    # get absolute path if relative localpath is provided
    if local_path:
        local_path = os.path.join(os.path.expanduser("~"), local_path.replace('\\', '\\\\'))
    if (not local_path) and args.localpathabs:
        local_path = os.path.relpath(args.localpathabs.replace('\\', '\\\\'))
    if not local_path:
        report_error_and_exit("Must specify either localpath or localpathabs")
    if not os.path.exists(local_path):
        report_error_and_exit(f"Local path {local_path} does not exist")

    logging.info("Share root: %s", share_root)   
    logging.info("Local path: %s", local_path)
    logging.info("Selected profile: %s", profile)
    logging.info("Delete source after copying: %s", delete_source)

    if (profile == Profile.IMAGE_CAPTURE_TO_NAS):
        copy_local_files_to_nas(local_path, server, share_root, username, password, CopyType.IMAGES, delete_source)
    elif (profile == Profile.WIP_TO_NAS):
        copy_local_files_to_nas(local_path, server, share_root, username, password, CopyType.WIP, delete_source)
    elif (profile == Profile.NAS_TO_PROCESSING):
        copy_nas_files_to_local(server_path, local_path, server, share_root, username, password)
    else:
        report_error_and_exit("Invalid profile")
    
