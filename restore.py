#!/usr/bin/env python3
from config import load_configs
from error import send_notification
import es
import json

def restore_indices(config_file, snapshot, backup_repository, indices_to_restore):
    for index in indices_to_restore.split():
        es.restore_index(config_file, backup_repository, snapshot, index)

if __name__ == "__main__":
    import argparse
    from argparse import RawTextHelpFormatter
    parser = argparse.ArgumentParser(description='Used to manually restore indices from a given snapshot for a specific client (Example - rollover.py --client ha)', formatter_class=RawTextHelpFormatter)
    parser.add_argument("--client", default="client_info.json", type=str, help="Set to a specific client name to limit the accounting script to one client")
    parser.add_argument("--notification", default="True", type=bool, help="Set to False to disable notifications")
    parser.add_argument("--snapshot", type=str, help="Set to snapshot to restore from")
    parser.add_argument("--backup-repository", type=str, help="Set to backup repository name")
    parser.add_argument("--file-with-indices-to-restore", default="restore.txt", type=str, help="Set to path to a file containing list of indices to restore")
    
    args = parser.parse_args()
    snapshot = args.snapshot
    backup_repository = args.backup_repository
    with open(args.client) as f:
        config_file = json.load(f)
    with open(args.file_with_indices_to_restore, "r") as file:
        indices_to_restore = file.read()  # read entire file as bytes
    
    if args.notification == "True":
        notification = True
    else:
        notification = False

    restore_indices(config_file, snapshot, backup_repository, indices_to_restore)