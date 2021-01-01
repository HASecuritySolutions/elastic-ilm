#!/usr/bin/env python3
from config import load_configs, load_settings
from error import send_notification
import es
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import threading

def get_rollover_policy(client_config):
    # Grab the client's rollover policies
    if "policy" in client_config:
        if "rollover" in client_config['policy']:
            index_rollover_policies = client_config['policy']['rollover']
        else:
            index_rollover_policies = { "global": { "size": 50, "days": 30 } }
    else:
        index_rollover_policies = { "global": { "size": 50, "days": 30 } }
    return index_rollover_policies

def apply_rollover_policy_to_alias(client_config, alias, index_rollover_policies):
    settings = load_settings()
    # Make sure alias does not match a special index
    if not es.check_special_index(alias['alias']):
        if alias['alias'] != 'tier2' and alias['is_write_index'] == 'true':
            # Pull back information about the index - need size and creation_date
            index = es.get_index_information(client_config, alias['index'])
            # Get the index specific rollover policy
            policy = es.check_index_rollover_policy(alias['index'], index_rollover_policies)
            # Get current datetime
            current_date = datetime.utcnow()
            # Get index datetime
            index_date = datetime.strptime(index['creation.date.string'], '%Y-%m-%dT%H:%M:%S.%fZ')
            # Figure out how many days since current_date vs. index_date
            days_ago = (current_date - index_date).days
            # Grab the primary store size (bytes) and convert to GB
            index_size_in_gb = round(int(index['pri.store.size']) / 1024 / 1024 / 1024, 0)
            if settings['settings']['debug']:
                print("Write index " + str(index['index']) + ' created ' + str(days_ago) + " days ago for alias " + alias['alias'] + " at " + str(index_size_in_gb) + " GB")
            # If policy is auto set size check to primary shard count times 50
            if index_rollover_policies[policy]["size"] == "auto":
                size_check = int(index['shardsPrimary']) * 50
            else:
                size_check = int(index_rollover_policies[policy]["size"])
            # Set initial rollover values
            rollover = False
            rollover_reason = ""
            # If size exceeds the policy's size check, set rollover
            if index_size_in_gb >= size_check:
                rollover_reason = 'Size Policy'
                rollover = True
            # If the # of days exceeds the policy's day check and the index size is at least 1 GB, set rollover
            if days_ago >= index_rollover_policies[policy]["days"] and index_size_in_gb >= 1:
                rollover_reason = 'Days Policy'
                rollover = True
            # If index is rollover ready, append to list
            if rollover:
                print("Adding index " + str(index['index']) + " to rollover due to " + rollover_reason)
                # Rollover the index
                if not settings['settings']['debug']:
                    # This triggers the actual rollover
                    if es.rollover_index(client_config, str(index['index']), str(alias['alias'])):
                        # Forcemerge index on rollover
                        es.forcemerge_index(client_config, str(index['index']))
                else:
                    print("Would have triggered rollover on " + index)

def rollover_client_indicies(client_config):
    settings = load_settings()
    # Get the rollover policy for the client
    index_rollover_policies = get_rollover_policy(client_config)
    # Check cluster health - Expect Yellow to continue
    if es.check_cluster_health_status(client_config, settings['rollover']['health_check_level']):
        # Get current aliases members
        aliases = es.get_all_index_aliases(client_config)
        with ThreadPoolExecutor(max_workers=es.get_lowest_data_node_thread_count(client_config)) as executor:
            # Apply rollover to aliases
            for alias in aliases:
                future = executor.submit(apply_rollover_policy_to_alias, client_config, alias, index_rollover_policies)
    else:
        if notification:
            message = "Rollover operation failed.\n\nIt is also possible that connections are unable to be made to the client/nginx node. Please fix.\n\nRemember that in order for client's to be properly build you will need to get their cluster status to **Green** or **Yellow** and then re-run the following command:\n\n**python3 /opt/cloud_operations/rollover.py --client " + client_name + "**"
            send_notification(client_config, "rollover", "Failed", message, teams=True)

def apply_rollover_policies(manual_client):
    settings = load_settings()
    if settings['rollover']['enabled']:
        # Load all client configurations from /opt/maintenance/*.json
        clients = load_configs()
        # Loop through each client to perform accounting per client
        for client in clients:
            # Set nice variable names
            client_name = clients[client]['client_name']
            client_config = clients[client]
            # If client set at command line only run it otherwise
            # execute for all clients
            if manual_client == "" or client_name == manual_client:
                if settings['settings']['limit_to_client'] == client or settings['settings']['limit_to_client'] == "":
                    print("Processing rollovers for " + client_name)
                    # Trigger rollover process
                    rollover_client_indicies(client_config)

if __name__ == "__main__":
    import argparse
    from argparse import RawTextHelpFormatter
    parser = argparse.ArgumentParser(description='Used to manually run accounting against a specific client (Example - rollover.py --client ha)', formatter_class=RawTextHelpFormatter)
    parser.add_argument("--client", default="", type=str, help="Set to a specific client name to limit the accounting script to one client")
    parser.add_argument("--notification", default="True", type=bool, help="Set to False to disable notifications")
    
    args = parser.parse_args()
    manual_client = args.client
    if args.notification == "True":
        notification = True
    else:
        notification = False

    apply_rollover_policies(manual_client)