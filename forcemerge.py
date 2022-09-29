#!/usr/bin/env python3
from email.base64mime import header_length
from config import load_configs, load_settings
from error import send_notification
import es
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import time
notification = False
def get_forcemerge_policy(client_config):
    if "policy" in client_config:
        if "forcemerge" in client_config['policy']:
            index_forcemerge_policies = client_config['policy']['forcemerge']
        else:
            index_forcemerge_policies = { "global": 32 }
    else:
        index_forcemerge_policies = { "global": 32 }
    return index_forcemerge_policies

def forcemerge_indices(client_config, index, index_forcemerge_policies):
    elastic_connection = es.build_es_connection(client_config)
    newest_record = ""
    newest_record = es.get_newest_document_date_in_index(client_config, index, elastic_connection)
    # make sure newest record is not empty
    if newest_record != "":
        # Get the index specific forcemerge policy
        policy = es.check_index_forcemerge_policy(index, index_forcemerge_policies)
        # Get policy forcemerge days from specific policy
        policy_days = index_forcemerge_policies[policy]
        # Get current datetime
        current_date = datetime.utcnow()
        # Figure out how many days since current_date vs. newest_record
        days_ago = (current_date - newest_record).days
        # Check if days_ago is greater than or equal to policy date
        # If greater than or equal to policy date, delete index
        if days_ago >= policy_days:
            # Delete old index
            status = elastic_connection.indices.forcemerge(index, max_num_segments=1, expand_wildcards="all")
            if '_shards' in status:
                if 'total' in status['_shards'] and 'successful' in status['_shards']:
                    if status['_shards']['total'] == status['_shards']['successful']:
                        print("Forcemerge for " + index + " successful")
                else:
                    print("Forcemerge for " + index + " unsuccessful")
            else:
                print("Forcemerge for " + index + " unsuccessful")
    elastic_connection.close()

def apply_forcemerge_to_indices(indices, index_forcemerge_policies, client_config):
    elastic_connection = es.build_es_connection(client_config)
    with ThreadPoolExecutor(max_workers=es.get_lowest_data_node_thread_count(client_config)) as executor:
        for index in indices:
            index = str(index['index'])
            # Only proceed if index is not a special index
            if not es.check_special_index(index):
                future = executor.submit(forcemerge_indices, client_config, index, index_forcemerge_policies)
    elastic_connection.close()

def apply_forcemerge_policies(manual_client=""):
    settings = load_settings()
    retry_count = 60
    sleep_time = 60
    success = 0
    if "forcemerge" in settings:
        if "enabled" in settings:
            forcemerge_enabled = settings['forcemerge']['enabled']
        else:
            forcemerge_enabled = True
    else:
        forcemerge_enabled = True
    if forcemerge_enabled:
        # Load all client configurations from /opt/maintenance/*.json
        clients = load_configs()
        # Loop through each client to perform accounting per client
        for client in clients:
            # Set nice variable names
            client_name = clients[client]['client_name']
            print("Processing forcemerge for " + client_name)
            client_config = clients[client]
            # If client set at command line only run it otherwise
            # execute for all clients
            if manual_client == "" or client_name == manual_client:
                if settings['settings']['limit_to_client'] == client or settings['settings']['limit_to_client'] == "":
                    while retry_count >= 0 and success == 0:
                        # Grab the client's forcemerge policies
                        index_forcemerge_policies = get_forcemerge_policy(client_config)
                        # Next, get information on all current indices in cluster
                        indices = es.es_get_indices(client_config)
                        # Get the list of indices that are older than the forcemerge policy
                        apply_forcemerge_to_indices(indices, index_forcemerge_policies, client_config)
                        success = 1
                    else:
                        if retry_count == 0:
                            message = "forcemerge operation failed.\n\nIt is also possible that connections are unable to be made to the client/nginx node. Please fix.\n\nRemember that in order for client's to be properly build you will need to get their cluster status to **Green** or **Yellow** and then re-run the following command:\n\n**python3 /opt/elastic-ilm/forcemerge.py --client " + client_name + "**"
                            send_notification(client_config, "forcemerge", "Failed", message, teams=settings['forcemerge']['ms-teams'], jira=settings['forcemerge']['jira'])
                    if success == 0:
                        # Decrese retry count by one before trying while statement again
                        retry_count = retry_count - 1
                        print("Retry attempts left for forcemerge operation set to " + str(retry_count) + " sleeping for " + str(sleep_time) + " seconds")
                        time.sleep(sleep_time)
                    
if __name__ == "__main__":
    import argparse
    from argparse import RawTextHelpFormatter
    parser = argparse.ArgumentParser(description='Used to manually run accounting against a specific client (Example - forcemerge.py --client ha)', formatter_class=RawTextHelpFormatter)
    parser.add_argument("--client", default="", type=str, help="Set to a specific client name to limit the accounting script to one client")
    parser.add_argument("--notification", default="True", type=str, help="Set to False to disable notifications")
    settings = load_settings()

    args = parser.parse_args()
    manual_client = args.client
    if args.notification == "True":
        notification = True
    else:
        notification = False

    apply_forcemerge_policies( manual_client)
