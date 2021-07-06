#!/usr/bin/env python3
from config import load_configs, load_settings
from error import send_notification
import es
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import time
notification = False
def get_retention_policy(client_config):
    if "policy" in client_config:
        if "retention" in client_config['policy']:
            index_retention_policies = client_config['policy']['retention']
        else:
            index_retention_policies = { "global": 3660 }
    else:
        index_retention_policies = { "global": 3660 }
    return index_retention_policies

def delete_old_indices(client_config, index, index_retention_policies):
    elastic_connection = es.build_es_connection(client_config)
    newest_record = ""
    newest_record = es.get_newest_document_date_in_index(client_config, index, elastic_connection)
    # make sure newest record is not empty
    if newest_record != "":
        # Get the index specific retention policy
        policy = es.check_index_retention_policy(index, index_retention_policies)
        # Get policy retention days from specific policy
        policy_days = index_retention_policies[policy]
        # Get current datetime
        current_date = datetime.utcnow()
        # Figure out how many days since current_date vs. newest_record
        days_ago = (current_date - newest_record).days
        # Check if days_ago is greater than or equal to policy date
        # If greater than or equal to policy date, delete index
        if days_ago >= policy_days:
            # Delete old index
            es.delete_index(client_config, index)
    elastic_connection.close()

def apply_retention_to_old_indices(indices, index_retention_policies, client_config):
    old_indices = []
    elastic_connection = es.build_es_connection(client_config)
    with ThreadPoolExecutor(max_workers=es.get_lowest_data_node_thread_count(client_config)) as executor:
        for index in indices:
            index = str(index['index'])
            # Only proceed if index is not a special index
            if not es.check_special_index(index):
                future = executor.submit(delete_old_indices, client_config, index, index_retention_policies)
    elastic_connection.close()
    return old_indices

def apply_retention_policies(health_check_level, manual_client):
    settings = load_settings()
    retry_count = 60
    sleep_time = 60
    success = 0
    if settings['retention']['enabled']:
        # Load all client configurations from /opt/maintenance/*.json
        clients = load_configs()
        # Loop through each client to perform accounting per client
        for client in clients:
            # Set nice variable names
            client_name = clients[client]['client_name']
            print("Processing retention for " + client_name)
            client_config = clients[client]
            # If client set at command line only run it otherwise
            # execute for all clients
            if manual_client == "" or client_name == manual_client:
                if settings['settings']['limit_to_client'] == client or settings['settings']['limit_to_client'] == "":
                    while retry_count >= 0 and success == 0:
                        # Check cluster health - Expect Yellow to continue
                        if es.check_cluster_health_status(client_config, health_check_level):
                            # Grab the client's retention policies
                            index_retention_policies = get_retention_policy(client_config)
                            # Next, get information on all current indices in cluster
                            indices = es.es_get_indices(client_config)
                            # Get the list of indices that are older than the retention policy
                            apply_retention_to_old_indices(indices, index_retention_policies, client_config)
                            success = 1
                        else:
                            if retry_count > 0:
                                print("Retention operation failed for " + client_config['client_name'] + ". Cluster health does not meet level:  " + settings['retention']['health_check_level'])
                            else:
                                message = "Retention operation failed.\n\nIt is also possible that connections are unable to be made to the client/nginx node. Please fix.\n\nRemember that in order for client's to be properly build you will need to get their cluster status to **Green** or **Yellow** and then re-run the following command:\n\n**python3 /opt/elastic-ilm/retention.py --client " + client_name + "**"
                                send_notification(client_config, "retention", "Failed", message, teams=settings['retention']['ms-teams'], jira=settings['retention']['jira'])
                        if success == 0:
                            # Decrese retry count by one before trying while statement again
                            retry_count = retry_count - 1
                            print("Retry attempts left for retention operation set to " + str(retry_count) + " sleeping for " + str(sleep_time) + " seconds")
                            time.sleep(sleep_time)

if __name__ == "__main__":
    import argparse
    from argparse import RawTextHelpFormatter
    parser = argparse.ArgumentParser(description='Used to manually run accounting against a specific client (Example - retention.py --client ha)', formatter_class=RawTextHelpFormatter)
    parser.add_argument("--client", default="", type=str, help="Set to a specific client name to limit the accounting script to one client")
    parser.add_argument("--notification", default="True", type=str, help="Set to False to disable notifications")

    args = parser.parse_args()
    manual_client = args.client
    if args.notification == "True":
        notification = True
    else:
        notification = False
    apply_retention_policies("yellow", manual_client)