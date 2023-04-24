#!/usr/bin/env python3
"""Applies retention policies"""
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import es
from config import load_configs, load_settings
from error import send_notification
NOTIFICATION = False


def get_retention_policy(client_config):
    """Get retention policy

    Args:
        client_config (dict): Client configuration

    Returns:
        dict: Client Configuration
    """
    if "policy" in client_config:
        if "retention" in client_config['policy']:
            index_retention_policies = client_config['policy']['retention']
        else:
            index_retention_policies = {"global": 3660}
    else:
        index_retention_policies = {"global": 3660}
    return index_retention_policies


def delete_old_indices(client_config, index, index_retention_policies):
    """Deletes indices past retention policy

    Args:
        client_config (dict): Client configuration
        index (str): Index name
        index_retention_policies (dict): Retention policy
    """
    settings = load_settings()
    elastic_connection = es.build_es_connection(client_config)
    newest_record = ""
    newest_record = es.get_newest_document_date_in_index(
        client_config, index, elastic_connection)
    # make sure newest record is not empty
    if newest_record != "":
        # Get the index specific retention policy
        policy = es.check_index_retention_policy(
            index, index_retention_policies)
        # Get policy retention days from specific policy
        policy_days = index_retention_policies[policy]
        # Get current datetime
        current_date = datetime.utcnow()
        # Figure out how many days since current_date vs. newest_record
        days_ago = (current_date - newest_record).days
        # Check if days_ago is greater than or equal to policy date
        # If greater than or equal to policy date, delete index
        index_group = es.get_index_group(index)
        if days_ago >= policy_days:
            # Delete old index
            print(f"Deleting index {index} due to age of {days_ago}"
                  f" vs policy limit of {policy_days}")

            index_group = es.get_index_group(index)
            try:
                data_stream_info = elastic_connection.indices.get_data_stream(
                    name=index_group)
                number_of_indices_in_ds = len(
                    data_stream_info['data_streams'][0]['indices'])
            except:
                number_of_indices_in_ds = 0
            if number_of_indices_in_ds == 1:
                if 'debug' in settings['settings']:
                    if settings['settings']['debug']:
                        print(f"DEBUG - Would have deleted data stream {index_group}")
                    else:
                        elastic_connection.indices.delete_data_stream(name=index_group)
                else:
                    elastic_connection.indices.delete_data_stream(name=index_group)

            else:
                if 'debug' in settings['settings']:
                    if settings['settings']['debug']:
                        print(f"DEBUG - Would have deleted data stream {index}")
                    else:
                        if es.delete_index(client_config, index):
                            success = True
                else:
                    if es.delete_index(client_config, index):
                        success = True
            if success is False:
                message = f"Retention operation failed for client {client_config['client_name']}."
                message = message + \
                    f"\nTried deleting index {index} due to age of "
                message = message + \
                    f"{days_ago} vs policy limit of {policy_days}"

                send_notification(
                    client_config,
                    "retention",
                    "Failed",
                    message,
                    teams=settings['retention']['ms-teams'],
                    jira=settings['retention']['jira']
                )
    elastic_connection.close()


def apply_retention_to_old_indices(indices, index_retention_policies, client_config):
    """Apply retention to indices older than policy

    Args:
        indices (array): List of indices
        index_retention_policies (dict): Retention policy
        client_config (dict): Client configuration
    """
    elastic_connection = es.build_es_connection(client_config)
    with ThreadPoolExecutor(
        max_workers=es.get_lowest_data_node_thread_count(client_config)
    ) as executor:
        for index in indices:
            index = str(index['index'])
            # Only proceed if index is not a special index
            if not es.check_special_index(index):
                executor.submit(delete_old_indices, client_config,
                                index, index_retention_policies)
    elastic_connection.close()


def apply_retention_policies(manual_client=""):
    """Apply retention policies

    Args:
        manual_client (str, optional): Name of client. Defaults to "".
    """
    settings = load_settings()
    retry_count = 60
    sleep_time = 60
    success = 0
    if settings['retention']['enabled']:
        # Load all client configurations from /opt/maintenance/*.json
        clients = load_configs()
        # Loop through each client to perform accounting per client\
        for key, client_config in clients.items():
            # Set nice variable names
            client_name = key
            limit_to_client = settings['settings']['limit_to_client']
            print("Processing retention for " + client_name)
            # If client set at command line only run it otherwise
            # execute for all clients
            if limit_to_client == manual_client or limit_to_client == "":
                while retry_count >= 0 and success == 0:
                    # Check cluster health - Expect Yellow to continue
                    if es.check_cluster_health_status(
                        client_config, settings['retention']['health_check_level']
                    ):
                        # Grab the client's retention policies
                        index_retention_policies = get_retention_policy(
                            client_config)
                        # Next, get information on all current indices in cluster
                        indices = es.es_get_indices(client_config)
                        # Get the list of indices that are older than the retention policy
                        apply_retention_to_old_indices(
                            indices,
                            index_retention_policies,
                            client_config
                        )
                        success = 1
                    else:
                        if retry_count > 0:
                            print("Retention operation failed for " + client_name +
                                  ". Cluster health does not meet level:  " +
                                  settings['retention']['health_check_level'])
                        else:
                            message = "Retention operation failed.\n\n" + \
                                "It is also possible that connections are " + \
                                "unable to be made to the client/nginx node." + \
                                "Please fix.\n\nRemember that in order for " + \
                                "client's to be properly build you will need " + \
                                "to get their cluster status to **Green** " + \
                                "or **Yellow** and then re-run the following" + \
                                " command:\n\n**python3 " + \
                                "/opt/elastic-ilm/retention.py --client " + \
                                client_name + "**"
                            send_notification(
                                client_config,
                                "retention",
                                "Failed",
                                message,
                                teams=settings['retention']['ms-teams'],
                                jira=settings['retention']['jira']
                            )
                    if success == 0:
                        # Decrese retry count by one before trying while statement again
                        retry_count = retry_count - 1
                        print("Retry attempts left for retention " +
                              "operation set to " + str(retry_count) +
                              " sleeping for " + str(sleep_time) + " seconds")
                        time.sleep(sleep_time)


if __name__ == "__main__":
    import argparse
    from argparse import RawTextHelpFormatter
    parser = argparse.ArgumentParser(
        description='Used to manually run accounting against a ' +
        'specific client (Example - retention.py --client ha)',
        formatter_class=RawTextHelpFormatter
    )
    parser.add_argument(
        "--client",
        default="",
        type=str,
        help="Set to a specific client name to limit the accounting script to one client"
    )
    parser.add_argument(
        "--notification",
        default="True",
        type=str,
        help="Set to False to disable notifications"
    )
    client_settings = load_settings()

    args = parser.parse_args()
    named_client = args.client
    if args.notification == "True":
        NOTIFICATION = True
    else:
        NOTIFICATION = False
    apply_retention_policies(
        named_client
    )
