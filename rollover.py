"""This script processes rollovers for clients"""
#!/usr/bin/env python3
import re
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from config import load_configs, load_settings
from error import send_notification
import es


def get_values_from_dictionary_array(array, field):
    """Extracts a specific field's values from a dictionary array

    Args:
        array (list): Array containing dictionary entries
        field (str): Field in all dictionarys to extract value from

    Returns:
        list: Sorted list of values from dictionary field
    """
    values = set()
    for item in array:
        values.add(item[field])
    return sorted(values)


def get_rollover_policy(client_config):
    """Retrieves the rollover policy per client

    Args:
        client_config (dict): Object containing client's config

    Returns:
        dict: Client rollover policy
    """
    # Grab the client's rollover policies
    if "policy" in client_config:
        if "rollover" in client_config['policy']:
            index_rollover_policies = client_config['policy']['rollover']
        else:
            index_rollover_policies = {"global": {"size": 50, "days": 30}}
    else:
        index_rollover_policies = {"global": {"size": 50, "days": 30}}
    return index_rollover_policies


def apply_rollover_policy_to_alias(client_config, alias, index_rollover_policies):
    """Applies rollovers to aliases that meet rollover policy conditions

    Args:
        client_config (dict): Client configuration
        alias (str): Alias to specific indices
        index_rollover_policies (dict): Rollover policy settings
    """
    settings = load_settings()
    # Make sure alias does not match a special index
    if not es.check_special_index(alias['alias']):
        if alias['alias'] != 'tier2' and alias['is_write_index'] == 'true':
            # Pull back information about the index - need size and creation_date
            index = es.get_index_information(client_config, alias['index'])
            # Get the index specific rollover policy
            policy = es.check_index_rollover_policy(
                alias['index'], index_rollover_policies)
            # Get current datetime
            current_date = datetime.utcnow()
            # Get index datetime
            index_date = datetime.strptime(
                index['creation.date.string'], '%Y-%m-%dT%H:%M:%S.%fZ')
            # Figure out how many days since current_date vs. index_date
            days_ago = (current_date - index_date).days
            # Grab the primary store size (bytes) and convert to GB
            index_size_in_gb = round(
                int(index['pri.store.size']) / 1024 / 1024 / 1024, 0)
            if settings['settings']['debug']:
                print("Write index " + str(index['index']) + ' created ' + str(days_ago) +
                      " days ago for alias " + alias['alias'] + " at " + str(index_size_in_gb) +
                      " GB")
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
            # If the # of days exceeds the policy's day check and the index size is at
            # least 1 GB, set rollover
            if days_ago >= index_rollover_policies[policy]["days"] and index_size_in_gb >= 1:
                rollover_reason = 'Days Policy'
                rollover = True
            # if alias['index'] == 'logstash-justin-test-000003':
            #     rollover = True
            # print(f"Processing index {index['index']} with size of {index_size_in_gb} and
            # age of {days_ago}")
            # If index is rollover ready, append to list
            if rollover:
                print(
                    f"Adding index {index['index']} to rollover due to {rollover_reason}. " +
                    f"Size={index_size_in_gb} Age={days_ago}")
                # Rollover the index
                if not settings['settings']['debug']:
                    retries = 3
                    success = False
                    while retries != 0 and success is False:
                        # This triggers the actual rollover
                        if es.rollover_index(
                                client_config,
                                str(index['index']),
                                str(alias['alias'])
                            ):
                            success = True
                        else:
                            retries = retries - 1
                    if success is False:
                        settings = load_settings()
                        message = "Rollover operation failed for client " + \
                            f"{client_config['client_name']}." + \
                            f"\nTried rolling over index {index} " + \
                            f"due to {rollover_reason}. " + \
                            f"Size={index_size_in_gb} Age={days_ago}"

                        send_notification(
                            client_config,
                            "rollover",
                            "Failed",
                            message,
                            teams=settings['rollover']['ms-teams'],
                            jira=settings['rollover']['jira']
                        )
                else:
                    print("Would have triggered rollover on " + index)


def rollover_client_indicies(client_config):
    """Forks off and processes rollover jobs

    Args:
        client_config (dict): Client configuration
    """
    settings = load_settings()
    # Get the rollover policy for the client
    index_rollover_policies = get_rollover_policy(client_config)
    retry_count = 60
    sleep_time = 60
    success = 0
    while retry_count >= 0 and success == 0:
        # Check cluster health - Expect Yellow to continue
        if es.check_cluster_health_status(
            client_config,
            settings['rollover']['health_check_level']
        ):
            # Get current aliases members
            aliases = es.get_all_index_aliases(client_config)
            with ThreadPoolExecutor(
                max_workers=es.get_lowest_data_node_thread_count(client_config)
            ) as executor:
                # Apply rollover to aliases
                for alias in aliases:
                    executor.submit(apply_rollover_policy_to_alias,
                                    client_config, alias, index_rollover_policies)
            success = 1
            aliases = []
            #data_streams_indices = es.es_get_data_stream_indices(client_config)
            data_stream_response = es.get_data_streams(client_config)
            for data_stream in data_stream_response['data_streams']:
                index_number = f"{data_stream['generation']:06}"
                # Look through indices in reverse as the last entry is likely
                # the most recent index
                write_index = ""
                for ds_index in reversed(data_stream['indices']):
                    if ds_index.endswith(str(index_number)):
                        write_index = ds_index
                        break
                if write_index != "":
                    alias = {
                        'alias': data_stream['name'],
                        'index': write_index,
                        'filter': "-",
                        'routing_search': "-",
                        "is_write_index": 'true'
                    }
                    aliases.append(alias)
            
            for data_stream in data_streams_indices:
                alias = {
                    'alias': data_stream['index'][4:-7],
                    'index': es.get_index_group(data_stream['index']),
                    'filter': "-",
                    'routing_search': "-",
                    "is_write_index": 'false'
                }
                aliases.append(alias)
            with ThreadPoolExecutor(
                max_workers=es.get_lowest_data_node_thread_count(client_config)
            ) as executor:
                # Apply rollover to aliases
                for alias in aliases:
                    executor.submit(apply_rollover_policy_to_alias,
                                    client_config, alias, index_rollover_policies)
            # unique_indices = get_values_from_dictionary_array(aliases, 'index')
            # unique_alias_names = get_values_from_dictionary_array(
            #     aliases, 'alias')
            # for alias in unique_alias_names:
            #     # print(f"Processing data stream mock alias of {alias}:")
            #     regex = "^.ds-" + alias + '-[0-9]{6,}$'
            #     *_, write_index = (index for index in unique_indices if re.match(regex, index))
            #     count = 0
            #     for data_stream_alias in aliases:
            #         if data_stream_alias['alias'] == alias and \
            #                 data_stream_alias['index'] == write_index:
            #             aliases[count]['is_write_index'] = 'true'
            #         count = count + 1
            # with ThreadPoolExecutor(
            #     max_workers=es.get_lowest_data_node_thread_count(client_config)
            # ) as executor:
            #     # Apply rollover to aliases
            #     for alias in aliases:
            #         executor.submit(apply_rollover_policy_to_alias,
            #                         client_config, alias, index_rollover_policies)
        else:
            if retry_count > 0:
                print("Rollover operation failed for " +
                      client_config['client_name'] + ". Cluster health does not meet level:  " +
                      settings['rollover']['health_check_level'])
            else:
                message = "Rollover operation failed.\n\nIt is also possible that connections " + \
                    "are unable to be made to the client/nginx node. Please fix.\n\nRemember " + \
                    "that in order for client's to be properly build you will need to get " + \
                    "their cluster status to **Green** or **Yellow** and then re-run the " + \
                    "following command:\n\n**python3 /opt/elastic-ilm/rollover.py --client " + \
                    client_config['client_name'] + "**"
                send_notification(
                    client_config,
                    "rollover",
                    "Failed",
                    message,
                    teams=settings['rollover']['ms-teams'],
                    jira=settings['rollover']['jira']
                )
        if success == 0:
            # Decrese retry count by one before trying while statement again
            retry_count = retry_count - 1
            print("Retry attempts left for rollover operation set to " +
                  str(retry_count) + " sleeping for " + str(sleep_time) + " seconds")
            time.sleep(sleep_time)


def apply_rollover_policies(client_to_process=""):
    """Starts overall rollover jobs

    Args:
        client_to_process (str, optional): Client to process. Defaults to "".
    """
    settings = load_settings()
    if settings['rollover']['enabled']:
        # Load all client configurations from /opt/maintenance/*.json
        clients = load_configs()
        # Loop through each client to perform accounting per client
        for key, client_config in clients.items():
            # Set nice variable names
            client_name = key
            # If client set at command line only run it otherwise
            # execute for all clients
            if client_to_process == "" or client_name == client_to_process:
                if settings['settings']['limit_to_client'] == client_name or \
                        settings['settings']['limit_to_client'] == "":
                    print("Processing rollovers for " + client_name)
                    # Trigger rollover process
                    rollover_client_indicies(client_config)


if __name__ == "__main__":
    import argparse
    from argparse import RawTextHelpFormatter
    parser = argparse.ArgumentParser(
        description='Used to manually run accounting against a ' +
        'specific client (Example - rollover.py --client ha)',
        formatter_class=RawTextHelpFormatter
    )
    parser.add_argument("--client", default="", type=str,
                        help="Set to a specific client name to " +
                        "limit the accounting script to one client")
    parser.add_argument("--notification", default="True",
                        type=bool, help="Set to False to disable notifications")

    args = parser.parse_args()
    manual_client = args.client
    if args.notification == "True":
        NOTIFICATION = True
    else:
        NOTIFICATION = False

    apply_rollover_policies(manual_client)
