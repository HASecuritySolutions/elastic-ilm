""" Allocate indices by tagging them"""
#!/usr/bin/env python3
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from config import load_configs, load_settings
#from error import send_notification
import es

NOTIFICATION = False

def get_allocation_policy(client_config):
    """Grab the current allocation policies

    Args:
        client_config (dict): Client configuration

    Returns:
        dict: Returns allocation policy
    """
    if "policy" in client_config:
        if "allocation" in client_config['policy']:
            index_allocation_policies = client_config['policy']['allocation']
        else:
            index_allocation_policies = {"global": {}}
    else:
        index_allocation_policies = {"global": {}}
    return index_allocation_policies


def allocate_indices(client_config, index, index_allocation_policies):
    """Processes index allocations per index age

    Args:
        client_config (dict): Client configuration
        index (str): Index name
        index_allocation_policies (dict): Allocation policy
    """
    elastic_connection = es.build_es_connection(client_config)
    newest_record = ""
    newest_record = es.get_newest_document_date_in_index(
        client_config, index, elastic_connection)
    # make sure newest record is not empty
    if newest_record != "":
        # Get the index specific allocation policy
        policy = es.check_index_allocation_policy(
            index, index_allocation_policies)
        # Get policy allocation days from specific policy
        policy_days = index_allocation_policies[policy]
        # Get current datetime
        current_date = datetime.utcnow()
        # Figure out how many days since current_date vs. newest_record
        days_ago = (current_date - newest_record).days
        # Check if days_ago is greater than or equal to policy date
        # If greater than or equal to policy date, delete index
        if days_ago >= policy_days:
          allocation_type = 'warm'
        else:
          allocation_type = 'hot'
        if allocation_type == 'warm':
            # Change index allocation per policy
            index_settings = elastic_connection.indices.get_settings(
                index=index
            )
            index_settings = index_settings[index]['settings']['index']
            box_type = 'hot'
            if 'routing' in index_settings:
                if 'allocation' in index_settings['routing']:
                    if "include" in index_settings['routing']['allocation']:
                        if "_tier_preference" in index_settings['routing']['allocation']['include']:
                            if "data_hot" in index_settings['routing']['allocation']['include']['_tier_preference']:
                                box_type = "hot"
                            if "data_warm" in index_settings['routing']['allocation']['include']['_tier_preference']:
                                box_type = "warm"
                    if 'require' in index_settings['routing']['allocation']:
                        if 'box_type' in index_settings['routing']['allocation']['require']:
                            box_type= index_settings['routing']['allocation']['require']['box_type']
            if allocation_type != box_type:
                tier_preference = False
                if 'routing' in index_settings:
                  if 'allocation' in index_settings['routing']:
                    if "include" in index_settings['routing']['allocation']:
                      if "_tier_preference" in index_settings['routing']['allocation']['include']:
                        tier_preference = True

                if tier_preference:
                  print(f"Changing allocation of index {index} to tier preference of data_warm")
                  elastic_connection.indices.put_settings(
                    index=index,
                    body={"index.routing.allocation.include._tier_preference": "data_warm"}
                  )
                else:
                  print(f"Changing allocation of index {index} to box type warm")
                  elastic_connection.indices.put_settings(
                    index=index,
                    body={"index.routing.allocation.require.box_type": allocation_type}
                  )

    elastic_connection.close()

def apply_allocation_to_indices(indices, index_allocation_policies, client_config):
    """Get indices and submit for allocation

    Args:
        indices (array): List of indices
        index_allocation_policies (dict): Allocation policy
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
                executor.submit(
                    allocate_indices, client_config, index, index_allocation_policies)
    elastic_connection.close()

def apply_allocation_policies(client_config=""):
    """Apply allocation policies

    Args:
        manual_client (str, optional): Client configuration. Defaults to "".
    """

    client_settings = load_settings()
    if 'allocation' not in client_settings:
        client_settings['allocation'] = {
            'enabled': False
        }
        print("Allocation not enabled in settings.toml")
    limit_to_client = client_settings['settings']['limit_to_client']
    if client_settings['allocation']['enabled']:
        # Load all client configurations from /opt/maintenance/*.json
        clients = load_configs()
        # Loop through each client to perform accounting per client
        for key, client_config in clients.items():
            client_name = key
            print("Processing allocation for " + client_name)
            # If client set at command line only run it otherwise
            # execute for all clients
            if limit_to_client == client_name or limit_to_client == "":
                # Grab the client's allocation policies
                index_allocation_policies = get_allocation_policy(
                    client_config)
                # Next, get information on all current indices in cluster
                indices = es.es_get_indices(client_config)
                # Get the list of indices that are older than the retention policy
                apply_allocation_to_indices(
                    indices, index_allocation_policies, client_config)

if __name__ == "__main__":
    import argparse
    from argparse import RawTextHelpFormatter
    parser = argparse.ArgumentParser(
        description='Used to manually run accounting against a specific client'
        + ' (Example - retention.py --client ha)',
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
    settings = load_settings()

    args = parser.parse_args()
    manual_client = args.client
    if args.notification == "True":
        NOTIFICATION = True
    else:
        NOTIFICATION = False

    apply_allocation_policies(manual_client)
