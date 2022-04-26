#!/usr/bin/env python3
from config import load_configs, load_settings
from error import send_notification
import es
import time
import dictdiffer 

def check_for_mapping_conflicts(client_config, indices, compare_mapping):
    elastic_connection = es.build_es_connection(client_config)
    for index in indices:
        mapping = elastic_connection.indices.get_mapping(index)
        index_mapping = mapping[index]['mappings']['properties']
        differences = []
        for diff in list(dictdiffer.diff(index_mapping, compare_mapping)):
            differences.append(diff)
        if len(differences) > 0:
            print("Index template does not match index " + index + ". Changes below")
            print(differences)

        # Grab X records from X indices to check values

        # Reindex if value will convert well to template settings

        # Otherwise, change template
        
def get_index_template(client_config, template_name):
    try:
        elastic_connection = es.build_es_connection(client_config)
        index_template = elastic_connection.indices.get_template(template_name)
        elastic_connection.close()
        return index_template
    except:
        return "Not found"

def create_index_template(client_config, group, last_index):
    # Base template settings
    template = {
        "order": 5,
        "version": 60001,
        "settings": {
            "index": {
                "mapping": {
                    "total_fields": {
                    "limit": "15000"
                    }
                },
                "refresh_interval": "30s",
                "number_of_shards": "1",
                "number_of_replicas": "1"
            }
        },
        "mappings": {
        },
        "aliases": {}
    }

    try:
        elastic_connection = es.build_es_connection(client_config)
        # Get index mappings from most current index
        field_mappings = elastic_connection.indices.get_mapping(last_index)
        # Extract mappings from most current index
        mapping = field_mappings[last_index]['mappings']
        # Update base template to have index mappings
        template['mappings'] = mapping
        # Set index patterns template should match on
        template['index_patterns'] = [ group + "-*"]
        # Create the template
        elastic_connection.indices.put_template(group, body=template)
        elastic_connection.close()
        return True
    except:
        return False

def fix_mapping_conflicts(manual_client):
    settings = load_settings()
    retry_count = 60
    sleep_time = 60
    success = 0
    if "fixmapping" in settings:
        if "enabled" in settings:
            fixmapping_enabled = settings['fixmapping']['enabled']
        else:
            fixmapping_enabled = True
    else:
        fixmapping_enabled = True
    if fixmapping_enabled:
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
                print("Processing fix mappings for " + client_name)
                if settings['settings']['limit_to_client'] == client or settings['settings']['limit_to_client'] == "":
                    while retry_count >= 0 and success == 0:
                        indices = es.es_get_indices(client_config)
                        index_groups = {}
                        for index in indices:
                            # Do not mess with special indices
                            if not es.check_special_index(index['index']):
                                index_group = es.get_index_group(index['index'])
                                if index_group not in index_groups:
                                    index_groups[index_group] = []
                                index_groups[index_group].append(index['index'])

                        for group in index_groups:
                            indices = index_groups[group]
                            indices.sort()
                            last_index = indices[-1]
                            if get_index_template(client_config, group) == "Not found":
                                print("Missing index template for " + str(group) + " - creating one off highest index number")
                                create_index_template(client_config, group, last_index)
                            # TESTING
                            template = get_index_template(client_config, group)
                            template_mappings = template[group]['mappings']['properties']
                            if group == "logstash-proofpoint":
                                check_for_mapping_conflicts(client_config, index_groups[group], template_mappings)

                        success = 1
                    else:
                        if retry_count == 0:
                            message = "Fix mapping operation failed.\n\nIt is also possible that connections are unable to be made to the client/nginx node. Please fix.\n\nRemember that in order for client's to be properly build you will need to get their cluster status to **Green** or **Yellow** and then re-run the following command:\n\n**python3 /opt/elastic-ilm/fix_mapping.py --client " + client_name + "**"
                            send_notification(client_config, "fixmapping", "Failed", message, teams=settings['fixmapping']['ms-teams'], jira=settings['fixmapping']['jira'])
                    if success == 0:
                        # Decrese retry count by one before trying while statement again
                        retry_count = retry_count - 1
                        print("Retry attempts left for fix mapping operation set to " + str(retry_count) + " sleeping for " + str(sleep_time) + " seconds")
                        time.sleep(sleep_time)

if __name__ == "__main__":
    import argparse
    from argparse import RawTextHelpFormatter
    parser = argparse.ArgumentParser(description='Used to manually run accounting against a specific client (Example - fix_mapping.py --client ha)', formatter_class=RawTextHelpFormatter)
    parser.add_argument("--client", default="", type=str, help="Set to a specific client name to limit the accounting script to one client")
    parser.add_argument("--notification", default="True", type=str, help="Set to False to disable notifications")
    settings = load_settings()

    args = parser.parse_args()
    manual_client = args.client
    if args.notification == "True":
        notification = True
    else:
        notification = False

    fix_mapping_conflicts(manual_client)
