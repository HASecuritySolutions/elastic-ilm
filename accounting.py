#!/usr/bin/env python3
from config import load_configs, load_settings
from error import send_notification
import es
import json
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import threading
from datetime import datetime
from os import path

def get_allocation_policy(client_config):
    # Grab the client specific allocation policy (tiering policy)
    if "policy" in client_config:
        if "allocation" in client_config['policy']:
            index_allocation_policies = client_config['policy']['allocation']
        else:
            index_allocation_policies = { "global": 30 }
    else:
            index_allocation_policies = { "global": 30 }
    return index_allocation_policies

def calculate_accounting(client_config, client_name):
    settings = load_settings()
    # Set today's current datetime
    today = datetime.now()
    date_time = today.strftime("%Y%m%d")
    # Check if client accounting data already calculated today
    if path.exists(settings['accounting']['output_folder'] + '/' + client_name + "_accounting-" + date_time + ".json"):
        print("Accounting already calculated for " + client_name + " today: " + str(date_time))
        return True
    else:
        print("Calculating accounting data for " + client_name)
        # Check cluster health - Expect Yellow to continue
        if es.check_cluster_health_status(client_config, settings['accounting']['health_check_level']):
            elastic_connection = es.build_es_connection(client_config)
            # Grab the client specific allocation policy (tiering policy)
            index_allocation_policies = get_allocation_policy(client_config)

            # Next, get information on all current indices in client cluster
            indices = es.es_get_indices(client_config)
            print("Client " + client_name + " has " + str(len(indices)) + ' indices')

            accounting_records = []
            special_index_size = 0
            # Loop through each index
            for index in indices:
                if not es.check_special_index(index['index']):
                    # Grab the current index's allocation policy based on index name
                    policy = es.check_index_allocation_policy(index['index'], index_allocation_policies)
                    # Lookup the policy's # of days setting
                    policy_days = index_allocation_policies[policy]

                    # Get current datetime
                    current_date = datetime.now()
                    # Get index datetime
                    index_date = datetime.strptime(index['creation.date.string'], '%Y-%m-%dT%H:%M:%S.%fZ')
                    # Figure out how many days since current_date vs. index_date
                    days_ago = (current_date - index_date).days
                    
                    # Build client specific daily accounting records
                    # Convert index size from bytes to gigabytes
                    index_size_in_gb = round(float(index['storeSize']) / 1024 / 1024 / 1024, 5)
                    # Calculate indices daily cost
                    # If index is older than policy_days, set disk type to sata
                    # and make sure index is set to proper allocation attribute
                    if days_ago >= policy_days:
                        cost = round(float(index_size_in_gb) * settings['accounting']['sata_cost'], 2)
                        disk_type = 'sata'
                        # TODO - Set proper allocation attribute to move data from SSD to SATA
                    else:
                        cost = round(float(index_size_in_gb) * settings['accounting']['ssd_cost'], 2)
                        disk_type = 'ssd'
                    index_group = es.get_index_group(index['index'])
                    accounting_record = {
                        'name': index['index'],
                        'client': client_name,
                        'size': float(index_size_in_gb),
                        'logs': int(index['docsCount']),
                        'disk': disk_type,
                        'cost': float(cost),
                        'index_creation_date': index['creation.date.string'],
                        '@timestamp': str(current_date.isoformat()),
                        'index_group': index_group,
                        'allocation_policy': str(policy),
                        'current_policy_days': int(policy_days)
                    }
                    accounting_records.append(accounting_record)
                else:
                    index_size_in_gb = round(float(index['storeSize']) / 1024 / 1024 / 1024, 5)
                    special_index_size += index_size_in_gb
            # Appends newest record date into accounting_record
            #for accounting_record in accounting_records:
                #accounting_record['newest_document_date'] = str(es.get_newest_document_date_in_index(client_config, index['index'], elastic_connection).isoformat())
            if not settings['settings']['debug'] and len(accounting_records) != 0:
                for accounting_record in accounting_records:
                    # Create a backup copy of each accounting record
                    with open(settings['accounting']['output_folder'] + '/' + client_name + "_accounting-" + date_time + ".json", 'a') as f:
                        json_content = json.dumps(accounting_record)
                        f.write(json_content)
                        f.write('\n')
            else:
                print("Debug enabled or no data to save. Not creating accounting file")

            elastic_connection.close()

            cluster_stats = es.get_cluster_stats(client_config)
            # Convert cluster size from bytes to gigabytes
            cluster_size = round(float(cluster_stats['indices']['store']['size_in_bytes']) / 1024 / 1024 / 1024, 2)
            print("Total cluster size is: " + str(cluster_size) + " GB")

            with open(settings['accounting']['output_folder'] + '/' + client_name + "_accounting-" + date_time + ".json") as f:
                accounting_file = f.readlines()
            total_accounting_size = 0
            for record in accounting_file:
                json_object = json.loads(record)
                total_accounting_size += float(json_object['size'])
            total_accounting_size = round(total_accounting_size, 2)
            print("Total accounting record size is: " + str(total_accounting_size) + " GB")

            special_index_size = round(special_index_size, 2)
            print("Total special index size is : " + str(special_index_size) + " GB")

            total_accounting_index_size = special_index_size + total_accounting_size
            print("Accounting and special index size equals : " + str(total_accounting_index_size) + " GB")

            difference_size = cluster_size - total_accounting_index_size
            print("Difference is " + str(difference_size) + " GB")
            if difference_size >= 0.5:
                message = "Accounting verification is off by more than 0.5 GB. Please find out why. This test is performed by comparing the current cluster size against the records in the accounting JSON output files."
                send_notification(client_config, "accounting verification", "Failed", message, jira=settings['accounting']['ms-teams'], teams=settings['accounting']['jira'])

            if len(accounting_records) != 0 and not settings['settings']['debug'] and settings['accounting']['output_to_es']:
                print("Sending accounting records to ES")
                elasticsearch_connection = es.build_es_connection(client_config)
                results = es.get_list_by_chunk_size(accounting_records, 100)
                for result in results:
                    es.bulk_insert_data_to_es(elasticsearch_connection, result, "accounting", bulk_size=100)
                elasticsearch_connection.close()
                clients = load_configs()
                if client_name != settings['accounting']['send_copy_to_client_name'] and settings['accounting']['send_copy_to_client_name'] != '':
                    elasticsearch_connection = es.build_es_connection(clients[settings['accounting']['send_copy_to_client_name']])
                    results = es.get_list_by_chunk_size(accounting_records, 100)
                    for result in results:
                        es.bulk_insert_data_to_es(elasticsearch_connection, result, "accounting", bulk_size=100)
                    elasticsearch_connection.close()
                return True
            else:
                if not settings['settings']['debug']:
                    print("No index data found for accounting")
                    return True
                else:
                    return True
        else:
            settings = load_settings()
            message = "Accounting operation failed.\n\nIt is also possible that connections are unable to be made to the client/nginx node. Please fix.\n\nRemember that in order for client's to be properly build you will need to get their cluster status to **Green** and then re-run the following command:\n\n**python3 /opt/elastic-ilm/accounting.py --client " + client_name + "**"
            send_notification(client_config, "accounting", "Failed", message, teams=settings['accounting']['ms-teams'], jira=settings['accounting']['jira'])
            return False

def run_accounting(manual_client):
    settings = load_settings()
    if settings['accounting']['enabled']:
        retry_count = settings['accounting']['retry_attempts']
        retry_list = []
        sleep_time = settings['accounting']['retry_wait_in_seconds']
        # Load all client configurations
        clients = load_configs()
        # Add all clients initially to retry_list for first run
        for client in clients:
            # If client set at command line only run it otherwise
            # execute for all clients
            if manual_client == "" or clients[client]['client_name'] == manual_client:
                retry_list.append(clients[client]['client_name'])
        # Loop through each client to perform accounting per client
        while retry_count >= 0 and len(retry_list) > 0:
            print("Accounting job processing for:")
            print(retry_list)
            print("Retry count set to " + str(retry_count))
            print("------------------------------\n")
            for client in clients:
                # Set nice variable names
                client_name = clients[client]['client_name']
                if client_name in retry_list:
                    client_config = clients[client]
                    if retry_count == 0:
                        # If on the last attempt, accept a health level of yellow
                        health_check_level = settings['accounting']['fallback_health_check_level']
                        message = "Accounting operation failed.\n\nDue to failing 10 times, the health level was set to " + settings['accounting']['fallback_health_check_level'] + " and ran for client " + clients[client]['client_name'] + ". \n\nThis is not optimal. Please check to see if data should be purged and re-inserted with a green cluster."
                        send_notification(clients[client], "accounting", "Failed", message, jira=settings['accounting']['ms-teams'], teams=settings['accounting']['jira'])
                    # If client set at command line only run it otherwise
                    # execute for all clients
                    if manual_client == "" or client_name == manual_client:
                        # Trigger calculate accounting process
                        result = calculate_accounting(client_config, client_name)
                        if result:
                            # Remove successful client from retry_list
                            retry_list.remove(clients[client]['client_name'])
                        else:
                            print("Client " + client_name + " did not process correctly.")
                            if retry_count == 0:
                                if notification:
                                    message = "Accounting operation failed.\n\nIt is also possible that connections are unable to be made to the client/nginx node. Please fix.\n\nRemember that in order for client's to be properly build you will need to get their cluster status to **Green** and then re-run the following command:\n\npython3 /opt/cloud_operations/accounting.py --client " + client_name + "\n\nIf a green cluster is not possible by end of day, please run the following command to force run with a different color cluster:\n\npython3 /opt/cloud_operations/accounting.py --client " + client_name + " --health yellow"
                                    send_notification(client_config, "accounting", "Failed", message, jira=settings['accounting']['ms-teams'], teams=settings['accounting']['jira'])
            # Lower the retry_count by 1
            retry_count = retry_count - 1
            if retry_count >= 0 and len(retry_list) > 0:
                print("The below client(s) failed to process. Retry necessary:")
                print(retry_list)
                print("Retry count set to " + str(retry_count) + " sleeping for " + str(sleep_time) + " seconds")
                time.sleep(sleep_time)

if __name__ == "__main__":
    import argparse
    from argparse import RawTextHelpFormatter
    parser = argparse.ArgumentParser(description='Used to manually run accounting against a specific client (Example - accounting.py --client ha)', formatter_class=RawTextHelpFormatter)
    parser.add_argument("--client", default="", type=str, help="Set to a specific client name to limit the accounting script to one client")
    parser.add_argument("--notification", default="True", type=str, help="Set to False to disable notifications")

    args = parser.parse_args()
    manual_client = args.client
    if args.notification == "True":
        notification = True
    else:
        notification = False

    run_accounting(manual_client)
