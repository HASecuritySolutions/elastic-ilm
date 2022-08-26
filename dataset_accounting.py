#!/usr/bin/env python3
"""Calculates per dataset accounting information"""
import json
from datetime import datetime, timedelta
from os.path import exists
import es
from config import load_configs, load_settings

known_index_patterns = [
    {"index": "logstash-f5", "dataset": "loadbalancer", "asset_type": "ip", "fields": ['source.ip']},
    {"index": "elastalert_status", "dataset": "elastalert", "asset_type": "rule", "fields": ['rule_name']},
    {"index": "logstash-adaudit", "dataset": "adaudit", "asset_type": "computer", "fields": ['cef.extensions.deviceCustomString3']},
    {"index": "logstash-switch", "dataset": "switch", "asset_type": "computer", "fields": ['log.source.ip']},
    {"index": "logstash-crowdstrike", "dataset": "edr", "asset_type": "user", "fields": ['destination.user.name']},
    {"index": "logstash-flow", "dataset": "flow", "asset_type": "ip", "fields": ['source.ip']},
    {"index": "logstash-iis", "dataset": "web", "asset_type": "computer", "fields": ['host.name']},
    {"index": "logstash-suricata", "dataset": "ids", "asset_type": "ip", "fields": ['source.ip']},
    {"index": "logstash-web-apache", "dataset": "web", "asset_type": "computer", "fields": ['log.source.ip']},
    {"index": "logstash-web-nginx", "dataset": "web", "asset_type": "computer", "fields": ['log.source.ip']},
    {"index": "logstash-apache", "dataset": "web", "asset_type": "computer", "fields": ['log.source.ip']},
    {"index": "logstash-duo", "dataset": "duo", "asset_type": "user", "fields": ['user.name']},
    {"index": "logstash-umbrella", "dataset": "umbrella", "asset_type": "user", "fields": ['PolicyIdentity']},
    {"index": "logstash-iboss", "dataset": "iboss", "asset_type": "ip", "fields": ['SourceIp']},
    {"index": "logstash-prism", "dataset": "prism", "asset_type": "user", "fields": ['user']},
    {"index": "logstash-nginx", "dataset": "web", "asset_type": "computer", "fields": ['log.source.ip']},
    {"index": "logstash-linux-vmware", "dataset": "vmware", "asset_type": "computer", "fields": ['log.source.ip']},
    {"index": "infrastructure-qumulo-storage-qumulo-1.0", "dataset": "qumulo", "asset_type": "computer", "fields": ['log.source.ip']},
    {"index": "winlogbeat", "dataset": "windows", "asset_type": "computer", "fields": ['winlog.computer_name']},
    {"index": "metricbeat", "dataset": "metricbeat", "asset_type": "computer", "fields": ['host.name']},
    {"index": "packetbeat", "dataset": "packetbeat", "asset_type": "computer", "fields": ['host.name']},
    {"index": "service-microsoft-azure-ad", "dataset": "azure_ad", "asset_type": "user", "fields": ['properties.servicePrincipalName']},
    {"index": "service-microsoft-office-365", "dataset": "o365", "asset_type": "user", "fields": ['user.name']},
    {"index": "filebeat-o365", "dataset": "o365", "asset_type": "user", "fields": ['user.name']},
    {"index": "infrastructure-clavister-firewall", "dataset": "firewall", "asset_type": "ip",  "fields": ['source.ip']},
    {"index": "infrastructure-fortinet-firewall", "dataset": "firewall", "asset_type": "ip",  "fields": ['source.ip']},
    {"index": "logstash-fortigate", "dataset": "firewall", "asset_type": "ip",  "fields": ['source.ip']},
    {"index": "logstash-zeek", "dataset": "zeek", "asset_type": "ip",  "fields": ['source.ip']},
    {"index": "auditbeat", "dataset": "auditbeat", "asset_type": "computer", "fields": ['host.name']}
]

def validate_field_in_results(field, result):
    """Validate field exists in search result

    Args:
        field (str): Field name
        result (dict): ES/OS result first hit

    Returns:
        str: Returns field name or empty string
    """
    original_field = field
    for field_level in field.split('.'):
        if field_level in result:
            status = True
            result = result[field_level]
        else:
            status = False
    if status:
        return original_field
    else:
        return ''

def verify_index_has_data(client_config, known_index, timeframe=1):
    """Validates index has data within the last X days

    Args:
        client_config (dict): Client's configuration
        known_index (str): Index name
        timeframe (int): How many hours of data to validate against

    Returns:
        str: Returns field name in existing index or empty string
    """
    query = {
        "query": {
            "range": {
                "@timestamp": {
                    "gte": f"now-{timeframe}d/d",
                    "lt": "now/d"
                }
            }
        }
    }
    connection = es.build_es_connection(client_config)
    result = es.run_search_dsl(
        connection,
        known_index['index'] + "*",
        query,
        sort='@timestamp',
        limit_to_fields=[],
        size=1
    )
    connection.close()
    if result['hits']['total']['value'] > 0:
        for field in known_index['fields']:
            field_name = validate_field_in_results(field, result['hits']['hits'][0]['_source'])
            if field_name != '':
                return field_name
        # It can only hit this if all fields do not exist in return results
        return ''
    else:
        return ''

def get_historical_day_to_index(client_config, index_pattern, field, date_start, date_end):
    """Retrieves indices found within date range

    Args:
        client_config (dict): Client's configuration
        index_pattern (str): Index pattern to search
        field (str): Field name that must exist
        date_start (str): Start date
        date_end (str): End date
    """
    connection = es.build_es_connection(client_config, timeout=120)
    response = es.aggregate_search(
        connection,
        index_pattern,
        '_exists_:' + field,
        'terms',
        '_index',
        sort='@timestamp',
        limit_to_fields=[],
        date_start=date_start,
        date_end=date_end,
        result_size=5000
    )
    connection.close()
    return response

def get_unique_field_count_index(client_config, index_pattern, field, date_start, date_end):
    """Retrieves indices found within date range

    Args:
        client_config (dict): Client's configuration
        index_pattern (str): Index pattern to search
        field (str): Field name that must exist
        date_start (str): Start date
        date_end (str): End date
    """
    connection = es.build_es_connection(client_config, timeout=120)
    return es.get_unique_count(connection, index_pattern, field, date_start, date_end)

def process_dataset_accounting(client_config):
    """Processes the dataset accounting records to file

    Args:
        client_config (dict): Client configuration
    """
    settings = load_settings()
    # Validate if any known index patterns exist and have data
    for known_index in known_index_patterns:
        print(f"Verify if index {known_index['index']} has data for {client_config['client_name']}")
        # If index has data and the proper fields, continue on to process. Otherwise, do nothing
        field_in_index = verify_index_has_data(client_config, known_index)
        # Above will be '' if the index or field required do not exist within data
        if field_in_index != '':
            for days_to_subtract in range(1, settings['dataset_accounting']['days_to_calculate']):
                date_start = datetime.today() - timedelta(days=days_to_subtract)
                date_start = date_start.replace(hour=0, minute=0, second=0, microsecond=0)
                date_end = date_start.replace(hour=23, minute=59, second=59, microsecond=999999)
                file_date = date_start.strftime("%Y%m%d")
                date_start = str(date_start.isoformat())
                date_end = str(date_end.isoformat())
                file = f"{client_config['client_name']}_{known_index['index']}_{file_date}.ndjson"
                if exists(f"{settings['dataset_accounting']['output_folder']}/{file}"):
                    if settings['settings']['debug']:
                        print("Dataset accounting already calculated")
                else:
                    print(f"Processing {known_index['index']} dataset accounting for {file_date}")
                
                    indices = get_historical_day_to_index(
                        client_config,
                        known_index['index'] + "*",
                        field_in_index,
                        date_start,
                        date_end
                    )
                    if settings['settings']['debug']:
                        print(indices)
                    total_used_size_in_gb = 0
                    for index_name, docs_in_index in indices.items():
                        stats = es.es_get_index_stats(client_config, index_name)
                        total_documents = stats['_all']['total']['docs']['count']
                        percent_documents = docs_in_index / total_documents
                        total_size = int(stats['_all']['total']['store']['size_in_bytes'])
                        total_size_gb = round(float(total_size) / 1024 / 1024 / 1024, 8)
                        used_size = total_size * percent_documents
                        used_size_gb = round(float(used_size) / 1024 / 1024 / 1024, 8)
                        total_used_size_in_gb = total_used_size_in_gb + used_size_gb
                        if settings['settings']['debug']:
                            print(f"Index {index_name} has {total_documents:,} " + \
                                f"docs with size of {total_size_gb} GB")
                            print(f"{percent_documents*100:.2f}% of {file_date} " + \
                                f"data found within {index_name}")
                            print(f"Capacity is : {used_size_gb} GB")

                    try:
                        field_uniq_count = get_unique_field_count_index(
                            client_config,
                            known_index['index'] + "*",
                            field_in_index,
                            date_start,
                            date_end
                        )
                    except:
                        field_uniq_count = 0

                    if field_uniq_count != 0:
                        with open(
                            f"{settings['dataset_accounting']['output_folder']}/{file}",
                            'a',
                            encoding='utf_8'
                        ) as wfile:
                            per_asset_gb = total_used_size_in_gb / field_uniq_count
                            per_asset_mb = per_asset_gb * 1024
                            wfile.write(
                                json.dumps({
                                    "dataset": known_index['dataset'],
                                    "asset_type": known_index['asset_type'],
                                    "total_size_gb": total_used_size_in_gb,
                                    "assets": field_uniq_count,
                                    "per_asset_gb": per_asset_gb,
                                    "per_asset_mb": per_asset_mb,
                                    "client": client_config['client_name']
                                })
                            )
                            wfile.write("\n")
                        if settings['settings']['debug']:
                            print(f"Field {field_in_index} has uniq count of {field_uniq_count}")
                            print(f"{known_index['index']} has capacity of " + \
                                f"{total_used_size_in_gb} GB on {file_date}")
                            print(f"Per asset allocation is {per_asset_gb:.2f} GB")
                            print(f"Per asset allocation is {per_asset_mb:.2f} MB")

def run_dataset_accounting(manual_client=""):
    """Triggers kick-off of dataset accounting

    Args:
        manual_client (str, optional): Client name. Defaults to "".
    """
    settings = load_settings()
    if settings['dataset_accounting']['enabled']:
        # Load all client configurations
        clients = load_configs()
        # Add all clients initially to retry_list for first run
        for key, client_config in clients.items():
            # Set nice variable names
            client_name = key
            # If client set at command line only run it otherwise
            # execute for all clients
            if manual_client == "" or client_name == manual_client:
                if settings['settings']['limit_to_client'] == client_name or \
                        settings['settings']['limit_to_client'] == "":
                    print("Processing dataset accounting for " + client_name)
                    # Trigger rollover process
                    process_dataset_accounting(client_config)

if __name__ == "__main__":
    import argparse
    from argparse import RawTextHelpFormatter
    parser = argparse.ArgumentParser(
        description='Used to manually run accounting against " + \
            "a specific client (Example - accounting.py --client ha)',
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
    args = parser.parse_args()
    CLIENT = args.client
    if args.notification == "True":
        NOTIFICATION = True
    else:
        NOTIFICATION = False
    run_dataset_accounting(CLIENT)
