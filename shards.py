"""This script reindexes small indices"""
import os
import ssl
import uuid
from datetime import datetime
from opensearchpy import OpenSearch
from opensearchpy import helpers
from config import load_configs, load_settings
from es import get_index_group, check_special_index


def build_os_connection(client_config, timeout=10):
    """Builds a connection to OpenSearch

    Args:
        client_config (dict): Client configuration loaded from json
        timeout (int, optional): Timeout for OpenSearch connections. Defaults to 10.

    Raises:
        e: _description_

    Returns:
        connection: OpenSearch connection
    """
    es_config = {}
    try:
        # Check to see if SSL is enabled
        ssl_enabled = False
        if "ssl_enabled" in client_config:
            if client_config['ssl_enabled']:
                ssl_enabled = True
            else:
                ssl_enabled = False
        else:
            ssl_enabled = False

        # Get the SSL settings for the connection if SSL is enabled
        if ssl_enabled:
            # Support older variable implementations of grabbing the ca.crt file
            ca_file = ""
            if "ca_file" in client_config:
                if os.path.exists(client_config['ca_file']):
                    ca_file = client_config['ca_file']
                else:
                    exit("CA file referenced does not exist")
            elif "client_file_location" in client_config:
                if os.path.exists(client_config['client_file_location'] + "/ca/ca.crt"):
                    ca_file = client_config['client_file_location'] + \
                        "/ca/ca.crt"

            if ca_file != "":
                context = ssl.create_default_context(
                    cafile=ca_file)
            else:
                context = ssl.create_default_context()

            if "check_hostname" in client_config:
                check_hostname = client_config['check_hostname']
            if check_hostname:
                context.check_hostname = True
            else:
                context.check_hostname = False

            if "ssl_certificate" in client_config:
                ssl_certificate = client_config['ssl_certificate']
            if ssl_certificate == "required":
                context.verify_mode = ssl.CERT_REQUIRED
            elif ssl_certificate == "optional":
                context.verify_mode = ssl.CERT_OPTIONAL
            else:
                context.verify_mode = ssl.CERT_NONE

            es_config = {
                "scheme": "https",
                "ssl_context": context,
            }

        # Enable authentication if there is a passwod section in the client JSON
        password_authentication = False
        if 'password_authentication' in client_config:
            if client_config['password_authentication']:
                password_authentication = True
        elif 'admin_password' in client_config['password']:
            password_authentication = True
        if password_authentication:
            user = ''
            password = ''
            if 'es_password' in client_config:
                password = client_config['es_password']
            elif 'admin_password' in client_config['password']:
                password = client_config['password']['admin_password']
            if 'es_user' in client_config:
                user = client_config['es_user']
            elif client_config['platform'] == "elastic":
                user = 'elastic'
            else:
                user = 'admin'
            es_config['http_auth'] = (
                user, password)

        # Get the Elasticsearch port to connect to
        if 'es_port' in client_config:
            es_port = client_config['es_port']
        elif client_config['client_number'] == 0:
            es_port = "9200"
        else:
            es_port = str(client_config['client_number']) + "03"

        # Get the Elasticsearch host to connect to
        if 'es_host' in client_config:
            es_host = client_config['es_host']
        else:
            es_host = client_config['client_name'] + "_client"

        es_config['retry_on_timeout'] = True
        es_config['max_retries'] = 10
        es_config['timeout'] = timeout
        if os.getenv('DEBUGON') == "1":
            print(es_config)
            print(es_host)
            print(es_port)
        return OpenSearch(
            [{'host': es_host, 'port': es_port}], **es_config)
    except ConnectionError as error:
        print(error)
        print("Connection attempt to Elasticsearch Failed")
        raise error


def get_not_current_indices_from_data_streams(connection, ignore_indices):
    """Returns all indices that are not the most current index
       in a data stream

    Args:
        connection (object): OpenSearch connection
        ignore_indices (list): List of indices to ignore

    Returns:
        list: List of index names
    """
    return_results = []
    try:
        data_stream_response = connection.indices.get_data_stream(name="*")
        if len(data_stream_response['data_streams']) > 0:
            for data_stream in data_stream_response['data_streams']:
                if len(data_stream['indices']) > 0:
                    generation = str(data_stream['generation'])
                    for index in data_stream['indices']:
                        if index['index_name'] in ignore_indices:
                            continue
                        if not index['index_name'].endswith(generation):
                            index_name = index['index_name']
                            if check_special_index(index_name) is False:
                                return_results.append(index_name)
    except ConnectionError as error:
        print(error)
    return return_results


def get_not_current_indices_from_aliases(connection, ignore_indices):
    """Returns all indices that are not the most current index
       in a alias

    Args:
        connection (object): OpenSearch connection
        ignore_indices (list): List of indices to ignore

    Returns:
        list: List of index names
    """
    return_results = []
    try:
        alias_response = connection.indices.get_alias(name="*")
        for index in alias_response.keys():
            if index in ignore_indices:
                continue
            if check_special_index(index) is False:
                index_group = get_index_group(index)
                if index_group in alias_response[index]['aliases']:
                    if 'is_write_index' in alias_response[index]['aliases'][index_group]:
                        if alias_response[index]['aliases'][index_group]['is_write_index'] is False:
                            return_results.append(index)
    except ConnectionError as error:
        print(error)
    return return_results


def is_small_shard_index(index, shard_size_check):
    """Checks if index shards are small

    Args:
        index (str): Index name
        shard_size_check (int): Minimum shard size expected

    Returns:
        bool: True or False
    """
    index_size_in_gb = round(
        int(index['pri.store.size']) / 1024 / 1024 / 1024, 0)
    primary_shard_size = index_size_in_gb / \
        int(index['shardsPrimary'])
    if primary_shard_size <= shard_size_check:
        return True
    else:
        return False


def get_index_information(index_request, connection):
    """Get index or indices information

    Args:
        index_request (str): Index or indices to pull from
                             Supports wildcards
        connection (object): OpenSearch connection

    Returns:
        list: List of indices as dictionaries
    """
    # h is used to select fields to return (see full list open Dev Tools and run this command)
    # GET /_cat/indices?help
    # s is used to sort the resulting output
    # bytes = b makes it return numeric bytes instead of human readable bytes
    # https://www.elastic.co/guide/en/elasticsearch/reference/current/cat.html
    try:
        return connection.cat.indices(
            index=index_request,
            format="json",
            h=(
                "index",
                "shardsPrimary",
                "storeSize",
                "pri.store.size"
            ),
            s="creation.date",
            bytes="b")
    except ConnectionError as error:
        print(error)
        return []


def get_small_indices(indices_information, processable_indices, shard_size_check):
    """Returns all small indices

    Args:
        indices_information (dict): Dictionary of index information from OpenSearch
        processable_indices (list): List of indices to process
        shard_size_check (int): Minimum shard size expected

    Returns:
        dict: Dictionary of index groups and indices that are small
    """
    small_indices = {}
    for entry in processable_indices:
        for index_details in indices_information:
            if entry == index_details['index']:
                if is_small_shard_index(index_details, shard_size_check):
                    index_group = get_index_group(entry)
                    if index_group not in small_indices:
                        small_indices[index_group] = []
                    small_indices[index_group].append(entry)
    return small_indices


clients = load_configs()
# Loop through each client to perform accounting per client
for key, config in clients.items():
    # Set nice variable names
    client_name = key
    # If client set at command line only run it otherwise
    # execute for all clients
    if client_name == "otava":
        settings = load_settings()
        if 'shard_minimum_size' in settings['rollover']:
            MINIMUM_SIZE = settings['rollover']['shard_minimum_size']
        else:
            MINIMUM_SIZE = 10
        print("Processing small indices for " + client_name)
        opensearch = build_os_connection(config)

        # Get indices that have jobs created within OpenSearch
        indices_with_pending_operations = []
        for job in helpers.scan(opensearch,
            query={"query": {"query_string": {"query": "operation:reindex"}}},
            index="elastic-ilm-jobs"
        ):
            indices_with_pending_operations = indices_with_pending_operations + \
                job['_source']['indices']
        indices_with_pending_operations.sort()

        indices_to_process = get_not_current_indices_from_data_streams(
            opensearch, indices_with_pending_operations)
        indices_to_process = indices_to_process + \
            get_not_current_indices_from_aliases(
                opensearch, indices_with_pending_operations)
        indices_to_process.sort()
        all_index_information = get_index_information("*", opensearch)

        reindex_small_indices = get_small_indices(
            all_index_information,
            indices_to_process,
            MINIMUM_SIZE
        )

        # Process in batches. Example
        for small_index_group, _ in reindex_small_indices.items():
            # Only reindex if there are more than 1 indices to reindex
            # otherwise you will reindex back into the same situation
            INDEX_COUNT = 0
            TOTAL_SIZE = 0
            REINDEX_BATCH = []
            if len(reindex_small_indices[small_index_group]) > 1:
                # print(
                #     f"{small_index_group} has {len(reindex_small_indices[small_index_group])}" +\
                #         " indices to reindex")
                for small_index in reindex_small_indices[small_index_group]:
                    for index_info in all_index_information:
                        if index_info['index'] == small_index:
                            index_size = round(
                                int(index_info['pri.store.size']) / 1024 / 1024 / 1024, 0)
                            break
                    # print(f"Need to reindex {small_index} with size of {index_size}")
                    INDEX_COUNT += 1
                    TOTAL_SIZE += index_size
                    REINDEX_BATCH.append(small_index)
                    if TOTAL_SIZE > MINIMUM_SIZE or INDEX_COUNT > 30:
                        print(REINDEX_BATCH)
                        document = {
                            'indices': REINDEX_BATCH,
                            "operation": "reindex",
                            "reason": "small_indices",
                            '@timestamp': datetime.now()
                        }
                        result = opensearch.index(
                            index="elastic-ilm-jobs",
                            id=str(uuid.uuid4()),
                            body=document,
                            op_type="create"
                        )
                        TOTAL_SIZE = 0
                        INDEX_COUNT = 0
                        REINDEX_BATCH = []
                if INDEX_COUNT > 0:
                    document = {
                        'indices': REINDEX_BATCH,
                        "operation": "reindex",
                        "reason": "small_indices",
                        '@timestamp': datetime.now()
                    }
                    result = opensearch.index(
                        index="elastic-ilm-jobs",
                        id=str(uuid.uuid4()),
                        body=document,
                        op_type="create"
                    )
        opensearch.close()
