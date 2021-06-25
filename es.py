#!/usr/bin/env python3
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch.connection import create_ssl_context
from error import send_jira_event, send_ms_teams_message, send_notification
import ssl
from itertools import islice
import sys
import os
import re
import json
from config import load_settings
from datetime import datetime
from sys import stdout
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def check_index_allocation_policy(index, policies):
    match_found = 0
    # This sorts the index allocation policies in descending order,
    # by length of characters
    policies = sorted(policies, key=lambda policy: len(policy), reverse=True)
    for policy in policies:
        # Ignore global as that's the fallback if no policy is found
        if policy != "global":
            if index.startswith(policy):
                match_found = 1
                return policy
    # No policy match found, set fallback of global
    if match_found == 0:
        return "global"

def check_index_retention_policy(index, policies):
    match_found = 0
    # This sorts the index retention policies in descending order,
    # by length of characters
    policies = sorted(policies, key=lambda policy: len(policy), reverse=True)
    for policy in policies:
        # Ignore global as that's the fallback if no policy is found
        if policy != "global":
            if index.startswith(policy):
                match_found = 1
                return policy
    # No policy match found, set fallback of global
    if match_found == 0:
        return "global"

def check_index_rollover_policy(index, policies):
    match_found = 0
    # This sorts the index rollover policies in descending order,
    # by length of characters
    policies = sorted(policies, key=lambda policy: len(policy), reverse=True)
    for policy in policies:
        # Ignore global as that's the fallback if no policy is found
        if policy != "global":
            if index.startswith(policy):
                match_found = 1
                return policy
    # No policy match found, set fallback of global
    if match_found == 0:
        return "global"

def get_index_alias_members(client, alias):
    es = build_es_connection(client)
    indices = list()
    members = es.cat.aliases(alias, format="json", h=("index"))
    for member in members:
        indices.append(member['index'])
    es.close()
    return indices

def get_all_index_aliases(client):
    es = build_es_connection(client)
    members = es.cat.aliases(format="json")
    return members

def get_cluster_stats(client):
    es = build_es_connection(client)
    cluster_stats = es.cluster.stats(format="json")
    return cluster_stats

def get_aliases(client):
    es = build_es_connection(client)
    members = es.cat.aliases(format="json")
    return members

def set_index_alias(client, alias, index, write_alias=False):
    es = build_es_connection(client)
    #es.indices.put_alias(index=index, name=alias)
    es.indices.update_aliases({
    #"actions": [
    #    { "add":    { "index": "tweets_2", "alias": "tweets_search" }}, 
    #    { "remove": { "index": "tweets_1", "alias": "tweets_index"  }}, 
    #    { "add":    { "index": "tweets_2", "alias": "tweets_index"  }}  
    #]
    #})
    "actions": [
        { "add":    { "index": index, "alias": alias }}
    ]
    })
    es.close()

def get_index_group(index):
    m = re.search('^([a-zA-Z0-9-._]+)(-.*)?-20[0-9][0-9](\.|-)[0-9]{2}(\.|-)[0-9]{2}$', index)
    if m:
        index_group = m.group(1)
    else:
        m = re.search('^([a-zA-Z0-9-._]+)(-.*)?-20[0-9][0-9](\.|-)[0-9]{2}$', index)
        if m:
            index_group = m.group(1)
        else:
            m = re.search('^([a-zA-Z0-9-._]+)(-.*)?-20[0-9][0-9]$', index)
            if m:
                index_group = m.group(1)
            else:
                m = re.search('^([a-zA-Z0-9-._]+)(-.*)?-[0-9]{6,}$', index)
                if m:
                    index_group = m.group(1)
                else:
                    m = re.search('^([a-zA-Z0-9-._]+)(-.*)?-[a-zA-Z0-9-._]{3,}$', index)
                    if m:
                        index_group = m.group(1)
                    else:
                        index_group = index
    return index_group

def es_get_indices(client):
    es = build_es_connection(client)
    indices = []
    # h is used to select fields to return (to see full list open Dev Tools and run the below command)
    # GET /_cat/indices?help
    # s is used to sort the resulting output
    # bytes = b makes it return numeric bytes instead of human readable bytes
    # More information at https://www.elastic.co/guide/en/elasticsearch/reference/current/cat.html
    for index in es.cat.indices(format="json", h=("health","status","index","uuid","shardsPrimary","shardsReplica","docsCount","docsDeleted","storeSize","creation.date.string","creation.date","memory.total", "pri.store.size"), s="creation.date", bytes="b"):
        indices.append(index)
    es.close()
    return indices


def get_lowest_data_node_thread_count(client_config):
    es = build_es_connection(client_config)
    # Grabs the jvm section of GET /_nodes/stats
    result = es.nodes.stats(metric="jvm")
    # Set an arbitrary starter value
    safe_thread_use = 99999
    # Loop through each node's thread count
    for node in result['nodes']:
        # If the current nodes thread count is less than safe_thread_use
        # lower safe_thread use to that number
        if result['nodes'][node]['jvm']['threads']['count'] < safe_thread_use:
            safe_thread_use = result['nodes'][node]['jvm']['threads']['count']
    # Limit the number of usable threads to 100 or one third of safe_thread_use, whichever is lower
    if safe_thread_use / 2 >= 100:
        safe_thread_use = 100
    else:
        safe_thread_use = round(safe_thread_use / 3,0)
    es.close()
    return safe_thread_use

def get_newest_document_date_in_index(client_config, index, elastic_connection):
    body = '{"sort" : [{ "@timestamp" : {"order" : "desc", "mode": "max"}}], "size": 1}'
    try:
        result = elastic_connection.search(index=index, body=body)
        newest_record = get_es_field_from_first_result(result, '@timestamp')
        newest_record = datetime.strptime(newest_record, '%Y-%m-%dT%H:%M:%S.%fZ')
    except:
        e = sys.exc_info()[1]
        # If this point is reached, index does not have an @timestamp field
        # Fallback to index creation_date
        index = get_index_information_using_connection(client_config, index, elastic_connection)
        index_date = datetime.strptime(index['creation.date.string'], '%Y-%m-%dT%H:%M:%S.%fZ')
        return index_date
    return newest_record

def check_special_index(index):
    special = False
    if str(index).startswith("accounting"):
        special = True
    if str(index).startswith(".kibana"):
        special = True
    if str(index).startswith(".skedler"):
        special = True
    if str(index).startswith(".tasks"):
        special = True
    if str(index).startswith(".apm"):
        special = True
    if str(index).startswith("ilm"):
        special = True
    if str(index).startswith("readonlyrest"):
        special = True
    if str(index).startswith(".readonlyrest"):
        special = True
    if str(index).startswith("elastalert"):
        special = True
    return special
    

# This function will return the value of a specific field in the first result
# It assumes that you are passing the result from an ES search
def get_es_field_from_first_result(result, field):
    if 'hits' in result:
        if 'hits' in result['hits']:
            if '_source' in result['hits']['hits'][0]:
                value = result['hits']['hits'][0]['_source'][field]
                return value
            else:
                return False
        else:
            return False
    return False

def get_index_information(client, index):
    try:
        es = build_es_connection(client)
        indices = []
        # h is used to select fields to return (to see full list open Dev Tools and run the below command)
        # GET /_cat/indices?help
        # s is used to sort the resulting output
        # bytes = b makes it return numeric bytes instead of human readable bytes
        # More information at https://www.elastic.co/guide/en/elasticsearch/reference/current/cat.html
        for index in es.cat.indices(index=index, format="json", h=("health","status","index","uuid","shardsPrimary","shardsReplica","docsCount","docsDeleted","storeSize","creation.date.string","creation.date","memory.total", "pri.store.size"), s="creation.date", bytes="b"):
            indices.append(index)
        es.close()
        return indices[0]
    except:
        e = sys.exc_info()[0]
        print("Failed to get index information")
        print(e)

def get_index_information_using_connection(client, index, elastic_connection):
    try:
        indices = []
        # h is used to select fields to return (to see full list open Dev Tools and run the below command)
        # GET /_cat/indices?help
        # s is used to sort the resulting output
        # bytes = b makes it return numeric bytes instead of human readable bytes
        # More information at https://www.elastic.co/guide/en/elasticsearch/reference/current/cat.html
        indices = elastic_connection.cat.indices(index=index, format="json", h=("health","status","index","uuid","shardsPrimary","shardsReplica","docsCount","docsDeleted","storeSize","creation.date.string","creation.date","memory.total", "pri.store.size"), s="creation.date", bytes="b")
        return indices[0]
    except:
        e = sys.exc_info()[0]
        print("Failed to get index information")
        print(e)

# Take current index number and increase by 1
def get_rollover_index_name(current_index):
    current_index_number_portion = str(re.findall(r'\d+$', current_index)[0])
    current_index_number_portion_length = len(current_index_number_portion)
    index_prefix = current_index[0:-current_index_number_portion_length]
    current_index_number = int(re.findall(r'\d+', current_index_number_portion)[-1])
    next_index_number = str(current_index_number + 1)
    new_index = index_prefix + next_index_number.zfill(current_index_number_portion_length)
    return new_index

def rollover_index(client_config, index, alias):
    try:
        es = build_es_connection(client_config)
        indices = []
        # Check if index is a single string or a list of indices
        if isinstance(index, str):
            indices.append(index)
        if isinstance(index, list):
            indices = index
        for index in indices:
            new_index = get_rollover_index_name(index)
            status = es.indices.create(index=new_index, ignore=400)
            if 'acknowledged' in status:
                if status['acknowledged']:
                    # Update writeable index
                    status = es.indices.update_aliases({
                    "actions": [
                        { "remove":    { "index": index, "alias": alias }}, 
                        { "add": { "index": index, "alias": alias, "is_write_index": "false"  }}, 
                        { "add":    { "index": new_index, "alias": alias, "is_write_index": "true"  }}  
                    ]
                    })
                    return get_index_operation_message(index, "rollover", status, client_config)
            else:
                print("Failed to create new index" + str(new_index) + " for rollover index")
                return False
        es.close()
    except:
        e = sys.exc_info()
        print("Rollover job failed")
        print(e)
        return False

def rollover_index_with_connection(client_config, index, alias, elastic_connection):
    try:
        indices = []
        # Check if index is a single string or a list of indices
        if isinstance(index, str):
            indices.append(index)
        if isinstance(index, list):
            indices = index
        for index in indices:
            new_index = get_rollover_index_name(index)
            status = elastic_connection.indices.create(index=new_index, ignore=400)
            if 'acknowledged' in status:
                if status['acknowledged']:
                    # Update writeable index
                    status = elastic_connection.indices.update_aliases({
                    "actions": [
                        { "remove":    { "index": index, "alias": alias }}, 
                        { "add": { "index": index, "alias": alias, "is_write_index": "false"  }}, 
                        { "add":    { "index": new_index, "alias": alias, "is_write_index": "true"  }}  
                    ]
                    })
                    return status
            else:
                print("Failed to create new index" + str(new_index) + " for rollover index")
                return False
    except:
        e = sys.exc_info()
        print("Rollover job failed")
        print(e)
        return False
    
def get_list_by_chunk_size(original_list, batch_size):
    # looping till length equals batch_size
    for i in range(0, len(original_list), batch_size):  
        yield original_list[i:i + batch_size] 

def get_dictionary_by_chunk_size(input_dictionary, chunk_size=100):
    results = []
    items = input_dictionary.items()
    dict_size = len(items)

    if chunk_size >= dict_size:
        return input_dictionary

    for i in range(0, dict_size, chunk_size):
        start = i
        end = i + chunk_size
        sub_d = dict(item for item in items[start:end])
        results.append(sub_d)

    return results

def get_index_operation_message(index, operation, status, client_config):
    if check_acknowledged_true(status):
        print(operation.capitalize() + " successful for " + index)
        return True
    else:
        print(operation.capitalize() + " failed for " + index + " with a status of\n\n:" + str(status))
        settings = load_settings()
        if operation == "delete":
            policy = 'retention'
        if operation == "rollover":
            policy = 'rollover'
        if operation == 'forcemerge':
            policy = 'rollover'
        # Set fallback policy for notification settings
        if operation != 'delete' and operation != 'rollover' and operation != 'forcemerge':
            policy = 'retention'
        
        send_notification(client_config, operation.capitalize(), operation.capitalize() + " Failure", operation.capitalize() + " failed for " + index + " with a status of\n\n:" + str(status), teams=settings[policy]['ms-teams'], jira=settings[policy]['jira'])
        return False

def bulk_insert_data_to_es(elasticsearch_connection, data, index, bulk_size=100):
    try:
        batch_data = get_list_by_chunk_size(data, bulk_size)
        for batch in batch_data:
            count = 0
            actions = []
            while count <= len(batch) - 1:
                action = {
                    "_index": index,
                    "_source": {}
                }
                action["_source"] = batch[count]
                actions.append(action)
                count = count + 1
            helpers.bulk(elasticsearch_connection, actions)
        return True
    except:
        e = sys.exc_info()
        print("Bulk insertion job failed")
        print(e)
        return False

def delete_index(client_config, index):
    try:
        # Start connection to Elasticsearch
        es = build_es_connection(client_config)
        # Check if index is a single string or a list of indices
        if isinstance(index, str):
            indices = index
            # Delete the index
            status = es.indices.delete(index=index)
            get_index_operation_message(indices, "delete", status, client_config)
        if isinstance(index, list):
            # Convert list into chunks of 50
            # This will create a list of lists up to 50 indices per list
            chunks = get_list_by_chunk_size(index, 50)
            for chunk in chunks:
                indices = ",".join(chunk)
                # Delete the group of indices
                status = es.indices.delete(index=indices)
                get_index_operation_message(indices, "delete", status, client_config)
        # Close Elasticsearch connection
        es.close()
    except:
        e = sys.exc_info()
        print("Deletion job failed")
        settings = load_settings()
        send_notification(client_config, "retention", "Failed", "Deletion job failed for indices " + str(indices), teams=settings['retention']['ms-teams'], jira=settings['retention']['jira'])
        print(e)

def forcemerge_index(client_config, index):
    try:
        es = build_es_connection(client_config)
        status = es.indices.forcemerge(index=index, max_num_segments=1)
        return es.get_index_operation_message(index, "forcemerge", status, client_config)
    except:
        e = sys.exc_info()[1]
        if str(e).startswith("ConnectionTimeout caused by - ReadTimeoutError(HTTPSConnectionPool"):
            status = {}
            status['acknowledged'] = "true"
            return True
        else:
            print(e)
            return False

# Not currently working
def put_index_template(client_config, name, template):
    try:
        es = build_es_connection(client_config)
        status = es.indices.put_index_template(name, template)
        if check_acknowledged_true(status):
            return True
        else:
            print(status)
            return False
    except:
        e = sys.exc_info()
        print("Deletion job failed")
        print(e)
        return False

def check_acknowledged_true(status):
    if "acknowledged" in status:
        if type(status['acknowledged']) == bool:
            return status['acknowledged']
        else:
            if status['acknowledged'] == "true" or status['acknowledged'] == "True":
                return True
            else:
                print("extra")
                print(status['acknowledged'])
                return False
    else:
        print("Malformed status message")
        print(status)
        return False

# Connection built similar to https://elasticsearch-py.readthedocs.io/en/7.10.0/api.html#elasticsearch
# Had trouble with check_hostname set to True for some reason
def build_es_connection(client_config):
    settings = load_settings()
    es_config = {}
    try:
        # Check to see if SSL is enabled
        ssl_enabled = False
        if "ssl_enabled" in client_config:
            if client_config['ssl_enabled']:
                ssl_enabled = True
        else:
            ssl_enabled = settings['settings']['ssl_enabled']

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
                    ca_file = client_config['client_file_location'] + "/ca/ca.crt"
            
            if ca_file != "":
                context = ssl.create_default_context(
                            cafile=ca_file)
            else:
                context = ssl.create_default_context()

            if "check_hostname" in client_config:
                check_hostname = client_config['check_hostname']
            else:
                check_hostname = settings['settings']['check_hostname']
            if check_hostname:
                context.check_hostname = True
            else:
                context.check_hostname = False

            if "ssl_certificate" in client_config:
                ssl_certificate = client_config['ssl_certificate']    
            else:
                ssl_certificate = settings['settings']['ssl_certificate']
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
        return Elasticsearch(
            [{'host': es_host, 'port': es_port}], **es_config) 
    except:
        e = sys.exc_info()
        print("Connection attempt to Elasticsearch Failed")
        print(e)
        return False

def check_cluster_health(client_config):
    try:
        es = build_es_connection(client_config)
        return es.cluster.health()
    except:
        e = sys.exc_info()
        print("Connection attempt to Elasticsearch Failed")
        print(e)
        print("Failed to get cluster health")
        health = {}
        health['status'] = "Failed to connect to ES"
        return health

def check_cluster_health_status(client_config, color):
    health = check_cluster_health(client_config)
    check = False
    if color == "green" and health['status'] == 'green':
        print("Client " + client_config['client_name'] + " has a healthy cluster (" + health['status'] + ")")
        check = True
    if color == "yellow" and (health['status'] == 'green' or health['status'] == "yellow"):
        print("Client " + client_config['client_name'] + " has a healthy cluster (" + health['status'] + ")")
        check = True
    if not check:
        # If cluster health check fails or is red, log and do not process rollovers
        print("Client " + client_config['client_name'] + " has a unhealthy cluster (" + health['status'] + ")")
    return check

def get_retention_policy(client_config):
    if "policy" in client_config:
        if "retention" in client_config['policy']:
            index_retention_policies = client_config['policy']['retention']
        else:
            index_retention_policies = { "global": 3660 }
    else:
        index_retention_policies = { "global": 3660 }
    return index_retention_policies

def print_in_place(string):
    stdout.write("\r" + str(string))
    stdout.flush()

def write_file(file, contents):
    with open(file, mode='w') as new_file:
        new_file.write(contents)
    return True
    
def load_file(file):
    if os.path.exists(file):
        with open(file) as my_file:
            contents = my_file.read()
        return contents
    else:
        return("File does not exist")

def load_json_file(file):
    data = []
    if os.path.exists(file):
        for line in open(file, 'r'):
            data.append(json.loads(line))
        return data
    else:
        return("File does not exist")

if __name__ == '__main__':
    pass
