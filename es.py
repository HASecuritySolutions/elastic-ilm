#!/usr/bin/env python3
import urllib3
from sys import stdout
from datetime import datetime
from config import load_settings
import json
import re
import sys
from itertools import islice
import ssl
from error import send_jira_event, send_ms_teams_message, send_notification
import os
import requests
from requests.auth import HTTPBasicAuth
from config import load_settings, retry
settings = load_settings()
if os.getenv('ILM_PLATFORM') == 'opensearch':
    from opensearchpy import OpenSearch as Elasticsearch
    from opensearchpy import helpers
    from opensearch_dsl import Search
    from opensearchpy.connection import create_ssl_context
else:
    from elasticsearch import Elasticsearch
    from elasticsearch import helpers
    from elasticsearch_dsl import Search
    from elasticsearch.connection import create_ssl_context


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def build_search(es_connection, index, query, sort='@timestamp', limit_to_fields=[]):
    """[summary]

    Args:
            index ([string]): [Index pattern to search against]
            query ([string]): [Lucene query to limit results]
            sort (str, optional): [Sort filter]. Defaults to '@timestamp'.
            limit_to_fields (list, optional): [Limit which fields to return]. Defaults to [].

    Returns:
            [type]: [description]
    """
    search = Search(using=es_connection, index=index, doc_type='_doc')
    search = search.query('query_string', query=query)
    if len(limit_to_fields) != 0:
        search = search.source(limit_to_fields)
    search = search.sort(sort)
    return search


def build_aggregation(search, name, aggregation_type, filter, metric_name,
                      metric, metric_field, size=10):
    """[summary]

    Args:
            name ([string]): [Name of aggregation]
            aggregation_type ([string]): [Type of aggregation such as terms]
            filter ([string]): [Name of field or filter to apply aggregation with]
            metric_name ([string]): [Name to apply to metric]
            metric ([string]): [The type of metric being applied such as sum or avg]
            metric_field ([string]): [The field to apply the metric against]
            size (int, optional): [Max results to return]. Defaults to 10.

    Returns:
            [object]: [Returns Elasticsearch object with aggregation added]
    """
    search.aggs.bucket(name, aggregation_type, field=filter, size=size)
    search.aggs.metric(metric_name, metric, field=metric_field)
    return Search


def aggregate_search(es_connection, index_pattern, search_query, aggregation_type, aggregation_field, sort='@timestamp', limit_to_fields=[], date_start='now-1d/d', date_end='now', result_size=100, interval='auto'):
    s = Search(using=es_connection, index=index_pattern, doc_type='_doc')
    s = s.query('query_string', query=search_query)
    if len(limit_to_fields) != 0:
        s = s.source(limit_to_fields)
    s = s.sort(sort)
    if date_start != 'ignore':
        s = s.filter('range', **{sort: {'gte': date_start, 'lt': date_end}})
    s.aggs.bucket(aggregation_field, 'terms',
                  field=aggregation_field, size=result_size)
    if aggregation_type == 'date_histogram':
        s.aggs[aggregation_field].metric(
            'Count', aggregation_type, field=aggregation_field, interval=interval)
    elif aggregation_type == "cardinality":
        s.aggs.metric(
            'uniq_count', aggregation_type, field=aggregation_field)
    else:
        s.aggs[aggregation_field].metric(
            'Count', aggregation_type, field=aggregation_field)
    response = s.execute()
    if aggregation_type in ["terms", "auto_date_histogram", "date_histogram"]:
        data = [x for x in response.aggregations[aggregation_field].buckets]
        return_dict = {}
        for row in data:
            field = row['key']
            value = row['doc_count']
            return_dict[field] = value
        return return_dict
    else:
        if aggregation_type == "cardinality":
            return response.aggregations['uniq_count']
        else:
            return [x for x in response.aggregations[aggregation_field].buckets]

def get_unique_count(es_connection, index_pattern, field_name, date_start, date_end, sort='@timestamp'):
    search = Search(using=es_connection, index=index_pattern, doc_type='_doc')
    search = search.sort(sort)
    search = search.filter('range', **{sort: {'gte': date_start, 'lt': date_end}})
    search.aggs.metric('uniq_count', 'cardinality', field=field_name)
    search = search.execute()
    return search.aggregations.uniq_count.value


def multiple_aggregate_search(es_connection, index_pattern, search_query, aggregation_type, aggregation_field_one, aggregation_field_two, sort='@timestamp', limit_to_fields=[], date_start='now-1d/d', date_end='now'):
    s = Search(using=es_connection, index=index_pattern, doc_type='_doc')
    s = s.query('query_string', query=search_query)
    if len(limit_to_fields) != 0:
        s = s.source(limit_to_fields)
    s = s.sort(sort)
    s = s.filter(
        'range', **{'@timestamp': {'gte': date_start, 'lt': date_end}})
    # The four lines above could be summarized into the line below based on your preference:
    # s = Search(using=es_connection, index='lab4.1-complete', doc_type='_doc').query('query_string', query='tags:internal_source').source(['source_ip']).sort('source_ip')
    s.aggs.bucket(aggregation_field_one, 'terms', field=aggregation_field_one,
                  size=999999).metric('Count', aggregation_type, field=aggregation_field_one)
    s.aggs.bucket(aggregation_field_two, 'terms', field=aggregation_field_two,
                  size=999999).metric('Count', aggregation_type, field=aggregation_field_two)
    response = s.execute()
    aggregation_one = [x['key']
                       for x in response.aggregations[aggregation_field_one].buckets]
    aggregation_two = [x['key']
                       for x in response.aggregations[aggregation_field_two].buckets]
    return {aggregation_one[i]: aggregation_two[i] for i in range(len(aggregation_one))}
    return list(zip([x['key'] for x in response.aggregations[aggregation_field_one].buckets], [x['key'] for x in response.aggregations[aggregation_field_two].buckets]))


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


def check_index_forcemerge_policy(index, policies):
    match_found = 0
    # This sorts the index forcemerge policies in descending order,
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
    try:
        es = build_es_connection(client)
        members = es.cat.aliases(format="json")
    except Exception as e:
        raise e
    return members


def get_write_alias_names(client_config):
    aliases = get_all_index_aliases(client_config)
    alias_return = []
    for alias in aliases:
        if alias['is_write_index'] == 'true':
            alias_return.append(alias['alias'])
    return alias_return


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
        # "actions": [
        #    { "add":    { "index": "tweets_2", "alias": "tweets_search" }},
        #    { "remove": { "index": "tweets_1", "alias": "tweets_index"  }},
        #    { "add":    { "index": "tweets_2", "alias": "tweets_index"  }}
        # ]
        # })
        "actions": [
            {"add":    {"index": index, "alias": alias}}
        ]
    })
    es.close()

def get_index_group(index):
    if str(index).startswith('.ds-'):
        index = index[4:]
    # First, find and remove possible dates
    m = re.search('-20[0-9][0-9](\.|-|_|:)[0-9]{2}(\.|-|_|:)[0-9]{2}$', index)
    if m:
        #print(f"Found date of {m.group(0)} in index {index}")
        index = index.replace(str(m.group(0)), '')
    m = re.search('20[0-9][0-9](\.|-|_|:)[0-9]{2}(\.|-|_|:)[0-9]{2}-', index)
    if m:
        #print(f"Found date of {m.group(0)} in index {index}")
        index = index.replace(str(m.group(0)), '')

    # Next, remove number sequence if found at end (ex: -000001)
    m = re.search('-[0-9]{1,6}$', index)
    if m:
        #print(f"Found ending number sequence for index {index}")
        index = index.replace(str(m.group(0)), '')

    # m = re.search('^([a-zA-Z0-9-._]+)(-.*)?-20[0-9][0-9](\.|-)[0-9]{2}(\.|-)[0-9]{2}$', index)
    # if m:
    #     index_group = m.group(1)
    # else:
    #     m = re.search('^([a-zA-Z0-9-._]+)(-.*)?-20[0-9][0-9](\.|-)[0-9]{2}$', index)
    #     if m:
    #         index_group = m.group(1)
    #     else:
    #         m = re.search('^([a-zA-Z0-9-._]+)(-.*)?-20[0-9][0-9]$', index)
    #         if m:
    #             index_group = m.group(1)
    #         else:
    #             m = re.search('^([a-zA-Z0-9-._]+)(-.*)?-[0-9]{6,}$', index)
    #             if m:
    #                 index_group = m.group(1)
    #             else:
    #                 m = re.search('^([a-zA-Z0-9-._]+)(-.*)?-[a-zA-Z0-9-._]{3,}$', index)
    #                 if m:
    #                     index_group = m.group(1)
    #                 else:
    #                     index_group = index
    return index

# test = ['.ds-winlogbeat-ds-2022.08.04-000028', 'logstash-cisco', 'logstash-cisco-2022.08.04', 'logstash-cisco-2022.08.04-000001','.ds-winlogbeat-ds-2022-08-04-000028', 'logstash-cisco-2022-08-04', 'logstash-cisco-2022-08-04-000001','.ds-winlogbeat-ds-2022:08:04-000028', 'logstash-cisco-2022:08:04', 'logstash-cisco-2022:08:04-000001','.ds-winlogbeat-ds-2022_08_04-000028', 'logstash-cisco-2022_08_04', 'logstash-cisco-2022_08_04-000001']
# for test_item in test:
#     print(f"Initial index is {test_item}")
#     group = get_index_group(test_item)
#     print(f"Index group set to {group}")


def es_get_indices(client):
    es = build_es_connection(client)
    indices = []
    # h is used to select fields to return (to see full list open Dev Tools and run the below command)
    # GET /_cat/indices?help
    # s is used to sort the resulting output
    # bytes = b makes it return numeric bytes instead of human readable bytes
    # More information at https://www.elastic.co/guide/en/elasticsearch/reference/current/cat.html
    for index in es.cat.indices(format="json", h=("health", "status", "index", "uuid", "shardsPrimary", "shardsReplica", "docsCount", "docsDeleted", "storeSize", "creation.date.string", "creation.date", "memory.total", "pri.store.size"), s="creation.date", bytes="b"):
        indices.append(index)
    es.close()
    return indices

def es_get_index_stats(client, index_name):
    es = build_es_connection(client)
    indices = []
    # h is used to select fields to return (to see full list open Dev Tools and run the below command)
    # GET /_cat/indices?help
    # s is used to sort the resulting output
    # bytes = b makes it return numeric bytes instead of human readable bytes
    # More information at https://www.elastic.co/guide/en/elasticsearch/reference/current/cat.html
    stats = es.indices.stats(index=index_name)
    es.close()
    return stats

def es_get_highest_index_from_indices(client, indices):
    es = build_es_connection(client)
    indices = []
    # h is used to select fields to return (to see full list open Dev Tools and run the below command)
    # GET /_cat/indices?help
    # s is used to sort the resulting output
    # bytes = b makes it return numeric bytes instead of human readable bytes
    # More information at https://www.elastic.co/guide/en/elasticsearch/reference/current/cat.html
    for index in es.cat.indices(indices + "*", format="json", h=("health", "status", "index", "uuid", "shardsPrimary", "shardsReplica", "docsCount", "docsDeleted", "storeSize", "creation.date.string", "creation.date", "memory.total", "pri.store.size"), s="creation.date", bytes="b"):
        indices.append(index)
    es.close()
    for index in indices:
        number = re.match
        index_number = re.match('.*?([0-9]+)$', index['index']).group(1)
    max_index = ''
    return max_index


def get_data_streams(client):
    es = build_es_connection(client)
    response = es.indices.get_data_stream(name="*")
    es.close()
    return response


def es_get_data_stream_indices(client):
    es = build_es_connection(client)
    indices = []
    # h is used to select fields to return (to see full list open Dev Tools and run the below command)
    # GET /_cat/indices?help
    # s is used to sort the resulting output
    # bytes = b makes it return numeric bytes instead of human readable bytes
    # More information at https://www.elastic.co/guide/en/elasticsearch/reference/current/cat.html
    for index in es.cat.indices(".ds-*", format="json", h=("health", "status", "index", "uuid", "shardsPrimary", "shardsReplica", "docsCount", "docsDeleted", "storeSize", "creation.date.string", "creation.date", "memory.total", "pri.store.size"), s="creation.date", bytes="b"):
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
        safe_thread_use = round(safe_thread_use / 3, 0)
    es.close()
    return safe_thread_use


def get_newest_document_date_in_index(client_config, index, elastic_connection):
    body = '{"sort" : [{ "@timestamp" : {"order" : "desc", "mode": "max"}}], "size": 1}'
    try:
        result = elastic_connection.search(index=index, body=body)
        newest_record = get_es_field_from_first_result(result, '@timestamp')
        newest_record = datetime.strptime(
            newest_record, '%Y-%m-%dT%H:%M:%S.%fZ')
        if isinstance(newest_record, datetime):
            return newest_record
        else:
            raise "Index " + \
                str(index) + " time record is not a datetime field with value " + \
                str(newest_record)
    except:
        e = sys.exc_info()[1]
        # If this point is reached, index does not have an @timestamp field
        # Fallback to index creation_date
        index = get_index_information_using_connection(
            client_config, index, elastic_connection)
        index_date = datetime.strptime(
            index['creation.date.string'], '%Y-%m-%dT%H:%M:%S.%fZ')
        if isinstance(index_date, datetime):
            return index_date
        else:
            raise e

def check_special_index(index):
    special = False
    if str(index).startswith("accounting"):
        special = True
    if str(index).startswith("elastic-ilm"):
        special = True
    if str(index).startswith(".kibana"):
        special = True
    if str(index).startswith(".async"):
        special = True
    if str(index).startswith(".fleet"):
        special = True
    if str(index).startswith(".reporting"):
        special = True
    if str(index).startswith(".opensearch"):
        special = True
    if str(index).startswith(".opendistro"):
        special = True
    if str(index).startswith(".security"):
        special = True
    if str(index).startswith(".tasks"):
        special = True
    if str(index).startswith(".apm"):
        special = True
    if str(index).startswith("ilm"):
        special = True
    if str(index).startswith("readonlyrest"):
        special = True
    if str(index).startswith("reflex-"):
        special = True
    if str(index).startswith(".readonlyrest"):
        special = True
    if str(index).startswith(".signal"):
        special = True
    if str(index).startswith("elastalert"):
        special = True
    if str(index).startswith(".siem-signals"):
        special = True
    if str(index).startswith(".transform"):
        special = True
    if str(index).startswith(".transform-internal"):
        special = True
    if str(index).startswith(".") and not str(index).startswith(".monitoring") and not str(index).startswith(".ds-"):
        special = True
    return special


def return_fields_from_query(response, fields={}):
    output = {}
    for record in response['hits']['hits']:
        for field in record['_source']:
            if field in fields:
                output[field] = record['_source'][field]
    return output

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
        for index in es.cat.indices(index=index, format="json", h=("health", "status", "index", "uuid", "shardsPrimary", "shardsReplica", "docsCount", "docsDeleted", "storeSize", "creation.date.string", "creation.date", "memory.total", "pri.store.size"), s="creation.date", bytes="b"):
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
        indices = elastic_connection.cat.indices(index=index, format="json", h=("health", "status", "index", "uuid", "shardsPrimary", "shardsReplica",
                                                 "docsCount", "docsDeleted", "storeSize", "creation.date.string", "creation.date", "memory.total", "pri.store.size"), s="creation.date", bytes="b")
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
    current_index_number = int(re.findall(
        r'\d+', current_index_number_portion)[-1])
    next_index_number = str(current_index_number + 1)
    new_index = index_prefix + \
        next_index_number.zfill(current_index_number_portion_length)
    if new_index[0:4] == '.ds-':
        new_index = new_index[4:]
    return new_index


def rollover_index(client_config, index, alias):
    try:
        indices = []
        # Check if index is a single string or a list of indices
        if isinstance(index, str):
            indices.append(index)
        if isinstance(index, list):
            indices = index
        for index in indices:
            if 'ca_file' in client_config:
                if client_config['ca_file'] != "":
                    verify_check = client_config['ca_file']
                else:
                    verify_check = False
            else:
                verify_check = False
            url = f"https://client:9200/{alias}/_rollover"
            if client_config['platform'] == 'opensearch':
                auth_user = 'admin'
            else:
                auth_user = 'elastic'
            response = requests.post(
                url,
                verify=verify_check,
                auth=HTTPBasicAuth(
                    auth_user, client_config['password']['admin_password']),
                json={}
            )
            if response.status_code == 200:
                get_index_operation_message_http_request(
                    index, "rollover", response.status_code, client_config)
                return True
            else:
                print(
                    f"Failed to rollover index {index} for rollover index with alias/ds of {alias} and url of {url}")
                return False
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
            status = elastic_connection.indices.create(
                index=new_index, ignore=400)
            if 'acknowledged' in status:
                if status['acknowledged']:
                    # Update writeable index
                    status = elastic_connection.indices.update_aliases({
                        "actions": [
                            {"remove":    {"index": index, "alias": alias}},
                            {"add": {"index": index, "alias": alias,
                                     "is_write_index": "false"}},
                            {"add":    {"index": new_index,
                                        "alias": alias, "is_write_index": "true"}}
                        ]
                    })
                    return status
            else:
                print("Failed to create new index" +
                      str(new_index) + " for rollover index")
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
        print(operation.capitalize() + " failed for " +
              index + " with a status of\n\n:" + str(status))
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

        send_notification(client_config, operation.capitalize(), operation.capitalize() + " Failure", operation.capitalize() + " failed for " +
                          index + " with a status of\n\n:" + str(status), teams=settings[policy]['ms-teams'], jira=settings[policy]['jira'])
        return False


def get_index_operation_message_http_request(index, operation, status_code, client_config):
    if status_code == 200:
        print(operation.capitalize() + " successful for " + index)
        return True
    else:
        print(operation.capitalize() + " failed for " + index +
              " with a status of\n\n:" + str(status_code))
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

        send_notification(client_config, operation.capitalize(), operation.capitalize() + " Failure", operation.capitalize() + " failed for " +
                          index + " with a status of\n\n:" + str(status_code), teams=settings[policy]['ms-teams'], jira=settings[policy]['jira'])
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
            # Delete the index
            status = es.indices.delete(index=index)
            return get_index_operation_message(index, "delete", status, client_config)
        else:
            if isinstance(index, list):
                # Convert list into chunks of 50
                # This will create a list of lists up to 50 indices per list
                chunks = get_list_by_chunk_size(index, 50)
                for chunk in chunks:
                    indices = ",".join(chunk)
                    # Delete the group of indices
                    status = es.indices.delete(index=indices)
                    return get_index_operation_message(indices, "delete", status, client_config)
        # Close Elasticsearch connection
        es.close()
    except:
        e = sys.exc_info()
        print(e)
        print("Deletion job failed")
        settings = load_settings()
        send_notification(client_config, "retention", "Failed", "Deletion job failed for indices " +
                          str(indices), teams=settings['retention']['ms-teams'], jira=settings['retention']['jira'])
        print(e)


def forcemerge_index(client_config, index):
    try:
        es = build_es_connection(client_config)
        status = es.indices.forcemerge(index=index, max_num_segments=1)
        return es.get_index_operation_message(index, "forcemerge", status, client_config)
    except:
        e = sys.exc_info()[1]
        print(e)
        if str(e).startswith("ConnectionTimeout caused by - ReadTimeoutError(HTTPSConnectionPool"):
            status = {}
            status['acknowledged'] = "true"
            return True
        else:
            raise Exception(e)

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


def build_es_connection(client_config, timeout=10):
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
                    ca_file = client_config['client_file_location'] + \
                        "/ca/ca.crt"

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

        es_config['retry_on_timeout'] = True
        es_config['max_retries'] = 10
        es_config['timeout'] = timeout
        if os.getenv('DEBUGON') == "1":
            print(es_config)
            print(es_host)
            print(es_port)
        return Elasticsearch(
            [{'host': es_host, 'port': es_port}], **es_config)
    except:
        e = sys.exc_info()
        print(e)
        print("Connection attempt to Elasticsearch Failed")
        raise e


def check_cluster_health(client_config):
    try:
        es = build_es_connection(client_config)
        health = es.cluster.health(request_timeout=30)
        es.close()
        return health
    except:
        e = sys.exc_info()
        print(e)
        es.close()
        raise Exception(e)


def restore_index(client_config, backup_repository, snapshot_name, index_name):
    """[summary]
    Restores indices from a snapshot

    Args:
        client_config ([dict]): [Client configuration]
        backup_repository ([str]): [ES/OS repository name]
        snapshot_name ([str]): [Name of snapshot to restore from]
        body ([dict]): [Details for backup job]
        index_name (str): [Name of index to restore]

    Raises:
        Exception: [If error, print error and retry]

    Returns:
        [bool]: [Backup restore status]
    """
    try:
        if check_cluster_health_status(client_config, 'yellow'):
            print("Cluster health check passed")
    except Exception as e:
        raise Exception(e)
    try:
        elastic_connection = build_es_connection(client_config)
        body = {
            "indices": index_name,
            "ignore_unavailable": True,
            "include_global_state": False,
            "include_aliases": False,
            "ignore_index_settings": "archived,archived.index.lifecycle,archived,archived.index.lifecycle.indexing_complete,archived.index.lifecycle.name,archived.index.lifecycle.rollover_alias,archived.index.lifecycle.index_complete,archived.index.frozen,index.lifecycle,index.lifecycle.name,index.lifecycle.rollover_alias,index.lifecycle.index_complete,index.lifecycle.indexing_complete,index.frozen,index.routing.allocation.require.box_type"
        }
        restore_job = elastic_connection.snapshot.restore(
            backup_repository, snapshot_name, body, wait_for_completion=False, request_timeout=30)
        elastic_connection.close()
        if 'accepted' in restore_job:
            if restore_job['accepted']:
                print("Restore of index " + index_name + ": successful")
                return True
            else:
                print("Restore of index " + index_name + ": failed")
                return False
    except Exception as e:
        elastic_connection.close()
        print(e)
        print("Operation failed - Restore snapshot " + snapshot_name +
              " for repo " + backup_repository + " for index name of : " + index_name)
        raise Exception(e)


def run_search(es_connection, index, query, sort='@timestamp', limit_to_fields=[], size=10):
    """[summary]

    Args:
            index ([string]): [Index pattern to search against]
            query ([string]): [Lucene query to limit results]
            sort (str, optional): [Sort filter]. Defaults to '@timestamp'.
            limit_to_fields (list, optional): [Limit which fields to return]. Defaults to [].

    Returns:
            [type]: [description]
    """

    search = es_connection.search(
        index=index, doc_type='_doc', q=query, _source_includes=limit_to_fields, sort=sort, size=size)
    return search

def run_search_dsl(es_connection, index, dsl, sort='@timestamp', limit_to_fields=[], size=10):
    """[summary]

    Args:
            index ([string]): [Index pattern to search against]
            query ([string]): [Lucene query to limit results]
            sort (str, optional): [Sort filter]. Defaults to '@timestamp'.
            limit_to_fields (list, optional): [Limit which fields to return]. Defaults to [].

    Returns:
            [type]: [description]
    """
    if len(limit_to_fields) != 0:
        search = es_connection.search(
            index=index, doc_type='_doc', body=dsl, _source_includes=limit_to_fields, sort=sort, size=size)
    else:
        search = es_connection.search(
            index=index, doc_type='_doc', body=dsl, sort=sort, size=size)
    return search

def return_fields_from_query(response, fields={}):
    output = []
    for record in response['hits']['hits']:
        row = {}
        for field in record['_source']:
            if field in fields:
                row[field] = record['_source'][field]
        output.append(row)
    return output


def return_field_mapped_to_value_from_query(response, field_name, value_name):
    output = {}
    for record in response['hits']['hits']:
        key = ''
        value = ''
        for field in record['_source']:
            if field_name == field:
                key = record['_source'][field]
            if value_name == field:
                value = record['_source'][field]
            if key != '' and value != '':
                output[key] = value
                key = ''
                value = ''
    return output


def check_cluster_health_status(client_config, color):
    health = check_cluster_health(client_config)
    check = False
    if color == "green" and health['status'] == 'green':
        print("Client " + client_config['client_name'] +
              " has a healthy cluster (" + health['status'] + ")")
        check = True
    if color == "yellow" and (health['status'] == 'green' or health['status'] == "yellow"):
        print("Client " + client_config['client_name'] +
              " has a healthy cluster (" + health['status'] + ")")
        check = True
    if color == "red" and (health['status'] == 'green' or health['status'] == "yellow" or health['status'] == "red"):
        print("Client " + client_config['client_name'] +
              " has a cluster status of " + health['status'])
        check = True
    if not check:
        # If cluster health check fails or is red, log and do not process rollovers
        print("Client " + client_config['client_name'] +
              " has a unhealthy cluster (" + health['status'] + ")")
    return check


def get_retention_policy(client_config):
    if "policy" in client_config:
        if "retention" in client_config['policy']:
            index_retention_policies = client_config['policy']['retention']
        else:
            index_retention_policies = {"global": 3660}
    else:
        index_retention_policies = {"global": 3660}
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
