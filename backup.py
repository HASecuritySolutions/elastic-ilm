#!/usr/bin/env python3
from dateutil import parser as dateparser
import json
from datetime import datetime
import es
from config import load_configs, load_settings, retry
from error import send_notification
import os
import re
# Test set environment as opensearch
os.environ["ILM_PLATFORM"] = "opensearch"
notification = False
DEBUG_ENABLED = os.getenv('DEBUG_ENABLED')

settings = load_settings()

# Used when .kibana or special in backup policy
special_indices_to_backup = [".kibana", ".opendistro", ".opensearch"]


@retry(Exception, tries=3, delay=10)
def validate_backup_repo_exists(client_config, repository):
    """[summary]
    Validates backup repository exists in ES/OS

    Args:
        client_config ([dict]): [Client configuration]
        repository ([str]): [Repository to verify exists]

    Raises:
        Exception: [On error, print error and retry]

    Returns:
        [bool]: [Does backup repository exist]
    """
    try:
        elastic_connection = es.build_es_connection(client_config)
        repositories = elastic_connection.cat.repositories(format='json')
        elastic_connection.close()
        for record in repositories:
            if repository == record['id']:
                print(
                    f"Backup repository {repository} exists and is registered")
                return True
    except Exception as e:
        elastic_connection.close()
        print("Operation failed - Validate backup repo exists")
        raise Exception(e)
    # If it makes it this far the repo does not exist, fail
    print(f"Backup repository {repository} not registered")
    return False


def get_backup_policy(client_config, repository):
    """[summary]
    Extracts backup policy from client configuration

    Args:
        client_config ([dict]): [Client configuration]
        repository ([str]): [Name of repository]

    Returns:
        [dict]: [Dictionary containing specific repo policy]
    """
    if "policy" in client_config:
        if "backup" in client_config['policy']:
            index_backup_policies = client_config['policy']['backup'][repository]
    return index_backup_policies


def get_repositories(client_config):
    repositories = []
    if "policy" in client_config:
        if "backup" in client_config['policy']:
            for repository in client_config['policy']['backup']:
                repositories.append(repository)
    return repositories


def validate_backup_enabled(client_config):
    """[summary]
    Validates backup configuration found in client_info.json

    Returns:
        [bool]: [Does backup section exist]
    """
    if 'policy' in client_config:
        if 'backup' in client_config['policy']:
            return True
        else:
            return False
    return False


@retry(Exception, tries=3, delay=10)
def get_snapshots_in_repository(client_config, repository):
    """[summary]
    Gets all snapshots from backup repository

    Args:
        client_config ([dict]): [Client configuration]
        repository ([str]): [Backup repository name]

    Raises:
        Exception: [On error, print error and retry]

    Returns:
        [dict]: [Dictionary of all snapshot information]
    """
    elastic_connection = es.build_es_connection(client_config)
    snapshots = {'snapshots': []}
    try:
        snapshots = elastic_connection.snapshot.get(repository, '_all')
        elastic_connection.close()
    except Exception as e:
        elastic_connection.close()
        print("Operation failed - Get snapshots from " + repository)
        raise Exception(e)
    elastic_connection.close()
    return snapshots


@retry(Exception, tries=3, delay=10)
def delete_snapshot_in_repository(client_config, repository, snapshot):
    """[summary]
    Deletes a snapshot from a backup repository

    Args:
        client_config ([dict]): [Client configuration]
        repository ([str]): [Backup repository]
        snapshot ([str]): [Snapshot full name]

    Raises:
        Exception: [On error, print error and retry]

    Return:
        ([bool]): [Did snapshot get removed]
    """
    elastic_connection = es.build_es_connection(client_config)
    try:
        delete_status = elastic_connection.snapshot.delete(
            repository, snapshot=snapshot)
        elastic_connection.close()
        if 'acknowledged' in delete_status:
            if delete_status['acknowledged'] == True:
                print("Snapshot " + snapshot + " deleted successfully")
                return True
            else:
                print("Snapshot " + snapshot + " failed to delete successfully")
                return False
    except Exception as e:
        elastic_connection.close()
        print("Operation failed - Delete snapshot " +
              snapshot + " from " + repository)
        raise Exception(e)
    return False


def build_snapshot_info(snapshot):
    """[summary]
    Converts ES/OS snapshot information into specific information for processing

    Args:
        snapshot ([dict]): [Snapshot information from ES/OS]

    Returns:
        [dict]: [Returns dictionary with short_name, name, and days_ago]
    """
    snap_info = {}
    snap_info['name'] = snapshot['snapshot']
    if re.match('special_[0-9]{4}-[0-9]{2}-[0-9]{2}_[0-9]{2}:[0-9]{2}:[0-9]{2}', snapshot['snapshot']):
        snap_info['short_name'] = snapshot['snapshot'][:-20]
    else:
        snap_info['short_name'] = snapshot['snapshot']
    if DEBUG_ENABLED == "1":
        print(snap_info['short_name'])
    try:
        snapshot_age = datetime.strptime(
            snapshot['snapshot'][len(snap_info['short_name'])+1:], '%Y-%m-%d_%H:%M:S')
        current_date = datetime.utcnow()
        snap_info['days_ago'] = (current_date - snapshot_age).days
        return snap_info
    except:
        try:
            snapshot_age = datetime.strptime(
                snapshot['snapshot'][len(snap_info['short_name'])+1:], '%Y-%m-%d_%H:%M')
            current_date = datetime.utcnow()
            snap_info['days_ago'] = (current_date - snapshot_age).days
            return snap_info
        except:
            return snap_info


def apply_backup_retention_policies(client_config, job, retention, repository):
    """[summary]
    Deletes snapshots older than backup policy retention

    Args:
        client_config ([dict]): [Client configuration]
        job ([str]): [Name of index to process such as winlogbeat]
        retention ([int]): [How many days to retain snapshot]
        repository ([str]): [Name of backup repository]
    """
    snapshots = get_snapshots_in_repository(client_config, repository)
    for snapshot in snapshots['snapshots']:
        snapshot_info = build_snapshot_info(snapshot)
        if snapshot_info['short_name'] == job:
            if DEBUG_ENABLED == "1":
                print("Snapshot " + snapshot_info['name'] + " is " + str(
                    snapshot_info['days_ago']) + " days old compared to policy of " + str(retention))
            # Check if days_ago is greater than or equal to policy date
            # If greater than or equal to policy date, delete snapshot
            if 'days_ago' in snapshot_info:
                if snapshot_info['days_ago'] >= retention:
                    print("Attempting to delete snapshot " +
                          snapshot_info['name'])
                    # Delete old snapshot
                    if not delete_snapshot_in_repository(client_config, repository, snapshot_info['name']):
                        # Should not hit this point unless retry failed for an hour
                        message = "Backup snapshot removal failed for " + \
                            client_config['client_name'] + " for " + \
                            job + " in repository " + repository
                        print(message)
                        send_notification(client_config, "backup", "Failed", message,
                                          teams=settings['backup']['ms-teams'], jira=settings['backup']['jira'])


def take_snapshot(client_config, repository, snapshot, body):
    """[summary]
    Creates a backup snapshot

    Args:
        client_config ([dict]): [Client configuration]
        repository ([str]): [ES/OS repository name]
        snapshot ([str]): [Name of snapshot to create]
        body ([dict]): [Details for backup job]

    Raises:
        Exception: [If error, print error and retry]

    Returns:
        [bool]: [Backup job status]
    """
    try:
        if es.check_cluster_health_status(client_config, 'yellow'):
            print("Cluster health check passed")
    except Exception as e:
        raise Exception(e)

    try:
        current_date = datetime.strftime(
            datetime.utcnow(), '%Y-%m-%d_%H:%M:%S')
        snapshot_name = f"{snapshot}_{current_date}"
        if DEBUG_ENABLED == "1":
            print(f"Triggering backup for {snapshot_name}*")
            print("Repository is " + repository +
                  "| snapshot is " + snapshot_name + " | body is:")
            print(json.dumps(body))
        elastic_connection = es.build_es_connection(client_config)
        backup_job = elastic_connection.snapshot.create(
            repository, snapshot_name, body, wait_for_completion=False, request_timeout=30)
        elastic_connection.close()
        if 'accepted' in backup_job:
            if backup_job['accepted']:
                return True
            else:
                print("Backup snapshot " + snapshot_name + " failed to create")
                return False

    except Exception as e:
        elastic_connection.close()
        print("Operation failed - Create snapshot " +
              snapshot + " for repo " + repository)
        raise Exception(e)


@retry(Exception, tries=3, delay=10)
def get_indices_within_limit_age(client_config, indices, limit_age):
    """[summary]
    Takes a list of indices and looks to see if the most recent document
    is within a specified @timestamp age based on limit_age

    Args:
        client_config ([dict]): [Client configuration]
        indices ([list]): [List of indices to look through]
        limit_age ([int]): [Age in terms of within X days ago]

    Raises:
        Exception: [If error, print and retry]

    Returns:
        [list]: [List of indices that were within limit_age]
    """
    limit_age = limit_age * 86400
    current_date = datetime.utcnow()
    indices_within_limit_age = []
    body = '{"aggs": {"indices": {"terms": {"field": "_index","order": {"1": "desc"},"size": 50000},"aggs": {"1": {"max": {"field": "@timestamp"}}}}},"size": 0,"_source": {"excludes": []}}'

    for index in indices:
        elastic_connection = es.build_es_connection(client_config)
        try:
            if DEBUG_ENABLED == "1":
                print("Index is " + index)
                print(f"Limit age is {limit_age}\nBody is\n{body}")
            result = elastic_connection.search(index=index + "*", body=body)
            elastic_connection.close()
            if DEBUG_ENABLED == "1":
                print(result)
            for index in result['aggregations']['indices']['buckets']:
                index_name = index['key']
                index_date = dateparser.parse(
                    index['1']['value_as_string']).replace(tzinfo=None)
                seconds_ago = (current_date - index_date).total_seconds()
                if DEBUG_ENABLED == "1":
                    print(f"Index name is {index_name}")
                    print(f"Policy {limit_age} vs index {seconds_ago}")
                if seconds_ago <= limit_age:
                    indices_within_limit_age.append(index_name)
        except Exception as e:
            elastic_connection.close()
            raise Exception(e)
    return indices_within_limit_age


def modify_indices_to_string(indices, wildcard=False):
    """[summary]
    Converts a list of indices into a string for search

    Example: ['winlogbeat-000001','winlogbeat-000002'] becomes:
    'winlogbeat-000001,winlogbeat-000002'

    Args:
        indices ([list]): [One or more indices in a list]
        wildcard ([bool]): [Append wildcard yes/no]

    Returns:
        [str]: [String version of indices for search]
    """
    index = ''
    for entry in indices:
        if wildcard:
            index += entry + "*,"
        else:
            index += entry + ","
    index = index[0:-1]
    return index


def take_snapshot_per_policies(client_config, job, backup_policy, repository, include_special=False):
    # Global backup grabs all indices
    if job == 'global':
        indices = es.get_write_alias_names(client_config)
        if 'limit_age' in backup_policy:
            indices = get_indices_within_limit_age(
                client_config, indices, backup_policy['limit_age'])
            index = modify_indices_to_string(
                indices) + "," + modify_indices_to_string(special_indices_to_backup, wildcard=True)
        else:
            index = '*'
    # .kibana backs up global state and .kibana indices
    elif job == '.kibana' or job == "special":
        index = modify_indices_to_string(
            special_indices_to_backup, wildcard=True)
    else:
        if 'limit_age' in backup_policy:
            backup_policy = {"retention": 1, "limit_age": 1}
            indices = get_indices_within_limit_age(
                client_config, [job], backup_policy['limit_age'])
            if include_special:
                index = modify_indices_to_string(
                    indices) + "," + modify_indices_to_string(special_indices_to_backup, wildcard=True)
            else:
                index = modify_indices_to_string(indices)
        else:
            index = str(job) + "*"

    body = {
        "indices": index,
        "ignore_unavailable": True,
        "include_global_state": True,
        "metadata": {
            "taken_by": "Elastic-ILM",
            "taken_because": "Scheduled backup per policy"
        }
    }
    if backup_policy[job] != 0:
        backup_job = take_snapshot(client_config, repository, job, body)
        if backup_job:
            print(f"Backup for {job} completed successfully")
        else:
            # Should not hit this point unless retry failed for an hour
            message = "Backup take snapshot failed for " + \
                client_config['client_name'] + " for " + \
                job + " in repository " + repository
            print(message)
            send_notification(client_config, "backup", "Failed", message,
                              teams=settings['backup']['ms-teams'], jira=settings['backup']['jira'])


def run_backup(manual_client=""):
    """[summary]
    Runs backup job for specific client configuration

    Args:
        manual_client ([dict]): [Client configuration for one client]
    """
    clients = load_configs(manual_client)
    for client in clients:
        if settings['settings']['limit_to_client'] == client or settings['settings']['limit_to_client'] == "":
            client_config = clients[client]

            if validate_backup_enabled(client_config):
                # Get repositories listed in backup policy section
                repositories = get_repositories(client_config)
                # Loop through each repository to process backups
                for repository in repositories:
                    if validate_backup_repo_exists(client_config, repository):
                        # Get Backup policy for each repository
                        backup_policy = get_backup_policy(
                            client_config, repository)
                        # Loop through each backup job found in policy
                        for job in backup_policy:
                            if backup_policy[job] != 0:
                                print(
                                    f"Processing backups for repository {repository} with job of {job}")
                                apply_backup_retention_policies(
                                    client_config, job, backup_policy[job], repository)
                                if 'include_special' in backup_policy:
                                    take_snapshot_per_policies(
                                        client_config, job, backup_policy[job], repository, include_special=backup_policy[job]['include_special'])
                                else:
                                    take_snapshot_per_policies(
                                        client_config, job, backup_policy, repository)
                    else:
                        print(f"Backup repo not found - {repository}")
            else:
                print("Backups not enabled")


if __name__ == "__main__":
    import argparse
    from argparse import RawTextHelpFormatter
    parser = argparse.ArgumentParser(
        description='Used to manually run accounting against a specific client (Example - retention.py --client ha)', formatter_class=RawTextHelpFormatter)
    parser.add_argument("--client", default="", type=str,
                        help="Set to a specific client name to limit the accounting script to one client")
    parser.add_argument("--notification", default="True",
                        type=str, help="Set to enable notifications")
    parser.add_argument("--debug", default="True",
                        type=str, help="Set enable debug")

    args = parser.parse_args()
    manual_client = args.client

    if args.notification == "True":
        notification = True
    else:
        notification = False
    if args.debug == "True":
        DEBUG_ENABLED = "1"
    run_backup(manual_client)
