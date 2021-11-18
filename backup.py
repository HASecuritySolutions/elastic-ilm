#!/usr/bin/env python3
from config import load_configs, load_settings
from error import send_notification
import es
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import time
notification = False

settings = load_settings()

def validate_backup_repo_exists():
  if 'backup' in settings:
    if 'backup_repo' in settings['backup']:
      repo = settings['backup']['backup_repo']
    else:
      print("No backup repository defined in settings.toml")
      return False
  elastic_connection = es.build_es_connection(client_config)
  repositories = elastic_connection.cat.repositories(format='json')
  elastic_connection.close()
  for repository in repositories:
    if repo == repository['id']:
      return True
  print(f"Backup repository {repo} not registered")
  return False

def get_backup_policy(client_config):
  if "policy" in client_config:
    if "backup" in client_config['policy']:
      index_backup_policies = client_config['policy']['backup']
    else:
      index_backup_policies = { "global": 0 }
  else:
    index_backup_policies = { "global": 0 }
  return index_backup_policies

def validate_backup_enabled():
  if 'backup' in settings:
    if settings['backup']['enabled']:
      if 'health_check_level' in settings['backup']:
        return True
      else:
        return False
    else:
      return False
  else:
    return False

def apply_backup_retention_policies(client_config, backup_policy):
  elastic_connection = es.build_es_connection(client_config)
  snapshots = elastic_connection.snapshot.get(settings['backup']['backup_repo'], "*")
  for snapshot in snapshots['snapshots']:
    snapshot_name = snapshot['snapshot'][:-17]
    snapshot_age = datetime.strptime(snapshot['snapshot'][len(snapshot_name)+1:], '%Y-%m-%d_%H:%M')
    current_date = datetime.utcnow()
    days_ago = (current_date - snapshot_age).days
    # Get policy retention days from specific policy
    policy_days = backup_policy[snapshot_name]
    # Check if days_ago is greater than or equal to policy date
    # If greater than or equal to policy date, delete snapshot
    if days_ago >= policy_days:
      # Delete old snapshot
      delete_job = elastic_connection.snapshot.delete(settings['backup']['backup_repo'], snapshot=snapshot['snapshot'])
      elastic_connection.close()
      if 'accepted' in delete_job:
        if delete_job['accepted'] == True:
          return True

def take_snapshot_per_policies(client_config, backup_policy):
  retry_count = 60
  sleep_time = 60
  for index_name in backup_policy:
    success = 0
    if index_name != 'global':
      index = str(index) + "*"
    else:
      index = '*'
    body = {
      "indices": index,
      "ignore_unavailable": True,
      "include_global_state": True,
      "metadata": {
        "taken_by": "Elastic-ILM",
        "taken_because": "Scheduled backup per policy"
      }
    }
    if backup_policy[index_name] != 0:
      while retry_count >= 0 and success == 0:
        # Check cluster health - Expect Yellow to continue
        if es.check_cluster_health_status(client_config, settings['backup']['health_check_level']):
          elastic_connection = es.build_es_connection(client_config)
          current_date = datetime.strftime(datetime.utcnow(), '%Y-%m-%d_%H:%M')
          backup_job = elastic_connection.snapshot.create(settings['backup']['backup_repo'], snapshot=f"{index_name}_{current_date}", body=body, wait_for_completion=False)
          elastic_connection.close()
          if 'accepted' in backup_job:
            if backup_job['accepted'] == True:
              success = 1
        else:
          if retry_count > 0:
              print("Backup operation failed for " + client_config['client_name'] + ". Cluster health does not meet level:  " + settings['backup']['health_check_level'])
          else:
              message = "Backup operation failed.\n\nIt is also possible that connections are unable to be made to the client/nginx node. Please fix.\n\nRemember that in order for client's to be properly build you will need to get their cluster status to **Green** or **Yellow** and then re-run the following command:\n\n**python3 /opt/elastic-ilm/retention.py --client " + client_config['client_name'] + "**"
              send_notification(client_config, "backup", "Failed", message, teams=settings['backup']['ms-teams'], jira=settings['backup']['jira'])
        if success == 0:
          # Decrese retry count by one before trying while statement again
          retry_count = retry_count - 1
          print("Retry attempts left for backup operation set to " + str(retry_count) + " sleeping for " + str(sleep_time) + " seconds")
          time.sleep(sleep_time)

def run_backup(manual_client):
  clients = load_configs(manual_client)
  for client in clients:
    if settings['settings']['limit_to_client'] == client or settings['settings']['limit_to_client'] == "":
      client_config = clients[client]
      
      if validate_backup_enabled():
        backup_policy = get_backup_policy(client_config)
        apply_backup_retention_policies(client_config, backup_policy)
        take_snapshot_per_policies(client_config, backup_policy)

if __name__ == "__main__":
    import argparse
    from argparse import RawTextHelpFormatter
    parser = argparse.ArgumentParser(description='Used to manually run accounting against a specific client (Example - retention.py --client ha)', formatter_class=RawTextHelpFormatter)
    parser.add_argument("--client", default="", type=str, help="Set to a specific client name to limit the accounting script to one client")
    parser.add_argument("--notification", default="True", type=str, help="Set to False to disable notifications")

    args = parser.parse_args()
    manual_client = args.client
    if args.notification == "True":
        notification = True
    else:
        notification = False
    run_backup(manual_client)