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

def run_custom_checks(manual_client):
    settings = load_settings()
    if settings['accounting']['enabled']:
        # Goal is to loop through each custom_check found in client_info.json
        # Run check as a subprocess - if exits cleanly you are done
        # If exit shows a failure, run the associated remdediate process/script
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
    parser = argparse.ArgumentParser(description='Used to manually run custom checks against a specific client (Example - custom_checks.py --client ha)', formatter_class=RawTextHelpFormatter)
    parser.add_argument("--client", default="", type=str, help="Set to a specific client name to limit the custom checks script to one client")
    parser.add_argument("--notification", default="True", type=str, help="Set to False to disable notifications")

    args = parser.parse_args()
    manual_client = args.client
    if args.notification == "True":
        notification = True
    else:
        notification = False

    run_custom_checks(manual_client)
