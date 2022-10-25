"""Generates a SOC report based on a given time period"""
import sys
import os
import ssl
import json
from datetime import datetime, timedelta
import urllib3
from opensearch_dsl import Search, A
from opensearchpy import OpenSearch

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def build_es_connection(client_config, timeout=10):
    """Builds OpenSearch connection

    Args:
        client_config (dict): Dictionary of client configuration
        timeout (int, optional): OS Timeout. Defaults to 10.

    Raises:
        e: OpenSearch Error

    Returns:
        Object: OpenSearch connection class
    """
    es_config = {}
    try:
        # Check to see if SSL is enabled
        ssl_enabled = False
        if "ssl_enabled" in client_config:
            if client_config['ssl_enabled']:
                ssl_enabled = True

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
        print(sys.exc_info())
        print("Connection attempt to Elasticsearch Failed")
        raise error

def get_organization_uuid(client_name):
    """Converts a client name to organization uuid

    Args:
        client_name (str): Client Name

    Returns:
        str: Organization UUID
    """
    try:
        search = Search(using=os_connection, index="reflex-organizations") \
            .query("query_string", query='name:"' + client_name + '"')
        response = search.execute()
        return response[0].uuid
    except ConnectionError:
        print("Unable to retrieve uuid")


def get_number_of_alerts():
    """Returns the number of alerts for an organization

    Returns:
        str: Organization UUID
    """
    try:
        search = Search(using=os_connection, index="reflex-events") \
            .filter('range',
                    **{'created_at': {'gte': BEGIN_DATE, 'lte': END_DATE,
                                      "time_zone": "-00:00"}}) \
            .query("query_string", query='organization:"' + ORGANIZATION + '"')
        return search.count()
    except ConnectionError:
        print("Unable to get number of alerts")


def get_closer_codes():
    """Returns top closure codes
    """
    try:
        search = Search(using=os_connection, index="reflex-events") \
            .filter('range',
                    **{'created_at': {'gte': BEGIN_DATE, 'lte': END_DATE,
                                      "time_zone": "-00:00"}}) \
            .query("query_string", query='organization:"' + ORGANIZATION + '"')
        agg = A('terms', field='dismiss_reason.keyword')
        search.aggs.bucket('closure_codes', agg)
        response = search.execute()

        values = []
        for entry in response.aggregations.closure_codes.buckets:
            values.append({entry.key: entry.doc_count})
        if response.aggregations.closure_codes.sum_other_doc_count > 0:
            values.append(
                {"Other": response.aggregations.closure_codes.sum_other_doc_count})
        return values

    except ConnectionError:
        print("Unable to get closure codes")


def generate_soc_report():
    """Generates and sends the SOC report

    Args:
        tenant (str): UUID of Organization
    """
    print(f"Number of alerts: {get_number_of_alerts()}")
    print(get_closer_codes())

def load_configs():
    """Load all JSON configuration files

    Returns:
        dict: All JSON configuration files
    """
    configs = {}
    for file in os.listdir(config_folder):
        if file.endswith(".json"):
            with open(config_folder + '/' + file, encoding="UTF-8") as file:
                client = json.load(file)
            if 'client_name' in client:
                client_name = client['client_name']
            else:
                print("File name " + file + " does not contain valid client information")
                sys.exit(1)
            configs[client_name] = client
    return configs

def soc_report():
    """Run SOC report

    Args:
        manual_client (str): Name of client. Empty means all
    """

    # Add all clients initially to retry_list for first run
    for client, _ in clients.items():
        if 'reflex_tenant' in clients[client]:
            tenant = clients[client]['reflex_tenant']
        else:
            continue

        # execute for all clients
        if manual_client == "" or clients[client]['client_name'] == manual_client:
            # This is intended so that ORGANIZATION does not have to be passed from
            # module to module
            global ORGANIZATION
            ORGANIZATION = get_organization_uuid(tenant)
            print(f"Processing and sending SOC report for client {client} with "
                f"UTC date range of:\n{BEGIN_DATE} to {END_DATE}\n")
            # If client set at command line only run it otherwise
            generate_soc_report()


if __name__ == "__main__":
    import argparse
    from argparse import RawTextHelpFormatter
    parser = argparse.ArgumentParser(
        description='Used to manually run mssp against a specific client '
                    + '(Example - mssp.py --client ha)',
        formatter_class=RawTextHelpFormatter)
    parser.add_argument("--client", default="", type=str,
                        help="Set to a specific client name to limit the mssp script to one client")
    parser.add_argument("--config-folder", default="/opt/maintenance", type=str,
                        help="Set to the folder containing all your JSON configuration files")
    parser.add_argument("--notification", default="True",
                        type=str, help="Set to False to disable notifications")
    parser.add_argument("--till-now", default="False",
                        type=str, help="Set to True if the report should have an end date of now")
    parser.add_argument("--days-ago", default=0,
                        type=int, help="How many days back to run report")
    parser.add_argument("--weeks-ago", default=0,
                        type=int, help="How many days back to run report")
    parser.add_argument("--months-ago", default=0,
                        type=int, help="How many days back to run report")

    args = parser.parse_args()
    manual_client = args.client
    till_now = args.till_now
    days_ago = args.days_ago
    weeks_ago = args.weeks_ago
    months_ago = args.months_ago

    config_folder = args.config_folder

    clients = load_configs()
    reflex_config = clients['ha']

    os_connection = build_es_connection(reflex_config, timeout=30)

    DATE_COUNT = 0
    if days_ago > 0:
        DATE_COUNT = DATE_COUNT + 1
    if weeks_ago > 0:
        DATE_COUNT = DATE_COUNT + 1
    if months_ago > 0:
        DATE_COUNT = DATE_COUNT + 1

    if DATE_COUNT != 1:
        exit("You must select one and only one timeframe to generate a report")

    current_date = datetime.utcnow()

    if days_ago > 0:
        BEGIN_DATE = current_date - timedelta(days=days_ago + 1)
        BEGIN_DATE = BEGIN_DATE.replace(
            hour=0, minute=0, second=0, microsecond=0)
        END_DATE = current_date - timedelta(days=1)
        END_DATE = END_DATE.replace(
            hour=23, minute=59, second=59, microsecond=999999)
    if weeks_ago > 0:
        BEGIN_DATE = current_date - timedelta(weeks=weeks_ago)
        BEGIN_DATE = BEGIN_DATE - timedelta(days=1)
        BEGIN_DATE = BEGIN_DATE.replace(
            hour=0, minute=0, second=0, microsecond=0)
        END_DATE = current_date - timedelta(days=1)
        END_DATE = END_DATE.replace(
            hour=23, minute=59, second=59, microsecond=999999)
    if months_ago > 0:
        END_DATE = current_date.today().replace(
            day=1, hour=23, minute=59, second=59, microsecond=999999) \
            - timedelta(days=1)
        BEGIN_DATE = current_date.today().replace(
            day=1, hour=0, minute=0, second=0, microsecond=0) \
            - timedelta(days=END_DATE.day)
    if till_now == "True":
        END_DATE = datetime.utcnow()
    BEGIN_DATE = BEGIN_DATE.isoformat()
    END_DATE = END_DATE.isoformat()

    ORGANIZATION = None

    soc_report()

    os_connection.close()
