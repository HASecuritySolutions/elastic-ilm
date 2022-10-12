"""Generates a SOC report based on a given time period"""
from datetime import datetime, timedelta
from opensearch_dsl import Search, A
import es
from config import load_configs, load_settings


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


def soc_report(client):
    """Run SOC report

    Args:
        manual_client (str): Name of client. Empty means all
    """
    settings = load_settings()
    if "soc_report" in settings:
        if settings['soc_report']['enabled'] is False:
            return
    else:
        return

    # Add all clients initially to retry_list for first run
    for client, _ in clients.items():
        tenant = clients[client]['reflex_tenant']
        # This is intended so that ORGANIZATION does not have to be passed from
        # module to module
        global ORGANIZATION
        ORGANIZATION = get_organization_uuid(tenant)
        print(f"Processing and sending SOC report for client {client} with "
              f"UTC date range of:\n{BEGIN_DATE} to {END_DATE}\n")
        # If client set at command line only run it otherwise
        # execute for all clients
        if manual_client == "" or clients[client]['client_name'] == manual_client:
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

    clients = load_configs()
    reflex_config = clients['ha']

    os_connection = es.build_es_connection(reflex_config, timeout=30)

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

    soc_report(manual_client)

    os_connection.close()
