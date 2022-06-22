"""Generates an audit report for MSSP services"""
#!/usr/bin/env python3
import re
from datetime import datetime, timedelta
import es
from config import load_configs, load_settings
from error import send_email


def valid_uuid(uuid):
    """Returns True if str is a UUID

    Args:
        uuid (str): UUID string

    Returns:
        bool: True or False
    """
    regex = re.compile(
        r'^[a-f0-9]{8}-?[a-f0-9]{4}-?4[a-f0-9]{3}-?[89ab][a-f0-9]{3}-?[a-f0-9]{12}\Z', re.I
    )
    match = regex.match(uuid)
    return bool(match)


def get_audit_trail(
    es_connection,
    start_time,
    uuid,
    aggregation_field,
    result_size=10,
    end_time="now"
):
    """Aggregates events from ES for mssp events

    Args:
        es_connection (object): Opensearch/Elastic connection
        start_time (str): Time start in string format
        uuid (str): uuid4 string
        aggregation_field (str): Field to aggregate on

    Returns:
        dict: Aggregation results
    """
    query = f"organization:{uuid} AND (status.name.keyword:Dismissed OR status.name.keyword:Closed)"
    results = es.aggregate_search(
        es_connection,
        'reflex-events',
        query,
        'terms',
        aggregation_field,
        sort='created_at',
        date_start=start_time,
        date_end=end_time,
        result_size=result_size
    )
    return results


def get_start_time():
    """Builds a start time for search based on 8x5 working day
    """
    if datetime.today().strftime('%A') == "Monday":
        number_days_ago = 3
    else:
        number_days_ago = 1
    today = datetime.now()
    n_days_ago = today - timedelta(days=number_days_ago)
    n_days_ago = datetime.combine(n_days_ago, datetime.min.time())
    return n_days_ago.isoformat()


def get_end_time():
    """Builds a end time for search based on 8x5 working day
    """
    number_days_ago = 1
    today = datetime.now()
    n_days_ago = today - timedelta(days=number_days_ago)
    n_days_ago = datetime.combine(n_days_ago, datetime.max.time())
    return n_days_ago.isoformat()


def calculate_audit_trail(client_config, settings):
    """Calculates an audit trail record for MSSP clients

    Args:
        client_config (dict): Client configuration file loaded from json content
        settings (dict): Settings loaded from settings.toml
    """
    es_connection = es.build_es_connection(client_config)
    response = es.run_search(
        es_connection,
        'reflex-organizations',
        '_exists_:name',
        sort="created_at",
        limit_to_fields=['name', 'uuid'],
        size=10000
    )
    organizations = es.return_field_mapped_to_value_from_query(
        response,
        'name',
        'uuid'
    )
    start_time = get_start_time()
    end_time = get_end_time()
    for client_name, value in settings['mssp']['clients'].items():
        tenant_name = settings['mssp']['clients'][client_name]['tenant_name']
        print(
            f"Processing tenant for {client_name} with tenant name of {tenant_name}")
        email = value['email']
        uuid = organizations[tenant_name]
        get_audit_trail(es_connection, start_time, uuid,
                        "severity", end_time=end_time)
        report = f"Audit Trail Report for {tenant_name}\r\n\r\n"
        report = report + "This report represents events reviewed by H and A Security Solutions" \
            + " LLC during daily event review activities. The report includes a breakdown of" \
            + " events that have completed review.\r\n\r\nEvent statuses:\r\n\r\n"
        total_events = 0
        dismissed_events = get_audit_trail(
            es_connection,
            start_time,
            uuid,
            "dismiss_reason.keyword",
            end_time=end_time
        )
        for name, _ in dismissed_events.items():
            if valid_uuid(name):
                dismiss_reason = "Other"
            else:
                dismiss_reason = name
            report = report + \
                f"{dismiss_reason} - Number of events {dismissed_events[name]}\r\n"
            total_events = total_events + dismissed_events[name]
        report = report + \
            f"\r\nTotal Events Reviewed: {total_events}\r\n\r\nTop Rules\r\n\r\n"
        total_events = 0
        top_rules = get_audit_trail(
            es_connection, start_time, uuid, "title", end_time=end_time)
        for rule, _ in top_rules.items():
            report = report + \
                f"{rule} - Number of events: {top_rules[rule]}\r\n"
        top_sources = get_audit_trail(
            es_connection,
            start_time,
            uuid,
            "source.keyword",
            end_time=end_time
        )
        report = report + "\r\nTop Sources\r\n\r\n"
        for source, _ in top_sources.items():
            report = report + \
                f"{source} - Number of events {top_sources[source]}\r\n"

        for address in list(email):
            send_email(address, "Audit Trail Report", report)
        print(report)


def run_mssp():
    """Runs all selected MSSP audits

    Args:
        manual_client (str): Name of client. Empty means all
    """
    settings = load_settings()
    if "mssp" in settings:
        if settings['mssp']['enabled']:
            print("Processsing mssp audit trail")
        else:
            return
    else:
        return

    # Load all client configurations
    clients = load_configs()
    # Add all clients initially to retry_list for first run
    for client, _ in clients.items():
        # If client set at command line only run it otherwise
        # execute for all clients
        if manual_client == "" or clients[client]['client_name'] == manual_client:
            calculate_audit_trail(clients[client], settings)


if __name__ == "__main__":
    import argparse
    from argparse import RawTextHelpFormatter
    parser = argparse.ArgumentParser(
        description='Used to manually run mssp against a specific client ' \
                    + '(Example - mssp.py --client ha)',
            formatter_class=RawTextHelpFormatter)
    parser.add_argument("--client", default="", type=str,
                        help="Set to a specific client name to limit the mssp script to one client")
    parser.add_argument("--notification", default="True",
                        type=str, help="Set to False to disable notifications")

    args = parser.parse_args()
    manual_client = args.client

    run_mssp()
