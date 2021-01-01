#!/usr/bin/env python3
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from models import db, Accounting
import pandas as pd
from scipy import stats
import numpy as np
import es
from config import load_configs
from datetime import datetime, timedelta
import json
import glob
import os
import argparse
from argparse import RawTextHelpFormatter
parser = argparse.ArgumentParser(description='Used to manually run against a specific client', formatter_class=RawTextHelpFormatter)
parser.add_argument("--client", default="", type=str, help="Set to a specific client name to limit the accounting script to one client")
parser.add_argument("--sqlite", default=0, type=int, help="Set to 1 to ingest JSON logs to sqlite")
parser.add_argument("--size", default=0, type=int, help="Set to 1 to calculate file sizes")
parser.add_argument("--elasticsearch", default=0, type=int, help="Set to 1 to push data to elasticsearch")
parser.add_argument("--debug", default=0, type=int, help="Set to 1 to prevent creating an accounting record")

args = parser.parse_args()
manual_client = args.client
sqlite = args.sqlite
size = args.size
elasticsearch = args.elasticsearch
debug = args.debug

basedir = os.path.abspath(os.path.dirname(__file__))

def convert_file_date(file_name, client):
    file_date = file_name[len(client)+12:]
    file_date = file_date[:-5]
    file_date = datetime.strptime(file_date, '%Y%m%d')
    return file_date

def load_json_file(file):
    complete_file = []
    with open(file) as f:
        for line in f:
            json_line = json.loads(line)
            complete_file.append(json_line)
    return complete_file

def map_field_to_db_entry(record, field, db_field):
    if field in record:
        record[db_field] = []
        record[db_field] = record[field]
        record.pop(field)
    return record       

def check_null_values(record, field):
    if field not in record:
        record[field] = None
    return record



def calculate_files_field_sum(files, field, disk_type="all"):
    all_sum = []
    if len(files) > 0:
        # Loop through each file
        for file in files:
            file_name = os.path.basename(file)
            # Load JSON file
            json_data = load_json_file(file)
            total = 0
            # Loop through each record in file
            for record in json_data:
                if field in record:
                    if disk_type == "all":
                        total += record[field]
                    else:
                        if record['disk'] == disk_type:
                            total += record[field]
            all_sum.append(total)
        return all_sum
    else:
        return []

def save_data_to_sqlite(files):
    # Loop through each file
    for file in files:
        # Load JSON file
        json_data = load_json_file(file)
        # Loop through each record in file
        for record in json_data:
            record = map_field_to_db_entry(record, 'name', 'index_name')
            record = map_field_to_db_entry(record, 'client', 'client_name')
            record = map_field_to_db_entry(record, 'logs', 'number_logs')
            record = map_field_to_db_entry(record, 'size', 'index_size_in_gb')
            check_null_values(record, 'index_creation_date')
            check_null_values(record, 'allocation_policy')
            check_null_values(record, 'current_policy_days')
            if 'newest_document_date' not in record:
                record['newest_document_date'] = record['index_creation_date']
            record['record_type'] = 'standard'
            db_entry = Accounting(record['index_creation_date'], record['newest_document_date'], record['index_name'], record['index_group'], record['disk'], record['client_name'], record['allocation_policy'], record['record_type'], record['index_size_in_gb'], record['number_logs'], record['cost'], record['current_policy_days'])
            # Add record to be committed to SQLite table
            db.session.add(db_entry)
            db.session.commit()

def extrapolate_files(files):
    # Find missing value and extrapolate
    print("Client has " + str(len(files)))
    files.sort()
    newest_file = files[len(files)-1]
    oldest_file = files[0]
    newest_file_date = convert_file_date(os.path.basename(newest_file), client)
    oldest_file_date = convert_file_date(os.path.basename(oldest_file), client)
    days = (newest_file_date - oldest_file_date).days
    if int(days) + 1 == len(files):
        print("PASS - Client " + client + " should have " + str(int(days) + 1) + " of accounting data. " + str(len(files)) + " found")
    else:
        print("FAIL - Client " + client + " should have " + str(int(days) + 1) + " of accounting data. " + str(len(files)) + " found")
        json_data = load_json_file(oldest_file)
        sizes = {}
        #growth_rates = {}
        while days > 0:
            oldest_file_date += timedelta(days=1)
            days = days - 1
            file_date = datetime.strftime(oldest_file_date, '%Y%m%d')
            file_to_check = "/cloud/cloud_configs/business_functions/accounting/" + client + "_accounting-" + file_date + ".json"
            if os.path.exists(file_to_check):
                json_data = load_json_file(file_to_check)
                for record in json_data:
                    index_group = record['index_group']
                    #if index_group not in growth_rates:
                    #    growth_rates[index_group] = []
                    if index_group not in sizes:
                        sizes[index_group] = []
                    if record['size'] > 0:
                        sizes[index_group].append(record['size'])
                    #if len(sizes[index_group]) > 1:
                    #    percent_difference = (sizes[index_group][-1] - sizes[index_group][-2]) / sizes[index_group][-1]
                    #    # Wait for at least 10 values before calculating IQR
                    #    if len(growth_rates[index_group]) > 10:
                    #        q1 = np.quantile(growth_rates[index_group],0.30)
                    #        q3 = np.quantile(growth_rates[index_group],0.70)
                    #        # Only save values above the 30% standard deviation but below 70%
                    #        # and change is not double in size
                    #        if growth_rates[index_group][-1] > q1 and growth_rates[index_group][-1] < q3 and abs(percent_difference) <= 100:
                    #            growth_rates[index_group].append(percent_difference)
                    #    else:
                    #        # Ignore change rates above 100 as that is extreme daily growth
                    #        # Often caused during rollout of new data sources or agents
                    #        if abs(percent_difference) <= 100:
                    #            growth_rates[index_group].append(percent_difference)
            else:
                print("File " + file_to_check + " not found")
                mean = 0
                es_data = []
                for record in json_data:
                    index_group = record['index_group']
                    if len(sizes[index_group]) != 0:
                        # Only apply moving averages if there are at least 5 stored
                        # growth rates or more
                        if len(sizes[index_group]) >= 5:
                            # Apply Exponential Moving Averages to smooth the data set
                            df=pd.DataFrame({'data':sizes[index_group]})
                            df['ewm_alpha_1'] = df['data'].ewm(span=7, adjust=False).mean()
                            # Store the latest moving average
                            mean = df['ewm_alpha_1'].iloc[-1]
                        else:
                            # Not enough values to safely predict moving average
                            # set mean to 0 for no growth
                            mean = 0
                    if debug == 1:
                        print(index_group + " previous size " + str(round(record['size'],2)) + " and cost " + str(round(record['cost'],2)) + " new moving average is " + str(mean))
                    if mean >= 0:
                        record['size'] = round(mean, 2)
                    else:
                        record['size'] = 0
                    if record['disk'] == 'ssd':
                        record['cost'] = round(record['size'] * .001, 2)
                    if record['disk'] == 'sata':
                        record['cost'] = round(record['size'] * .003, 2)
                    record['@timestamp'] = str(oldest_file_date.isoformat())
                    es_data.append(record)
                    if debug == 1:
                        print("New size " + str(record['size']) + " and cost " + str(record['cost']))
                    # Create a backup copy of each accounting record
                    if debug == 0:
                        with open(file_to_check, 'a') as f:
                            json_content = json.dumps(record)
                            f.write(json_content)
                            f.write('\n')
                    else:
                        pass
                if debug == 0:
                    if len(es_data) != 0 and debug == 0:
                        elasticsearch_connection = es.build_es_connection(clients[client])
                        results = es.get_list_by_chunk_size(es_data, 100)
                        for result in results:
                            es.bulk_insert_data_to_es(elasticsearch_connection, result, "accounting", bulk_size=100)
                        elasticsearch_connection.close()
                        elasticsearch_connection = es.build_es_connection(clients["ha"])
                        results = es.get_list_by_chunk_size(es_data, 100)
                        for result in results:
                            es.bulk_insert_data_to_es(elasticsearch_connection, result, "accounting", bulk_size=100)
                        elasticsearch_connection.close()

def outlier_check_zscore(data, threshold):
    if len(data) > 0:
        mean = np.mean(data)
        std = np.std(data)
        threshold = 3
        outlier = [] 
        for i in data: 
            z = (i - mean) / std 
            if z > threshold: 
                outlier.append(i) 
        return outlier
    else:
        return []

def outlier_to_file_mapping(outlier, data, files, method):
    if len(outlier) > 0:
        print('Outlier(s) in dataset are', outlier, " using method ", method) 
        for entry in outlier:
            index = data.index(entry)
            print(files[index])
    else:
        print("No outliers found using " + str(method))

def check_within_iqr(data, q1=25, q3=75, outlier_constant=1.5):
    a = np.array(data)
    if len(data) > 0:
        q1_result = np.percentile(data, q1)
        q3_result = np.percentile(data, q3)
        iqr = (q3_result - q1_result) * outlier_constant
        quartile_set = (q1_result - iqr, q3_result + iqr)
        resultList = []
        for y in a.tolist():
            if y >= quartile_set[0] and y <= quartile_set[1]:
                resultList.append(y)
        return resultList
    else:
        return []

def check_within_ewm(data):
    df=pd.DataFrame({'data':data})
    df['ewm_span_7'] = df['data'].ewm(span=7, adjust=False).mean()
    df['ewm_alpha_1'] = df['data'].ewm(alpha=.1).mean()
    df['ewm_alpha_3'] = df['data'].ewm(alpha=.3).mean()
    df['ewm_alpha_6'] = df['data'].ewm(alpha=.6).mean()
    print(df)

def check_within_percentage_from_mean(data, last_x_entries, acceptable_diff=0.50):
    if len(data) > 0:
        outlier = []
        for entry in data:
            # Only calculate if last_x_entries or more records in
            position = data.index(entry)
            if data.index(entry) > last_x_entries - 1:
                # Grab the mean of the last_x_entries
                mean = sum(data[position - last_x_entries:position]) / last_x_entries
                max_acceptable = mean + (mean * acceptable_diff)
                min_acceptable = mean - (mean * acceptable_diff)
                if debug == 1:
                    print(str(entry) + " vs mean of " + str(mean) + " with max accept " + str(max_acceptable) + " and min accept " + str(min_acceptable))
                if entry > max_acceptable or entry < min_acceptable:
                    outlier.append(entry)
        return outlier
    else:
        return []

if __name__ == "__main__":
    clients = load_configs()
    # Add all clients initially to retry_list for first run
    for client in clients:
        # If client set at command line only run it otherwise
        # execute for all clients
        if manual_client == "" or client == manual_client:
            print("Running for client " + client)
            files = glob.glob("/cloud/cloud_configs/business_functions/accounting/" + client + "*_accounting*.json")
            files.sort()
            if sqlite == 1:
                save_data_to_sqlite(files)

            if size == 1:
                ssd_sizes = calculate_files_field_sum(files, 'size', disk_type='ssd')
                print("Checking for outliers in SSD dataset")
                outlier = outlier_check_zscore(ssd_sizes, 3.5)
                outlier_to_file_mapping(outlier, ssd_sizes, files, 'Z score')
                outlier = check_within_percentage_from_mean(ssd_sizes, 7, acceptable_diff=1)
                outlier_to_file_mapping(outlier, ssd_sizes, files, 'mean')

                sata_sizes = calculate_files_field_sum(files, 'size', disk_type='sata')
                print("Checking for outliers in SATA dataset")
                outlier = outlier_check_zscore(sata_sizes, 3.5)
                outlier_to_file_mapping(outlier, sata_sizes, files, 'Z score')
                outlier = check_within_percentage_from_mean(sata_sizes, 7, acceptable_diff=1)
                outlier_to_file_mapping(outlier, sata_sizes, files, 'mean')