#!/usr/bin/env python3
import pandas as pd
from shutil import copyfile
from scipy import stats
import numpy as np
import pandas as pd
import es
from config import load_configs, load_settings
from datetime import datetime, timedelta
import json
import glob
import os
import argparse
from argparse import RawTextHelpFormatter
parser = argparse.ArgumentParser(description='Used to manually run accounting against a specific client (Example - accounting.py --client ha)', formatter_class=RawTextHelpFormatter)
parser.add_argument("--client", default="", type=str, help="Set to a specific client name to limit the accounting script to one client")
parser.add_argument("--reingest", default=0, type=int, help="Set to 1 to bulk reingest logs")
parser.add_argument("--esclient", default="ha", type=str, help="Set to specific client name to send es data to their cluster")
parser.add_argument("--days", default=-1, type=int, help="How many days of files to handle")
parser.add_argument("--correct", default=0, type=int, help="Set to 1 to correct log file timestamps and cost calculations")
parser.add_argument("--missing", default=0, type=int, help="Set to 1 to fix missing accounting files and indices")
parser.add_argument("--elasticsearch", default=0, type=int, help="Set to 1 to push data to elasticsearch")
parser.add_argument("--debug", default=0, type=int, help="Set to 1 to prevent creating an accounting record")

args = parser.parse_args()
manual_client = args.client
reingest = args.reingest
esclient = args.esclient
days = args.days
correct = args.correct
missing = args.missing
elasticsearch = args.elasticsearch
debug = args.debug

settings = load_settings()

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

def correct_data(json_data, file_name):
    file_date = datetime.strptime(file_name[-13:-5], '%Y%m%d')
    corrected_data = []
    file_needs_updated = 0
    for json_line in json_data:
        correct_cost = 0
        # Per each record, validate cost is calculated correctly
        if json_line['disk'] == "ssd" or json_line['disk'] == "sata":
            if json_line['disk'] == "ssd":
                correct_cost = json_line['size'] * settings['accounting']['ssd_cost']
            if json_line['disk'] == "sata":
                correct_cost = json_line['size'] * settings['accounting']['sata_cost']
        else:
            correct_cost = json_line['cost']
        # Fix index group if incorrectly parsed out (regex)
        if json_line['index_group'] != es.get_index_group(json_line['name']):
            json_line['index_group'] = es.get_index_group(json_line['name'])
            file_needs_updated = 1
        # Set newest_document_date to index_creation_date if index_creation_date exists
        # but newest_document_date does not
        if 'index_creation_date' in json_line:
            if 'newest_document_date' not in json_line:
                json_line['newest_document_date'] = json_line['index_creation_date']
        # Check if file date does not match record timestamps - If so overwrite
        current_date = datetime.strptime(json_line['@timestamp'][0:10], '%Y-%m-%d')
        if file_date != current_date:
            json_line['@timestamp'] = file_date.isoformat()
            file_needs_updated = 1
            print("Timestamp mismatch of", current_date, "vs", file_date, "in file", file_name)
        # You cannot divide by zero
        #if correct_cost != 0:
        #    percent_difference = abs((correct_cost - json_line['cost']) / correct_cost * 100)
        #    # If cost is off by 10% or more, update it
        #    if percent_difference >= 10:
        #        json_line['cost'] = correct_cost
        #        print("File " + file_name + " has corrected cost " + str(correct_cost) + " vs. " + str(json_line['cost']) + " difference of " + str(percent_difference) + "% for index " + json_line['name'])
        #        file_needs_updated = 1
        corrected_data.append(json_line)

    # Fix files that need updated (wrong cost calculations, timestamps, etc)
    if file_needs_updated == 1:
        print("Writing updated file to : " + settings['accounting']['output_folder'] + "/" + file_name)
        # Delete file if it already exists
        if os.path.exists(settings['accounting']['output_folder'] + "/" + file_name):
            os.remove(settings['accounting']['output_folder'] + "/" + file_name)
        with open(settings['accounting']['output_folder'] + "/" + file_name, 'a') as f:
            for record in corrected_data:
                json_content = json.dumps(record)
                f.write(json_content)
                f.write('\n')

def reingest_data(json_data, esclient):
    elasticsearch_connection = es.build_es_connection(clients[esclient])
    results = es.get_list_by_chunk_size(json_data, 100)
    for result in results:
        es.bulk_insert_data_to_es(elasticsearch_connection, result, "accounting", bulk_size=100)
    elasticsearch_connection.close()

def get_day_range_in_files(files):
    newest_file_date = get_newest_file_by_name(files)
    oldest_file_date = get_oldest_file_by_name(files)
    days = (newest_file_date - oldest_file_date).days
    return days

def get_newest_file_by_name(files):
    files.sort()
    if len(files) > 0:
        newest_file = files[len(files)-1]
    else:
        newest_file = files[0]
    newest_file_date = convert_file_date(os.path.basename(newest_file), client)
    return newest_file_date

def get_oldest_file_by_name(files):
    files.sort()
    oldest_file = files[0]
    oldest_file_date = convert_file_date(os.path.basename(oldest_file), client)
    return oldest_file_date

def build_panda_time_index(files, days):
    time_index = pd.date_range(get_oldest_file_by_name(files), periods=days + 1, freq="D")
    return time_index

def interpolate_date(data, time_index):
    df = pd.DataFrame(data)
    df.index = time_index
    return df.interpolate()

def get_index_group_interpolation(dataset, index_group):
    # Native dataset
    df = pd.Series(dataset[index_group])
    df = df.interpolate(method='linear', limit_direction='forward')
    return df

def fix_missing(files):
    dataset = {}
    print("Client has " + str(len(files)))
    days = get_day_range_in_files(files)
    time_index = build_panda_time_index(files, days)
    if int(days) + 1 == len(files):
        print("PASS - Client " + client + " should have " + str(int(days) + 1) + " of accounting data. " + str(len(files)) + " found")
    else:
        print("FAIL - Client " + client + " should have " + str(int(days) + 1) + " of accounting data. " + str(len(files)) + " found")
    files_array = []
    for date in time_index:
        file_date = datetime.strftime(date, '%Y%m%d')
        file_to_check = settings['accounting']['output_folder'] + "/" + client + "_accounting-" + file_date + ".json"
        files_array.append(file_to_check)
        if os.path.exists(file_to_check):
            if debug == 1:
                print("File " + file_to_check + " found")
        else:
            if debug == 1:
                print("File " + file_to_check + " not found")
            for data in dataset:
                dataset[data].append(np.nan)
            # Copy yesterday's file to today
            old_file_date = datetime.strftime(date +  timedelta(days=-1), '%Y%m%d')
            old_file_check = settings['accounting']['output_folder'] + "/" + client + "_accounting-" + old_file_date + ".json"
            new_file_check = settings['accounting']['output_folder'] + "/" + client + "_accounting-" + file_date + ".json"
            print("Creating file ", new_file_check, " by cloning ", old_file_check)
            json_data = load_json_file(old_file_check)
            for record in json_data:
                new_timestamp = datetime.strptime(record['@timestamp'][0:19], '%Y-%m-%dT%H:%M:%S')
                new_timestamp += timedelta(days=1)
                record['@timestamp'] = new_timestamp.isoformat()
                if debug == 0 and len(record) != 0:
                    # Create a backup copy of each accounting record
                    with open(new_file_check, 'a') as f:
                        json_content = json.dumps(record)
                        f.write(json_content)
                        f.write('\n')

def get_files_within_x_days(days, files, client):
    # A value of -1 means process all files
    if days < 0:
        return files
    else:
        file_array = []
        next_file_date = get_newest_file_by_name(files)
        next_file_date_string = datetime.strftime(next_file_date, '%Y%m%d')
        next_file = settings['accounting']['output_folder'] + "/" + client + "_accounting-" + next_file_date_string + ".json"
        file_array.append(next_file)
        count = 1
        while count != days:
            next_file_date = next_file_date + timedelta(days=-1)
            next_file_date_string = datetime.strftime(next_file_date, '%Y%m%d')
            next_file = settings['accounting']['output_folder'] + "/" + client + "_accounting-" + next_file_date_string + ".json"
            file_array.append(next_file)
            count += 1
        return file_array

if __name__ == "__main__":
    clients = load_configs()
    # Add all clients initially to retry_list for first run
    for client in clients:
        # If client set at command line only run it otherwise
        # execute for all clients
        if manual_client == "" or client == manual_client:
            print("Running for client " + client)
            files = glob.glob(settings['accounting']['output_folder'] + "/" + client + "*_accounting*.json")
            files.sort()
            # Fix files with bad timestamps or miscalculated costs if set correct is set to 1
            if correct == 1:
                for file in files:
                    file_needs_updated = 0
                    file_name = os.path.basename(file)
                    if debug == 1:
                        print("Running against file: " + file)
                    json_data = load_json_file(file)
                    correct_data(json_data, file_name)

            # Check if daily accounting records do not exist in accounting index
            # If missing, ingest
            # NEEDS DONE AT SOME POINT
            if 1 == 1:
                pass

            # Reingest logs if reingest is set to 1, useful for completely reloading index data
            if reingest == 1:
                if len(files) > 0:
                    files = get_files_within_x_days(days, files, client)
                    print("Reingesting ", len(files), " files")
                    count = 1
                    for file in files:
                        file_needs_updated = 0
                        file_name = os.path.basename(file)
                        print("Running against file ", count, " : " + file)
                        if os.path.exists(file):
                            json_data = load_json_file(file)
                            reingest_data(json_data, esclient)
                        else:
                            print("Error - file does not exist")
                        count += 1

            # Fix missing files using extrapolation if missing is set to 1
            if missing == 1 and len(files) > 0:
                fix_missing(files)