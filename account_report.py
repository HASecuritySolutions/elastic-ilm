#!/usr/bin/env python3
"""Generates report for accounting data"""
import glob
import json

# Path to accounting data
PATH = '/cloud/cloud_configs/business_functions/accounting'
# You can set CLIENT to '' to grab all clients
CLIENT = 'otava'
# Adjust this to limit the date range to analyze
# EX: 202208 would only analyze things in the year 2022 with month of 08
# EX: 2022 would analyze all of 2022
DATERANGE = '202208'

fileList = glob.glob(f"{PATH}/{CLIENT}_accounting-{DATERANGE}*.json")
fileList.sort()
last_file = {
    "name": "",
    "ssd": 0,
    "sata": 0
}
daily_changes = {
    "ssd": [],
    "sata": [],
    "total": []
}

def average(lst):
    """Returns average of a list

    Args:
        lst (list): A list of numbers

    Returns:
        int: Returns average of numbers
    """

    return sum(lst) / len(lst)

for file in fileList:
    record = {
        "name": file,
        "ssd": 0,
        "sata": 0
    }
    with open(file, 'r', encoding='utf_8') as file_read:
        file_contents = file_read.read()
    for content in file_contents.splitlines():
        line = json.loads(content)
        if line['disk'] == 'ssd':
            record['ssd'] = record['ssd'] + line['size']
        if line['disk'] == 'sata':
            record['sata'] = record['sata'] + line['size']
    if last_file['name'] != "":
        SSD_CHANGE = record['ssd'] - last_file['ssd']
        SATA_CHANGE = record['sata'] - last_file['sata']
        TOTAL_CHANGE = SSD_CHANGE + SATA_CHANGE
        daily_changes['ssd'].append(SSD_CHANGE)
        daily_changes['sata'].append(SATA_CHANGE)
        daily_changes['total'].append(TOTAL_CHANGE)
    last_file = record

for day_change in daily_changes['total']:
    print(f"Change rate is {day_change}")

print(f"Average daily change rate is : {average(daily_changes['total'])}")
print(f"Max daily change rate is {max(daily_changes['total'])}")
print(f"Minimum daily change rate is {min(daily_changes['total'])}")
