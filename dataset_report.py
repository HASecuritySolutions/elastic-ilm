#!/usr/bin/env python3
"""Generates report for dataset data"""
import glob
import json
import numpy
import statistics
import scipy.stats as stats


# Path to accounting data
PATH = '/cloud/cloud_configs/business_functions/accounting/dataset_accounting'
# You can set CLIENT to '' to grab all clients
CLIENT = 'otava'
# Adjust this to limit the date range to analyze
# EX: 202208 would only analyze things in the year 2022 with month of 08
# EX: 2022 would analyze all of 2022
# EX: '' would analyze all dates
DATERANGE = ''
# What percentile rank do you want calculated
HIGH_PERCENTILE = 95
MID_PERCENTILE = 80
MID2_PERCENTILE = 75
LOW_PERCENTILE = 50

standard_deviation = 1.3

fileList = glob.glob(f"{PATH}/{CLIENT}_*_{DATERANGE}*.ndjson")
fileList.sort()

records = {}

def average(lst):
    """Returns average of a list

    Args:
        lst (list): A list of numbers

    Returns:
        int: Returns average of numbers
    """

    return sum(lst) / len(lst)

for file in fileList:
    with open(file, 'r', encoding='utf_8') as file_read:
        file_contents = file_read.read()
    for content in file_contents.splitlines():
        line = json.loads(content)
        dataset = line['dataset']
        if dataset not in records:
            records[dataset] = {
                "assets": [],
                "per_asset_gb": [],
                "per_asset_mb": []
            }
        records[dataset]['assets'].append(line['assets'])
        records[dataset]['per_asset_gb'].append(line['per_asset_gb'])
        records[dataset]['per_asset_mb'].append(line['per_asset_mb'])

# for dataset, record in records.items():
#     data = numpy.array(records[dataset]['per_asset_mb'])
#     print(stats.zscore(data))

for dataset in records:
    print(f"Results for dataset: {dataset}")
    print(f"Average asset count : {average(records[dataset]['assets'])}")
    print(f"Max asset count is {max(records[dataset]['assets'])}")
    print(f"Minimum asset count is {min(records[dataset]['assets'])}")
    print(f"{HIGH_PERCENTILE} percent lower than: {numpy.percentile(records[dataset]['assets'], HIGH_PERCENTILE)}")
    print(f"{MID_PERCENTILE} percent lower than: {numpy.percentile(records[dataset]['assets'], MID_PERCENTILE)}")
    print(f"{MID2_PERCENTILE} percent lower than: {numpy.percentile(records[dataset]['assets'], MID2_PERCENTILE)}")
    print(f"{LOW_PERCENTILE} percent lower than: {numpy.percentile(records[dataset]['assets'], LOW_PERCENTILE)}")
    print("")
    print(f"Average per asset GB : {average(records[dataset]['per_asset_gb'])}")
    print(f"Max per asset GB is {max(records[dataset]['per_asset_gb'])}")
    print(f"Minimum per asset GB is {min(records[dataset]['per_asset_gb'])}")
    print(f"{HIGH_PERCENTILE} percent lower than: {numpy.percentile(records[dataset]['per_asset_gb'], HIGH_PERCENTILE)}")
    print(f"{MID_PERCENTILE} percent lower than: {numpy.percentile(records[dataset]['per_asset_gb'], MID_PERCENTILE)}")
    print(f"{MID2_PERCENTILE} percent lower than: {numpy.percentile(records[dataset]['per_asset_gb'], MID2_PERCENTILE)}")
    print(f"{LOW_PERCENTILE} percent lower than: {numpy.percentile(records[dataset]['per_asset_gb'], LOW_PERCENTILE)}")
    print("")
    print(f"Average per asset MB : {average(records[dataset]['per_asset_mb'])}")
    print(f"Max per asset MB is {max(records[dataset]['per_asset_mb'])}")
    print(f"Minimum per asset MB is {min(records[dataset]['per_asset_mb'])}")
    print(f"{HIGH_PERCENTILE} percent lower than: {numpy.percentile(records[dataset]['per_asset_mb'], HIGH_PERCENTILE)}")
    print(f"{MID_PERCENTILE} percent lower than: {numpy.percentile(records[dataset]['per_asset_mb'], MID_PERCENTILE)}")
    print(f"{MID2_PERCENTILE} percent lower than: {numpy.percentile(records[dataset]['per_asset_mb'], MID2_PERCENTILE)}")
    print(f"{LOW_PERCENTILE} percent lower than: {numpy.percentile(records[dataset]['per_asset_mb'], LOW_PERCENTILE)}")
