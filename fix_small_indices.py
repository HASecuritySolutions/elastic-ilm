"""This script reindexes small indices"""
from encodings import utf_8
import json
import re

# def get_index_group(index):
#     """Returns index group for index

#     Args:
#         index (str): Full index name

#     Returns:
#         str: Index group name
#     """
#     match = re.search(r'^([a-zA-Z0-9-._]+)(-.*)?-20[0-9][0-9](\.|-)[0-9]{2}(\.|-)[0-9]{2}$', index)
#     if match:
#         index_group = match.group(1)
#     else:
#         match = re.search(r'^([a-zA-Z0-9-._]+)(-.*)?-20[0-9][0-9](\.|-)[0-9]{2}$', index)
#         if match:
#             index_group = match.group(1)
#         else:
#             match = re.search(r'^([a-zA-Z0-9-._]+)(-.*)?-20[0-9][0-9]$', index)
#             if match:
#                 index_group = match.group(1)
#             else:
#                 match = re.search(r'^([a-zA-Z0-9-._]+)(-.*)?-[0-9]{6,}$', index)
#                 if match:
#                     index_group = match.group(1)
#                 else:
#                     match = re.search(r'^([a-zA-Z0-9-._]+)(-.*)?-[a-zA-Z0-9-._]{3,}$', index)
#                     if match:
#                         index_group = match.group(1)
#                     else:
#                         index_group = index
#     return index_group

def get_index_group(index):
    """Gets the index group of a given index

    Args:
        index (str): Full index name

    Returns:
        str: Index group name
    """
    if str(index).startswith('.ds-'):
        index = index[4:]
    # First, find and remove possible dates
    m = re.search('-20[0-9][0-9](\.|-|_|:)[0-9]{2}(\.|-|_|:)[0-9]{2}$', index)
    if m:
        #print(f"Found date of {m.group(0)} in index {index}")
        index = index.replace(str(m.group(0)), '')
    m = re.search('20[0-9][0-9](\.|-|_|:)[0-9]{2}(\.|-|_|:)[0-9]{2}-', index)
    if m:
        #print(f"Found date of {m.group(0)} in index {index}")
        index = index.replace(str(m.group(0)), '')

    # Next, remove number sequence if found at end (ex: -000001)
    m = re.search('-[0-9]{1,6}$', index)
    if m:
        #print(f"Found ending number sequence for index {index}")
        index = index.replace(str(m.group(0)), '')
    return index

# Opening JSON file
FILE = open('/opt/elastic-ilm/small_indices.json', encoding='utf_8')

TOTAL_COUNT = 0
GBCOUNT = 0
KILOBYTECOUNT = 0
BYTECOUNT = 0
MEGABYTECOUNT = 0
data = json.load(FILE)
queue = {}

def evaluate(entry):
    """Evaluates entries into groups

    Args:
        entry (dict): Index dictionary
    """
    index_group = get_index_group(entry['index'])
    if index_group not in queue:
        queue[index_group] = []
    queue[index_group].append(entry)

for s in data:
    TOTAL_COUNT = TOTAL_COUNT + 1
    if s['store.size'][-2:] == 'tb':
        # print(s)
        # COUNT = COUNT + 1
        continue
    if s['index'][0:1] == '.' and s['index'][0:3] != '.ds':
        print(f"Skipping : {s['index']}")
        continue
    if s['store.size'][-2:] == 'gb':
        if float(s['store.size'][0:len(s['store.size'])-2]) < 1.5:
            GBCOUNT = GBCOUNT + 1
            TOTAL_COUNT = TOTAL_COUNT + 1
            evaluate(s)
    if s['store.size'][-2:] == 'mb':
        MEGABYTECOUNT = MEGABYTECOUNT + 1
        TOTAL_COUNT = TOTAL_COUNT + 1
        evaluate(s)
    if s['store.size'][-2:] == 'kb':
        KILOBYTECOUNT = KILOBYTECOUNT + 1
        TOTAL_COUNT = TOTAL_COUNT + 1
        evaluate(s)
    if s['store.size'][-2:] not in ['kb', 'mb','gb','tb']:
        if s['store.size'][-1] == 'b':
            BYTECOUNT = BYTECOUNT + 1
            TOTAL_COUNT = TOTAL_COUNT + 1
            evaluate(s)
        else:
            print(f"Unknown: {s}")

print(f"Total COUNT: {TOTAL_COUNT}")
print(f"GBCOUNT: {GBCOUNT}")
print(f"MEGABYTECOUNT: {MEGABYTECOUNT}")
print(f"KILOBYTECOUNT: {KILOBYTECOUNT}")
print(f"BYTECOUNT: {BYTECOUNT}")
for group in queue.keys():
    SIZE = 0
    indices = []
    for item in queue[group]:
        if item['store.size'][-2:] == 'gb':
            SIZE = SIZE + float(item['store.size'][0:len(item['store.size'])-2]) * 1024 * 1024 *1024
            indices.append(item['index'])
        if item['store.size'][-2:] == 'mb':
            SIZE = SIZE + float(item['store.size'][0:len(item['store.size'])-2]) * 1024 * 1024
            indices.append(item['index'])
        if item['store.size'][-2:] == 'kb':
            SIZE = SIZE + float(item['store.size'][0:len(item['store.size'])-2]) * 1024
            indices.append(item['index'])
        if item['store.size'][-2:] not in ['kb', 'mb','gb','tb']:
            if item['store.size'][-1] == 'b':
                SIZE = SIZE + float(item['store.size'][0:len(item['store.size'])-1])
                indices.append(item['index'])
    SIZE = SIZE / 1024 / 1024 / 1024
    if 'winlogbeat' not in group and 'cumulus' not in group and 'fortinet' not in group and 'ubuntu' not in group and 'clavister' not in group and 'cisco-nx-os' not in group and 'trend-micro' not in group:
        print(f"Group {group} has total GB size of {SIZE}")
        print(f"POST _reindex?wait_for_completion=false")
        csv_indices = ""
        for index in indices:
            csv_indices = csv_indices + index + ","
        csv_indices = csv_indices[0:len(csv_indices) - 1]
        print('{ "source": { "index": "' + str(csv_indices) + '"}, "dest": { "index": "' + group + '-reindex-000001"}}')
