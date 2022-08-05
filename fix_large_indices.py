"""This script reindexes small indices"""
from encodings import utf_8
import json
import re

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
TBCOUNT = 0
GBCOUNT = 0
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
        print(f"Need to reindex {s['index']} with size of {s['store.size']}")
        TBCOUNT = TBCOUNT + 1
    elif s['store.size'][-2:] == 'gb':
        if float(s['store.size'][0:len(s['store.size'])-2]) > 400:
            print(f"Need to reindex {s['index']} with size of {s['store.size']}")
            GBCOUNT = GBCOUNT + 1
    else:
        continue

print(f"Total COUNT: {TOTAL_COUNT}")
print(f"TBCOUNT: {TBCOUNT}")
print(f"GBCOUNT: {GBCOUNT}")
