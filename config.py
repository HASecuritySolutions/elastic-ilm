#!/usr/bin/env python3
import os
import json
import sys
import toml
from functools import wraps
import time

base_dir = os.path.abspath(os.path.dirname(__file__))
if "base_dir" not in locals():
    base_dir = "/etc/elastic-ilm"
if os.path.exists("/etc/elastic-ilm/settings.toml"):
    settings_file = "/etc/elastic-ilm/settings.toml"
elif os.path.exists("/etc/maintenance/modules/settings.toml"):
    settings_file = "/etc/maintenance/modules/settings.toml"
else:
    settings_file = base_dir + "/settings.toml"
if not os.path.exists(settings_file):
    print("settings.toml not found - exiting")
    exit()

def retry(ExceptionToCheck, tries=5, delay=1, backoff=1, logger=None):
    """Retry calling the decorated function using an exponential backoff.

    http://www.saltycrane.com/blog/2009/11/trying-out-retry-decorator-python/
    original from: http://wiki.python.org/moin/PythonDecoratorLibrary#Retry

    :param ExceptionToCheck: the exception to check. may be a tuple of
        exceptions to check
    :type ExceptionToCheck: Exception or tuple
    :param tries: number of times to try (not retry) before giving up
    :type tries: int
    :param delay: initial delay between retries in seconds
    :type delay: int
    :param backoff: backoff multiplier e.g. value of 2 will double the delay
        each retry
    :type backoff: int
    :param logger: logger to use. If None, print
    :type logger: logging.Logger instance
    """
    def deco_retry(f):

        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck as e:
                    msg = "%s, Retrying in %d seconds..." % (str(e), mdelay)
                    if logger:
                        logger.warning(msg)
                    else:
                        print(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)

        return f_retry  # true decorator

    return deco_retry

def load_settings(format='toml'):
    if format == 'toml':
        settings = toml.load(settings_file)
        if settings['settings']['client_json_folder'] == "":
            settings['settings']['client_json_folder'] = base_dir
        return settings
    if format == 'bytes':
        with open(settings_file, "rb") as file:
            read_bytes = file.read()  # read entire file as bytes
        return read_bytes

def load_configs(client_value=""):
    settings = load_settings()
    clients = {}
    for file in os.listdir(settings['settings']['client_json_folder']):
        if client_value != "":
            if file.endswith(".json") and file.startswith(str(client_value)):
                with open(settings['settings']['client_json_folder'] + '/' + file) as f:
                    client = json.load(f)
                if 'client_name' in client:
                    client_name = client['client_name']
                else:
                    print("File name " + file + " does not contain valid client information")
                    sys.exit(1)
                clients[client_name] = client
        else:
            if file.endswith(".json"):
                with open(settings['settings']['client_json_folder'] + '/' + file) as f:
                    client = json.load(f)
                if 'client_name' in client:
                    client_name = client['client_name']
                else:
                    print("File name " + file + " does not contain valid client information")
                    sys.exit(1)
                clients[client_name] = client
    return clients

def load_config(client_value=""):
    settings = load_settings()
    for file in os.listdir(settings['settings']['client_json_folder']):
        if client_value != "":
            if file.endswith(".json") and file.startswith(str(client_value)):
                with open(settings['settings']['client_json_folder'] + '/' + file) as f:
                    client = json.load(f)
                return client