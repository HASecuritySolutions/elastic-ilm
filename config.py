#!/usr/bin/env python3
import os
import json
import sys
import toml

base_dir = os.path.abspath(os.path.dirname(__file__))
settings_file = base_dir + "/settings.toml"

def load_settings():
    if os.path.exists(settings_file):
        settings = toml.load(settings_file)
        if settings['settings']['client_json_folder'] == "":
            settings['settings']['client_json_folder'] = base_dir
    else:
        print("No settings.toml file found. Please clone and then edit settings.toml.example")
        print("Then retry.")
        exit()
    return settings

def load_configs():
    settings = load_settings()
    clients = {}
    for file in os.listdir(settings['settings']['client_json_folder']):
        if file.endswith(".json"):
            with open(settings['settings']['client_json_folder'] + '/' + file) as f:
                client = json.load(f)
            if client['client_name']:
                client_name = client['client_name']
            else:
                print("File name " + file + " does not contain valid client information")
                sys.exit(1)
            clients[client_name] = client
    return clients