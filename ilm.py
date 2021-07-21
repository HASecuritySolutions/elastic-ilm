#!/usr/bin/env python3
import schedule
import sd_notify
import threading
from config import load_settings
from accounting import run_accounting
from retention import apply_retention_policies
from rollover import apply_rollover_policies
import time
import argparse
from argparse import RawTextHelpFormatter
parser = argparse.ArgumentParser(description='Used to manually run script (Example: ilm.py --manual 1)', formatter_class=RawTextHelpFormatter)
parser.add_argument("--manual", default=0, type=int, help="Set to 1 to manually run script")
parser.add_argument("--client", default="", type=str, help="Set to a specific client name to limit calls to one client")
parser.add_argument("--notification", default="True", type=str, help="Set to False to disable notifications")

args = parser.parse_args()
manual = args.manual
manual_client = args.client
notification = args.notification
if args.notification == "True":
    notification = True
else:
    notification = False

if manual == 0:
    notify = sd_notify.Notifier()
    if not notify.enabled():
        # Then it's probably not running is systemd with watchdog enabled
        raise Exception("Watchdog not enabled")
    notify.status("Initializing service...")

def run_threaded(job_func, *arguments):
    job_thread = threading.Thread(target=job_func, args=tuple(arguments))
    job_thread.start()

if __name__ == "__main__":
    settings = load_settings()
    if manual == 0:
        notify.ready()
    
    # On service startup, immediately run retention, rollover, and accounting
    apply_retention_policies(settings['retention']['health_check_level'], manual_client)
    apply_rollover_policies(manual_client)
    run_accounting(manual_client)

    if settings['accounting']['enabled']:
        schedule.every(settings['accounting']['minutes_between_run']).minutes.do(run_threaded, run_accounting, "")
    if settings['retention']['enabled']:
        schedule.every(settings['retention']['minutes_between_run']).minutes.do(run_threaded, apply_retention_policies, settings['retention']['health_check_level'], manual_client)
    if settings['rollover']['enabled']:
        schedule.every(settings['rollover']['minutes_between_run']).minutes.do(run_threaded, apply_rollover_policies, manual_client)

    while True:
        schedule.run_pending()
        # Sleep 1 minute between jobs
        time.sleep(60)