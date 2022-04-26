#!/usr/bin/env python3
"""Launches Elastic ILM"""
import hashlib
import argparse
import time
from argparse import RawTextHelpFormatter
from apscheduler.schedulers.background import BackgroundScheduler
from config import load_settings
from accounting import run_accounting
#from custom_checks import run_custom_checks
from retention import apply_retention_policies
from allocation import apply_allocation_policies
from rollover import apply_rollover_policies
from forcemerge import apply_forcemerge_policies
from backup import run_backup
parser = argparse.ArgumentParser(
    description='Used to manually run script (Example: ilm.py --manual 1)',
    formatter_class=RawTextHelpFormatter
)
parser.add_argument(
    "--manual",
    default=0,
    type=int,
    help="Set to 1 to manually run script"
)
parser.add_argument(
    "--client",
    default="",
    type=str,
    help="Set to a specific client name to limit calls to one client"
)
parser.add_argument(
    "--notification",
    default="True",
    type=str,
    help="Set to False to disable notifications"
)

args = parser.parse_args()
manual = args.manual
manual_client = args.client
NOTIFICATION = args.notification
if args.NOTIFICATION == "True":
    NOTIFICATION = True
else:
    NOTIFICATION = False

if manual == 0:
    sched = BackgroundScheduler(daemon=True)
else:
    sched = BackgroundScheduler(daemon=False)

def start_jobs():
    """Starts background jobs
    """
    settings = load_settings()

    if "accounting" in settings:
        if settings['accounting']['enabled']:
            sched.add_job(
                run_accounting,
                'interval',
                minutes=settings['accounting']['minutes_between_run'],
                args=[manual_client]
            )

    if 'backup' in settings:
        if settings['backup']['enabled']:
            sched.add_job(run_backup, 'interval', minutes=settings['backup']['minutes_between_run'])

    if 'retention' in settings:
        if settings['retention']['enabled']:
            sched.add_job(
                apply_retention_policies,
                'interval',
                minutes=settings['retention']['minutes_between_run'],
                args=[settings['retention']['health_check_level']]
            )

    if 'allocation' in settings:
        if settings['allocation']['enabled']:
            sched.add_job(
                apply_allocation_policies,
                'interval',
                minutes=settings['allocation']['minutes_between_run']
            )

    if 'rollover' in settings:
        if settings['rollover']['enabled']:
            sched.add_job(
                apply_rollover_policies,
                'interval',
                minutes=settings['rollover']['minutes_between_run']
            )

    if 'forcemerge' in settings:
        if settings['forcemerge']['enabled']:
            sched.add_job(
                apply_forcemerge_policies,
                'interval',
                minutes=settings['forcemerge']['minutes_between_run']
            )
        else:
            sched.add_job(
                apply_forcemerge_policies,
                'interval',
                minutes=60
            )
    else:
        sched.add_job(
            apply_forcemerge_policies,
            'interval',
            minutes=60
        )

    sched.start()

if __name__ == "__main__":
    settings_as_bytes = load_settings(format='bytes')
    CONFIG_HASH = hashlib.sha256(settings_as_bytes).hexdigest()

    start_jobs()

    while True:
        time.sleep(5)
        settings_as_bytes = load_settings(format='bytes')
        CURRENT_HASH = hashlib.sha256(settings_as_bytes).hexdigest()
        if CURRENT_HASH != CONFIG_HASH:
            print("Configuration changed. Reloading jobs")
            CONFIG_HASH = CURRENT_HASH
            sched.shutdown()
            if manual == 0:
                sched = BackgroundScheduler(daemon=True)
            else:
                sched = BackgroundScheduler(daemon=False)
            start_jobs()
