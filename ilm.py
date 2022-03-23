#!/usr/bin/env python3
from apscheduler.schedulers.background import BackgroundScheduler
from config import load_settings
from accounting import run_accounting
#from custom_checks import run_custom_checks
from retention import apply_retention_policies
from rollover import apply_rollover_policies
from backup import run_backup
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
    sched = BackgroundScheduler(daemon=True)
else:
    sched = BackgroundScheduler(daemon=False)


if __name__ == "__main__":
    settings = load_settings()
    
    if "accounting" in settings:
        if settings['accounting']['enabled']:
            sched.add_job(run_accounting,'interval',minutes=settings['accounting']['minutes_between_run'])
    
    if 'backup' in settings:
        if settings['backup']['enabled']:
            sched.add_job(run_backup,'interval',minutes=settings['backup']['minutes_between_run'])
        
    if 'retention' in settings:
        if settings['retention']['enabled']:
            sched.add_job(apply_retention_policies, 'interval', minutes=settings['retention']['minutes_between_run'], args=[settings['retention']['health_check_level'],manual_client])

    if 'rollover' in settings:
        if settings['rollover']['enabled']:
            sched.add_job(apply_rollover_policies, 'interval', minutes=settings['rollover']['minutes_between_run'], args=[manual_client])

    sched.start()

    # # On service startup, immediately run retention, rollover, backup, and accounting
    # run_backup(manual_client)
    # apply_retention_policies(settings['retention']['health_check_level'], manual_client)
    # apply_rollover_policies(manual_client)
    # run_accounting(manual_client)

    
    # if 'backup' in settings:
    #     if settings['backup']['enabled']:
    #         schedule.every(settings['backup']['minutes_between_run']).minutes.do(run_threaded, run_backup, "")
    # #if settings['custom_checks']['enabled']:
    # #    schedule.every(settings['custom_checks']['minutes_between_run']).minutes.do(run_threaded, run_custom_checks, "")
    # # Example client info entry
    # # "custom_checks": {
    # #     "0": {
    # #     "check": "/bin/bash /opt/elastic_stack/scripts/verify_acuity_custom_service.sh",
    # #     "remediate": "/bin/bash /opt/elastic_stack/scripts/restart_acuity_custom_service.sh",
    # #     "schedule": "15"
    # #     }
    # # },
    # if 'retention' in settings:
    #     if settings['retention']['enabled']:
    #         schedule.every(settings['retention']['minutes_between_run']).minutes.do(run_threaded, apply_retention_policies, settings['retention']['health_check_level'], manual_client)
    # if 'rollover' in settings:
    #     if settings['rollover']['enabled']:
    #         schedule.every(settings['rollover']['minutes_between_run']).minutes.do(run_threaded, apply_rollover_policies, manual_client)

