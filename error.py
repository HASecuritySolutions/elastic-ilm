#!/usr/bin/env python3
import pymsteams
import smtplib
from config import load_settings

def send_ms_teams_message(title, message):
    settings = load_settings()
    if settings['notification']['ms-teams'] == 'enabled':
        try:
            myTeamsMessage = pymsteams.connectorcard(settings['ms-teams']['webhook'])
            myTeamsMessage.title(title)
            myTeamsMessage.text(message)
            myTeamsMessage.send()
            return True
        except ValueError:
            print("Unable to send message to teams")
            return False

def send_email(to, subject, message):
    settings = load_settings()
    if settings['notification']['smtp'] == 'enabled':
        try:
            mailserver = smtplib.SMTP(settings['smtp']['smtp_host'],settings['smtp']['smtp_port'])
            mailserver.ehlo()
            mailserver.starttls()
            mailserver.login(settings['smtp']['username'], settings['smtp']['password'])
            mail_message = 'Subject: {}\n\n{}'.format(subject, message)
            mailserver.sendmail(settings['smtp']['from_email'], to, mail_message)
            mailserver.quit()
            return True
        except ValueError:
            print("Failed to send email")
            return False

def send_jira_event(subject, message):
    settings = load_settings()
    if settings['notification']['smtp'] == 'enabled' and settings['notification']['jira'] == 'enabled':
        send_email(settings['jira']['jira_email_address'], subject, message)

def send_notification(client_config, operation, status, message, jira=False, teams=False):
    if teams:
        send_ms_teams_message("Client " + client_config['client_name'] + " " + operation + " status: " + status, message)
    if jira:
        send_jira_event("Client " + client_config['client_name'] + " " + operation + " status: " + status, message)