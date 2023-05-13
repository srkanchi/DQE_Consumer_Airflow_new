import pymsteams
import json 
import requests


def send_teams_message(team_web_hook_url, msg):
	myTeamsMessage = pymsteams.connectorcard(team_web_hook_url)
	myTeamsMessage.text(msg)
	myTeamsMessage.send()


# ### function to send alerts
# def send_slack_message(msg, web_hook_url, icon_emoji):
# 	slack_msg = {'text':msg, 'icon_emoji': icon_emoji, 'username': 'fst-apc-crop-circle-bot'}
# 	requests.post(web_hook_url,data=json.dumps(slack_msg))

# ## send msg to team
team_web_hook_url = 'https://bayergroup.webhook.office.com/webhookb2/e7393891-1fcc-4e5f-8913-37f789eda14e@fcb2b37b-5da0-466b-9b83-0014b67a7c78/IncomingWebhook/239e0359cf2049ff979dc582ab2c2100/9897f95a-e721-486b-9dc9-2f4c2f1d4d9e'
team_msg = 'Team MS Test2 DQE System notification;'
#send_teams_message(team_web_hook_url, team_msg)
