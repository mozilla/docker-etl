import os

config = {
    "slack_host": "https://slack.com/",
    "slack_token": os.environ.get("slack_token", "")
}
