import time
import requests
import random

WEBHOOK = "https://discord.com/api/webhooks/1402693667256270888/MMZGxEdH8xp3YPDATqNvBXNE-yc2NxrQznk-IlXf4JaMPWkz8jKdDEP7bkFygoMfSdDp"

def send_signal():
    signals = [
        "BTC LONG\nEntry: 72,300\nSL: 71,800\nTP: 73,500",
        "BTC SHORT\nEntry: 74,100\nSL: 74,600\nTP: 72,900",
        "SOL LONG\nEntry: 180\nSL: 175\nTP: 195",
        "SOL SHORT\nEntry: 195\nSL: 200\nTP: 180"
    ]
    msg = random.choice(signals)
    requests.post(WEBHOOK, json={"content": msg})

while True:
    send_signal()
    time.sleep(600)
