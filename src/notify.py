"""Notification de fraude : un simple webhook (Slack ou Discord).

"""
import os

import requests

WEBHOOK_URL = os.environ.get("NOTIFY_WEBHOOK_URL")
WEBHOOK_KIND = os.environ.get("NOTIFY_WEBHOOK_KIND", "discord")


def send(message: str) -> None:
    """Envoie un message générique (utilisé par les alertes ET le rapport quotidien)."""
    if not WEBHOOK_URL:
        print("NOTIFY_WEBHOOK_URL non configuré : message ignoré :\n" + message)
        return
    payload = {"content": message} if WEBHOOK_KIND == "discord" else {"text": message}
    resp = requests.post(WEBHOOK_URL, json=payload, timeout=10)
    resp.raise_for_status()


def notify_fraud(prediction: dict) -> None:
    message = (
        f"🚨 Fraude détectée — transaction `{prediction['trans_num']}` "
        f"(score {prediction['score']:.2%}, modèle {prediction['model_version']})"
    )
    send(message)
