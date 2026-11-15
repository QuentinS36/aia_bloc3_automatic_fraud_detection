"""Orchestration partagée : contrôle qualité, stockage, scoring, notification.

Utilisée par le DAG d'ingestion réelle et par le DAG de démonstration, pour
garantir qu'une transaction injectée manuellement suit EXACTEMENT le même
chemin de traitement qu'une transaction reçue de l'API.
"""
import json

import ingestion as ing
import notify
import scoring


def process(row: dict) -> dict:
    """Valide, stocke, puis score et notifie si nécessaire. Renvoie un résumé."""
    valid, errors = ing.validate(row)
    key = ing.store(row, valid, errors)
    result = {"key": key, "valid": valid, "errors": errors}
    if not valid:
        return result

    prediction = scoring.score(row)
    prediction["seen_in_training"] = ing.seen_in_training(row["_orig_index"])
    day = ing.ingestion_day(row)
    ing.s3_client().put_object(
        Bucket=ing.BUCKET,
        Key=f"predictions/dt={day}/{prediction['trans_num']}.json",
        Body=json.dumps(prediction, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )
    if prediction["is_fraud_predicted"]:
        notify.notify_fraud(prediction)

    result["prediction"] = prediction
    return result
