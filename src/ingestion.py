"""Logique d'ingestion, PARTAGÉE entre les tâches du DAG Airflow.

Rôle : appeler l'API, contrôler la qualité, puis stocker, dans
le bucket S3 (ou son imitation locale MinIO — seul `endpoint_url` change).

Idempotence : chaque transaction est écrite sous la clé `.../{trans_num}.json`.
Si l'API renvoie deux fois la même transaction (cas mesuré : ~2 fois/jour),
la seconde écriture écrase la première avec un contenu identique. Aucune
notification ni aucun compteur n'est donc dupliqué : pas besoin d'une base
d'état séparée pour dédupliquer.
"""
import json
import os
from datetime import datetime, timezone

import boto3
import requests

API_URL = os.environ.get("API_URL", "https://sdacelo-real-time-fraud-detection.hf.space/current-transactions")
BUCKET = os.environ.get("S3_BUCKET", "fraud-detection")
MODEL_METADATA_PATH = os.environ.get("MODEL_METADATA_PATH", "/opt/airflow/models/model_v1_metadata.json")

REQUIRED_COLUMNS = [
    "cc_num", "merchant", "category", "amt", "first", "last", "gender", "street",
    "city", "state", "zip", "lat", "long", "city_pop", "job", "dob", "trans_num",
    "merch_lat", "merch_long", "is_fraud", "current_time",
]


def s3_client():
    """Client boto3 pointé vers MinIO en local ; il suffira de changer
    AWS_ENDPOINT_URL (une variable d'environnement) pour basculer sur le vrai S3."""
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("AWS_ENDPOINT_URL") or None,  # None -> vrai AWS S3
        region_name=os.environ.get("AWS_REGION", "eu-west-3"),
    )


def fetch_transaction() -> dict:
    """Appelle l'API et décode la réponse (double encodage JSON, cf. exploration)."""
    resp = requests.get(API_URL, timeout=30)
    resp.raise_for_status()
    payload = json.loads(resp.json())  # 1er niveau : décodé par requests -> str ; 2e niveau : json.loads
    row = dict(zip(payload["columns"], payload["data"][0]))
    row["_orig_index"] = payload["index"][0]
    return row


def validate(row: dict) -> tuple[bool, list[str]]:
    """Contrôle qualité. Renvoie (valide, liste des erreurs trouvées).

    Volontairement simple et explicite : chaque règle correspond à un besoin
    métier réel, pas à une validation générique."""
    errors = []

    missing = [c for c in REQUIRED_COLUMNS if c not in row]
    if missing:
        errors.append(f"colonnes manquantes : {missing}")
        return False, errors

    if row.get("trans_num") in (None, ""):
        errors.append("trans_num vide (clé d'idempotence indisponible)")

    try:
        amt = float(row["amt"])
        if amt <= 0:
            errors.append(f"amt non positif : {amt}")
    except (TypeError, ValueError):
        errors.append(f"amt non numérique : {row.get('amt')!r}")

    for field in ("lat", "merch_lat"):
        try:
            if not (-90 <= float(row[field]) <= 90):
                errors.append(f"{field} hors intervalle : {row[field]}")
        except (TypeError, ValueError):
            errors.append(f"{field} non numérique : {row.get(field)!r}")

    for field in ("long", "merch_long"):
        try:
            if not (-180 <= float(row[field]) <= 180):
                errors.append(f"{field} hors intervalle : {row[field]}")
        except (TypeError, ValueError):
            errors.append(f"{field} non numérique : {row.get(field)!r}")

    try:
        datetime.strptime(row["dob"], "%Y-%m-%d")
    except (TypeError, ValueError):
        errors.append(f"dob non parsable : {row.get('dob')!r}")

    return len(errors) == 0, errors


def seen_in_training(orig_index: int) -> bool:
    """Indique si cette transaction faisait partie des données vues par le
    modèle (entraînement + validation), pour ne pas fausser le monitoring
    de la performance en production."""
    try:
        with open(MODEL_METADATA_PATH, encoding="utf-8") as f:
            meta = json.load(f)
        return orig_index < meta["index_ranges"]["test"][0]
    except (FileNotFoundError, KeyError):
        return False  # métadonnées absentes : on suppose prudemment "non vu"


def ingestion_day(row: dict) -> str:
    """Date de PARTITIONNEMENT du stockage (jour où NOUS avons reçu la
    transaction), à ne jamais confondre avec la date d'origine de la
    transaction, que l'API ne fournit pas."""
    ms = row.get("current_time")
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc) if ms else datetime.now(timezone.utc)
    return dt.strftime("%Y-%m-%d")


def store(row: dict, valid: bool, errors: list[str]) -> str:
    """Écrit la transaction dans la zone appropriée (`raw/` ou `quarantine/`).
    Renvoie la clé S3 utilisée."""
    zone = "raw" if valid else "quarantine"
    day = ingestion_day(row)
    # Si le schéma est si cassé que `trans_num` lui-même manque, on ne renonce
    # pas à stocker la ligne pour autant : un identifiant de repli, basé
    # sur l'heure d'ingestion, garantit qu'on écrit toujours quelque part.
    trans_id = row.get("trans_num") or f"inconnu-{row.get('current_time', 'sans-horodatage')}"
    key = f"{zone}/dt={day}/{trans_id}.json"

    body = dict(row)
    if valid:
        body["seen_in_training"] = seen_in_training(row["_orig_index"])
    else:
        body["_quality_errors"] = errors

    s3_client().put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )
    return key
