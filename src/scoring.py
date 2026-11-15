"""Scoring d'une transaction avec le modèle entraîné (src/train.py).

Charge une seule fois le modèle par processus (variable de module), pour ne
pas resérialiser le fichier .joblib à chaque exécution du DAG.
"""
import os
import sys
from functools import lru_cache

import joblib
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
import features

MODEL_PATH = os.environ.get("MODEL_PATH", "/opt/airflow/models/model_v1.joblib")


@lru_cache(maxsize=1)
def load_model():
    return joblib.load(MODEL_PATH)


def score(row: dict) -> dict:
    """Calcule le score de fraude d'une transaction (dict brut de l'API).

    Ne renvoie jamais `is_fraud` en entrée du modèle : ce champ n'existe pas
    en conditions réelles de production."""
    artefact = load_model()
    df = pd.DataFrame([row])
    proba = float(artefact["pipeline"].predict_proba(df)[0, 1])
    return {
        "trans_num": row["trans_num"],
        "current_time": row["current_time"],
        "score": round(proba, 4),
        "is_fraud_predicted": proba >= artefact["threshold"],
        "is_fraud_actual": bool(row.get("is_fraud")), 
        "model_version": artefact["version"],
    }
