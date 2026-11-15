"""Entraînement du modèle de détection de fraude.

Usage :  python src/train.py data/fraudTest.csv
"""
import json
import os
import sys
from datetime import datetime, timezone

import joblib
import numpy as np
import pandas as pd
import sklearn
from sklearn.compose import ColumnTransformer
from sklearn.dummy import DummyClassifier
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import (average_precision_score, confusion_matrix,
                             precision_recall_curve, precision_score, recall_score)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer, OneHotEncoder


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from features import CAT_FEATURES, FEATURES, NUM_FEATURES, build_features

DATA_PATH = sys.argv[1] if len(sys.argv) > 1 else "data/fraudTest.csv"
VERSION = "v1"
TRAIN_FRAC, VAL_FRAC = 0.70, 0.15          # le reste (15 %) = test
BETA = 2                                    # on privilégie le rappel (rater une fraude coûte plus cher)


def load(path):
    df = pd.read_csv(path, index_col=0, parse_dates=["trans_date_trans_time"])
    return df.sort_values("trans_date_trans_time")


def temporal_split(df):
    """Découpage chronologique : on entraîne sur le passé, on évalue sur le futur."""
    n = len(df)
    i1, i2 = int(n * TRAIN_FRAC), int(n * (TRAIN_FRAC + VAL_FRAC))
    return df.iloc[:i1], df.iloc[i1:i2], df.iloc[i2:]


def describe(name, part):
    print(f"{name:<6}: {len(part):>7} lignes | {part['trans_date_trans_time'].min():%Y-%m-%d} -> "
          f"{part['trans_date_trans_time'].max():%Y-%m-%d} | fraudes = {int(part['is_fraud'].sum())} "
          f"({part['is_fraud'].mean():.3%})")


def build_pipeline():
    """UN seul objet : préparation des variables + encodage + modèle."""
    encode = ColumnTransformer(
        [("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), CAT_FEATURES),
         ("num", "passthrough", NUM_FEATURES)]
    )
    return Pipeline([
        ("features", FunctionTransformer(build_features)),
        ("encode", encode),
        ("clf", HistGradientBoostingClassifier(class_weight="balanced", random_state=42)),
    ])


def choose_threshold(y_val, scores_val):
    """Seuil qui maximise le F-beta sur la validation (paramètre métier à discuter)."""
    p, r, t = precision_recall_curve(y_val, scores_val)
    fbeta = (1 + BETA**2) * p * r / (BETA**2 * p + r + 1e-12)
    return float(t[np.argmax(fbeta[:-1])])


def main():
    df = load(DATA_PATH)
    train, val, test = temporal_split(df)
    for name, part in (("train", train), ("val", val), ("test", test)):
        describe(name, part)

    # Plages d'index d'origine : l'API renvoie `index`, ce qui permettra de savoir
    # si une transaction scorée en production faisait partie des données d'entraînement.
    index_ranges = {n: [int(p.index.min()), int(p.index.max())]
                    for n, p in (("train", train), ("val", val), ("test", test))}
    print("Plages d'index :", index_ranges)
    if index_ranges["train"][1] >= index_ranges["test"][0]:
        print("ATTENTION : l'ordre des index ne suit pas l'ordre chronologique.")

    model = build_pipeline()
    model.fit(train, train["is_fraud"])

    threshold = choose_threshold(val["is_fraud"], model.predict_proba(val)[:, 1])
    scores = model.predict_proba(test)[:, 1]
    pred = (scores >= threshold).astype(int)
    y = test["is_fraud"]

    baseline = DummyClassifier(strategy="prior").fit(train, train["is_fraud"])
    metrics = {
        "pr_auc": round(average_precision_score(y, scores), 4),
        "pr_auc_baseline_hasard": round(average_precision_score(y, baseline.predict_proba(test)[:, 1]), 4),
        "precision": round(precision_score(y, pred, zero_division=0), 4),
        "recall": round(recall_score(y, pred, zero_division=0), 4),
        "threshold": round(threshold, 4),
        "confusion_matrix [[TN, FP], [FN, TP]]": confusion_matrix(y, pred).tolist(),
    }
    print("\n=== Résultats sur le jeu de TEST (jamais vu) ===")
    print(json.dumps(metrics, indent=2))

    joblib.dump({"pipeline": model, "threshold": threshold, "version": VERSION, "features": FEATURES,
                 "test_start_index": index_ranges["test"][0]},
                f"models/model_{VERSION}.joblib")
    meta = {
        "version": VERSION,
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "sklearn_version": sklearn.__version__,
        "train_period": [str(train["trans_date_trans_time"].min()), str(train["trans_date_trans_time"].max())],
        "test_period": [str(test["trans_date_trans_time"].min()), str(test["trans_date_trans_time"].max())],
        "features": FEATURES,
        "index_ranges": index_ranges,
        "metrics_test": metrics,
    }
    with open(f"models/model_{VERSION}_metadata.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)
    print(f"\nModèle sauvegardé : models/model_{VERSION}.joblib (+ metadata.json)")


if __name__ == "__main__":
    main()
