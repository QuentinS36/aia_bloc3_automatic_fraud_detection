"""Pioche une vraie transaction frauduleuse du dataset, au format de l'API.

Sert uniquement à la démonstration : le mode réel ne doit jamais
utiliser `is_fraud` comme variable, mais on s'en sert ici
pour choisir une ligne à injecter.
"""
import time

import pandas as pd

API_COLUMNS = [
    "cc_num", "merchant", "category", "amt", "first", "last", "gender", "street",
    "city", "state", "zip", "lat", "long", "city_pop", "job", "dob", "trans_num",
    "merch_lat", "merch_long", "is_fraud",
]


def pick_fraud_transaction(csv_path: str, seed: int | None = None) -> dict:
    df = pd.read_csv(csv_path, index_col=0)
    frauds = df[df["is_fraud"] == 1]
    row = frauds.sample(1, random_state=seed).iloc[0]

    # .item() convertit les types numpy (int64, float64...) en types Python
    # natifs : indispensable, car json.dumps ne sait pas sérialiser numpy.
    def to_native(v):
        return v.item() if hasattr(v, "item") else v

    payload = {col: to_native(row[col]) for col in API_COLUMNS}
    payload["current_time"] = int(time.time() * 1000)  # comme le ferait vraiment l'API
    payload["_orig_index"] = int(row.name)
    return payload
