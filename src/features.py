"""Préparation des variables, PARTAGÉE entre l'entraînement et la production.

On n'utilise que des variables qui existent dans le dataset
d'entraînement ET dans la réponse de l'API, avec exactement le même sens.

Volontairement exclus :
- l'heure / la date : l'API ne fournit pas l'heure de la transaction, seulement
  `current_time` (= heure de l'appel), qui n'a aucun lien avec l'heure d'origine ;
- les identifiants et données personnelles (nom, prénom, adresse, cc_num, trans_num) ;
- `is_fraud` : c'est la cible, jamais une variable.
"""
import numpy as np
import pandas as pd

# Date de référence FIXE pour calculer l'âge : on ne dépend jamais de l'horloge.
REF_DATE = pd.Timestamp("2020-12-31")

NUM_FEATURES = ["amt", "city_pop", "age", "distance_km"]
CAT_FEATURES = ["category", "gender"]
FEATURES = NUM_FEATURES + CAT_FEATURES


def haversine_km(lat1, lon1, lat2, lon2):
    """Distance à vol d'oiseau (km) entre le domicile du client et le commerçant."""
    lat1, lon1, lat2, lon2 = map(np.radians, (lat1, lon1, lat2, lon2))
    a = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    return 6371.0 * 2 * np.arcsin(np.sqrt(a))


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """Transforme des transactions brutes (CSV ou API) en variables du modèle."""
    out = pd.DataFrame(index=df.index)
    out["amt"] = df["amt"].astype(float)
    out["city_pop"] = df["city_pop"].astype(float)
    out["age"] = (REF_DATE - pd.to_datetime(df["dob"])).dt.days / 365.25
    out["distance_km"] = haversine_km(df["lat"], df["long"], df["merch_lat"], df["merch_long"])
    out["category"] = df["category"].astype(str)
    out["gender"] = df["gender"].astype(str)
    return out[FEATURES]
