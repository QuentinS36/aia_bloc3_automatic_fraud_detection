"""Tableau de bord de monitoring (C4) : lit Neon en lecture seule.

Volontairement mis en cache (60 s) pour ménager le quota de calcul gratuit
de Neon : le tableau de bord ne sollicite Neon que lorsqu'on le consulte, 
jamais en continu.
"""
import os
import sys
from datetime import datetime, timedelta, timezone

import psycopg
import pandas as pd
import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import queries as q  # noqa: E402

st.set_page_config(page_title="Détection de fraude — Monitoring", layout="wide")

NEON_DSN = os.environ["NEON_CONNECTION_STRING"]


@st.cache_data(ttl=60)
def load_data(since_iso: str):
    """Ouvre une connexion à chaque appel (mise en cache par Streamlit
    seulement via ce décorateur, pas via la connexion elle-même) : Neon peut
    ainsi se remettre en veille librement entre deux consultations du tableau
    de bord, sans risquer de réutiliser une connexion devenue invalide."""
    since = datetime.fromisoformat(since_iso)
    with psycopg.connect(NEON_DSN) as conn:
        return {
            "kpis": q.kpis(conn, since),
            "volume": q.daily_volume(conn, since),
            "fraud": q.daily_fraud(conn, since),
            "quarantine": q.recent_quarantine(conn),
            "runs": q.recent_runs(conn),
        }


st.title("🕵️ Détection de fraude — Monitoring")

days = st.slider("Fenêtre d'observation (jours)", 1, 30, 7)
since = datetime.now(timezone.utc) - timedelta(days=days)
data = load_data(since.isoformat())

k = data["kpis"]
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Transactions", k["n_tx"])
c2.metric("Alertes envoyées", k["n_alerts"])
c3.metric("En quarantaine", k["n_quarantine"])
c4.metric("Non scorées ⚠️", k["n_unscored"])
recall = f"{k['n_unseen_detected']}/{k['n_unseen_fraud']}" if k["n_unseen_fraud"] else "—"
c5.metric("Rappel (transactions inédites)", recall)

st.subheader("Volume quotidien")
if not data["volume"].empty:
    st.line_chart(data["volume"].set_index("jour"))
else:
    st.info("Pas encore de données sur cette période.")

st.subheader("Fraude : alertes vs. réalité, par jour")
if not data["fraud"].empty:
    df = data["fraud"].set_index("jour")
    st.line_chart(df[["fraudes_reelles", "alertes", "fraudes_detectees"]])

    df["taux_reel_%"] = (df["fraudes_reelles"] / df["total"].replace(0, pd.NA) * 100).round(3)
    st.caption("Taux de fraude réel par jour (%) — pour repérer une dérive :")
    st.line_chart(df[["taux_reel_%"]])
else:
    st.info("Pas encore de prédictions sur cette période.")

st.subheader("Dernières lignes en quarantaine")
st.dataframe(data["quarantine"], use_container_width=True)

st.subheader("Journal des exécutions du chargement Neon")
st.dataframe(data["runs"], use_container_width=True)

st.caption(
    "Lecture seule, données mises en cache 60 s pour ménager le quota de calcul gratuit de Neon."
)
