"""Requêtes SQL du tableau de bord.

Volontairement séparées de app.py (l'interface Streamlit elle-même) : ces
fonctions ne dépendent que de psycopg/pandas, donc testables sans lancer
Streamlit. app.py se contente de les appeler et de les mettre en cache.
"""
import pandas as pd


def kpis(conn, since) -> dict:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM transactions WHERE received_at >= %s", (since,))
        (n_tx,) = cur.fetchone()

        cur.execute(
            "SELECT count(*) FROM predictions WHERE received_at >= %s AND is_fraud_predicted", (since,)
        )
        (n_alerts,) = cur.fetchone()

        cur.execute("SELECT count(*) FROM quarantine WHERE received_at >= %s", (since,))
        (n_quarantine,) = cur.fetchone()

        # Transactions stockées mais jamais scorées : signal d'incident (cf. rapport quotidien).
        cur.execute(
            """SELECT count(*) FROM transactions t
               LEFT JOIN predictions p ON p.trans_num = t.trans_num
               WHERE t.received_at >= %s AND p.trans_num IS NULL""",
            (since,),
        )
        (n_unscored,) = cur.fetchone()

        # Rappel calculé UNIQUEMENT sur les transactions inédites (cf. cadrage du modèle).
        cur.execute(
            """SELECT count(*) FILTER (WHERE is_fraud_actual),
                      count(*) FILTER (WHERE is_fraud_actual AND is_fraud_predicted)
               FROM predictions WHERE received_at >= %s AND NOT seen_in_training""",
            (since,),
        )
        n_unseen_fraud, n_unseen_detected = cur.fetchone()

    return {
        "n_tx": n_tx, "n_alerts": n_alerts, "n_quarantine": n_quarantine, "n_unscored": n_unscored,
        "n_unseen_fraud": n_unseen_fraud, "n_unseen_detected": n_unseen_detected,
    }


def daily_volume(conn, since) -> pd.DataFrame:
    return pd.read_sql(
        """SELECT date_trunc('day', received_at)::date AS jour, count(*) AS transactions
           FROM transactions WHERE received_at >= %(since)s GROUP BY 1 ORDER BY 1""",
        conn, params={"since": since},
    )


def daily_fraud(conn, since) -> pd.DataFrame:
    """Alertes vs. réalité, par jour : sert à repérer une dérive du taux de fraude
    (on a déjà observé qu'il varie fortement dans le temps, cf. cadrage du modèle)."""
    return pd.read_sql(
        """SELECT date_trunc('day', received_at)::date AS jour,
                  count(*) FILTER (WHERE is_fraud_actual) AS fraudes_reelles,
                  count(*) FILTER (WHERE is_fraud_predicted) AS alertes,
                  count(*) FILTER (WHERE is_fraud_actual AND is_fraud_predicted) AS fraudes_detectees,
                  count(*) AS total
           FROM predictions WHERE received_at >= %(since)s GROUP BY 1 ORDER BY 1""",
        conn, params={"since": since},
    )


def recent_quarantine(conn, limit=20) -> pd.DataFrame:
    return pd.read_sql(
        """SELECT trans_num, received_at, quality_errors
           FROM quarantine ORDER BY received_at DESC LIMIT %(limit)s""",
        conn, params={"limit": limit},
    )


def recent_runs(conn, limit=20) -> pd.DataFrame:
    return pd.read_sql(
        """SELECT dag_id, run_id, day_loaded, started_at,
                  rows_raw, rows_predictions, rows_quarantine, status
           FROM pipeline_runs ORDER BY started_at DESC LIMIT %(limit)s""",
        conn, params={"limit": limit},
    )
