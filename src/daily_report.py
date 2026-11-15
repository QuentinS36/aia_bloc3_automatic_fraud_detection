"""Rapport quotidien : statistiques de la veille, interrogées dans Neon.

Inclut un indicateur `n_unscored` : transactions stockées (`raw/`) mais
jamais scorées (aucune ligne dans `predictions`). Ce n'est théoriquement
jamais censé arriver (chaque transaction valide est scorée juste après son
stockage), mais un incident (bug, panne) peut casser ce lien silencieusement.
Plutôt que de corriger ça au cas par cas, on le surveille ici en continu.
"""
import os
import sys
from datetime import datetime, timedelta, timezone

import psycopg

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import notify

NEON_DSN = os.environ["NEON_CONNECTION_STRING"]


def compute_report(conn, day) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*), coalesce(sum(amt), 0) FROM transactions WHERE received_at::date = %s", (day,)
        )
        n_transactions, total_amt = cur.fetchone()

        cur.execute(
            "SELECT count(*) FROM predictions WHERE received_at::date = %s AND is_fraud_predicted", (day,)
        )
        (n_fraud_predicted,) = cur.fetchone()

        cur.execute(
            "SELECT count(*) FROM predictions WHERE received_at::date = %s AND is_fraud_actual", (day,)
        )
        (n_fraud_actual,) = cur.fetchone()

        # Rappel calculé UNIQUEMENT sur les transactions inédites (cf. cadrage :
        # ~85 % des transactions ont été vues à l'entraînement, ce qui fausserait
        # la mesure si on ne filtrait pas).
        cur.execute(
            """SELECT count(*) FILTER (WHERE is_fraud_actual),
                      count(*) FILTER (WHERE is_fraud_actual AND is_fraud_predicted)
               FROM predictions WHERE received_at::date = %s AND NOT seen_in_training""",
            (day,),
        )
        n_fraud_unseen, n_fraud_unseen_detected = cur.fetchone()

        cur.execute("SELECT count(*) FROM quarantine WHERE received_at::date = %s", (day,))
        (n_quarantine,) = cur.fetchone()

        cur.execute(
            """SELECT count(*) FROM transactions t
               LEFT JOIN predictions p ON p.trans_num = t.trans_num
               WHERE t.received_at::date = %s AND p.trans_num IS NULL""",
            (day,),
        )
        (n_unscored,) = cur.fetchone()

    return {
        "day": str(day),
        "n_transactions": n_transactions,
        "total_amt": float(total_amt),
        "n_fraud_predicted": n_fraud_predicted,
        "n_fraud_actual": n_fraud_actual,
        "n_fraud_unseen": n_fraud_unseen,
        "n_fraud_unseen_detected": n_fraud_unseen_detected,
        "n_quarantine": n_quarantine,
        "n_unscored": n_unscored,
    }


def render_message(stats: dict) -> str:
    recall = (
        f"{stats['n_fraud_unseen_detected']}/{stats['n_fraud_unseen']}"
        if stats["n_fraud_unseen"] else "aucune fraude inédite ce jour-là"
    )
    lines = [
        f"📊 **Rapport quotidien — {stats['day']}**",
        f"Transactions traitées : {stats['n_transactions']} (total {stats['total_amt']:.2f} €)",
        f"Alertes de fraude envoyées : {stats['n_fraud_predicted']}",
        f"Fraudes réelles (vérité terrain) : {stats['n_fraud_actual']}",
        f"Dont détectées (sur transactions inédites, non vues à l'entraînement) : {recall}",
        f"Lignes en quarantaine (qualité) : {stats['n_quarantine']}",
    ]
    if stats["n_unscored"]:
        lines.append(f"⚠️ {stats['n_unscored']} transaction(s) reçues mais jamais scorées — à investiguer.")
    return "\n".join(lines)


def run(day=None) -> dict:
    day = day or (datetime.now(timezone.utc).date() - timedelta(days=1))
    with psycopg.connect(NEON_DSN) as conn:
        stats = compute_report(conn, day)
    notify.send(render_message(stats))
    return stats


if __name__ == "__main__":
    print(run())
