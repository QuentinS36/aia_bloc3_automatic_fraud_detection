"""Chargement par lots (ELT) : lit les zones S3/MinIO du jour, charge dans Neon.

Idempotent et sans état externe : chaque exécution reliste simplement les
objets DU JOUR (au plus ~1 500, cf. volumétrie mesurée) et les charge avec
`ON CONFLICT DO NOTHING`. Une même transaction peut donc être relue plusieurs
fois sans créer de doublon en base. Pas besoin de mémoriser un "curseur"
entre deux exécutions : plus simple, et donc moins de choses qui peuvent
casser.
"""
import hashlib
import json
import os
import sys
from datetime import datetime, timezone

import psycopg

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ingestion as ing 

NEON_DSN = os.environ["NEON_CONNECTION_STRING"]
HASH_SALT = os.environ["HASH_SALT"]  # doit rester STABLE dans le temps (voir .env.example)
SCHEMA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "infra", "neon", "schema.sql")


def hash_cc_num(cc_num) -> str:
    """Pseudonymisation du numéro de carte (RGPD) : jamais stocké en clair
    dans la base structurée. Le sel (HASH_SALT) empêche de reconstituer le
    numéro par une simple table de correspondance."""
    return hashlib.sha256(f"{HASH_SALT}:{cc_num}".encode()).hexdigest()


def ensure_schema(conn) -> None:
    with open(SCHEMA_PATH, encoding="utf-8") as f:
        with conn.cursor() as cur:
            cur.execute(f.read())
    conn.commit()


def list_day_objects(zone: str, day: str) -> list[dict]:
    """Lit tous les objets JSON d'une zone (`raw`, `predictions`, `quarantine`)
    pour le jour `day` (format AAAA-MM-JJ)."""
    s3 = ing.s3_client()
    prefix = f"{zone}/dt={day}/"
    keys, token = [], None
    while True:
        kwargs = {"Bucket": ing.BUCKET, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)
        keys += [o["Key"] for o in resp.get("Contents", [])]
        if not resp.get("IsTruncated"):
            break
        token = resp["NextContinuationToken"]

    objects = []
    for key in keys:
        body = s3.get_object(Bucket=ing.BUCKET, Key=key)["Body"].read()
        objects.append(json.loads(body))
    return objects


def _received_at(row: dict):
    ms = row.get("current_time")
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc) if ms else None


def load_transactions(conn, rows: list[dict]) -> int:
    if not rows:
        return 0
    with conn.cursor() as cur:
        for r in rows:
            cur.execute(
                """
                INSERT INTO transactions
                    (trans_num, current_time_ms, received_at, amt, category, gender,
                     city, state, city_pop, cc_num_hash, is_fraud_actual, seen_in_training)
                VALUES
                    (%(trans_num)s, %(current_time)s, %(received_at)s, %(amt)s, %(category)s, %(gender)s,
                     %(city)s, %(state)s, %(city_pop)s, %(cc_hash)s, %(is_fraud)s, %(seen)s)
                ON CONFLICT (trans_num) DO NOTHING
                """,
                {
                    "trans_num": r.get("trans_num"),
                    "current_time": r.get("current_time"),
                    "received_at": _received_at(r),
                    "amt": r.get("amt"),
                    "category": r.get("category"),
                    "gender": r.get("gender"),
                    "city": r.get("city"),
                    "state": r.get("state"),
                    "city_pop": r.get("city_pop"),
                    "cc_hash": hash_cc_num(r.get("cc_num")),
                    "is_fraud": bool(r.get("is_fraud")),
                    "seen": r.get("seen_in_training"),
                },
            )
    conn.commit()
    return len(rows)


def load_predictions(conn, rows: list[dict]) -> int:
    if not rows:
        return 0
    with conn.cursor() as cur:
        for r in rows:
            cur.execute(
                """
                INSERT INTO predictions
                    (trans_num, received_at, score, is_fraud_predicted, is_fraud_actual,
                     model_version, seen_in_training)
                VALUES
                    (%(trans_num)s, %(received_at)s, %(score)s, %(is_fraud_predicted)s,
                     %(is_fraud_actual)s, %(model_version)s, %(seen)s)
                ON CONFLICT (trans_num) DO NOTHING
                """,
                {
                    "trans_num": r.get("trans_num"),
                    "received_at": _received_at(r),
                    "score": r.get("score"),
                    "is_fraud_predicted": r.get("is_fraud_predicted"),
                    "is_fraud_actual": r.get("is_fraud_actual"),
                    "model_version": r.get("model_version"),
                    "seen": r.get("seen_in_training"),
                },
            )
    conn.commit()
    return len(rows)


def load_quarantine(conn, rows: list[dict]) -> int:
    if not rows:
        return 0
    with conn.cursor() as cur:
        for r in rows:
            trans_num = r.get("trans_num") or f"inconnu-{r.get('current_time', 'sans-horodatage')}"
            cur.execute(
                """
                INSERT INTO quarantine (trans_num, received_at, quality_errors)
                VALUES (%(trans_num)s, %(received_at)s, %(errors)s)
                ON CONFLICT (trans_num) DO NOTHING
                """,
                {
                    "trans_num": trans_num,
                    "received_at": _received_at(r) or datetime.now(timezone.utc),
                    "errors": json.dumps(r.get("_quality_errors", [])),
                },
            )
    conn.commit()
    return len(rows)


def log_run(conn, day, started_at, ended_at, counts, status, notes="", dag_id="load_neon", run_id="manuel"):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_runs
                (dag_id, run_id, day_loaded, started_at, ended_at,
                 rows_raw, rows_predictions, rows_quarantine, status, notes)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (dag_id, run_id, day, started_at, ended_at,
             counts["raw"], counts["predictions"], counts["quarantine"], status, notes),
        )
    conn.commit()


def run(dag_id="load_neon", run_id="manuel") -> dict:
    started_at = datetime.now(timezone.utc)
    day = started_at.strftime("%Y-%m-%d")
    counts = {"raw": 0, "predictions": 0, "quarantine": 0}
    status, notes = "success", ""

    with psycopg.connect(NEON_DSN) as conn:
        ensure_schema(conn)
        try:
            counts["raw"] = load_transactions(conn, list_day_objects("raw", day))
            counts["predictions"] = load_predictions(conn, list_day_objects("predictions", day))
            counts["quarantine"] = load_quarantine(conn, list_day_objects("quarantine", day))
        except Exception as e:
            status, notes = "error", str(e)
            raise
        finally:
            log_run(conn, day, started_at, datetime.now(timezone.utc), counts, status, notes, dag_id, run_id)

    return counts


if __name__ == "__main__":
    print(run())
