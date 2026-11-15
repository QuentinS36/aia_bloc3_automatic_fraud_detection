-- Schéma Neon (PostgreSQL). Appliqué automatiquement par neon_loader.ensure_schema(),
-- mais peut aussi être exécuté à la main pour inspection.
--
-- Pas de clé étrangère entre les tables : chaque table est chargée
-- indépendamment depuis sa propre zone S3, à des instants légèrement
-- différents. Les coupler fragiliserait le chargement par lots sans
-- bénéfice réel ici.

CREATE TABLE IF NOT EXISTS transactions (
    trans_num        TEXT PRIMARY KEY,
    current_time_ms  BIGINT NOT NULL,
    received_at      TIMESTAMPTZ NOT NULL,
    amt              NUMERIC(12, 2) NOT NULL,
    category         TEXT NOT NULL,
    gender           TEXT,
    city             TEXT,
    state            TEXT,
    city_pop         INTEGER,
    cc_num_hash      TEXT NOT NULL,   -- jamais le numéro de carte en clair (RGPD)
    is_fraud_actual  BOOLEAN,         -- vérité terrain de l'API, pour le monitoring uniquement
    seen_in_training BOOLEAN,
    loaded_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS predictions (
    trans_num          TEXT PRIMARY KEY,
    received_at        TIMESTAMPTZ NOT NULL,
    score              NUMERIC(6, 4) NOT NULL,
    is_fraud_predicted BOOLEAN NOT NULL,
    is_fraud_actual    BOOLEAN,
    model_version      TEXT NOT NULL,
    seen_in_training   BOOLEAN,
    loaded_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS quarantine (
    trans_num      TEXT PRIMARY KEY,
    received_at    TIMESTAMPTZ NOT NULL,
    quality_errors JSONB NOT NULL,
    loaded_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Journal des exécutions du job de chargement (traçabilité, C4).
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id              BIGSERIAL PRIMARY KEY,
    dag_id          TEXT NOT NULL,
    run_id          TEXT NOT NULL,
    day_loaded      DATE NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL,
    ended_at        TIMESTAMPTZ NOT NULL,
    rows_raw        INTEGER NOT NULL DEFAULT 0,
    rows_predictions INTEGER NOT NULL DEFAULT 0,
    rows_quarantine INTEGER NOT NULL DEFAULT 0,
    status          TEXT NOT NULL,
    notes           TEXT
);

CREATE INDEX IF NOT EXISTS idx_predictions_received_at ON predictions (received_at);
CREATE INDEX IF NOT EXISTS idx_transactions_received_at ON transactions (received_at);
