-- V1: core payment lifecycle tables (Tier 1 scope)
-- No outbox_events table yet — v1 publishes to Kafka post-commit in
-- application code. Known gap: possible crash between commit and publish.
-- Documented trade-off, not an oversight — see README design decisions.

CREATE TABLE payments (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    merchant_id      VARCHAR(255) NOT NULL,
    amount           NUMERIC(19, 4) NOT NULL CHECK (amount > 0),
    currency         VARCHAR(3) NOT NULL,
    status           VARCHAR(20) NOT NULL,
    idempotency_key  VARCHAR(255) NOT NULL,
    version          INTEGER NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_payments_idempotency_key UNIQUE (idempotency_key)
);

CREATE INDEX idx_payments_status ON payments (status);
CREATE INDEX idx_payments_merchant_id ON payments (merchant_id);

CREATE TABLE ledger_entries (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    payment_id      UUID NOT NULL REFERENCES payments (id),
    account         VARCHAR(30) NOT NULL,
    entry_type      VARCHAR(10) NOT NULL CHECK (entry_type IN ('DEBIT', 'CREDIT')),
    amount          NUMERIC(19, 4) NOT NULL CHECK (amount > 0),
    related_event   VARCHAR(20) NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()

    -- Deliberately no updated_at: rows in this table are never updated,
    -- only inserted. That immutability is the whole point of the ledger.
);

CREATE INDEX idx_ledger_entries_payment_id ON ledger_entries (payment_id);
