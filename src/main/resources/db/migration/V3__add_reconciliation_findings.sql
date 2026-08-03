CREATE TABLE reconciliation_findings (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    payment_id    UUID NOT NULL,
    finding_type  VARCHAR(40) NOT NULL,
    detail        TEXT NOT NULL,
    detected_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_reconciliation_findings_payment_id ON reconciliation_findings (payment_id);
