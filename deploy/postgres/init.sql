CREATE TABLE IF NOT EXISTS wallet (
    customer_id uuid PRIMARY KEY,
    available_balance bigint NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS outbox_events (
    id bigserial PRIMARY KEY,
    event_type text NOT NULL,
    event_version integer NOT NULL,
    payload jsonb NOT NULL,
    idempotency_key text NOT NULL UNIQUE,
    updated_at timestamptz NOT NULL DEFAULT now()
);
