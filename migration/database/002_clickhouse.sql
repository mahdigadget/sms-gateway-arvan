-- ClickHouse: جدول تحلیلی پیامک برای گزارش
-- اجرا: clickhouse-client --multiquery < migration/database/002_clickhouse.sql
--
-- پارتیشن ماهانه: PARTITION BY toYYYYMM(created_at)
-- هر ماه یک پارتیشن جدا (مثلاً 202610) تا prune روی بازهٔ تاریخ سریع بماند.

CREATE DATABASE IF NOT EXISTS sms;

CREATE TABLE IF NOT EXISTS sms.sms_messages
(
    tenant_id       String,
    id              UUID,
    customer_id     UUID,
    idempotency_key String,
    kind            LowCardinality(String),
    recipient       String,
    sender          String,
    body            String,
    cost            Int64,
    event_type      LowCardinality(String),
    deadline_at     Nullable(DateTime64(3, 'UTC')),
    created_at      DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (tenant_id, created_at, idempotency_key);
