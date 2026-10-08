-- جدول تحلیلی پیامک. گزارش فقط از همین جدول می‌خواند.
-- پر شدن جدول (مثلاً از outbox پستگرس) خارج از این پکیج است.
-- event_type همان مقدارهای outbox است: message.accepted / message.rejected / message.refunded
-- kind همان حسابداری است: otp وقتی deadline دارد، وگرنه normal

CREATE DATABASE IF NOT EXISTS sms;

CREATE TABLE IF NOT EXISTS sms.sms_messages
(
    tenant_id        String,
    id               UUID,
    customer_id      UUID,
    idempotency_key  String,
    kind             LowCardinality(String),
    recipient        String,
    sender           String,
    body             String,
    cost             Int64,
    event_type       LowCardinality(String),
    deadline_at      Nullable(DateTime64(3, 'UTC')),
    created_at       DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (tenant_id, created_at, idempotency_key);
