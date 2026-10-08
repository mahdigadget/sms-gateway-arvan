sudo apt update
sudo apt install -y build-essential pkgconf

# SMS Gateway Arvan

درگاه پیامک با دو باینری جدا: `api` برای ارسال (پورت `9095`) و `report` برای گزارش از ClickHouse (پورت `9096`).

## بالا آوردن پروژه

### ۱. استک با Docker Compose

```bash
docker compose up -d --build
```

سرویس‌ها:

| سرویس | پورت روی هاست | نقش |
| --- | --- | --- |
| api | `9095` | ارسال VIP / Normal |
| report | `9096` | گزارش پیامک |
| redis | `6380` | کیف‌پول و idempotency |
| postgres | `5433` | wallet و outbox |
| kafka | `9094` | صف‌ها (`advertised` داخل شبکه: `kafka:9092`) |
| clickhouse | `9000` / `8123` | دادهٔ گزارش |
| otel-collector | `14317` | دریافت OTLP |
| prometheus | `9090` | متریک |
| grafana | `3010` | متریک / لاگ / تریس |
| loki | `3100` | لاگ |
| tempo | `3200` | تریس |

پایین آوردن:

```bash
docker compose down
```

### ۲. اسکیما و تاپیک‌ها (migration)

جداول و تاپیک‌ها در `migration/` هستند. بعد از بالا آمدن استک:

```bash
# Postgres
psql 'postgres://postgres:1234@127.0.0.1:5433/sms_gateway?sslmode=disable' \
  -f migration/database/001_postgres.sql

# ClickHouse (اگر با volume خالی بالا آمده و init اجرا نشده)
clickhouse-client --host 127.0.0.1 --password clickhouse \
  --multiquery < migration/database/002_clickhouse.sql

# Kafka — از داخل کانتینر، چون advertised برای کلاینت هاست مناسب نیست
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic sms.vip --partitions 16 --replication-factor 1
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic sms.normal --partitions 16 --replication-factor 1
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic sms.accounting.compensation --partitions 4 --replication-factor 1
```

یا اسکریپت آماده (روی بروکری که از هاست در دسترس است):

```bash
BOOTSTRAP=127.0.0.1:9092 ./migration/database/003_kafka.sh
```

ایندکس Elasticsearch (اختیاری):

```bash
ELASTIC=http://127.0.0.1:9200 ./migration/elastic/001_create.sh
```

### ۳. دادهٔ ماک برای تست

```bash
POSTGRES_DSN='postgres://postgres:1234@127.0.0.1:5433/sms_gateway?sslmode=disable' \
REDIS_PORT=6380 ./mock
```

کاربر ماک (همان `tenantId` در Bruno):

`11111111-1111-1111-1111-111111111111`

### ۴. تست API با Bruno

پوشهٔ `bruno/` را در Bruno باز کنید. محیط **Docker** را انتخاب کنید و درخواست‌های send / report را بزنید.

### ۵. تست‌های خودکار

```bash
# فقط یونیت
go test ./...

# یکپارچه (Redis / Postgres / Kafka / ClickHouse با testcontainers)
go test -tags integration -count=1 -timeout 8m ./internal/integration
```

---

## چرا کافکا ۱۶ پارتیشن دارد؟

تاپیک‌های `sms.vip` و `sms.normal` با **۱۶ پارتیشن** ساخته می‌شوند. دلیل هم‌زمان توان هدف و مدل کلید پارتیشن در کد است.

فرمول:

```text
partitions = max(
  ceil(R_target / R_partition),
  N_consumers,
  WhaleSaltBuckets
)
```

| نماد | معنی | مقدار این پروژه |
| --- | --- | --- |
| `R_target` | توان هدف سیستم | حدود `10000` msg/s |
| `R_partition` | توان پایدار تقریبی هر پارتیشن برای پیامک کوچک | حدود `625` msg/s |
| `ceil(10000 / 625)` | حداقل پارتیشن از روی توان | **۱۶** |
| `WhaleSaltBuckets` | ثابت کد برای پخش بار tenantهای پرحجم (`tenant:N`) | **۱۶** |
| `N_consumers` | سقف اینستنس‌های یک consumer group | نباید از تعداد پارتیشن بیشتر شود |

۱۶ پارتیشن یعنی هم توان هدف پوشش داده می‌شود، هم سالت نهنگ واقعاً روی پارتیشن‌های جدا می‌افتد. تاپیک جبران `sms.accounting.compensation` حجم خیلی کمتری دارد و با **۴** پارتیشن کافی است.

جزئیات در `migration/database/003_kafka.sh` و ثابت `WhaleSaltBuckets` در `internal/platform/kafka/producer.go` است.

---

## متریک‌ها (Four Golden Signals)

متریک‌ها فقط روی HTTP و با middleware نوشته می‌شوند (بدنهٔ هندلر دست نمی‌خورد). هر `۱۵` ثانیه با OTLP gRPC به Collector می‌روند. نام سرویس‌ها: `sms-gateway-api` و `sms-gateway-report`.

| متریک | سیگنال | نوع | معنی |
| --- | --- | --- | --- |
| `http.server.requests` | **Traffic** | Counter | تعداد درخواست‌های HTTP تمام‌شده |
| `http.server.request.duration` | **Latency** | Histogram (ثانیه) | مدت اجرای هر درخواست؛ باکت‌ها برای VIP حساس به میلی‌ثانیه تنظیم شده‌اند (`5ms` … `5s`) |
| `http.server.request.errors` | **Errors** | Counter | پاسخ‌هایی با وضعیت `≥ 500` |
| `http.server.active_requests` | **Saturation** | UpDownCounter | تعداد درخواست‌هایی که همین الان داخل سرور در حال پردازش‌اند |

لیبل‌های مشترک:

- `http.request.method` — متد (`POST` / `GET` / …)
- `http.route` — مسیر قالب Gin (مثلاً `/api/v1/sms/vip`)؛ مسیر ناشناس `unmatched`
- `http.response.status_code` — کد پاسخ
- `service.name` — کدام باینری (`sms-gateway-api` یا `sms-gateway-report`)

---

## کجا و چطور داده‌ها را ببینید؟

### Grafana (پیشنهادی)

آدرس: [http://localhost:3010](http://localhost:3010)

دیتاسورس‌ها از قبل provision شده‌اند:

| دیتاسورس | برای چه |
| --- | --- |
| **Prometheus** | متریک‌های Golden Signals |
| **Loki** | لاگ‌ها؛ از `trace_id` به Tempo لینک می‌شود |
| **Tempo** | تریس‌ها؛ از تریس می‌توان به لاگ‌های همان TraceID برگشت |

نمونهٔ کوئری Prometheus در Explore:

```promql
rate(http_server_requests_total{service_name="sms-gateway-api"}[1m])
histogram_quantile(0.99, sum(rate(http_server_request_duration_seconds_bucket{service_name="sms-gateway-api"}[1m])) by (le))
rate(http_server_request_errors_total[1m])
http_server_active_requests
```

نام سری‌ها بعد از export ممکن است با پسوند `_total` / `_seconds` و برچسب `service_name` دیده شوند؛ اگر خالی بود در Prometheus با `{__name__=~".*http_server.*"}` جستجو کنید.

### Prometheus مستقیم

[http://localhost:9090](http://localhost:9090) — همان متریک‌ها از job مربوط به otel-collector (`8889` داخل شبکه، روی هاست `18889`).

### تریس و لاگ

- اپ همهٔ اسپن‌ها را می‌فرستد؛ **Tail Sampling** در Collector است: خطای کامل، جبران Saga (`compensate`)، VIP کندتر از `90ms`، و حدود `10%` بقیه.
- لاگ OTLP است و با `TraceID` / `SpanID` به تریس وصل می‌شود.
- در Grafana: Explore → Tempo (با `service.name`) یا Loki؛ کلیک روی `trace_id` تریس را باز می‌کند.

### دادهٔ کسب‌وکار

| داده | کجا ببینید |
| --- | --- |
| کیف‌پول / outbox | Postgres `5433` — جداول `wallet`، `outbox_events` |
| موجودی لحظه‌ای ارسال | Redis `6380` — کلید `wallet:{tenant_id}` |
| گزارش پیامک | API report روی `9096` یا ClickHouse `sms.sms_messages` |
| تست دستی API | Bruno در پوشهٔ `bruno/` |
