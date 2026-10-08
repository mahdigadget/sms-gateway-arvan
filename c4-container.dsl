/*
 * =====================================================================
 *  C4 Model — Container diagram
 *  System: SMS Gateway Arvan
 * =====================================================================
 *
 * منابع:
 *   • mind_map.drawio  — جریان VIP/Normal، Telecom backpressure، Accounting batch،
 *                        Compensation Saga، Hold Sweeper، outbox برای Debezium
 *   • کد فعلی          — cmd/api (ارسال+ورکرها)، cmd/report (جدا)، Docker Compose
 *
 * Paste در:
 *   • https://structurizr.com/dsl
 *   • Structurizr Lite / Cloud (workspace DSL)
 *   • ابزارهای سازگار با Structurizr DSL (مثلاً IcePanel import)
 *
 * نکتهٔ سطح Container:
 *   Telecom / Accounting / Compensation / HoldSweeper جدا Deploy نمی‌شوند؛
 *   همگی goroutine داخل کانتینر «API Process» هستند (همان cmd/api).
 *   برای جزئیات داخلی‌شان از mind_map یا سطح Component استفاده کنید.
 */

workspace "SMS Gateway Arvan" "درگاه ارسال و گزارش پیامک." {

    !identifiers hierarchical

    model {

        # ---------- Actors & external systems ----------

        customer = person "Customer" "سامانه یا کاربر بالادستی که پیام می‌فرستد و گزارش می‌گیرد."

        telco = softwareSystem "Telco Gateway" "درگاه اپراتور. در کد: sendToTelcoGateway (فعلاً stub)." {
            tags "External"
        }

        analyticsPipeline = softwareSystem "Analytics Pipeline" "خارج از این ریپو. outbox را (مثلاً با Debezium) به ClickHouse می‌رساند — اشارهٔ mind_map." {
            tags "External"
        }

        observability = softwareSystem "Observability Platform" "OTLP Collector + Prometheus + Grafana + Loki + Tempo (compose)." {
            tags "External"
        }

        # ---------- Softwaresystem under design ----------

        sms = softwareSystem "SMS Gateway" "رزرو اعتبار، صف، تحویل مخابرات، حسابداری/جبران، گزارش." {

            api = container "API Process" \
                "یک پروسهٔ Deploy‌شونده (cmd/api، پورت 9095).\n\nHTTP:\n- POST /api/v1/sms/vip\n- POST /api/v1/sms/normal\n\nمسیر ارسال (mind_map):\nidempotency → Reserve Redis → whale salt → Produce Kafka → StoreResponse 202\nشکست Kafka → Rollback\n\nWorkers داخل همین کانتینر:\n1) Telecom [telecom-group]\n   Consume sms.vip + sms.normal\n   VIP latency >90ms → pause normal partitions\n   <50ms → resume\n   deadline_at از created_at → compensation\n   tele:dispatched IN_FLIGHT|SENT → Telco\n2) Accounting [accounting-group]\n   Batch flush → outbox_events + wallet deduct\n   سپس Commit reserved در Redis\n   DB health هر 2s → pause/resume consumer\n3) Compensation [accounting-compensation-group]\n   Saga: rejected tombstone | refunded + RefundAvailable\n4) Hold Sweeper\n   هر 5s holds:processing یتیم (>2min)\n\nOTel: Golden Signals + messaging traces + correlated logs" \
                "Go · Gin · confluent-kafka-go" {
                tags "Application"
            }

            reportApi = container "Report Process" \
                "پروسهٔ جدا (cmd/report، پورت 9096) تا اسکن ClickHouse مسیر ارسال ۱۰k msg/s را نبندد.\n\nHTTP:\n- GET /api/v1/sms/report/messages\n- GET /api/v1/sms/report/summary\n\nفقط خواندن؛ نوشتن در این ریپو نیست." \
                "Go · Gin · clickhouse-go" {
                tags "Application"
            }

            redis = container "Redis" \
                "State داغ و قفل توزیع‌شده.\n\nKeys:\n- wallet:{tenant} → available, reserved\n- idemp:{tenant}:{hash} → processing | JSON 202 (TTL 24h)\n- holds:processing → ZSET زمان رزرو\n- tele:dispatched:{hash} → IN_FLIGHT → SENT\n- acct:done:{hash}, saga:refund:{hash}" \
                "Redis 8" {
                tags "Datastore"
            }

            database = container "PostgreSQL" \
                "منبع حقیقت حسابداری.\n\nTables:\n- wallet (customer_id, available_balance)\n- outbox_events (event_type, event_version, payload jsonb, idempotency_key UNIQUE)\n\nevent_type ∈ {message.accepted, message.rejected, message.refunded}\nذهنیت mind_map: outbox خوراک Debezium/ClickHouse." \
                "PostgreSQL 18" {
                tags "Datastore"
            }

            broker = container "Apache Kafka" \
                "باس و جداسازی اولویت.\n\nTopics (migration/003_kafka.sh):\n- sms.vip — 16 partitions · acks=all · linger 0\n- sms.normal — 16 partitions · acks=all · linger 20 · snappy\n- sms.accounting.compensation — 4 partitions\n\nچرا 16؟\npartitions = max(ceil(R_target / R_partition), N_consumers, WhaleSaltBuckets)\n= max(ceil(10000/625), …, 16) = 16\nWhale salt در producer: tenant:N روی چند پارتیشن.\n\nTimestamp = سیگنال بک‌پرشر ۹۰ms\ncreated_at در payload = زمان دریافت API" \
                "Apache Kafka 4" {
                tags "MessageBroker"
            }

            warehouse = container "ClickHouse" \
                "انبار تحلیلی گزارش.\n\nsms.sms_messages\nENGINE ReplacingMergeTree\nPARTITION BY toYYYYMM(created_at)\nORDER BY (tenant_id, created_at, idempotency_key)\n\nReport با FINAL می‌خواند. Insert از Analytics Pipeline." \
                "ClickHouse 26" {
                tags "Datastore"
            }
        }

        # ---------- Relationships ----------

        customer -> sms.api "ارسال VIP و Normal" "JSON/HTTPS"
        customer -> sms.reportApi "گزارش messages و summary" "JSON/HTTPS"

        sms.api -> sms.redis "Reserve, Store, Rollback, Commit, Refund, dispatch lock, holds" "Redis"
        sms.api -> sms.broker "Produce/Consume vip, normal, compensation" "Kafka"
        sms.api -> sms.database "outbox insert/update, wallet debit/credit, sweeper lookup" "SQL"
        sms.api -> telco "Deliver SMS" "Telco API"
        sms.api -> observability "OTLP metrics, traces, logs" "gRPC :4317"

        sms.reportApi -> sms.warehouse "Query list/summary (FINAL)" "Native :9000"
        sms.reportApi -> observability "OTLP metrics, traces, logs" "gRPC :4317"

        analyticsPipeline -> sms.database "CDC / read outbox_events" "Debezium or equivalent"
        analyticsPipeline -> sms.warehouse "Insert sms_messages" "ClickHouse insert"
    }

    views {

        systemContext sms "1-System-Context" {
            include *
            autoLayout lr
            description "Context: مشتری، SMS Gateway، Telco، Pipeline تحلیلی، Observability."
        }

        container sms "2-Containers" {
            include *
            autoLayout lr
            description "Container view اصلی — قابل Deployها و datastoreها مطابق docker-compose و کد."
        }

        container sms "3-Containers-Send" {
            include customer sms.api sms.redis sms.broker sms.database telco
            autoLayout lr
            description "مسیر ارسال mind_map: Client → API → Redis/Kafka/Postgres/Telco."
        }

        container sms "4-Containers-Report" {
            include customer sms.reportApi sms.warehouse analyticsPipeline
            autoLayout lr
            description "مسیر گزارش جدا از ارسال."
        }

        styles {
            element "Person" {
                shape Person
                background #08427b
                color #ffffff
                fontSize 20
            }
            element "Software System" {
                background #1168bd
                color #ffffff
                fontSize 20
            }
            element "Container" {
                background #438dd5
                color #ffffff
                fontSize 18
            }
            element "Application" {
                background #1b7f3a
                color #ffffff
            }
            element "Datastore" {
                shape Cylinder
                background #f4d03f
                color #1a1a1a
            }
            element "MessageBroker" {
                shape Pipe
                background #e67e22
                color #ffffff
            }
            element "External" {
                background #8a8a8a
                color #ffffff
            }
            relationship {
                routing Orthogonal
                thickness 2
                fontSize 14
            }
        }
    }
}
