#!/usr/bin/env bash
# Kafka: ساخت تاپیک‌ها و پارتیشن‌ها
# اجرا: BOOTSTRAP=127.0.0.1:9092 ./migration/database/003_kafka.sh
#
# ---------------------------------------------------------------------------
# فرمول تعداد پارتیشن:
#
#   partitions = max(
#     ceil(R_target / R_partition),
#     N_consumers,
#     WhaleSaltBuckets
#   )
#
#   R_target     = توان هدف سیستم (msg/s)          → اینجا 10000
#   R_partition  = توان پایدار تقریبی هر پارتیشن   → ~625 msg/s برای پیامک کوچک
#                  ceil(10000 / 625) = 16
#   N_consumers  = حداکثر اینستنس‌های یک consumer group
#   WhaleSaltBuckets = 16  (همان ثابت کد: کلید نهنگ tenant:N روی چند پارتیشن پخش می‌شود)
#
# بنابراین برای sms.vip و sms.normal → 16 پارتیشن.
# compensation حجم خیلی کمتری دارد → 4 پارتیشن کافی است
#   (ceil(کم‌حجم / 625) و چند consumer جبرانی).
#
# replication-factor: روی کلاستر تک‌نود ۱؛ در پروداکشن معمولاً ۳.
# ---------------------------------------------------------------------------

set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-127.0.0.1:9092}"
RF="${REPLICATION_FACTOR:-1}"
KAFKA_TOPICS_BIN="${KAFKA_TOPICS_BIN:-kafka-topics.sh}"

create_topic() {
  local topic="$1"
  local partitions="$2"
  "${KAFKA_TOPICS_BIN}" --bootstrap-server "${BOOTSTRAP}" \
    --create --if-not-exists \
    --topic "${topic}" \
    --partitions "${partitions}" \
    --replication-factor "${RF}"
}

# VIP: کم‌حجم‌تر از normal ولی latency-sensitive؛ پارتیشن = فرمول بالا (۱۶)
create_topic "sms.vip" 16

# Normal: بخش عمدهٔ ۱۰k msg/s؛ پارتیشن = فرمول بالا (۱۶)
create_topic "sms.normal" 16

# Compensation: رویداد Saga / شکست تحویل؛ پارتیشن = ۴
create_topic "sms.accounting.compensation" 4

echo "kafka topics ready (bootstrap=${BOOTSTRAP}, rf=${RF})"
