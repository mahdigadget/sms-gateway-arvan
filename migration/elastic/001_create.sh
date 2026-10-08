#!/usr/bin/env bash
# Elasticsearch: ساخت ایندکس sms_messages
# اجرا: ELASTIC=http://127.0.0.1:9200 ./migration/elastic/001_create.sh
#
# ---------------------------------------------------------------------------
# فرمول تعداد شارد (معادل پارتیشن کافکا):
#
#   shards = max(
#     ceil(R_target / R_shard),
#     WhaleSaltBuckets
#   )
#
#   R_target = 10000 msg/s
#   R_shard  ≈ 625 msg/s  →  ceil(10000 / 625) = 16
#   WhaleSaltBuckets = 16
#
# بنابراین number_of_shards = 16 (همان مقدار داخل 001_sms_messages.json)
# ---------------------------------------------------------------------------

set -euo pipefail

ELASTIC="${ELASTIC:-http://127.0.0.1:9200}"
INDEX="${INDEX:-sms_messages}"
DIR="$(cd "$(dirname "$0")" && pwd)"

curl -sS -X PUT "${ELASTIC}/${INDEX}" \
  -H "Content-Type: application/json" \
  --data-binary @"${DIR}/001_sms_messages.json"

echo
echo "elastic index ready: ${INDEX}"
