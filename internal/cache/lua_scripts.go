package cache

// ReserveScript:
// KEYS[1]: wallet key -> "wallet:{tenant_id}" (Hash: "available", "reserved")
// KEYS[2]: idempotency key -> "idemp:{tenant_id}:{request_hash}"
// ARGV[1]: هزینه (در اینجا عدد 1)
// ARGV[2]: مدت‌زمان انقضای کلید idempotency به ثانیه (۲۴ ساعت = 86400)
// ARGV[3]: زمان یونیکس رزرو، برای مجموعه‌مرتب holds:processing
//
// خروجی‌ها:
//
//	 1: موفقیت‌آمیز، رزرو انجام شد و کلید processing شد.
//	-1: درخواست هنوز processing است و جواب نهایی نداریم.
//	 رشتهٔ JSON: جواب ذخیره‌شدهٔ درخواست قبلی.
//	-2: کلید کاربر در ردیس وجود ندارد (Cold Start -> خطای 503).
//	-3: موجودی ناکافی است.
const ReserveScript = `
local existing = redis.call("GET", KEYS[2])
if existing == "processing" then
    return -1
end
if existing then
    return existing
end

if redis.call("EXISTS", KEYS[1]) == 0 then
    return -2
end

local available = tonumber(redis.call("HGET", KEYS[1], "available") or "0")
local cost = tonumber(ARGV[1])

if available < cost then
    return -3
end

redis.call("HINCRBY", KEYS[1], "available", -cost)
redis.call("HINCRBY", KEYS[1], "reserved", cost)
redis.call("SET", KEYS[2], "processing", "EX", ARGV[2])
redis.call("ZADD", "holds:processing", ARGV[3], KEYS[2])

return 1
`

// StoreScript:
// بعد از ack کافکا، جواب نهایی را جای processing می‌نویسد
// KEYS[1]: idempotency key -> "idemp:{tenant_id}:{request_hash}"
// ARGV[1]: JSON جواب (http_status + body)
// ARGV[2]: مدت‌زمان انقضای کلید به ثانیه
//
// اگر جواب قبلی نشسته باشد دست نمی‌زنیم.
// اگر کلید نباشد (وسط کار پاک شده) باز هم جواب را می‌نویسیم تا تکرار دوباره رزرو نکند.
const StoreScript = `
local current = redis.call("GET", KEYS[1])
if current and current ~= "processing" then
    return 0
end

redis.call("SET", KEYS[1], ARGV[1], "EX", ARGV[2])
return 1
`

// ExpireHoldScript:
// sweeper برای رزرو گیرکرده. فقط اگر کلید هنوز processing باشد پول برمی‌گردد.
// KEYS[1]: wallet key -> "wallet:{tenant_id}"
// KEYS[2]: idempotency key -> "idemp:{tenant_id}:{request_hash}"
// KEYS[3]: مجموعه‌مرتب holds:processing
// ARGV[1]: هزینه برگشتی
// ARGV[2]: عضو مجموعه‌مرتب (همان KEYS[2])
//
//	1: رزرو برگشت
//	0: جواب نهایی نشسته یا کلید رفته؛ فقط hold حذف شد
const ExpireHoldScript = `
if redis.call("GET", KEYS[2]) ~= "processing" then
    redis.call("ZREM", KEYS[3], ARGV[2])
    return 0
end

local cost = tonumber(ARGV[1])
redis.call("HINCRBY", KEYS[1], "available", cost)
redis.call("HINCRBY", KEYS[1], "reserved", -cost)
redis.call("DEL", KEYS[2])
redis.call("ZREM", KEYS[3], ARGV[2])
return 1
`

// RollbackScript:
// فقط اگر کلید هنوز processing باشد پول برمی‌گردد. بار دوم no-op است.
// KEYS[1]: wallet key -> "wallet:{tenant_id}"
// KEYS[2]: idempotency key -> "idemp:{tenant_id}:{request_hash}"
// KEYS[3]: holds:processing
// ARGV[1]: هزینه برگشتی (1)
const RollbackScript = `
local current = redis.call("GET", KEYS[2])
if current ~= "processing" then
    redis.call("ZREM", KEYS[3], KEYS[2])
    return 0
end

local cost = tonumber(ARGV[1])
redis.call("HINCRBY", KEYS[1], "available", cost)
redis.call("HINCRBY", KEYS[1], "reserved", -cost)
redis.call("DEL", KEYS[2])
redis.call("ZREM", KEYS[3], KEYS[2])
return 1
`

// CommitScript:
// reserved را برای هر idemp فقط یک بار کم می‌کند.
// KEYS[1]: wallet key -> "wallet:{tenant_id}"
// KEYS[2]: acct:done:{idemp_hash}
// KEYS[3]: holds:processing
// ARGV[1]: هزینه (1)
// ARGV[2]: ttl فلگ، ثانیه
// ARGV[3]: عضو zset، همان کلید idempotency
const CommitScript = `
if redis.call("SET", KEYS[2], "1", "NX", "EX", ARGV[2]) then
    redis.call("HINCRBY", KEYS[1], "reserved", -tonumber(ARGV[1]))
    redis.call("ZREM", KEYS[3], ARGV[3])
    return 1
end
return 0
`

// RefundScript:
// available را یک بار برمی‌گرداند. اگر Commit هنوز reserved را کم نکرده، همان‌جا خالی می‌شود.
// KEYS[1]: wallet:{tenant}
// KEYS[2]: saga:refund:{idemp}
// KEYS[3]: acct:done:{idemp}
// KEYS[4]: holds:processing
// ARGV[1]: مبلغ
// ARGV[2]: ttl فلگ
// ARGV[3]: عضو zset
const RefundScript = `
if redis.call("SET", KEYS[2], "1", "NX", "EX", ARGV[2]) then
    redis.call("HINCRBY", KEYS[1], "available", tonumber(ARGV[1]))
    if redis.call("SET", KEYS[3], "1", "NX", "EX", ARGV[2]) then
        redis.call("HINCRBY", KEYS[1], "reserved", -tonumber(ARGV[1]))
        redis.call("ZREM", KEYS[4], ARGV[3])
    end
    return 1
end
return 0
`
