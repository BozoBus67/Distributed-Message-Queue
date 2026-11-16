package control

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

var acquireSlotScript = redis.NewScript(`
local key = KEYS[1]
local taskID = ARGV[1]
local max = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])
local time = redis.call('TIME')
local now = tonumber(time[1])
redis.call('ZREMRANGEBYSCORE', key, '-inf', now)
local cur = tonumber(redis.call('ZCARD', key))
if cur >= max then
	return 0
end
redis.call('ZADD', key, now + ttl, taskID)
return 1
`)

var acquireTokenScript = redis.NewScript(`
local tkKey = KEYS[1]
local rkKey = KEYS[2]
local cap = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local time = redis.call('TIME')
local nowMs = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
local tokens = tonumber(redis.call('GET', tkKey) or cap)
local last = tonumber(redis.call('GET', rkKey) or nowMs)
local delta = math.max(0, nowMs - last)
tokens = math.min(cap, tokens + delta * rate / 1000)
if tokens < 1 then
	redis.call('SET', tkKey, tokens)
	redis.call('SET', rkKey, nowMs)
	return 0
end
redis.call('SET', tkKey, tokens - 1)
redis.call('SET', rkKey, nowMs)
return 1
`)

func AcquireSlot(ctx context.Context, rdb *redis.Client, queue, taskID string, max, ttlSeconds int) (bool, error) {
	res, err := acquireSlotScript.Run(ctx, rdb, []string{slotKey(queue)}, taskID, max, ttlSeconds).Int64()
	if err != nil {
		return false, fmt.Errorf("acquire slot: %w", err)
	}
	return res == 1, nil
}

func ReleaseSlot(ctx context.Context, rdb *redis.Client, queue, taskID string) error {
	return rdb.ZRem(ctx, slotKey(queue), taskID).Err()
}

func AcquireToken(ctx context.Context, rdb *redis.Client, queue string, capacity, refillPerSec int) (bool, error) {
	res, err := acquireTokenScript.Run(ctx, rdb, []string{tokensKey(queue), refilledAtKey(queue)}, capacity, refillPerSec).Int64()
	if err != nil {
		return false, fmt.Errorf("acquire token: %w", err)
	}
	return res == 1, nil
}

func slotKey(queue string) string       { return "slot:" + queue }
func tokensKey(queue string) string     { return "rl:" + queue + ":tokens" }
func refilledAtKey(queue string) string { return "rl:" + queue + ":refilled_at" }
