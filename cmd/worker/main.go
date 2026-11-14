package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"taskqueue/internal/handlers"
	"taskqueue/internal/task"
)

const (
	defaultQueue       = "queue:default"
	processingQueue    = "queue:processing"
	delayedQueue       = "queue:delayed"
	deadQueue          = "queue:dead"
	visibilityTimeout  = 30 * time.Second
	reaperInterval     = 10 * time.Second
	schedulerInterval  = 1 * time.Second
	maxBackoff         = 30 * time.Second
	maxShift           = 5
	maxAttempts        = 5
	processedKeyPrefix = "processed:"
	processedTTL       = 24 * time.Hour
)


func claimTask(ctx context.Context, rdb *redis.Client, queue string) (*task.Task, string, error) {
	res, err := rdb.BRPop(ctx, 0, queue).Result()
	if err != nil {
		return nil, "", fmt.Errorf("brpop: %w", err)
	}
	var t task.Task
	if err := json.Unmarshal([]byte(res[1]), &t); err != nil {
		return nil, "", fmt.Errorf("decode: %w", err)
	}
	t.StartedAt = time.Now().UTC()
	t.Attempts++
	data, err := json.Marshal(t)
	if err != nil {
		return nil, "", fmt.Errorf("re-encode: %w", err)
	}
	if err := rdb.LPush(ctx, processingQueue, data).Err(); err != nil {
		return nil, "", fmt.Errorf("lpush processing: %w", err)
	}
	return &t, string(data), nil
}

func nextRetryDelay(attempts int) time.Duration {
	if attempts >= maxShift {
		return maxBackoff
	}
	return time.Second << attempts
}

func scheduleRetry(ctx context.Context, rdb *redis.Client, t *task.Task, entry string) error {
	removed, err := rdb.LRem(ctx, processingQueue, 1, entry).Result()
	if err != nil {
		return fmt.Errorf("lrem processing: %w", err)
	}
	if removed == 0 {
		return nil
	}
	if t.Attempts >= maxAttempts {
		if err := rdb.LPush(ctx, deadQueue, entry).Err(); err != nil {
			return fmt.Errorf("lpush dead: %w", err)
		}
		log.Printf("task sent to dead letter queue id=%s type=%s attempts=%d", t.ID, t.Type, t.Attempts)
		return nil
	}
	readyAt := time.Now().UTC().Add(nextRetryDelay(t.Attempts)).UnixMilli()
	z := redis.Z{Score: float64(readyAt), Member: entry}
	if err := rdb.ZAdd(ctx, delayedQueue, z).Err(); err != nil {
		return fmt.Errorf("zadd delayed: %w", err)
	}
	return nil
}

func completeTask(ctx context.Context, rdb *redis.Client, entry string) error {
	return rdb.LRem(ctx, processingQueue, 1, entry).Err()
}

func dispatchWithDedup(ctx context.Context, rdb *redis.Client, registry *task.Registry, t task.Task) (bool, error) {
	key := processedKeyPrefix + t.ID
	exists, err := rdb.Exists(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("dedup exists: %w", err)
	}
	if exists == 1 {
		return true, nil
	}
	if err := registry.Dispatch(t); err != nil {
		return false, err
	}
	if err := rdb.Set(ctx, key, "1", processedTTL).Err(); err != nil {
		return false, fmt.Errorf("dedup set marker: %w", err)
	}
	return false, nil
}

func runWorker(ctx context.Context, rdb *redis.Client, registry *task.Registry, queue string, workerID int) {
	log.Printf("worker %d waiting for tasks on %s", workerID, queue)
	for {
		t, entry, err := claimTask(ctx, rdb, queue)
		if err != nil {
			log.Printf("worker %d claim error: %v", workerID, err)
			continue
		}
		age := time.Since(t.EnqueuedAt).Round(time.Millisecond)
		log.Printf("worker %d picked id=%s type=%s age=%v attempts=%d", workerID, t.ID, t.Type, age, t.Attempts)
		dup, dispatchErr := dispatchWithDedup(ctx, rdb, registry, *t)
		if dispatchErr != nil {
			log.Printf("worker %d dispatch error id=%s type=%s attempts=%d: %v", workerID, t.ID, t.Type, t.Attempts, dispatchErr)
			if err := scheduleRetry(ctx, rdb, t, entry); err != nil {
				log.Printf("worker %d schedule retry error id=%s: %v", workerID, t.ID, err)
			}
			continue
		}
		if dup {
			log.Printf("worker %d skipped duplicate id=%s type=%s attempts=%d", workerID, t.ID, t.Type, t.Attempts)
		}
		if err := completeTask(ctx, rdb, entry); err != nil {
			log.Printf("worker %d complete error id=%s: %v", workerID, t.ID, err)
		}
	}
}

func runReaper(ctx context.Context, rdb *redis.Client, queue string) {
	log.Printf("reaper running every %v, visibility timeout=%v", reaperInterval, visibilityTimeout)
	ticker := time.NewTicker(reaperInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := reapStale(ctx, rdb, queue)
			if err != nil {
				log.Printf("reaper error: %v", err)
				continue
			}
			if n > 0 {
				log.Printf("reaper requeued %d stale tasks", n)
			}
		}
	}
}

func reapStale(ctx context.Context, rdb *redis.Client, queue string) (int, error) {
	entries, err := rdb.LRange(ctx, processingQueue, 0, -1).Result()
	if err != nil {
		return 0, fmt.Errorf("lrange: %w", err)
	}
	now := time.Now().UTC()
	reaped := 0
	for _, entry := range entries {
		var t task.Task
		if err := json.Unmarshal([]byte(entry), &t); err != nil {
			continue
		}
		if t.StartedAt.IsZero() || now.Sub(t.StartedAt) < visibilityTimeout {
			continue
		}
		removed, err := rdb.LRem(ctx, processingQueue, 1, entry).Result()
		if err != nil || removed == 0 {
			continue
		}
		if err := rdb.RPush(ctx, queue, entry).Err(); err != nil {
			continue
		}
		reaped++
	}
	return reaped, nil
}

func runScheduler(ctx context.Context, rdb *redis.Client, queue string) {
	log.Printf("scheduler running every %v", schedulerInterval)
	ticker := time.NewTicker(schedulerInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := moveReadyRetries(ctx, rdb, queue)
			if err != nil {
				log.Printf("scheduler error: %v", err)
				continue
			}
			if n > 0 {
				log.Printf("scheduler moved %d ready retries to %s", n, queue)
			}
		}
	}
}

func moveReadyRetries(ctx context.Context, rdb *redis.Client, queue string) (int, error) {
	now := time.Now().UTC().UnixMilli()
	entries, err := rdb.ZRangeByScore(ctx, delayedQueue, &redis.ZRangeBy{
		Min: "-inf",
		Max: fmt.Sprintf("%d", now),
	}).Result()
	if err != nil {
		return 0, fmt.Errorf("zrangebyscore: %w", err)
	}
	moved := 0
	for _, entry := range entries {
		removed, err := rdb.ZRem(ctx, delayedQueue, entry).Result()
		if err != nil || removed == 0 {
			continue
		}
		if err := rdb.LPush(ctx, queue, entry).Err(); err != nil {
			continue
		}
		moved++
	}
	return moved, nil
}

func runWorkerPool(ctx context.Context, rdb *redis.Client, registry *task.Registry, queue string, n int) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		runReaper(ctx, rdb, queue)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		runScheduler(ctx, rdb, queue)
	}()
	for i := 0; i < n; i++ {
		wg.Add(1)
		workerID := i
		go func() {
			defer wg.Done()
			runWorker(ctx, rdb, registry, queue, workerID)
		}()
	}
	wg.Wait()
}

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	ctx := context.Background()

	registry := task.NewRegistry()
	handlers.RegisterAll(registry)

	runWorkerPool(ctx, rdb, registry, defaultQueue, 10)
}
