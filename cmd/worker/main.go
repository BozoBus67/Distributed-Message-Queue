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
	defaultQueue      = "queue:default"
	processingQueue   = "queue:processing"
	visibilityTimeout = 30 * time.Second
	reaperInterval    = 10 * time.Second
)

// claimTask pops from the main queue, stamps StartedAt, LPUSHes into the
// processing queue, and returns the task plus the exact processing-queue
// bytes needed for the matching LREM in completeTask.
// Known gap: BRPOP + LPUSH is not atomic. A crash between them loses the task.
// Production closes this with a Lua script.
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
	data, err := json.Marshal(t)
	if err != nil {
		return nil, "", fmt.Errorf("re-encode: %w", err)
	}
	if err := rdb.LPush(ctx, processingQueue, data).Err(); err != nil {
		return nil, "", fmt.Errorf("lpush processing: %w", err)
	}
	return &t, string(data), nil
}

func completeTask(ctx context.Context, rdb *redis.Client, entry string) error {
	return rdb.LRem(ctx, processingQueue, 1, entry).Err()
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
		if err := registry.Dispatch(*t); err != nil {
			log.Printf("worker %d dispatch error id=%s type=%s: %v", workerID, t.ID, t.Type, err)
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
		if err := rdb.LRem(ctx, processingQueue, 1, entry).Err(); err != nil {
			continue
		}
		if err := rdb.RPush(ctx, queue, entry).Err(); err != nil {
			continue
		}
		reaped++
	}
	return reaped, nil
}

func runWorkerPool(ctx context.Context, rdb *redis.Client, registry *task.Registry, queue string, n int) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		runReaper(ctx, rdb, queue)
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
