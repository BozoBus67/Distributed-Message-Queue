package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/redis/go-redis/v9"
	"taskqueue/internal/handlers"
	"taskqueue/internal/task"
)

func popTask(ctx context.Context, rdb *redis.Client, queue string) (*task.Task, error) {
	res, err := rdb.BRPop(ctx, 0, queue).Result()
	if err != nil {
		return nil, fmt.Errorf("brpop: %w", err)
	}
	var t task.Task
	if err := json.Unmarshal([]byte(res[1]), &t); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	return &t, nil
}

func runWorker(ctx context.Context, rdb *redis.Client, registry *task.Registry, queue string, workerID int) {
	log.Printf("worker %d waiting for tasks on %s", workerID, queue)
	for {
		t, err := popTask(ctx, rdb, queue)
		if err != nil {
			log.Printf("worker %d pop error: %v", workerID, err)
			continue
		}
		if err := registry.Dispatch(*t); err != nil {
			log.Printf("worker %d dispatch error type=%s: %v", workerID, t.Type, err)
		}
	}
}

func runWorkerPool(ctx context.Context, rdb *redis.Client, registry *task.Registry, queue string, n int) {
	var wg sync.WaitGroup
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

	runWorkerPool(ctx, rdb, registry, "queue:default", 10)
}
