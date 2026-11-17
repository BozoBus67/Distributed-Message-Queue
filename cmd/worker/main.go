package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"taskqueue/internal/control"
	"taskqueue/internal/events"
	"taskqueue/internal/handlers"
	"taskqueue/internal/metrics"
	"taskqueue/internal/queueconfig"
	"taskqueue/internal/task"
)

const (
	processingQueue   = "queue:processing"
	delayedQueue      = "queue:delayed"
	deadQueue         = "queue:dead"
	visibilityTimeout = 30 * time.Second
	reaperInterval    = 10 * time.Second
	schedulerInterval = 1 * time.Second
	maxBackoff        = 30 * time.Second
	maxShift          = 5
	maxAttempts       = 5
	claimTimeout      = 1 * time.Second
	denyBackoff       = 500 * time.Millisecond
	highPreference    = 0.8
	workerCount       = 10
	metricsAddr       = ":8081"
	shutdownTimeout   = 10 * time.Second
)

func main() {
	events.Init()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

	registry := task.NewRegistry()
	handlers.RegisterAll(registry)

	configs := queueconfig.DefaultConfigs()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		runMetricsServer(ctx, metricsAddr)
	}()

	runWorkerPool(ctx, rdb, registry, configs, workerCount)
	wg.Wait()
	events.WorkerPoolShutdown()
}

func runMetricsServer(ctx context.Context, addr string) {
	srv := metrics.NewServer(addr)
	events.MetricsServing(addr)
	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()
	select {
	case <-ctx.Done():
	case err, ok := <-errCh:
		if ok && err != nil {
			events.MetricsError(err)
		}
		return
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		events.MetricsError(err)
	}
}

func runWorkerPool(ctx context.Context, rdb *redis.Client, registry *task.Registry, configs []queueconfig.Config, n int) {
	byName := queueconfig.ByName(configs)
	summary := summarizeQueues(configs)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		runReaper(ctx, rdb)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		runScheduler(ctx, rdb)
	}()
	for i := 0; i < n; i++ {
		wg.Add(1)
		workerID := i
		go func() {
			defer wg.Done()
			runWorker(ctx, rdb, registry, configs, byName, summary, workerID)
		}()
	}
	wg.Wait()
}

func runWorker(ctx context.Context, rdb *redis.Client, registry *task.Registry, configs []queueconfig.Config, byName map[string]queueconfig.Config, summary string, workerID int) {
	events.WorkerStarting(workerID, summary)
	for {
		if ctx.Err() != nil {
			return
		}
		processOne(ctx, rdb, registry, configs, byName, workerID)
	}
}

func processOne(ctx context.Context, rdb *redis.Client, registry *task.Registry, configs []queueconfig.Config, byName map[string]queueconfig.Config, workerID int) {
	ordered := pickQueueOrder(configs)
	res, err := rdb.BRPop(ctx, claimTimeout, ordered...).Result()
	if err == redis.Nil {
		return
	}
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		events.ClaimError(workerID, err)
		return
	}
	// BRPop already removed the entry from pending, so no early ctx.Err() check here; finalCtx below pushes it to processing even on shutdown.
	srcQueue, rawEntry := res[0], res[1]
	cfg := byName[srcQueue]

	// Once we own the task (post-LPush to processing), run finalization on a
	// context that ignores shutdown cancel so we don't orphan Redis state.
	finalCtx := context.WithoutCancel(ctx)

	t, entry, err := stampAndRecord(finalCtx, rdb, rawEntry)
	if err != nil {
		events.ClaimError(workerID, err)
		return
	}

	if ok, err := control.AcquireSlot(finalCtx, rdb, srcQueue, t.ID, cfg.MaxConcurrency, int(visibilityTimeout.Seconds())); err != nil {
		events.ControlError(workerID, srcQueue, "slot", err)
		revertToDelayed(finalCtx, rdb, entry, rawEntry)
		return
	} else if !ok {
		events.SlotDenied(workerID, srcQueue)
		revertToDelayed(finalCtx, rdb, entry, rawEntry)
		return
	}
	defer func() {
		if err := control.ReleaseSlot(finalCtx, rdb, srcQueue, t.ID); err != nil {
			events.ControlError(workerID, srcQueue, "release slot", err)
		}
	}()

	if ok, err := control.AcquireToken(finalCtx, rdb, srcQueue, cfg.RateCapacity, cfg.RateRefillPerSec); err != nil {
		events.ControlError(workerID, srcQueue, "token", err)
		revertToDelayed(finalCtx, rdb, entry, rawEntry)
		return
	} else if !ok {
		events.TokenDenied(workerID, srcQueue)
		revertToDelayed(finalCtx, rdb, entry, rawEntry)
		return
	}

	events.TaskClaimed(workerID, *t, time.Since(t.EnqueuedAt).Round(time.Millisecond))

	if err := registry.Dispatch(*t); err != nil {
		events.DispatchError(workerID, *t, err)
		if err := scheduleRetry(finalCtx, rdb, t, entry); err != nil {
			events.ScheduleRetryError(workerID, *t, err)
		}
		return
	}
	if err := completeTask(finalCtx, rdb, entry); err != nil {
		events.CompleteError(workerID, *t, err)
		return
	}
	events.TaskCompleted(workerID, *t)
}

func pickQueueOrder(configs []queueconfig.Config) []string {
	var highs, lows []string
	for _, c := range configs {
		if c.Priority == "high" {
			highs = append(highs, c.Name())
		} else {
			lows = append(lows, c.Name())
		}
	}
	rand.Shuffle(len(highs), func(i, j int) { highs[i], highs[j] = highs[j], highs[i] })
	rand.Shuffle(len(lows), func(i, j int) { lows[i], lows[j] = lows[j], lows[i] })
	if rand.Float64() < highPreference {
		return append(highs, lows...)
	}
	return append(lows, highs...)
}

func stampAndRecord(ctx context.Context, rdb *redis.Client, rawEntry string) (*task.Task, string, error) {
	var t task.Task
	if err := json.Unmarshal([]byte(rawEntry), &t); err != nil {
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

func revertToDelayed(ctx context.Context, rdb *redis.Client, incrementedEntry, rawEntry string) {
	removed, err := rdb.LRem(ctx, processingQueue, 1, incrementedEntry).Result()
	if err != nil || removed == 0 {
		return
	}
	readyAt := time.Now().UTC().Add(denyBackoff).UnixMilli()
	rdb.ZAdd(ctx, delayedQueue, redis.Z{Score: float64(readyAt), Member: rawEntry})
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
		events.TaskDLQ(*t)
		return nil
	}
	readyAt := time.Now().UTC().Add(nextRetryDelay(t.Attempts)).UnixMilli()
	return rdb.ZAdd(ctx, delayedQueue, redis.Z{Score: float64(readyAt), Member: entry}).Err()
}

func nextRetryDelay(attempts int) time.Duration {
	if attempts >= maxShift {
		return maxBackoff
	}
	return time.Second << attempts
}

func completeTask(ctx context.Context, rdb *redis.Client, entry string) error {
	return rdb.LRem(ctx, processingQueue, 1, entry).Err()
}

func runReaper(ctx context.Context, rdb *redis.Client) {
	events.ReaperStarting(reaperInterval, visibilityTimeout)
	ticker := time.NewTicker(reaperInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := reapStale(ctx, rdb)
			if err != nil {
				events.ReaperError(err)
				continue
			}
			if n > 0 {
				events.ReaperRequeued(n)
			}
		}
	}
}

func reapStale(ctx context.Context, rdb *redis.Client) (int, error) {
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
		if t.Queue == "" {
			continue
		}
		removed, err := rdb.LRem(ctx, processingQueue, 1, entry).Result()
		if err != nil || removed == 0 {
			continue
		}
		if err := rdb.RPush(ctx, t.Queue, entry).Err(); err != nil {
			continue
		}
		reaped++
	}
	return reaped, nil
}

func runScheduler(ctx context.Context, rdb *redis.Client) {
	events.SchedulerStarting(schedulerInterval)
	ticker := time.NewTicker(schedulerInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := moveReadyRetries(ctx, rdb)
			if err != nil {
				events.SchedulerError(err)
				continue
			}
			if n > 0 {
				events.SchedulerMoved(n, "per-task queue")
			}
		}
	}
}

func moveReadyRetries(ctx context.Context, rdb *redis.Client) (int, error) {
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
		var t task.Task
		if err := json.Unmarshal([]byte(entry), &t); err != nil || t.Queue == "" {
			continue
		}
		removed, err := rdb.ZRem(ctx, delayedQueue, entry).Result()
		if err != nil || removed == 0 {
			continue
		}
		if err := rdb.LPush(ctx, t.Queue, entry).Err(); err != nil {
			continue
		}
		moved++
	}
	return moved, nil
}

func summarizeQueues(configs []queueconfig.Config) string {
	names := make([]string, 0, len(configs))
	for _, c := range configs {
		names = append(names, c.Name())
	}
	return fmt.Sprintf("%v", names)
}
