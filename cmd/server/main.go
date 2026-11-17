package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"taskqueue/internal/events"
	"taskqueue/internal/metrics"
	"taskqueue/internal/queueconfig"
	"taskqueue/internal/task"
)

const shutdownTimeout = 10 * time.Second

func main() {
	events.Init()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	byName := queueconfig.ByName(queueconfig.DefaultConfigs())

	mux := http.NewServeMux()
	mux.HandleFunc("/enqueue", enqueueHandler(rdb, byName))
	mux.Handle("/metrics", metrics.Handler())

	runServer(ctx, ":8080", mux)
}

func runServer(ctx context.Context, addr string, mux *http.ServeMux) {
	srv := &http.Server{Addr: addr, Handler: mux}
	events.ServerStarting(addr)
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
			events.ServerFatal(err)
		}
		return
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		events.ServerFatal(err)
	}
	events.ServerShutdown()
}

func enqueueHandler(rdb *redis.Client, byName map[string]queueconfig.Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cfg, taskType, payload, err := parseEnqueue(r, byName)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := enqueueTask(r.Context(), rdb, cfg, taskType, payload); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Write([]byte("enqueued\n"))
	}
}

func parseEnqueue(r *http.Request, byName map[string]queueconfig.Config) (queueconfig.Config, string, string, error) {
	q := r.URL.Query()
	taskType := q.Get("type")
	if taskType == "" {
		return queueconfig.Config{}, "", "", fmt.Errorf("missing ?type=")
	}
	tenant := q.Get("tenant")
	if tenant == "" {
		tenant = "default"
	}
	priority := q.Get("priority")
	if priority == "" {
		priority = "low"
	}
	name := fmt.Sprintf("queue:%s:%s", tenant, priority)
	cfg, ok := byName[name]
	if !ok {
		return queueconfig.Config{}, "", "", fmt.Errorf("unknown queue %q (bad tenant or priority)", name)
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return queueconfig.Config{}, "", "", fmt.Errorf("read body: %w", err)
	}
	return cfg, taskType, string(body), nil
}

func enqueueTask(ctx context.Context, rdb *redis.Client, cfg queueconfig.Config, taskType, payload string) error {
	t := task.Task{
		ID:         uuid.NewString(),
		Type:       taskType,
		Payload:    payload,
		Queue:      cfg.Name(),
		EnqueuedAt: time.Now().UTC(),
	}
	data, err := json.Marshal(t)
	if err != nil {
		return err
	}
	if err := rdb.LPush(ctx, cfg.Name(), data).Err(); err != nil {
		return err
	}
	events.TaskEnqueued(t)
	return nil
}
