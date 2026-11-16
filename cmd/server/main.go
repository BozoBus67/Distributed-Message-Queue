package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"taskqueue/internal/events"
	"taskqueue/internal/queueconfig"
	"taskqueue/internal/task"
)

func main() {
	events.Init()
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	ctx := context.Background()
	byName := queueconfig.ByName(queueconfig.DefaultConfigs())

	mux := http.NewServeMux()
	mux.HandleFunc("/enqueue", enqueueHandler(ctx, rdb, byName))

	runServer(":8080", mux)
}

func runServer(addr string, mux *http.ServeMux) {
	events.ServerStarting(addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		events.ServerFatal(err)
	}
}

func enqueueHandler(ctx context.Context, rdb *redis.Client, byName map[string]queueconfig.Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cfg, taskType, payload, err := parseEnqueue(r, byName)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := enqueueTask(ctx, rdb, cfg, taskType, payload); err != nil {
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
	return rdb.LPush(ctx, cfg.Name(), data).Err()
}
