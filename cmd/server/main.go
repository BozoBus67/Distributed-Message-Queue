package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"taskqueue/internal/events"
	"taskqueue/internal/metrics"
	"taskqueue/internal/queueconfig"
	"taskqueue/internal/task"
)

const (
	shutdownTimeout    = 10 * time.Second
	deadQueue          = "queue:dead"
	workerMetricsURL   = "http://localhost:8081/metrics"
	workerProxyTimeout = 3 * time.Second
	maxEnqueueCount    = 100
)

//go:embed dashboard.html
var dashboardHTML string

func main() {
	events.Init()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	byName := queueconfig.ByName(queueconfig.DefaultConfigs())

	mux := http.NewServeMux()
	mux.HandleFunc("/enqueue", enqueueHandler(rdb, byName))
	mux.Handle("/metrics", metrics.Handler())
	mux.HandleFunc("/worker/metrics", workerMetricsProxyHandler())
	mux.HandleFunc("/dlq", dlqListHandler(rdb))
	mux.HandleFunc("/dlq/requeue", dlqRequeueHandler(rdb))
	mux.HandleFunc("/dlq/drop", dlqDropHandler(rdb))
	mux.HandleFunc("/dashboard", dashboardHandler())
	mux.HandleFunc("/", rootRedirectHandler())

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
		cfg, taskType, payload, count, err := parseEnqueue(r, byName)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		for i := 0; i < count; i++ {
			if err := enqueueTask(r.Context(), rdb, cfg, taskType, payload); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
		fmt.Fprintf(w, "enqueued %d\n", count)
	}
}

func parseEnqueue(r *http.Request, byName map[string]queueconfig.Config) (queueconfig.Config, string, string, int, error) {
	q := r.URL.Query()
	taskType := q.Get("type")
	if taskType == "" {
		return queueconfig.Config{}, "", "", 0, fmt.Errorf("missing ?type=")
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
		return queueconfig.Config{}, "", "", 0, fmt.Errorf("unknown queue %q (bad tenant or priority)", name)
	}
	count := 1
	if raw := q.Get("count"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			return queueconfig.Config{}, "", "", 0, fmt.Errorf("invalid ?count=%q (must be a positive integer)", raw)
		}
		if n > maxEnqueueCount {
			return queueconfig.Config{}, "", "", 0, fmt.Errorf("?count=%d exceeds max %d", n, maxEnqueueCount)
		}
		count = n
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return queueconfig.Config{}, "", "", 0, fmt.Errorf("read body: %w", err)
	}
	return cfg, taskType, string(body), count, nil
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

func dlqListHandler(rdb *redis.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		entries, err := rdb.LRange(r.Context(), deadQueue, 0, -1).Result()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		tasks := make([]task.Task, 0, len(entries))
		for _, entry := range entries {
			var t task.Task
			if err := json.Unmarshal([]byte(entry), &t); err != nil {
				continue
			}
			tasks = append(tasks, t)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(tasks)
	}
}

func dlqRequeueHandler(rdb *redis.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST required", http.StatusMethodNotAllowed)
			return
		}
		id := r.URL.Query().Get("id")
		if id == "" {
			http.Error(w, "missing ?id=", http.StatusBadRequest)
			return
		}
		t, _, removed, err := findAndRemoveFromDLQ(r.Context(), rdb, id)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if !removed {
			http.Error(w, "task not found in DLQ", http.StatusNotFound)
			return
		}
		t.Attempts = 0
		t.StartedAt = time.Time{}
		data, err := json.Marshal(t)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if err := rdb.LPush(r.Context(), t.Queue, data).Err(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		events.DLQRequeued(t.ID, t.Queue)
		w.Write([]byte("requeued\n"))
	}
}

func dlqDropHandler(rdb *redis.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST required", http.StatusMethodNotAllowed)
			return
		}
		id := r.URL.Query().Get("id")
		if id == "" {
			http.Error(w, "missing ?id=", http.StatusBadRequest)
			return
		}
		t, _, removed, err := findAndRemoveFromDLQ(r.Context(), rdb, id)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if !removed {
			http.Error(w, "task not found in DLQ", http.StatusNotFound)
			return
		}
		events.DLQDropped(t.ID)
		w.Write([]byte("dropped\n"))
	}
}

func findAndRemoveFromDLQ(ctx context.Context, rdb *redis.Client, id string) (task.Task, string, bool, error) {
	entries, err := rdb.LRange(ctx, deadQueue, 0, -1).Result()
	if err != nil {
		return task.Task{}, "", false, err
	}
	for _, entry := range entries {
		var t task.Task
		if err := json.Unmarshal([]byte(entry), &t); err != nil || t.ID != id {
			continue
		}
		removed, err := rdb.LRem(ctx, deadQueue, 1, entry).Result()
		if err != nil {
			return t, entry, false, err
		}
		return t, entry, removed > 0, nil
	}
	return task.Task{}, "", false, nil
}

var workerMetricsClient = &http.Client{Timeout: workerProxyTimeout}

func workerMetricsProxyHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, workerMetricsURL, nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		resp, err := workerMetricsClient.Do(req)
		if err != nil {
			http.Error(w, "worker metrics unreachable", http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		if ct := resp.Header.Get("Content-Type"); ct != "" {
			w.Header().Set("Content-Type", ct)
		}
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
	}
}

func dashboardHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		w.Write([]byte(dashboardHTML))
	}
}

func rootRedirectHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		http.Redirect(w, r, "/dashboard", http.StatusFound)
	}
}
