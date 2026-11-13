package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"taskqueue/internal/task"
)

type enqueueReq struct {
	Type    string
	Payload string
}

func parseEnqueue(r *http.Request) (*enqueueReq, error) {
	taskType := r.URL.Query().Get("type")
	if taskType == "" {
		return nil, fmt.Errorf("missing ?type=")
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	return &enqueueReq{Type: taskType, Payload: string(body)}, nil
}

func enqueueTask(ctx context.Context, rdb *redis.Client, req *enqueueReq) error {
	t := task.Task{
		ID:         uuid.NewString(),
		Type:       req.Type,
		Payload:    req.Payload,
		EnqueuedAt: time.Now().UTC(),
		Attempts:   0,
	}
	data, err := json.Marshal(t)
	if err != nil {
		return err
	}
	return rdb.LPush(ctx, "queue:default", data).Err()
}

func enqueueHandler(ctx context.Context, rdb *redis.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		req, err := parseEnqueue(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := enqueueTask(ctx, rdb, req); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Write([]byte("enqueued\n"))
	}
}

func runServer(addr string, mux *http.ServeMux) {
	log.Printf("server listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	ctx := context.Background()

	mux := http.NewServeMux()
	mux.HandleFunc("/enqueue", enqueueHandler(ctx, rdb))

	runServer(":8080", mux)
}
