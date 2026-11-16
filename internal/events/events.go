package events

import (
	"log/slog"
	"os"
	"time"

	"taskqueue/internal/task"
)

func Init() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, nil)))
}

func ServerStarting(addr string) {
	slog.Info("server starting", "addr", addr)
}

func ServerFatal(err error) {
	slog.Error("server fatal", "error", err)
	os.Exit(1)
}

func WorkerStarting(workerID int, queue string) {
	slog.Info("worker starting", "worker_id", workerID, "queue", queue)
}

func ReaperStarting(interval, visibilityTimeout time.Duration) {
	slog.Info("reaper starting", "interval", interval, "visibility_timeout", visibilityTimeout)
}

func SchedulerStarting(interval time.Duration) {
	slog.Info("scheduler starting", "interval", interval)
}

func TaskClaimed(workerID int, t task.Task, age time.Duration) {
	slog.Info("task claimed",
		"worker_id", workerID,
		"task_id", t.ID,
		"type", t.Type,
		"attempts", t.Attempts,
		"age", age,
	)
}

func TaskDeduped(workerID int, t task.Task) {
	slog.Info("task deduped",
		"worker_id", workerID,
		"task_id", t.ID,
		"type", t.Type,
		"attempts", t.Attempts,
	)
}

func TaskDLQ(t task.Task) {
	slog.Warn("task sent to DLQ",
		"task_id", t.ID,
		"type", t.Type,
		"attempts", t.Attempts,
	)
}

func ReaperRequeued(n int) {
	slog.Info("reaper requeued stale tasks", "count", n)
}

func SchedulerMoved(n int, queue string) {
	slog.Info("scheduler moved ready retries", "count", n, "to", queue)
}

func ClaimError(workerID int, err error) {
	slog.Error("claim failed", "worker_id", workerID, "error", err)
}

func DispatchError(workerID int, t task.Task, err error) {
	slog.Error("dispatch failed",
		"worker_id", workerID,
		"task_id", t.ID,
		"type", t.Type,
		"attempts", t.Attempts,
		"error", err,
	)
}

func ScheduleRetryError(workerID int, t task.Task, err error) {
	slog.Error("schedule retry failed",
		"worker_id", workerID,
		"task_id", t.ID,
		"error", err,
	)
}

func CompleteError(workerID int, t task.Task, err error) {
	slog.Error("complete failed",
		"worker_id", workerID,
		"task_id", t.ID,
		"error", err,
	)
}

func ReaperError(err error) {
	slog.Error("reaper tick failed", "error", err)
}

func SchedulerError(err error) {
	slog.Error("scheduler tick failed", "error", err)
}

func SlotDenied(workerID int, queue string) {
	slog.Info("slot denied", "worker_id", workerID, "queue", queue)
}

func TokenDenied(workerID int, queue string) {
	slog.Info("token denied", "worker_id", workerID, "queue", queue)
}

func ControlError(workerID int, queue, op string, err error) {
	slog.Error("control error", "worker_id", workerID, "queue", queue, "op", op, "error", err)
}
