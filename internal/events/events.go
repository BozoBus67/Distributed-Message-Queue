package events

import (
	"log/slog"
	"os"
	"time"

	"taskqueue/internal/metrics"
	"taskqueue/internal/task"
)

func Init() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
}

func ServerStarting(addr string) {
	slog.Info("server starting", "addr", addr)
}

func ServerFatal(err error) {
	slog.Error("server fatal", "error", err)
	os.Exit(1)
}

func ServerShutdown() {
	slog.Info("server shutdown complete")
}

func WorkerStarting(workerID int, queue string) {
	slog.Info("worker starting", "worker_id", workerID, "queue", queue)
}

func WorkerPoolShutdown() {
	slog.Info("worker pool shutdown complete")
}

func MetricsServing(addr string) {
	slog.Info("metrics serving", "addr", addr)
}

func MetricsError(err error) {
	slog.Error("metrics server error", "error", err)
}

func ReaperStarting(interval, visibilityTimeout time.Duration) {
	slog.Info("reaper starting", "interval", interval, "visibility_timeout", visibilityTimeout)
}

func SchedulerStarting(interval time.Duration) {
	slog.Info("scheduler starting", "interval", interval)
}

func DepthSamplerStarting(interval time.Duration) {
	slog.Info("depth sampler starting", "interval", interval)
}

func DepthSamplerError(err error) {
	slog.Error("depth sampler tick failed", "error", err)
}

func DLQRequeued(taskID, queue string) {
	slog.Info("dlq entry requeued", "task_id", taskID, "queue", queue)
}

func DLQDropped(taskID string) {
	slog.Warn("dlq entry dropped", "task_id", taskID)
}

func TaskEnqueued(t task.Task) {
	metrics.Enqueued.WithLabelValues(t.Queue, t.Type).Inc()
	slog.Info("task enqueued",
		"task_id", t.ID,
		"type", t.Type,
		"queue", t.Queue,
	)
}

func TaskClaimed(workerID int, t task.Task, age time.Duration) {
	metrics.Claimed.WithLabelValues(t.Queue, t.Type).Inc()
	slog.Info("task claimed",
		"worker_id", workerID,
		"task_id", t.ID,
		"type", t.Type,
		"queue", t.Queue,
		"attempts", t.Attempts,
		"age", age,
	)
}

func TaskCompleted(workerID int, t task.Task) {
	metrics.Completed.WithLabelValues(t.Queue, t.Type).Inc()
	metrics.TaskDuration.WithLabelValues(t.Queue, t.Type).Observe(time.Since(t.EnqueuedAt).Seconds())
	slog.Info("task completed",
		"worker_id", workerID,
		"task_id", t.ID,
		"type", t.Type,
		"queue", t.Queue,
		"attempts", t.Attempts,
		"latency", time.Since(t.EnqueuedAt).Round(time.Millisecond),
	)
}

func TaskDLQ(t task.Task) {
	metrics.DLQ.WithLabelValues(t.Queue, t.Type).Inc()
	slog.Warn("task sent to DLQ",
		"task_id", t.ID,
		"type", t.Type,
		"queue", t.Queue,
		"attempts", t.Attempts,
	)
}

func ReaperRequeued(n int) {
	metrics.ReaperRequeued.Add(float64(n))
	slog.Info("reaper requeued stale tasks", "count", n)
}

func SchedulerMoved(n int, queue string) {
	metrics.SchedulerMoved.Add(float64(n))
	slog.Info("scheduler moved ready retries", "count", n, "to", queue)
}

func ClaimError(workerID int, err error) {
	slog.Error("claim failed", "worker_id", workerID, "error", err)
}

func DispatchError(workerID int, t task.Task, err error) {
	metrics.DispatchFailed.WithLabelValues(t.Queue, t.Type).Inc()
	slog.Error("dispatch failed",
		"worker_id", workerID,
		"task_id", t.ID,
		"type", t.Type,
		"queue", t.Queue,
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
	metrics.SlotDenied.WithLabelValues(queue).Inc()
	slog.Info("slot denied", "worker_id", workerID, "queue", queue)
}

func TokenDenied(workerID int, queue string) {
	metrics.TokenDenied.WithLabelValues(queue).Inc()
	slog.Info("token denied", "worker_id", workerID, "queue", queue)
}

func ControlError(workerID int, queue, op string, err error) {
	slog.Error("control error", "worker_id", workerID, "queue", queue, "op", op, "error", err)
}
