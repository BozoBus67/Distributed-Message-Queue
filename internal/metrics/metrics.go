package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	Enqueued = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "taskqueue_enqueued_total",
		Help: "Tasks pushed onto a pending queue.",
	}, []string{"queue", "type"})

	Claimed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "taskqueue_claimed_total",
		Help: "Tasks claimed by a worker (post slot + token gates).",
	}, []string{"queue", "type"})

	Completed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "taskqueue_completed_total",
		Help: "Tasks that ran their handler without error.",
	}, []string{"queue", "type"})

	DispatchFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "taskqueue_dispatch_failed_total",
		Help: "Handler invocations that returned an error.",
	}, []string{"queue", "type"})

	DLQ = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "taskqueue_dlq_total",
		Help: "Tasks moved to the dead-letter queue after exhausting retries.",
	}, []string{"queue", "type"})

	SlotDenied = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "taskqueue_slot_denied_total",
		Help: "Slot gate denials (concurrency cap at capacity).",
	}, []string{"queue"})

	TokenDenied = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "taskqueue_token_denied_total",
		Help: "Token gate denials (rate limiter empty).",
	}, []string{"queue"})

	ReaperRequeued = promauto.NewCounter(prometheus.CounterOpts{
		Name: "taskqueue_reaper_requeued_total",
		Help: "Stale processing entries requeued by the reaper.",
	})

	SchedulerMoved = promauto.NewCounter(prometheus.CounterOpts{
		Name: "taskqueue_scheduler_moved_total",
		Help: "Delayed entries released back to a pending queue by the scheduler.",
	})

	TaskDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "taskqueue_task_duration_seconds",
		Help:    "End-to-end task latency measured from enqueue to successful completion.",
		Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60},
	}, []string{"queue", "type"})

	QueueDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "taskqueue_queue_depth",
		Help: "Current number of entries per queue by state.",
	}, []string{"queue", "state"})
)

func NewServer(addr string) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	return &http.Server{Addr: addr, Handler: mux}
}

func Handler() http.Handler {
	return promhttp.Handler()
}
