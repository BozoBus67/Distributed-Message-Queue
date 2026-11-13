package task

import (
	"fmt"
	"time"
)

type Task struct {
	ID         string    `json:"id"`
	Type       string    `json:"type"`
	Payload    string    `json:"payload"`
	EnqueuedAt time.Time `json:"enqueued_at"`
	StartedAt  time.Time `json:"started_at,omitempty"`
	Attempts   int       `json:"attempts"`
}

type Handler func(payload string) error

type Registry struct {
	handlers map[string]Handler
}

func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]Handler)}
}

func (r *Registry) Register(taskType string, h Handler) {
	r.handlers[taskType] = h
}

func (r *Registry) Dispatch(t Task) error {
	h, ok := r.handlers[t.Type]
	if !ok {
		return fmt.Errorf("no handler for task type %q", t.Type)
	}
	return h(t.Payload)
}
