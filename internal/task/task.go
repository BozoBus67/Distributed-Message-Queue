package task

import "fmt"

type Task struct {
	Type    string `json:"type"`
	Payload string `json:"payload"`
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
