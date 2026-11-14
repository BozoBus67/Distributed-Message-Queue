package handlers

import (
	"log/slog"
	"time"

	"taskqueue/internal/task"
)

func RegisterAll(r *task.Registry) {
	r.Register("email", emailHandler)
	r.Register("image_resize", imageResizeHandler)
	r.Register("report", reportHandler)
}

func emailHandler(payload string) error {
	slog.Info("email handler: sending", "recipient", payload)
	time.Sleep(500 * time.Millisecond)
	slog.Info("email handler: sent", "recipient", payload)
	return nil
}

func imageResizeHandler(payload string) error {
	slog.Info("image_resize handler: would resize", "path", payload)
	return nil
}

func reportHandler(payload string) error {
	slog.Info("report handler: would generate", "subject", payload)
	return nil
}
