package handlers

import (
	"log"
	"time"

	"taskqueue/internal/task"
)

func RegisterAll(r *task.Registry) {
	r.Register("email", emailHandler)
	r.Register("image_resize", imageResizeHandler)
	r.Register("report", reportHandler)
}

func emailHandler(payload string) error {
	log.Printf("email handler: sending email to %q", payload)
	time.Sleep(500 * time.Millisecond)
	log.Printf("email handler: sent to %q", payload)
	return nil
}

func imageResizeHandler(payload string) error {
	log.Printf("image_resize handler: would resize image at %q", payload)
	return nil
}

func reportHandler(payload string) error {
	log.Printf("report handler: would generate report for %q", payload)
	return nil
}
