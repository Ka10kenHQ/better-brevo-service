package main

import (
	"log"
	"time"
	"github.com/Ka10ken1/better-brevo-service/internal/background"
	"github.com/robfig/cron/v3"
)

func main() {
	loc, err := time.LoadLocation("Local")
	if err != nil {
		log.Fatalf("Failed to load local timezone: %v", err)
	}

	c := cron.New(cron.WithLocation(loc))

	// Run() at 2:00 AM every day
	// 0 - Minutes
	// 2 - Hours
	_, err = c.AddFunc("0 2 * * *", func() {
		log.Println("Running scheduled task at", time.Now().Format(time.RFC3339))
		background.Run()
	})

	if err != nil {
		log.Fatalf("Failed to schedule task: %v", err)
	}

	c.Start()

	log.Println("Scheduler is running. Task will run at 2:00 AM every day.")

	select {} // block forever
}

