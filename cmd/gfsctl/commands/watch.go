package commands

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// WatchConfig holds configuration for watch mode.
type WatchConfig struct {
	Enabled  bool
	Interval time.Duration
}

// WatchFunc is a function that can be watched (called repeatedly).
type WatchFunc func(ctx context.Context) error

// RunWithWatch executes the given function once or repeatedly based on watch mode.
// If watch mode is enabled, it clears the screen and re-runs the function
// at the configured interval until interrupted.
func RunWithWatch(cfg WatchConfig, fn WatchFunc) error {
	if !cfg.Enabled {
		return fn(context.Background())
	}

	// Set up signal handling for graceful exit
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Channel for watch loop to signal completion
	done := make(chan struct{})

	go func() {
		defer close(done)
		ticker := time.NewTicker(cfg.Interval)
		defer ticker.Stop()

		// Initial run
		clearScreen()
		printWatchHeader(cfg.Interval)
		if err := fn(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		}

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				clearScreen()
				printWatchHeader(cfg.Interval)
				if err := fn(ctx); err != nil {
					fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				}
			}
		}
	}()

	// Wait for signal or context cancellation
	select {
	case <-sigCh:
		cancel()
		<-done
		fmt.Println("\nWatch mode terminated.")
	case <-done:
	}

	return nil
}

// clearScreen clears the terminal screen using ANSI escape codes.
func clearScreen() {
	// Move cursor to home position and clear screen
	fmt.Print("\033[H\033[2J")
}

// printWatchHeader prints the watch mode header with timestamp.
func printWatchHeader(interval time.Duration) {
	now := time.Now().Format("2006-01-02 15:04:05")
	fmt.Printf("Every %s: gfsctl (updated: %s) [Ctrl+C to exit]\n\n",
		formatInterval(interval), now)
}

// formatInterval formats a duration for display.
func formatInterval(d time.Duration) string {
	if d >= time.Minute {
		return fmt.Sprintf("%.0fm", d.Minutes())
	}
	return fmt.Sprintf("%.0fs", d.Seconds())
}
