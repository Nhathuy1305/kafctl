package helpers

import (
	"context"
	"kafctl/internal/output"
	"os"
	"os/signal"
)

func CreateTerminalContext() context.Context {
	ctx := context.Background()

	// Trap Ctrl+C and call cancel on the context
	ctx, cancel := context.WithCancel(ctx)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	go func() {
		select {
		case <-signals:
			output.Debugf("cancel terminal context")
			cancel()
		case <-ctx.Done():
		}
	}()

	return ctx
}
