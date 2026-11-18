package main

import (
	"context"
	"flag"
	"fmt"
	"lamina/internal/engine"
	"lamina/internal/snapshot"
	"lamina/internal/transport"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	if err := run(); err != nil {
		slog.Error("lamina stopped", "error", err)
		os.Exit(1)
	}
}
func run() error {
	dir := flag.String("data", "var/lamina", "data directory exclusively owned by this process")
	addr := flag.String("listen", "127.0.0.1:7386", "HTTP bind address")
	restore := flag.String("restore", "", "validated backup to install into a new data directory before serving")
	replace := flag.Bool("replace", false, "let -restore replace an existing offline lamina data directory, reverting it to the selected backup")
	abort := flag.Bool("replace-abort", false, "roll an interrupted replacement in the data directory back to the old state before serving")
	flag.Parse()
	if *abort && (*restore != "" || *replace) {
		return fmt.Errorf("-replace-abort cannot be combined with -restore or -replace")
	}
	if *replace && *restore == "" {
		return fmt.Errorf("-replace requires -restore")
	}
	if *abort {
		if err := engine.AbortReplace(*dir); err != nil {
			return err
		}
	} else if *restore != "" {
		info, err := os.Stat(*restore)
		if err != nil {
			return err
		}
		if info.Size() > snapshot.MaxImage {
			return fmt.Errorf("backup exceeds limit")
		}
		b, err := os.ReadFile(*restore)
		if err != nil {
			return err
		}
		if *replace {
			err = engine.RestoreReplace(*dir, b)
		} else {
			err = engine.Restore(*dir, b)
		}
		if err != nil {
			return err
		}
	}
	db, err := engine.Open(*dir)
	if err != nil {
		return err
	}
	defer db.Close()
	server := &http.Server{Addr: *addr, Handler: transport.New(db), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 45 * time.Second, MaxHeaderBytes: 32 << 10}
	stopped := make(chan os.Signal, 1)
	signal.Notify(stopped, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(stopped)
	finished := make(chan error, 1)
	go func() { slog.Info("lamina ready", "address", *addr); finished <- server.ListenAndServe() }()
	select {
	case err = <-finished:
		if err != http.ErrServerClosed {
			return err
		}
	case <-stopped:
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err = server.Shutdown(ctx); err != nil {
			server.Close()
			return err
		}
	}
	return nil
}
