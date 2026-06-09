package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"pg-loadgen/config"
	"pg-loadgen/db"
	"pg-loadgen/metrics"
	"pg-loadgen/workload"
)

var ready = false

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := db.NewPool(ctx, cfg)
	if err != nil {
		log.Fatalf("create pool: %v", err)
	}
	defer pool.Close()

	if err := db.MigrateWithLock(ctx, pool, cfg); err != nil {
		log.Fatalf("migrate: %v", err)
	}

	metrics.Register()
	prometheus.MustRegister(metrics.NewPoolCollector(pool))

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if ready {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	})

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.MetricsPort),
		Handler: mux,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("metrics server: %v", err)
		}
	}()

	ready = true
	log.Printf("starting %d workers", cfg.Workers)

	ring := workload.NewSessionRing(cfg.RingSize)

	var runCtx context.Context
	var runCancel context.CancelFunc
	if cfg.RunDurationSecs > 0 {
		runCtx, runCancel = context.WithTimeout(ctx, time.Duration(cfg.RunDurationSecs)*time.Second)
	} else {
		runCtx, runCancel = context.WithCancel(ctx)
	}
	defer runCancel()

	var wg sync.WaitGroup
	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			workload.RunWorker(runCtx, pool, ring, cfg, id)
		}(i)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	select {
	case sig := <-sigCh:
		log.Printf("received signal %v — shutting down", sig)
		cancel()
	case <-runCtx.Done():
		log.Println("run duration elapsed — shutting down")
		cancel()
	}

	wg.Wait()
	log.Println("all workers stopped — goodbye")

	shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutCancel()
	srv.Shutdown(shutCtx) //nolint
}
