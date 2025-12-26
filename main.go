package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"
)

var SERVER_ADDRESS = ":4000"
var TLSPATH = "/Users/markoristic/open-source/kvstore/tls"
var DEBUG_MODE = true
var LOG_REQUESTS = true

type LatencyHistogram struct {
	mu      sync.RWMutex
	bukcets []int64
	counts  []int64
	total   int64
}

func NewLatencyHistogram(bounds []int64) *LatencyHistogram {
	slices.Sort(bounds)
	return &LatencyHistogram{
		bukcets: bounds,
		counts:  make([]int64, len(bounds)+1),
	}
}

func (h *LatencyHistogram) Add(duration time.Duration) {
	ms := int64(duration)
	h.mu.Lock()
	defer h.mu.Unlock()

	h.total++
	for k, v := range h.bukcets {
		if ms <= v {
			h.counts[k]++
			return
		}
	}
	h.counts[len(h.counts)-1]++
}

// GetPercentile is a method from histogram that returns bounds for specified percentile p50;p90;p99
// p is number between 0.0 and 1.0
func (h *LatencyHistogram) GetPercentile(p float64) int64 {
	if p < 0 || p > 1 {
		return 0
	}
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.total == 0 {
		return 0
	}

	targetCount := p * float64(h.total)
	var currentCount int64 = 0

	for i, count := range h.counts {
		currentCount += count
		if float64(currentCount) >= targetCount {
			if i >= len(h.bukcets) {
				return h.bukcets[len(h.bukcets)-1]

			}
			return h.bukcets[i]
		}
	}
	return 0
}

func main() {
	serverErrorLog := log.New(os.Stderr, "[ERROR]\t", log.Ldate|log.Ltime|log.Lshortfile)
	tlsConfig := &tls.Config{
		CurvePreferences: []tls.CurveID{tls.X25519, tls.CurveP256},
		MinVersion:       tls.VersionTLS13,
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	mux := http.NewServeMux()
	server := http.Server{
		Addr:         ":4000",
		ErrorLog:     serverErrorLog,
		Handler:      registerRoutes(mux, LOG_REQUESTS),
		TLSConfig:    tlsConfig,
		IdleTimeout:  time.Minute,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		slog.Info(fmt.Sprintf("Starting server on %s", SERVER_ADDRESS))
		cert := fmt.Sprintf("%s/cert.pem", TLSPATH)
		key := fmt.Sprintf("%s/key.pem", TLSPATH)

		if err := server.ListenAndServeTLS(cert, key); err != nil && err != http.ErrServerClosed {
			fmt.Println("server error:", err)
		}
	}()

	<-ctx.Done()
	slog.Info("shutdown signal received; gracefully shutting down server.")

}

func registerRoutes(mux *http.ServeMux, logRequestFlag bool) http.Handler {

	mux.Handle("GET /{key}", http.HandlerFunc(getValue))
	mux.Handle("PUT /{key}", http.HandlerFunc(putValue))
	mux.Handle("GET /histogram", http.HandlerFunc(showHistogram))

	if logRequestFlag {
		return metricsMiddleware(recoverPanic(logRequest(mux)))
	}
	return metricsMiddleware(recoverPanic(mux))
}

var kv_store = make(map[string]string)

var histogram = NewLatencyHistogram([]int64{3000, 6000, 9000, 12000, 15000, 25000, 35000, 70000})

func showHistogram(w http.ResponseWriter, r *http.Request) {
	p50 := histogram.GetPercentile(0.5)
	p90 := histogram.GetPercentile(0.9)
	p99 := histogram.GetPercentile(0.99)
	slog.Info(fmt.Sprintf("Value for p50 is %d ns", p50))
	slog.Info(fmt.Sprintf("Value for p90 is %d ns", p90))
	slog.Info(fmt.Sprintf("Value for p99 is %d ns", p99))
	w.WriteHeader(200)
}

func putValue(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	defer r.Body.Close()

	bodyString := string(body)
	kv_store[key] = bodyString
	w.WriteHeader(200)
}

func getValue(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	res, ok := kv_store[key]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.WriteHeader(200)
	_, err := fmt.Fprint(w, res)
	if err != nil {
		slog.Error("There was an error when writing a response", slog.String("err", err.Error()))
	}
}

func recoverPanic(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				w.Header().Set("Connection", "close")
				ServeError(w, fmt.Errorf("%s", err))
			}
		}()

		next.ServeHTTP(w, r)
	})
}

func metricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		next.ServeHTTP(w, r)

		since := time.Since(start)
		histogram.Add(since)
	})
}

func logRequest(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		url := r.URL.RequestURI()
		if !strings.Contains(url, "/static") && !strings.Contains(url, "/.well-known") {
			slog.Info(
				"Received Request",
				slog.String("remmote_addr", r.RemoteAddr),
				slog.String("proto", r.Proto),
				slog.String("method", r.Method),
				slog.String("uri", r.URL.RequestURI()),
			)
		}
		next.ServeHTTP(w, r)
	})
}

func ServeError(w http.ResponseWriter, err error) {
	trace := fmt.Sprintf("%s\n%s", err.Error(), debug.Stack())
	slog.Error("there was an error", "error_trace", trace)
	if DEBUG_MODE {
		http.Error(w, trace, http.StatusInternalServerError)
		return
	}

	http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
}
