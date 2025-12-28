package main

import (
	"bufio"
	"cmp"
	"container/list"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var SERVER_ADDRESS = ":4000"
var TLSPATH = "/Users/markoristic/open-source/kvstore/tls"
var DEBUG_MODE = true
var LOG_REQUESTS = true

var ErrWrongPercentileFormat = errors.New("wrong percentile format, please use number between 0 and 1")
var ErrHistogramEmpty = errors.New("histogram does not have any data")
var ErrNoRecord = errors.New("record does not exist in database")

var storage = NewStorage()
var histogram = NewLatencyHistogram([]int64{3000, 6000, 9000, 12000, 15000, 25000, 35000, 70000, 90000, 110000, 200000, 300_000, 600_000, 800_000, 1_000_000, 1_500_000, 2_000_000, 2_500_000, 3_000_000})

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

// GetPercentile is a method from histogram that returns bounds for specified percentile e.g. p50,p90,p99;
//
// p is number between 0.0 and 1.0
func (h *LatencyHistogram) GetPercentile(p float64) (int64, error) {
	if p < 0 || p > 1 {
		return 0, ErrWrongPercentileFormat
	}
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.total == 0 {
		return 0, ErrHistogramEmpty
	}

	targetCount := p * float64(h.total)
	var currentCount int64 = 0

	for i, count := range h.counts {
		currentCount += count
		if float64(currentCount) >= targetCount {
			if i >= len(h.bukcets) {
				return h.bukcets[len(h.bukcets)-1], nil

			}
			return h.bukcets[i], nil
		}
	}
	return 0, errors.New("something went wrong")
}

type SSTableEntry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type LruCache struct {
	capacity int
	items    map[string]*list.Element
	queue    *list.List
}

func NewLruCache() *LruCache {
	return &LruCache{
		capacity: 500,
		items:    make(map[string]*list.Element),
		queue:    list.New(),
	}
}

func (l *LruCache) Get(key string) (string, bool) {
	if element, ok := l.items[key]; ok {
		l.queue.MoveToFront(element)
		return element.Value.(*SSTableEntry).Value, true
	}
	return "", false
}

func (l *LruCache) Put(entry SSTableEntry) {
	if element, ok := l.items[entry.Key]; ok {
		l.queue.MoveToFront(element)
		element.Value.(*SSTableEntry).Value = entry.Value
		return
	}
	if l.queue.Len() >= l.capacity {
		oldest := l.queue.Back()
		if oldest != nil {
			l.queue.Remove(oldest)
			kv := oldest.Value.(*SSTableEntry)
			delete(l.items, kv.Key)
		}
	}

	element := l.queue.PushFront(&entry)
	l.items[entry.Key] = element
}

type Storage struct {
	memTableMu    sync.RWMutex
	memTable      map[string]string
	lruCache      *LruCache
	sstIdx        int64
	manifestCache []string
}

func NewStorage() *Storage {
	memMap := make(map[string]string, 2100)

	storage := Storage{memTable: memMap, lruCache: NewLruCache()}
	file, err := os.OpenFile("./MANIFEST", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		panic(fmt.Sprintf("There was an error trying to open MANIFEST: %v", err))
	}
	defer file.Close()

	var lastLine string
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		lastLine = scanner.Text()
		storage.manifestCache = append(storage.manifestCache, lastLine)
	}

	if lastLine != "" {
		temp := strings.Split(lastLine, "-")
		idxStr := strings.TrimSuffix(temp[1], ".json")
		idx, err := strconv.Atoi(idxStr)
		if err != nil {
			slog.Error(fmt.Sprintf("Error converting %s: %v\n", idxStr, err))
			panic("error loading sstable id")
		}
		storage.sstIdx = int64(idx) + 1

	}

	return &storage

}

func (s *Storage) Shutdown() error {
	return s.Flush(true)

}

func (s *Storage) Flush(isShuttingDown bool) error {
	s.memTableMu.Lock()
	defer s.memTableMu.Unlock()

	if len(s.memTable) < 2000 && !isShuttingDown {
		return errors.New("not ready to flush yet, need 2000 records for flush")
	}

	entries := make([]SSTableEntry, 0, len(s.memTable))
	for k, v := range s.memTable {
		entries = append(entries, SSTableEntry{Key: k, Value: v})
	}

	slices.SortFunc(entries, func(a, b SSTableEntry) int {
		return cmp.Compare(a.Key, b.Key)
	})

	filename := fmt.Sprintf("./sstables/sst-%d.json", s.sstIdx)
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		slog.Error("Flush not successfull", "err", err.Error())
		return fmt.Errorf("unsuccessfull flush: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(entries); err != nil {
		slog.Error("could not encode data to file")

		return fmt.Errorf("unsuccessfull flush: %w", err)
	}

	manifestFile, err := os.OpenFile("./MANIFEST", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		slog.Error(fmt.Sprintf("There was an error trying to open MANIFEST: %v", err))
		panic("could not open MANIFEST file")
	}
	defer manifestFile.Close()

	_, err = fmt.Fprint(manifestFile, filename, "\n")
	if err != nil {
		slog.Error("Flush not successfull, manifest write failed", "err", err.Error())
		err := file.Close()
		if err != nil {
			slog.Error("Could not close file %s, before its removal", "file name", file.Name())
			panic("Error when closing file before its removal, cant proceed")
		}
		err = os.Remove(filename)
		if err != nil {
			slog.Error("Failed to remove sstable file after failed manifest write. ShutdownDB")
			panic("Panic:Failed to remove sstable file after failed manifest write. ShutdownDB")
		}
		return fmt.Errorf("unsuccessfull flush: %w", err)
	}

	// NOTE: Here I have this at this time until I get to sparse indexes.
	storage.lruCache = NewLruCache()
	storage.manifestCache = append(storage.manifestCache, filename)
	clear(s.memTable)
	s.sstIdx++
	return nil
}

func (s *Storage) AddValue(key, value string) {
	s.memTableMu.Lock()
	s.memTable[key] = value
	s.memTableMu.Unlock()

	if len(s.memTable) >= 2000 {
		_ = s.Flush(false)
	}
}

func (s *Storage) RetrieveValue(key string) (string, error) {
	s.memTableMu.RLock()
	defer s.memTableMu.RUnlock()

	value, ok := s.memTable[key]
	if ok {
		return value, nil
	}

	cacheValue, ok := s.lruCache.Get(key)
	if ok {
		return cacheValue, nil
	}

	return s.RetrieveFromSST(key)

}

func (s *Storage) RetrieveFromSST(key string) (string, error) {
	for i := len(s.manifestCache) - 1; i >= 0; i-- {
		filename := s.manifestCache[i]
		bytes, err := os.ReadFile(filename)
		if err != nil {
			return "", err
		}

		var entries []SSTableEntry
		if err := json.Unmarshal(bytes, &entries); err != nil {
			return "", err
		}

		for _, v := range entries {
			if v.Key == key {
				s.lruCache.Put(v)
				return v.Value, nil
			}
		}
	}
	return "", ErrNoRecord
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

	slog.Info("Waiting for shutdown signal...")
	<-ctx.Done()
	err := storage.Shutdown()
	if err != nil {
		slog.Error("There was an error when shutting down storage", slog.String("err", err.Error()))
	}
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

func showHistogram(w http.ResponseWriter, r *http.Request) {
	p50, _ := histogram.GetPercentile(0.5)
	p90, _ := histogram.GetPercentile(0.9)
	p99, _ := histogram.GetPercentile(0.99)
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
	storage.AddValue(key, bodyString)
	w.WriteHeader(200)
}

func getValue(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	res, err := storage.RetrieveValue(key)
	if err != nil {
		if errors.Is(err, ErrNoRecord) {
			w.WriteHeader(http.StatusNotFound)
		} else {
			slog.Error("error on retrieving value", slog.String("err", err.Error()))
			w.WriteHeader(http.StatusInternalServerError)
		}
		return
	}

	w.WriteHeader(200)
	_, err = fmt.Fprint(w, res)
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
			// slog.Info(
			// 	"Received Request",
			// 	slog.String("remmote_addr", r.RemoteAddr),
			// 	slog.String("proto", r.Proto),
			// 	slog.String("method", r.Method),
			// 	slog.String("uri", r.URL.RequestURI()),
			// )
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
