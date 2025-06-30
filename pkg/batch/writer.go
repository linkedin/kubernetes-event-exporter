package batch

import (
	"context"
	"time"

	"github.com/pingcap/log"
)

// BufferWriter allows to buffer some items and call the Handler function either when the buffer is full or the timeout is
// reached. There will also be support for concurrency for high volume. The handler function is supposed to return an
// array of booleans to indicate whether the transfer was successful or not. It can be replaced with status codes in
// the future to differentiate I/O errors, rate limiting, authorization issues.
type BufferWriter struct {
	cfg      BufferWriterConfig
	Handler  Callback
	buffer   []bufferItem
	len      int
	done     chan bool
	stopDone chan bool
	items    chan interface{}
}

type bufferItem struct {
	v       interface{}
	attempt int
}

type Callback func(ctx context.Context, items []interface{}) []bool

type BufferWriterConfig struct {
	// Max events queued for a batch before a flush.
	BatchSizeEvents int `yaml:"batchSizeEvents"`
	// Max retries for each individual event.
	MaxRetriesPerEvent int `yaml:"maxRetriesPerEvent"`
	// Batches are processed
	BatchIntervalSeconds int `yaml:"batchIntervalSeconds"`
	// TODO: this doesn't do anything!
	BatchTimeoutSeconds int `yaml:"batchTimeoutSeconds"`
}

func NewBufferWriter(cfg BufferWriterConfig, cb Callback) *BufferWriter {
	log.Info().Msgf("New Buffer Writer created with config: %+v", cfg)
	return &BufferWriter{
		cfg:     cfg,
		Handler: cb,
		buffer:  make([]bufferItem, cfg.BatchSizeEvents),
	}
}

// Indicates the start to accept the
func (w *BufferWriter) Start() {
	w.done = make(chan bool)
	w.items = make(chan interface{})
	w.stopDone = make(chan bool)
	ticker := time.NewTicker(time.Duration(w.cfg.BatchIntervalSeconds) * time.Second)

	go func() {
		shouldGoOn := true
		for shouldGoOn {
			select {
			case item := <-w.items:
				if w.len >= w.cfg.BatchSizeEvents {
					w.processBuffer(context.Background())
					w.len = 0
				}

				w.buffer[w.len] = bufferItem{v: item, attempt: 0}
				w.len++
			case <-w.done:
				w.processBuffer(context.Background())
				shouldGoOn = false
				w.stopDone <- true
				ticker.Stop()
			case <-ticker.C:
				w.processBuffer(context.Background())
			}
		}
	}()
}

func (w *BufferWriter) processBuffer(ctx context.Context) {
	if w.len == 0 {
		return
	}

	// Need to copy the underlying item to another slice
	slice := make([]interface{}, w.len)
	for i := 0; i < w.len; i++ {
		slice[i] = w.buffer[i].v
	}

	// Call the actual method
	responses := w.Handler(ctx, slice)

	// Overwriting buffer here. Since the newItemsCount will always be equal to or smaller than buffer, it's safe to
	// overwrite the existing items whilst traversing the items.
	var newItemsCount int
	for idx, success := range responses {
		if !success {
			item := w.buffer[idx]
			if item.attempt >= w.cfg.MaxRetriesPerEvent {
				// It's dropped, sorry you asked for it
				continue
			}

			w.buffer[newItemsCount] = bufferItem{
				v:       item.v,
				attempt: item.attempt + 1,
			}

			newItemsCount++
		}
	}

	w.len = newItemsCount
	// TODO(makin) an edge case, if all items fail, and the buffer is full, new item cannot be added to buffer.
}

// Used to signal writer to stop processing items and exit.
func (w *BufferWriter) Stop() {
	w.done <- true
	<-w.stopDone
}

// Submit pushes the items to the income buffer and they are placed onto the actual buffer from there.
func (w *BufferWriter) Submit(items ...interface{}) {
	for _, item := range items {
		w.items <- item
	}
}
