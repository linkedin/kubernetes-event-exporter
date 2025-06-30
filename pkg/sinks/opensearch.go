package sinks

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	opensearch "github.com/opensearch-project/opensearch-go"
	opensearchapi "github.com/opensearch-project/opensearch-go/opensearchapi"
	"github.com/resmoio/kubernetes-event-exporter/pkg/batch"
	"github.com/resmoio/kubernetes-event-exporter/pkg/kube"
	"github.com/rs/zerolog/log"
)

type OpenSearchConfig struct {
	// Connection specific
	Hosts    []string `yaml:"hosts"`
	Username string   `yaml:"username"`
	Password string   `yaml:"password"`
	// Indexing preferences
	UseEventID bool `yaml:"useEventID"`
	// DeDot all labels and annotations in the event. For both the event and the involvedObject
	DeDot       bool                   `yaml:"deDot"`
	Index       string                 `yaml:"index"`
	IndexFormat string                 `yaml:"indexFormat"`
	TLS         TLS                    `yaml:"tls"`
	Layout      map[string]interface{} `yaml:"layout"`
	// The key to place the greater of eventTime and lastTimestamp into. Useful for datastreams.
	CombineTimestampTo string `yaml:"combineTimestampTo"`

	// Batching options. If this block is not present, events are sent individually.
	// Note that opensearch calls this "bulk".
	Batch *batch.BufferWriterConfig `yaml:"batch"`
}

func NewOpenSearch(cfg *OpenSearchConfig) (*OpenSearch, error) {

	tlsClientConfig, err := setupTLS(&cfg.TLS)
	if err != nil {
		return nil, fmt.Errorf("failed to setup TLS: %w", err)
	}

	client, err := opensearch.NewClient(opensearch.Config{
		Addresses: cfg.Hosts,
		Username:  cfg.Username,
		Password:  cfg.Password,
		Transport: &http.Transport{
			TLSClientConfig: tlsClientConfig,
		},
	})
	if err != nil {
		return nil, err
	}

	// Init the object first
	ptr := &OpenSearch{
		client: client,
		cfg:    cfg,
		batch:  nil,
	}

	// Initialize the batch writer if needed.
	var bufferWriter *batch.BufferWriter
	if cfg.Batch != nil {
		bufferWriter = batch.NewBufferWriter(*cfg.Batch, ptr.SendBatch)
		bufferWriter.Start()
	}

	// Update the object with the writer.
	ptr.batch = bufferWriter

	return ptr, nil
}

type OpenSearch struct {
	client *opensearch.Client
	cfg    *OpenSearchConfig
	batch  *batch.BufferWriter
}

var osRegex = regexp.MustCompile(`(?s){(.*)}`)

func osFormatIndexName(pattern string, when time.Time) string {
	m := osRegex.FindAllStringSubmatchIndex(pattern, -1)
	current := 0
	var builder strings.Builder

	for i := 0; i < len(m); i++ {
		pair := m[i]

		builder.WriteString(pattern[current:pair[0]])
		builder.WriteString(when.Format(pattern[pair[0]+1 : pair[1]-1]))
		current = pair[1]
	}

	builder.WriteString(pattern[current:])

	return builder.String()
}

func (e *OpenSearch) getIndex() string {
	var index string
	if len(e.cfg.IndexFormat) > 0 {
		now := time.Now()
		index = osFormatIndexName(e.cfg.IndexFormat, now)
	} else {
		index = e.cfg.Index
	}
	return index
}

func (e *OpenSearch) prepareEvent(ev *kube.EnhancedEvent) ([]byte, error) {
	var toSend []byte

	// DeDot the event if needed
	if e.cfg.DeDot {
		de := ev.DeDot()
		ev = &de
	}
	// Handle layout if set
	if e.cfg.Layout != nil {
		res, err := convertLayoutTemplate(e.cfg.Layout, ev)
		if err != nil {
			return nil, err
		}

		toSend, err = json.Marshal(res)
		if err != nil {
			return nil, err
		}
	} else {
		toSend = ev.ToJSON()
		if e.cfg.CombineTimestampTo != "" {
			// Janky approach but avoids modifying the underlying functions or struct, for a fairly
			// sink-specific need.
			var jsonEvent map[string]interface{}
			// We just marshalled, an error here would be unbelievable.
			json.Unmarshal(toSend, &jsonEvent)
			if _, ok := jsonEvent[e.cfg.CombineTimestampTo]; ok {
				// We don't want to overwrite the existing value.
				return nil, fmt.Errorf("key '%s' already exists in event", e.cfg.CombineTimestampTo)
			}
			// Can't use .GetTimestamp* since that's *first* timestamp.
			var timestamp string
			if !ev.LastTimestamp.Time.IsZero() {
				timestamp = ev.LastTimestamp.Time.Format(time.RFC3339)
			} else {
				timestamp = ev.EventTime.Time.Format(time.RFC3339)
			}
			jsonEvent[e.cfg.CombineTimestampTo] = timestamp
			var err error
			toSend, err = json.Marshal(jsonEvent)
			if err != nil {
				// Most likely a strange value in the key.
				log.Error().Msgf("Failed to marshal event with timestamp '%s' in key '%s': %s", ev.GetTimestampISO8601(), e.cfg.CombineTimestampTo, err)
				return nil, err
			}
		}
	}

	return toSend, nil
}

func (e *OpenSearch) Send(ctx context.Context, ev *kube.EnhancedEvent) error {
	if e.batch != nil {
		log.Debug().Msgf("Event %s submitted to batch", ev.Message)
		e.batch.Submit(ev)
		return nil
	} else {
		log.Debug().Msgf("Sending event %s individually", ev.Message)
		return e.SendSingle(ctx, ev)
	}
}

func (e *OpenSearch) SendSingle(ctx context.Context, ev *kube.EnhancedEvent) error {
	toSend, err := e.prepareEvent(ev)
	if err != nil {
		return err
	}

	index := e.getIndex()

	req := opensearchapi.IndexRequest{
		Body:  bytes.NewBuffer(toSend),
		Index: index,
	}

	if e.cfg.UseEventID {
		req.DocumentID = string(ev.UID)
	}

	resp, err := req.Do(ctx, e.client)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	if resp.StatusCode > 399 {
		rb, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		log.Error().Msgf("Indexing failed: %s", string(rb))
	}
	return nil
}

func (e *OpenSearch) PrepareBatchItem(item interface{}) (string, error) {
	toSend, err := e.prepareEvent(item.(*kube.EnhancedEvent))
	if err != nil {
		return "", err
	}

	var firstline string
	var secondline string
	if e.cfg.UseEventID {
		// Must upsert the event
		firstline = fmt.Sprintf(`{"update": {"_index": "%s", "_id": "%s"}}`, e.getIndex(), item.(*kube.EnhancedEvent).UID)
		secondline = fmt.Sprintf(`{"doc": %s, "doc_as_upsert": true}`, string(toSend))
	} else {
		// Every event is unique, so they should all be creation.
		firstline = fmt.Sprintf(`{"create": {"_index": "%s"}}`, e.getIndex())
		secondline = string(toSend)
	}

	return fmt.Sprintf("%s\n%s", firstline, secondline), nil
}

// Unfortunately the opensearch client does not define non-generic response types.
type BulkResponseItem struct {
	Id     string `json:"_id"`
	Index  string `json:"_index"`
	Result string `json:"result"`
	Status int    `json:"status"`
	Error  struct {
		Type   string `json:"type"`
		Reason string `json:"reason"`
	} `json:"error"`
}
type BulkResponseItemWrapper struct {
	// Only one of these will be present.
	Update BulkResponseItem `json:"update"`
	Create BulkResponseItem `json:"create"`
}
type BulkResponse struct {
	Errors bool                      `json:"errors"`
	Items  []BulkResponseItemWrapper `json:"items"`
}

func (e *OpenSearch) SendBatch(ctx context.Context, items []interface{}) []bool {
	res := make([]bool, len(items))
	for i := range items {
		res[i] = true
	}

	bulkItems := make([]string, len(items))
	failed := 0
	for i, item := range items {
		prepared, err := e.PrepareBatchItem(item)
		if err != nil {
			log.Error().Msgf("Could not prepare batch item: %s", err)
			failed++
			continue
		}
		// Skip any failed items so empty items are left at the end.
		bulkItems[i-failed] = prepared
	}

	if failed > 0 {
		log.Error().Msgf("Failed to prepare %d batch items", failed)
	}
	// Remove all empty items.
	bulkItems = bulkItems[:len(items)-failed]
	// Make sure there's a list left.
	if len(bulkItems) == 0 {
		log.Error().Msg("No items to send!")
		return []bool{}
	}

	bulk := opensearchapi.BulkRequest{
		// For some reason, bulk requests must have a trailing newline.
		Body: bytes.NewBuffer([]byte(strings.Join(bulkItems, "\n") + "\n")),
	}

	// Actually send the request.
	resp, err := bulk.Do(ctx, e.client)
	// Request failures are likely related to connection issues.
	if err != nil {
		log.Error().Msgf("Could not send bulk request: %s", err)
		// Mark all items as failed
		for i := range items {
			res[i] = false
		}
		return res
	}

	log.Debug().Msgf("Bulk request of %d items was processed.", len(items))

	defer resp.Body.Close()

	// Deeper error inspection -- it could be that the request succeeded but some items failed.
	var bulkResponse BulkResponse
	json.NewDecoder(resp.Body).Decode(&bulkResponse)

	if bulkResponse.Errors {
		for i, item := range bulkResponse.Items {
			var inner BulkResponseItem
			if item.Update != (BulkResponseItem{}) {
				inner = item.Update
			} else {
				inner = item.Create
			}
			if inner.Error.Type != "" {
				res[i] = false
				log.Error().Msgf("Document %s failed to index in index %s: %s (status %d)", inner.Id, inner.Index, inner.Error.Reason, inner.Status)
			}
		}
	}

	return res
}

func (e *OpenSearch) Close() {
	if e.batch != nil {
		e.batch.Stop()
	}
	// No-op if no batch writer,
}
