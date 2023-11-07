package loki

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/grafana/loki/pkg/push"
	"github.com/klauspost/compress/s2"
)

type ClientOptions struct {
	// Default labels passed with each message
	DefaultLabels map[string]string

	// Max wait before sending batch
	BatchWait time.Duration
	// Max batch size
	BatchSize uint

	// Callback what called on every error ocurred in sender.
	// Due to asynchronous nature of sender errors cant be suppied with return values/
	ErrorCallback func(err error)

	// by default uses http.DefaultClient
	HttpClient HttpClient

	// User json instead of protobuf for log pushing
	UseJson bool
}

type HttpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type Client interface {
	Log(ts time.Time, msg string, lables, metadata map[string]string)
	Flush(ctx context.Context)
	Shutdown(ctx context.Context)
}

type clientImpl struct {
	endpoint string

	quit   chan struct{}
	flush  chan struct{}
	closed bool

	entries     chan push.Stream
	waitGroup   sync.WaitGroup
	errCallback func(err error)

	maxBatchSize int
	maxBatchWait time.Duration

	useJson bool
	client  HttpClient
}

// Creates a loki client for v1 api with automatic batching.
// Endpoint example http://localhost:3100/loki/api/v1/push
// Options can be nil for default values
func NewClient(endpoint string, o *ClientOptions) (Client, error) {
	if o == nil {
		o = &ClientOptions{}
	}

	if o.HttpClient == nil {
		o.HttpClient = http.DefaultClient
	}

	client := clientImpl{
		endpoint: endpoint,
		closed:   false,

		maxBatchSize: int(o.BatchSize),
		maxBatchWait: o.BatchWait,

		quit:        make(chan struct{}),
		entries:     make(chan push.Stream, 1),
		errCallback: o.ErrorCallback,

		client:  o.HttpClient,
		useJson: o.UseJson,
	}

	client.waitGroup.Add(1)
	go client.run()

	return &client, nil
}

func (c *clientImpl) Log(ts time.Time, msg string, lables, metadata map[string]string) {
	if c.closed {
		return
	}

	c.entries <- push.Stream{
		Labels: labelsMapToString(lables),
		Entries: []push.Entry{
			{
				Timestamp:          ts,
				Line:               msg,
				StructuredMetadata: mapToAdapter(metadata),
			},
		},
	}
}

func (c *clientImpl) Flush(ctx context.Context) {
	select {
	case <-ctx.Done():
	case c.flush <- struct{}{}:
	}
}

func (c *clientImpl) Shutdown(ctx context.Context) {
	c.closed = true
	select {
	case <-ctx.Done():
	case c.quit <- struct{}{}:
	}
	close(c.quit)
	close(c.flush)
	close(c.entries)
	c.waitGroup.Wait()
}

func (c *clientImpl) run() {
	batch := make([]push.Stream, 0, c.maxBatchSize)
	wait := time.NewTimer(c.maxBatchWait)

	var err error
LOOP:
	for {
		select {
		case <-c.quit:
			break LOOP
		case entry := <-c.entries:
			batch = append(batch, entry) // TODO batch optimization by combining streams with same lables
			if len(batch) >= int(c.maxBatchSize) {
				err = c.send(batch)
				batch = make([]push.Stream, 0, c.maxBatchSize)
				wait.Reset(c.maxBatchWait)
			}

		case <-wait.C:
			if len(batch) > 0 {
				err = c.send(batch)
				batch = make([]push.Stream, 0, c.maxBatchSize)
			}
			wait.Reset(c.maxBatchWait)
		}
		if err != nil && c.errCallback != nil {
			c.errCallback(err)
		}
	}

	for entry := range c.entries {
		batch = append(batch, entry)
	}
	c.send(batch)
	c.waitGroup.Done()
}

func (c *clientImpl) send(streams []push.Stream) error {
	push := push.PushRequest{
		Streams: streams, // TODO optimize by grouping streams with same lables
	}

	var body []byte
	var contentType, contentEncoding string
	var err error
	if c.useJson {
		contentType = "application/json"
		body, err = json.Marshal(push)
		if err != nil {
			return err
		}
	} else {
		contentType = "application/x-protobuf"
		contentEncoding = "snappy"
		body, err = push.Marshal()
		if err != nil {
			return fmt.Errorf("unable to marshal PushRequest: %w", err)
		}
		body = s2.EncodeSnappy(nil, body)
	}

	req, err := http.NewRequest(http.MethodPost, c.endpoint, bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", contentType)
	if contentEncoding != "" {
		req.Header.Set("Content-Encoding", contentEncoding)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("http request error: %w", err)
	}

	if resp.StatusCode != 204 {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("read response body: %w", err)
		}

		return fmt.Errorf("unexpected HTTP status code: %d, message: %s", resp.StatusCode, string(body))
	}

	err = resp.Body.Close()
	if err != nil {
		return fmt.Errorf("unable to close HTTP response body: %w", err)
	}

	return nil
}

func mapToAdapter(ls map[string]string) []push.LabelAdapter {
	if ls == nil || len(ls) == 0 {
		return []push.LabelAdapter{}
	}

	adapters := []push.LabelAdapter{}
	for k, v := range ls {
		adapters = append(adapters, push.LabelAdapter{
			Name:  k,
			Value: v,
		})
	}
	return adapters
}

func labelsMapToString(ls map[string]string) string {
	var b strings.Builder
	totalSize := 2
	lstrs := make([]string, 0, len(ls))

	for l, v := range ls {

		lstrs = append(lstrs, l)
		// guess size increase: 2 for `, ` between labels and 3 for the `=` and quotes around label value
		totalSize += len(l) + 2 + len(v) + 3
	}

	b.Grow(totalSize)
	b.WriteByte('{')
	slices.Sort(lstrs)
	for i, l := range lstrs {
		if i > 0 {
			b.WriteString(", ")
		}

		b.WriteString(string(l))
		b.WriteString(`=`)
		b.WriteString(strconv.Quote(string(ls[l])))
	}
	b.WriteByte('}')

	return b.String()
}
