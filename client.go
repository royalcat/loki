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

	// Callback what called on every error occurred in sender.
	// Due to asynchronous nature of sender errors cant be suppied with return values/
	ErrorCallback func(err error)

	// by default uses http.DefaultClient
	HTTPClient HTTPClient

	// User json instead of protobuf for log pushing
	UseJSON bool
}

type HTTPClient interface {
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

	useJSON bool
	client  HTTPClient
}

// Creates a loki client for v1 api with automatic batching.
// Endpoint example http://localhost:3100/loki/api/v1/push
// Options can be nil for default values
func NewClient(endpoint string, o *ClientOptions) (Client, error) {
	if o == nil {
		o = &ClientOptions{}
	}

	if o.HTTPClient == nil {
		o.HTTPClient = http.DefaultClient
	}
	if o.BatchWait == 0 {
		o.BatchWait = time.Second
	}
	if o.BatchSize == 0 {
		o.BatchSize = 5
	}

	client := clientImpl{
		endpoint: endpoint,
		closed:   false,

		maxBatchSize: int(o.BatchSize),
		maxBatchWait: o.BatchWait,

		quit:  make(chan struct{}),
		flush: make(chan struct{}),

		entries:     make(chan push.Stream, 1),
		errCallback: o.ErrorCallback,

		client:  o.HTTPClient,
		useJSON: o.UseJSON,

		waitGroup: sync.WaitGroup{},
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

type batch map[string][]push.Entry

func (c *clientImpl) run() {
	b := make(batch, c.maxBatchSize)
	wait := time.NewTicker(c.maxBatchWait)

	var err error
LOOP:
	for {
		select {
		case <-c.quit:
			break LOOP
		case entry := <-c.entries:
			b[entry.Labels] = append(b[entry.Labels], entry.Entries...)
			if len(b) >= int(c.maxBatchSize) {
				err = c.send(b)
				b = make(batch, c.maxBatchSize)
				wait.Reset(c.maxBatchWait)
			}

		case <-wait.C:
			if len(b) > 0 {
				err = c.send(b)
				b = make(batch, c.maxBatchSize)
			}
			wait.Reset(c.maxBatchWait)
		}
		if err != nil && c.errCallback != nil {
			c.errCallback(err)
		}
	}

	for entry := range c.entries {
		b[entry.Labels] = append(b[entry.Labels], entry.Entries...)
	}
	err = c.send(b)
	if err != nil {
		c.errCallback(err)
	}
	c.waitGroup.Done()
}

func (c *clientImpl) send(batch batch) error {
	pushReq := push.PushRequest{
		Streams: make([]push.Stream, 0, len(batch)),
	}
	for l, e := range batch {
		pushReq.Streams = append(pushReq.Streams, push.Stream{
			Labels:  l,
			Entries: e,
		})
	}

	var body []byte
	var contentType, contentEncoding string
	var err error
	if c.useJSON {
		contentType = "application/json"
		body, err = json.Marshal(pushReq)
		if err != nil {
			return err
		}
	} else {
		contentType = "application/x-protobuf"
		contentEncoding = "snappy"
		body, err = pushReq.Marshal()
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
