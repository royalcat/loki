package lokislog

import (
	"context"
	"strings"
	"time"

	"log/slog"

	"github.com/royalcat/loki"
)

type MetadataHandler func(group []string, attr slog.Attr) (metaGroup []string, metaAttr slog.Attr, isMetadata bool)

type HandlerOptions struct {
	// log level (default: debug)
	Level slog.Leveler

	// default attributes for all messages
	DefaultAttrs []slog.Attr

	// label name for log level, default: "level"
	// this label always will be overridden by slog.Record level
	LevelKey string

	GroupSplitter string

	// log batching
	BatchWait time.Duration
	BatchSize uint

	// Func to dynamically split attributes between labels and static metadata, this will never be called on group.
	//
	// By default any label in group "static_metadata" or longer than 1024 symbols will be moved to static metadata
	MetadataHandler MetadataHandler
}

func NewHandler(client loki.Client, o HandlerOptions) slog.Handler {
	if o.Level == nil {
		o.Level = slog.LevelDebug
	}
	if o.BatchWait == 0 {
		o.BatchWait = 5 * time.Second
	}
	if o.BatchSize == 0 {
		o.BatchSize = 5
	}
	if o.LevelKey == "" {
		o.LevelKey = "level"
	}
	if o.GroupSplitter == "" {
		o.GroupSplitter = "."
	}
	if o.MetadataHandler == nil {
		o.MetadataHandler = func(groups []string, attr slog.Attr) (metaGroups []string, metaAttr slog.Attr, isMetadata bool) {
			if len(attr.Value.String()) > 1024 {
				return groups, attr, true
			}
			if len(groups) > 0 && groups[0] == "static_metadata" {
				return groups[1:], attr, true
			}

			return nil, slog.Attr{}, false
		}
	}

	return &LokiHandler{
		level:    o.Level,
		levelKey: o.LevelKey,

		lb: newLabelBuilder(o.GroupSplitter, o.MetadataHandler).withAttrs(o.DefaultAttrs),

		client: client,
	}
}

var _ slog.Handler = (*LokiHandler)(nil)

type LokiHandler struct {
	client loki.Client

	level    slog.Leveler
	levelKey string

	lb labelBuilder
}

func (h *LokiHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level.Level()
}

func (h *LokiHandler) Handle(ctx context.Context, record slog.Record) error {
	labels := make(map[string]string, 1+len(h.lb.labels))
	metadata := make(map[string]string, len(h.lb.metadata))

	labels, metadata = h.lb.build(labels, metadata)
	labels, metadata = newLabelBuilder(h.lb.groupSplitter, h.lb.isMetadata).withAttrs(getAttrs(record)).build(labels, metadata)

	labels[h.levelKey] = record.Level.String()

	h.client.Log(record.Time, record.Message, labels, metadata)

	return nil
}

func getAttrs(record slog.Record) []slog.Attr {
	attrs := make([]slog.Attr, 0, record.NumAttrs())
	record.Attrs(func(a slog.Attr) bool {
		attrs = append(attrs, a)
		return true
	})
	return attrs
}

type labelBuilder struct {
	group         []string
	groupSplitter string

	labels   map[string]string
	metadata map[string]string

	isMetadata MetadataHandler
}

func newLabelBuilder(groupSplitter string, isMetadata MetadataHandler) labelBuilder {
	return labelBuilder{
		groupSplitter: groupSplitter,
		labels:        map[string]string{},
		metadata:      map[string]string{},
		isMetadata:    isMetadata,
	}
}

func (b labelBuilder) withAttrs(attrs []slog.Attr) labelBuilder {
	b = b.copy()

	for _, a := range attrs {
		switch a.Value.Kind() {
		case slog.KindGroup:
			b.withGroup(a.Key).withAttrs(a.Value.Group()).build(b.labels, b.metadata)
		default:

			if group, attr, isMetadata := b.isMetadata(b.group, a); isMetadata {
				key := strings.Join(append(group, a.Key), b.groupSplitter)
				b.metadata[key] = attr.Value.String()
			} else {
				key := strings.Join(append(b.group, a.Key), b.groupSplitter)
				b.labels[key] = a.Value.String()
			}
		}

	}
	return b
}

func (b labelBuilder) withGroup(name string) labelBuilder {
	b = b.copy()
	b.group = append(b.group, name)
	return b
}

func (b labelBuilder) copy() labelBuilder {
	b.labels = copyMap(b.labels)
	b.metadata = copyMap(b.metadata)
	return b
}

func (b labelBuilder) build(label, metadata map[string]string) (map[string]string, map[string]string) {
	for k, v := range b.labels {
		label[k] = v
	}
	for k, v := range b.metadata {
		metadata[k] = v
	}

	return label, metadata
}

func (h *LokiHandler) WithAttrs(attrs []slog.Attr) slog.Handler {

	return &LokiHandler{
		client: h.client,

		level:    h.level,
		levelKey: h.levelKey,

		lb: h.lb.withAttrs(attrs),
	}
}

func (h *LokiHandler) WithGroup(name string) slog.Handler {
	return &LokiHandler{
		client: h.client,

		level:    h.level,
		levelKey: h.levelKey,

		lb: h.lb.withGroup(name),
	}
}

func copyMap[K comparable, V any](data map[K]V) map[K]V {
	newMap := make(map[K]V, len(data))

	for key, value := range data {
		newMap[key] = value
	}

	return newMap
}
