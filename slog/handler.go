package lokislog

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"log/slog"

	"github.com/royalcat/loki"
)

type LabelHandler func(group []string, attr slog.Attr) (isLabel bool, keyOverwrite string)

type HandlerOptions struct {
	// log level (default: debug)
	Level slog.Leveler

	// default attributes for all messages
	DefaultAttrs []slog.Attr

	// label name for log level, default: "level"
	// this label always will be overridden by slog.Record level
	LevelKey string

	GroupSplitter string

	// Func to dynamically split attributes between labels and static metadata, this will never be called on group.
	//
	// By default any label in group "static_metadata" or longer than 1024 symbols will be moved to static metadata
	LabelHandler LabelHandler
}

var lableNameRegex = regexp.MustCompile("^[a-zA-Z_:][a-zA-Z0-9_:]*$")

func NewHandler(client loki.Client, o HandlerOptions) (slog.Handler, error) {
	if o.Level == nil {
		o.Level = slog.LevelDebug
	}
	if o.LevelKey == "" {
		o.LevelKey = "level"
	}
	if o.GroupSplitter == "" {
		o.GroupSplitter = "_"
	} else {
		if !lableNameRegex.Match([]byte(o.GroupSplitter)) {
			return nil, fmt.Errorf("GroupSplitter not valid. Metric names may contain ASCII letters, digits, underscores, and colons. It must match the regex '[a-zA-Z_:][a-zA-Z0-9_:]*.'")
		}

	}
	if o.LabelHandler == nil {
		o.LabelHandler = func(groups []string, attr slog.Attr) (isLabel bool, keyOverwrite string) {
			return false, ""
		}
	}

	return &LokiHandler{
		level:    o.Level,
		levelKey: o.LevelKey,

		lb: newLabelBuilder(o.GroupSplitter, o.LabelHandler).withAttrs(o.DefaultAttrs),

		client: client,
	}, nil
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

func (h *LokiHandler) Handle(_ context.Context, record slog.Record) error {
	labels := make(map[string]string, 1+len(h.lb.labels))
	metadata := make(map[string]string, len(h.lb.metadata))

	labels, metadata = h.lb.build(labels, metadata)
	labels, metadata = newLabelBuilder(h.lb.groupSplitter, h.lb.LabelHandler).withAttrs(getAttrs(record)).build(labels, metadata)

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

	LabelHandler LabelHandler
}

func newLabelBuilder(groupSplitter string, labelHandler LabelHandler) labelBuilder {
	return labelBuilder{
		groupSplitter: groupSplitter,
		labels:        map[string]string{},
		metadata:      map[string]string{},
		LabelHandler:  labelHandler,
	}
}

func (b labelBuilder) withAttrs(attrs []slog.Attr) labelBuilder {
	b = b.copy()

	for _, a := range attrs {
		switch a.Value.Kind() {
		case slog.KindGroup:
			b.withGroup(a.Key).withAttrs(a.Value.Group()).build(b.labels, b.metadata)
		default:
			key := strings.Join(append(b.group, a.Key), b.groupSplitter)
			if isLabel, override := b.LabelHandler(b.group, a); isLabel {
				if override != "" {
					key = override
				}
				b.labels[key] = a.Value.String()
			} else {
				b.metadata[key] = a.Value.String()
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
