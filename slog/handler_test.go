package lokislog_test

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/royalcat/loki"
	slogloki "github.com/royalcat/loki/slog"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestEnabled(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	handler, err := slogloki.NewHandler(nil, slogloki.HandlerOptions{
		Level: slog.LevelInfo,
	})
	require.Nil(err)
	ctx := context.Background()
	require.False(handler.Enabled(ctx, slog.LevelDebug))
	require.True(handler.Enabled(ctx, slog.LevelInfo))
	require.True(handler.Enabled(ctx, slog.LevelWarn))
	require.True(handler.Enabled(ctx, slog.LevelError))

}

type MockedClient struct {
	mock.Mock
}

// Flush implements loki.Client.
func (*MockedClient) Flush(ctx context.Context) {
}

// Shutdown implements loki.Client.
func (*MockedClient) Shutdown(ctx context.Context) {
}

// Log implements loki.Client.
func (m *MockedClient) Log(ts time.Time, msg string, lables map[string]string, metadata map[string]string) {
	m.Called(ts, msg, lables, metadata)
}

var _ loki.Client = (*MockedClient)(nil)

func TestHandle(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	require := require.New(t)
	client := MockedClient{}
	handler, err := slogloki.NewHandler(&client, slogloki.HandlerOptions{
		DefaultAttrs: []slog.Attr{slog.String("a", "b")},
	})
	require.Nil(err)
	handler = handler.
		WithAttrs([]slog.Attr{
			slog.String("foo1", "bar1"),
			slog.String("err", strings.Repeat(".", 1050)),
			slog.Group("static_metadata",
				"f", 0.3,
			),
		})
	{

		handler := handler.
			WithAttrs([]slog.Attr{slog.String("first", "run")}).
			WithAttrs([]slog.Attr{slog.Group("static_metadata",
				"f", 1,
			)})
		rec := slog.Record{
			Time:    time.Now(),
			Message: "start",
			Level:   slog.LevelInfo,
		}
		client.On("Log",
			rec.Time,
			rec.Message,
			map[string]string{
				"a":     "b",
				"foo1":  "bar1",
				"first": "run",
				"level": rec.Level.String(),
			},
			map[string]string{
				"err": strings.Repeat(".", 1050),
				"f":   "1",
			},
		).Return()
		err := handler.Handle(ctx, rec)
		require.Nil(err)
	}
	for i := 0; i < 5; i++ {
		handler := handler.
			WithGroup("baz1").
			WithAttrs([]slog.Attr{slog.String("foo2", "bar2")}).
			WithGroup("baz2").
			WithAttrs([]slog.Attr{slog.String("foo3", "bar3")})

		rec := slog.Record{
			Time:    time.Now(),
			Message: "test",
			Level:   slog.LevelError,
		}
		rec.AddAttrs(
			slog.Bool("true", true),
			slog.Group("fizz",
				"buzz", "buzz",
			),
		)

		client.On("Log",
			rec.Time,
			rec.Message,
			map[string]string{
				"a":              "b",
				"foo1":           "bar1",
				"baz1_foo2":      "bar2",
				"baz1_baz2_foo3": "bar3",
				"fizz_buzz":      "buzz",
				"true":           "true",
				"level":          rec.Level.String(),
			},
			map[string]string{
				"f":   "0.3",
				"err": strings.Repeat(".", 1050),
			},
		).Return()
		err := handler.Handle(ctx, rec)
		require.Nil(err)
	}
}
