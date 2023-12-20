# Loki

## Client

[![GoDoc](https://godoc.org/github.com/royalcat/loki?status.png)](https://godoc.org/github.com/royalcat/loki)

Log pushing client library for [Loki](https://github.com/grafana/loki) using subpackage from official repository (<https://github.com/grafana/loki/tree/main/pkg/push>)

This library supports both JSON and Protobuf payloads.

## Slog

[![GoDoc](https://godoc.org/github.com/royalcat/loki/slog?status.png)](https://godoc.org/github.com/royalcat/loki/slog)

Includes an experimental loki slog handler, import as

```go
    import (
        lokislog "github.com/royalcat/loki/slog"
    )
```
