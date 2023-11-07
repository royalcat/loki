# Loki

Log pushing client library for [Loki](https://github.com/grafana/loki) using subpackage from official repository (<https://github.com/grafana/loki/tree/main/pkg/push>)

This library supports both JSON and Protobuf payloads.

Includes loki slog handler in loki/slog, import as

```go
    import (
        lokislog "github.com/royalcat/loki/slog"
    )
```
