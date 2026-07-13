semnautics/
├── cmd/
│   └── main.go                 # Daemon entry point, signal handling, and wiring
├── internal/
│   ├── dag/                    # A* heuristic search, dynamic graph state
│   ├── mapreduce/              # Go channels, consistent hashing shuffle, Mappers, Reducers
│   ├── memory/                 # Apache Arrow memory mapped buffers
│   ├── ingest/                 # Ingestion layer
│   │   ├── clickhouse/         # ClickHouse specific tailing and batch fetching
│   │   ├── postgres/           # Postgres logical replication (WAL) consumer
│   │   └── source.go           # Dependency Inversion interface for all sources
│   └── server/                 # Arrow Flight egress (we will build this last)
├── pkg/
│   └── types/                  # Core data structures (e.g., event.go)
├── deploy/
│   ├── semnautics.service      # Ubuntu systemctl daemon configuration
│   └── config.yaml             # DB connection strings and routing rules
├── go.mod
└── go.sum