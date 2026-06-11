module github.com/arloliu/zapwire/otlp/internal/conformance

go 1.25.0

replace github.com/arloliu/zapwire/otlp => ../..

require (
	github.com/arloliu/zapwire/otlp v0.0.0-00010101000000-000000000000
	github.com/stretchr/testify v1.11.1
	go.opentelemetry.io/otel/trace v1.44.0
	go.opentelemetry.io/proto/otlp v1.10.0
	go.uber.org/zap v1.28.0
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.28.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/rogpeppe/go-internal v1.15.0 // indirect
	go.opentelemetry.io/otel v1.44.0 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	golang.org/x/net v0.50.0 // indirect
	golang.org/x/sys v0.41.0 // indirect
	golang.org/x/text v0.34.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260209200024-4cfbd4190f57 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260209200024-4cfbd4190f57 // indirect
	google.golang.org/grpc v1.79.2 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
