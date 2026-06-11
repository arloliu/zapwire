module github.com/arloliu/zapwire/examples

go 1.25.0

replace github.com/arloliu/zapwire => ../

replace github.com/arloliu/zapwire/otlp => ../otlp

require (
	github.com/arloliu/zapwire v0.0.0-00010101000000-000000000000
	github.com/arloliu/zapwire/otlp v0.0.0
	github.com/tinylib/msgp v1.6.4
	go.opentelemetry.io/otel/trace v1.44.0
	go.uber.org/zap v1.28.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/philhofer/fwd v1.2.0 // indirect
	go.opentelemetry.io/otel v1.44.0 // indirect
	go.uber.org/multierr v1.10.0 // indirect
)
