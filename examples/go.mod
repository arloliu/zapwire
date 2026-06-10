module github.com/arloliu/zapwire/examples

go 1.25.0

replace github.com/arloliu/zapwire => ../

require (
	github.com/arloliu/zapwire v0.0.0-00010101000000-000000000000
	github.com/tinylib/msgp v1.6.4
	go.uber.org/zap v1.28.0
)

require (
	github.com/philhofer/fwd v1.2.0 // indirect
	go.uber.org/multierr v1.10.0 // indirect
)
