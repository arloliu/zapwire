# 300 - Testing

- Table-driven tests with `testify` (`require` for fatal, `assert` for soft).
- Run with `-race`. Network tests use ephemeral UDS paths under `os.TempDir()` and a real
  `net.Listen` mock server; clean up sockets in `defer`/cleanup.
- For goroutine-leak / reconnect assertions, poll from the test goroutine (not inside
  `require.Eventually`'s spawned goroutine) when counting `runtime.NumGoroutine()`.
- Encoder/Framer: assert golden wire bytes AND round-trip decode.
