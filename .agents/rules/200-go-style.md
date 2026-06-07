# 200 - Go Style

- Idioms: Effective Go. `any` over `interface{}`. Use `slices`/`maps` stdlib.
- Errors: `errors.New` for static; `fmt.Errorf("ctx: %w", err)` to wrap; `errors.Is/As` to
  check. Sentinels `Err*`; error types `*Error`. Errors are the last return value.
- Type assert with comma-ok. Interface assertions: `var _ Iface = (*T)(nil)` after the type
  (or in `_test.go` for public packages that would otherwise import-cycle).
- File layout: package → imports → consts → vars → types → constructors → exported funcs →
  unexported funcs → exported methods → unexported methods.
- Functions ≤ 100 lines (prefer < 50), complexity ≤ 22. Short, consistent receivers.
