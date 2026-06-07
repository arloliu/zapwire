# 600 - Go After Write

After modifying any `.go` file:
1. `go fix ./<pkg>/...` (touched packages only; never repo-wide in a feature commit).
2. `make lint` — fix all issues.
3. `make test` — re-run until green.

If lint output looks stale: `make clean-linter-cache && make lint`. Don't keep a `//nolint`
that a cold-cache run no longer needs.
