package fluent

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEntry_RoundTrip(t *testing.T) {
	want := time.Unix(1692959400, 123456789).UTC()
	e := Entry{Time: EventTime(want), Record: map[string]any{"level": "info", "msg": "hi"}}

	b, err := e.MarshalMsg(nil)
	require.NoError(t, err)

	var got Entry
	rest, err := got.UnmarshalMsg(b)
	require.NoError(t, err)
	require.Empty(t, rest, "no bytes must remain after decoding the entry")

	gotTime := time.Time(got.Time)
	require.True(t, gotTime.Equal(want), "time round-trips: got %v want %v", gotTime, want)
	rec, ok := got.Record.(map[string]any)
	require.True(t, ok)
	require.Equal(t, "info", rec["level"])
	require.Equal(t, "hi", rec["msg"])
}
