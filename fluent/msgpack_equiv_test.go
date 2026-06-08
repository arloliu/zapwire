// fluent/msgpack_equiv_test.go
package fluent

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// transcodeRecord runs fields through a real zap JSON encoder + the transcode Encoder and returns
// the decoded record (the proven baseline).
func transcodeRecord(t *testing.T, cfg zapcore.EncoderConfig, ent zapcore.Entry, fields []zapcore.Field) map[string]any {
	t.Helper()
	jsonEnc := zapcore.NewJSONEncoder(cfg)
	buf, err := jsonEnc.EncodeEntry(ent, fields)
	require.NoError(t, err)
	out, err := NewEncoderWithCodec(EpochNanosCodec(cfg.TimeKey)).Encode(nil, buf.Bytes())
	require.NoError(t, err)

	return decodeEntry(t, out).Record.(map[string]any)
}

func TestNative_EquivalentToTranscode_ScalarSubset(t *testing.T) {
	cfg := zap.NewProductionEncoderConfig()
	cfg.TimeKey = "ts"
	cfg.EncodeTime = zapcore.EpochNanosTimeEncoder
	ent := zapcore.Entry{Level: zapcore.InfoLevel, Message: "req done", Time: time.Unix(1692959400, 0)}
	fields := []zapcore.Field{
		zap.String("svc", "zapwire"),
		zap.Bool("ok", true),
		zap.Int64("id", 9007199254740993), // > 2^53
		zap.Uint64("big", math.MaxUint64),
		zap.Float64("ratio", 1.5),
		zap.Float64("nan", math.NaN()),
		zap.Float64("pinf", math.Inf(1)),
	}

	native := decodeEntryRecord(t, newMsgpackEncoder(cfg), ent, fields)
	transcoded := transcodeRecord(t, cfg, ent, fields)

	// Both lift time out of the record (native via the extension; transcode via the codec key),
	// so compare the remaining record fields.
	delete(transcoded, "ts")
	assert.Equal(t, transcoded, native, "native record must equal the transcode record for the scalar subset")
}
