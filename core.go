package zapwire

import "go.uber.org/zap/zapcore"

// Writer satisfies zapcore.WriteSyncer (Write + Sync).
var _ zapcore.WriteSyncer = (*Writer)(nil)

// NewCore builds a zapcore.Core that encodes entries with enc and ships the bytes to ws at
// or above level. It is a thin convenience over zapcore.NewCore so callers need not import
// zapcore for the common case.
func NewCore(enc zapcore.Encoder, ws zapcore.WriteSyncer, level zapcore.LevelEnabler) zapcore.Core {
	return zapcore.NewCore(enc, ws, level)
}
