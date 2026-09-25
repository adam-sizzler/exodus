package users

import (
	"testing"
)

func BenchmarkFormatUserID(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = FormatUserID(int64(i % 10000))
	}
}

func BenchmarkFormatUserIDHigh(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = FormatUserID(int64(20000 + (i % 5000)))
	}
}
