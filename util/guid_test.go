package util

import (
	// . "code.sohuno.com/kzapp/push-server/util"
	"testing"
)

func BenchmarkGUID(b *testing.B) {
	factory := &GuidFactory{}
	for i := 0; i < b.N; i++ {
		_, err := factory.NewGUID(0)
		if err != nil {
			continue
		}
	}
}
