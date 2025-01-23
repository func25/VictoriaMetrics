package streamaggr

import (
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/cespare/xxhash/v2"
)

func countSeriesInitFn(v *aggrValues, enableWindows bool) {
	v.blue = append(v.blue, &countSeriesAggrValue{
		samples: make(map[uint64]struct{}),
	})
	if enableWindows {
		v.green = append(v.green, &countSeriesAggrValue{
			samples: make(map[uint64]struct{}),
		})
	}
}

type countSeriesAggrValue struct {
	samples map[uint64]struct{}
}

func (av *countSeriesAggrValue) pushSample(inputKey string, _ *pushSample, _ int64) {
	// Count unique hashes over the inputKeys instead of unique inputKey values.
	// This reduces memory usage at the cost of possible hash collisions for distinct inputKey values.
	h := xxhash.Sum64(bytesutil.ToUnsafeBytes(inputKey))
	if _, ok := av.samples[h]; !ok {
		av.samples[h] = struct{}{}
	}
}

func (av *countSeriesAggrValue) flush(ctx *flushCtx, key string) {
	ctx.appendSeries(key, "count_series", float64(len(av.samples)))
	clear(av.samples)
}
