package expv2

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.k6.io/k6/output/cloud/expv2/pbcloud"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestValueBacket(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in  float64
		exp uint32
	}{
		{in: -1029, exp: 0},
		{in: -12, exp: 0},
		{in: -0.82673, exp: 0},
		{in: 10, exp: 10},
		{in: 12, exp: 12},
		{in: 12.5, exp: 13},
		{in: 20, exp: 20},
		{in: 255, exp: 255},
		{in: 256, exp: 256},
		{in: 282.29, exp: 269},
		{in: 1029, exp: 512},
		{in: (1 << 30) - 1, exp: 3071},
		{in: (1 << 30), exp: 3072},
		{in: math.MaxInt32, exp: 3199},
		{in: math.MaxInt32 + 1, exp: 2147483648}, // int32 overflow
	}
	for _, tc := range tests {
		assert.Equal(t, int(tc.exp), int(resolveBucketIndex(tc.in)), tc.in)
	}
}

func TestNewHistogramWithSimpleValue(t *testing.T) {
	t.Parallel()
	res := newHistogram([]float64{100})

	exp := histogram{
		Buckets:            []uint32{1},
		FirstNotZeroBucket: 100,
		LastNotZeroBucket:  100,
		ExtraLowBucket:     0,
		ExtraHighBucket:    0,
		Max:                100,
		Min:                100,
		Sum:                100,
		Count:              1,
	}
	assert.Equal(t, exp, res)
}

func TestNewHistogramWithUntrackables(t *testing.T) {
	t.Parallel()
	res := newHistogram([]float64{5, -3.14, 2 * 1e9, 1})

	exp := histogram{
		Buckets:            []uint32{1, 0, 0, 0, 1},
		FirstNotZeroBucket: 1,
		LastNotZeroBucket:  5,
		ExtraLowBucket:     1,
		ExtraHighBucket:    1,
		Max:                2 * 1e9,
		Min:                -3.14,
		Sum:                2*1e9 + 5 + 1 - 3.14,
		Count:              4,
	}
	assert.Equal(t, exp, res)
}

func TestNewHistogramWithMultipleValues(t *testing.T) {
	t.Parallel()
	res := newHistogram([]float64{51.8, 103.6, 103.6, 103.6, 103.6})

	exp := histogram{
		FirstNotZeroBucket: 52,
		LastNotZeroBucket:  104,
		Max:                103.6,
		Min:                51.8,
		ExtraLowBucket:     0,
		ExtraHighBucket:    0,
		Buckets:            append(append([]uint32{1}, make([]uint32, 51)...), 4),
		// Buckets = {1, 0 for 51 times, 4}
		Sum:   466.20000000000005,
		Count: 5,
	}
	assert.Equal(t, exp, res)
}

func TestNewHistogramWithNegativeNum(t *testing.T) {
	t.Parallel()
	res := newHistogram([]float64{-2.42314})

	exp := histogram{
		FirstNotZeroBucket: 0,
		Max:                -2.42314,
		Min:                -2.42314,
		Buckets:            nil,
		ExtraLowBucket:     1,
		ExtraHighBucket:    0,
		Sum:                -2.42314,
		Count:              1,
	}
	assert.Equal(t, exp, res)
}

func TestNewHistogramWithMultipleNegativeNums(t *testing.T) {
	t.Parallel()
	res := newHistogram([]float64{-0.001, -0.001, -0.001})

	exp := histogram{
		Buckets:            nil,
		FirstNotZeroBucket: 0,
		ExtraLowBucket:     3,
		ExtraHighBucket:    0,
		Max:                -0.001,
		Min:                -0.001,
		Sum:                -0.003,
		Count:              3,
	}
	assert.Equal(t, exp, res)
}

func TestNewHistoramWithNoVals(t *testing.T) {
	t.Parallel()
	res := newHistogram([]float64{})
	exp := histogram{
		Buckets:            nil,
		FirstNotZeroBucket: 0,
		ExtraLowBucket:     0,
		ExtraHighBucket:    0,
		Max:                0,
		Min:                0,
		Sum:                0,
	}
	assert.Equal(t, exp, res)
}

func TestHistogramTrimzeros(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in  histogram
		exp []uint32
	}{
		{in: histogram{Buckets: []uint32{}}, exp: []uint32{}},
		{in: histogram{Buckets: []uint32{0}}, exp: []uint32{}},
		{in: histogram{Buckets: []uint32{0, 0, 0}}, exp: []uint32{}},
		{
			in: histogram{
				Buckets:            []uint32{0, 0, 0, 0, 0, 0, 1, 0},
				FirstNotZeroBucket: 6,
				LastNotZeroBucket:  6,
			},
			exp: []uint32{1},
		},
		{
			in: histogram{
				Buckets:            []uint32{0, 0, 0, 1, 9, 0, 0, 1, 0, 0, 0},
				FirstNotZeroBucket: 3,
				LastNotZeroBucket:  7,
			},
			exp: []uint32{1, 9, 0, 0, 1},
		},
	}

	for _, tc := range cases {
		h := tc.in
		h.Count = 1
		h.trimzeros()
		assert.Equal(t, tc.exp, h.Buckets, tc.in.Buckets)
	}
}

func TestHistogramAsProto(t *testing.T) {
	t.Parallel()

	uint32ptr := func(v uint32) *uint32 {
		return &v
	}

	cases := []struct {
		name string
		in   histogram
		exp  *pbcloud.TrendHdrValue
	}{
		{
			name: "empty histogram",
			in:   histogram{},
			exp:  &pbcloud.TrendHdrValue{},
		},
		{
			name: "not trackable values",
			in:   newHistogram([]float64{-0.23, 1<<30 + 1}),
			exp: &pbcloud.TrendHdrValue{
				Count:                  2,
				ExtraLowValuesCounter:  uint32ptr(1),
				ExtraHighValuesCounter: uint32ptr(1),
				Counters:               nil,
				LowerCounterIndex:      0,
				MinValue:               -0.23,
				MaxValue:               1<<30 + 1,
			},
		},
		{
			name: "normal values",
			in:   newHistogram([]float64{2, 1.1, 3}),
			exp: &pbcloud.TrendHdrValue{
				Count:                  3,
				ExtraLowValuesCounter:  nil,
				ExtraHighValuesCounter: nil,
				Counters:               []uint32{2, 1},
				LowerCounterIndex:      2,
				MinValue:               1.1,
				MaxValue:               3,
			},
		},
	}

	for _, tc := range cases {
		tc.exp.MinResolution = 1.0
		tc.exp.SignificantDigits = 2
		tc.exp.Time = &timestamppb.Timestamp{Seconds: 1}
		tc.exp.Sum = tc.in.Sum
		assert.Equal(t, tc.exp, histogramAsProto(tc.in, time.Unix(1, 0)), tc.name)
	}
}
