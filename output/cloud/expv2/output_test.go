package expv2

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/metrics"
	"go.k6.io/k6/output/cloud/expv2/pbcloud"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestOutputCollectSamples(t *testing.T) {
	t.Parallel()

	o := Output{
		activeSeries: make(map[*metrics.Metric]aggregatedSamples),
	}
	r := metrics.NewRegistry()

	m1 := r.MustNewMetric("metric1", metrics.Counter)
	s1 := metrics.Sample{
		TimeSeries: metrics.TimeSeries{
			Metric: m1,
			Tags:   r.RootTagSet().With("key1", "val1"),
		},
	}
	subs1 := metrics.Sample{
		TimeSeries: metrics.TimeSeries{
			Metric: m1,
			Tags:   r.RootTagSet().With("key1", "valsub1"),
		},
	}
	m2 := r.MustNewMetric("metric2", metrics.Counter)
	s2 := metrics.Sample{
		TimeSeries: metrics.TimeSeries{
			Metric: m2,
			Tags:   r.RootTagSet().With("key2", "val2"),
		},
	}

	o.collectSamples([]metrics.SampleContainer{
		metrics.Samples{s1},
		metrics.Samples{s2},
		metrics.Samples{subs1},
		metrics.Samples{s1},
	})
	require.Len(t, o.activeSeries, 2)

	assert.Equal(t, []*metrics.Sample{&s1, &s1}, o.activeSeries[m1].Samples[s1.TimeSeries])
	assert.Equal(t, []*metrics.Sample{&subs1}, o.activeSeries[m1].Samples[subs1.TimeSeries])
	assert.Equal(t, []*metrics.Sample{&s2}, o.activeSeries[m2].Samples[s2.TimeSeries])
}

func TestOutputMapMetricProto(t *testing.T) {
	t.Parallel()

	o := Output{}
	r := metrics.NewRegistry()

	m1 := r.MustNewMetric("metric1", metrics.Counter)
	s1 := metrics.Sample{
		TimeSeries: metrics.TimeSeries{
			Metric: m1,
			Tags:   r.RootTagSet().With("key1", "val1"),
		},
	}

	aggSamples := aggregatedSamples{
		Samples: map[metrics.TimeSeries][]*metrics.Sample{
			s1.TimeSeries: {&s1},
		},
	}

	protodata := o.mapMetricProto(m1, aggSamples)
	assert.Equal(t, "metric1", protodata.Name)
	assert.Equal(t, "METRIC_TYPE_COUNTER", pbcloud.MetricType_name[int32(protodata.Type)])
	assert.Len(t, protodata.TimeSeries, 1)
}

func TestAggregatedSamplesMapAsProto(t *testing.T) {
	t.Parallel()

	expLabels := []*pbcloud.Label{
		{Name: "test_run_id", Value: "fake-test-id"},
		{Name: "key1", Value: "val1"},
	}
	now := time.Now()
	tests := []struct {
		mtyp   metrics.MetricType
		expmap *pbcloud.TimeSeries
	}{
		{
			mtyp: metrics.Counter,
			expmap: &pbcloud.TimeSeries{
				Labels: append([]*pbcloud.Label{{Name: "__name__", Value: "metriccounter"}}, expLabels...),
				Samples: &pbcloud.TimeSeries_CounterSamples{
					CounterSamples: &pbcloud.CounterSamples{
						Values: []*pbcloud.CounterValue{
							{Time: timestamppb.New(now), Value: 42},
							{Time: timestamppb.New(now), Value: 42},
						},
					},
				},
			},
		},
		{
			mtyp: metrics.Gauge,
			expmap: &pbcloud.TimeSeries{
				Labels: append([]*pbcloud.Label{{Name: "__name__", Value: "metricgauge"}}, expLabels...),
				Samples: &pbcloud.TimeSeries_GaugeSamples{
					GaugeSamples: &pbcloud.GaugeSamples{
						Values: []*pbcloud.GaugeValue{
							{Time: timestamppb.New(now), Last: 42, Min: 42, Max: 42, Avg: 42, Count: 1},
							{Time: timestamppb.New(now), Last: 42, Min: 42, Max: 42, Avg: 42, Count: 1},
						},
					},
				},
			},
		},
		{
			mtyp: metrics.Rate,
			expmap: &pbcloud.TimeSeries{
				Labels: append([]*pbcloud.Label{{Name: "__name__", Value: "metricrate"}}, expLabels...),
				Samples: &pbcloud.TimeSeries_RateSamples{
					RateSamples: &pbcloud.RateSamples{
						Values: []*pbcloud.RateValue{
							{Time: timestamppb.New(now), NonzeroCount: 1, TotalCount: 1},
							{Time: timestamppb.New(now), NonzeroCount: 1, TotalCount: 1},
						},
					},
				},
			},
		},
		// {mtyp: metrics.Trend},
	}

	r := metrics.NewRegistry()

	for _, tc := range tests {
		tc := tc
		t.Run(tc.mtyp.String(), func(t *testing.T) {
			t.Parallel()

			s1 := metrics.Sample{
				TimeSeries: metrics.TimeSeries{
					Metric: r.MustNewMetric(fmt.Sprintf("metric%s", tc.mtyp.String()), tc.mtyp),
					Tags:   r.RootTagSet().With("key1", "val1"),
				},
				Time:  now,
				Value: 42.0,
			}

			aggSamples := aggregatedSamples{
				Samples: map[metrics.TimeSeries][]*metrics.Sample{
					s1.TimeSeries: {&s1, &s1},
				},
			}
			pbsamples := aggSamples.MapAsProto("fake-test-id")
			require.Len(t, pbsamples, 1)
			assert.Equal(t, tc.expmap.Labels, pbsamples[0].Labels)
			assert.Equal(t, tc.expmap.Samples, pbsamples[0].Samples)
		})
	}
}
