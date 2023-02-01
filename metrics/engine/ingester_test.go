package engine

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/lib"
	"go.k6.io/k6/lib/testutils"
	"go.k6.io/k6/metrics"
)

func TestIngesterOutputFlushMetrics(t *testing.T) {
	t.Parallel()

	piState := newTestPreInitState(t)
	testMetric, err := piState.Registry.NewMetric("test_metric", metrics.Trend)
	require.NoError(t, err)

	ingester := outputIngester{
		logger: piState.Logger,
		metricsEngine: &MetricsEngine{
			observedMetrics: make(map[*metrics.Metric]*ObservedMetric),
		},
	}
	ingester.metricsEngine.observedMetrics[testMetric] = &ObservedMetric{
		Metric: testMetric,
		sink:   &metrics.TrendSink{},
	}

	require.NoError(t, ingester.Start())
	ingester.AddMetricSamples([]metrics.SampleContainer{metrics.Sample{
		TimeSeries: metrics.TimeSeries{Metric: testMetric},
		Value:      21,
	}})
	ingester.AddMetricSamples([]metrics.SampleContainer{metrics.Sample{
		TimeSeries: metrics.TimeSeries{Metric: testMetric},
		Value:      21,
	}})
	require.NoError(t, ingester.Stop())

	ometric := ingester.metricsEngine.observedMetrics[testMetric]
	require.NotNil(t, ometric)
	require.NotNil(t, ometric.sink)
	assert.Equal(t, testMetric, ometric.Metric)

	sink := ometric.sink.(*metrics.TrendSink) //nolint:forcetypeassert
	assert.Equal(t, 42.0, sink.Sum)
}

func TestIngesterOutputFlushSubmetrics(t *testing.T) {
	t.Parallel()

	piState := newTestPreInitState(t)
	testMetric, err := piState.Registry.NewMetric("test_metric", metrics.Gauge)
	require.NoError(t, err)

	me := &MetricsEngine{
		test: &lib.TestRunState{
			TestPreInitState: piState,
		},
		observedMetrics: make(map[*metrics.Metric]*ObservedMetric),
	}
	submetric, err := me.getThresholdMetricOrSubmetric("test_metric{a:1}")
	require.NoError(t, err)

	// assert that observed metrics is empty before to start
	require.Len(t, me.observedMetrics, 0)

	ingester := outputIngester{
		logger:        piState.Logger,
		metricsEngine: me,
	}
	require.NoError(t, ingester.Start())
	ingester.AddMetricSamples([]metrics.SampleContainer{metrics.Sample{
		TimeSeries: metrics.TimeSeries{
			Metric: testMetric,
			Tags: piState.Registry.RootTagSet().WithTagsFromMap(
				map[string]string{"a": "1", "b": "2"}),
		},
		Value: 21,
	}})
	require.NoError(t, ingester.Stop())

	require.Len(t, ingester.metricsEngine.observedMetrics, 2)

	// assert the parent has been observed
	ometric := ingester.metricsEngine.observedMetrics[testMetric]
	require.NotNil(t, ometric)
	require.NotNil(t, ometric.sink)
	assert.IsType(t, &metrics.GaugeSink{}, ometric.sink)
	assert.Equal(t, 21, ometric.sink.(*metrics.GaugeSink).Value)

	// assert the submetric has been observed
	ometric = ingester.metricsEngine.observedMetrics[submetric]
	require.NotNil(t, ometric)
	require.NotNil(t, ometric.sink)
	require.NotNil(t, ometric.Metric.Sub)
	assert.EqualValues(t, map[string]string{"a": "1"}, ometric.Metric.Sub.Tags.Map())
	assert.IsType(t, &metrics.GaugeSink{}, ometric.sink)
	assert.Equal(t, 21, ometric.sink.(*metrics.GaugeSink).Value)
}

func newTestPreInitState(tb testing.TB) *lib.TestPreInitState {
	reg := metrics.NewRegistry()
	logger := testutils.NewLogger(tb)
	logger.SetLevel(logrus.DebugLevel)
	return &lib.TestPreInitState{
		Logger:         logger,
		RuntimeOptions: lib.RuntimeOptions{},
		Registry:       reg,
		BuiltinMetrics: metrics.RegisterBuiltinMetrics(reg),
	}
}
