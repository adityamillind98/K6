// Package expv2 contains a Cloud output using a Protobuf
// binary format for encoding payloads.
package expv2

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"
	"go.k6.io/k6/cloudapi"
	"go.k6.io/k6/metrics"
	"go.k6.io/k6/output"
	"go.k6.io/k6/output/cloud/expv2/pbcloud"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestName is the default Cloud test name
const (
	TestName = "k6 test"
)

// Output sends result data to the k6 Cloud service.
type Output struct {
	output.SampleBuffer

	config      cloudapi.Config
	referenceID string

	logger          logrus.FieldLogger
	client          *MetricsClient
	periodicFlusher *output.PeriodicFlusher
}

// New creates a new cloud output.
func New(logger logrus.FieldLogger, conf cloudapi.Config) (*Output, error) {
	return &Output{
		config: conf,
		client: NewMetricsClient(logger, conf.Host.String),
		logger: logger.WithFields(logrus.Fields{"output": "cloudv2"}),
	}, nil
}

// Start starts the output.
func (o *Output) Start() error {
	o.logger.Debug("Starting...")

	// TODO: merge here the part executed by v1 when we will drop it
	pf, err := output.NewPeriodicFlusher(o.config.MetricPushInterval.TimeDuration(), o.flushMetrics)
	if err != nil {
		return err
	}
	o.logger.Debug("Started!")
	o.periodicFlusher = pf
	return nil
}

// StopWithTestError stops the output.
func (o *Output) StopWithTestError(testErr error) error {
	o.logger.Debug("Stopping...")
	defer o.logger.Debug("Stopped!")
	o.periodicFlusher.Stop()
	return nil
}

// SetReferenceID sets the Cloud's test ID.
func (o *Output) SetReferenceID(refID string) {
	o.referenceID = refID
}

// AddMetricSamples receives a set of metric samples.
func (o *Output) flushMetrics() {
	if o.referenceID == "" {
		// TODO: should it warn?
		return
	}

	start := time.Now()

	series := o.collectSamples()
	if series == nil {
		return
	}

	metricSet := make([]*pbcloud.Metric, 0, len(series))
	for m, aggr := range series {
		metricSet = append(metricSet, o.mapMetricProto(m, aggr))
	}

	ctx, cancel := context.WithTimeout(context.Background(), o.config.MetricPushInterval.TimeDuration())
	defer cancel()

	err := o.client.Push(ctx, o.referenceID, &pbcloud.MetricSet{Metrics: metricSet})
	if err != nil {
		o.logger.Error(err)
		return
	}

	o.logger.WithField("t", time.Since(start)).Debug("Successfully flushed buffered samples to the cloud")
}

func (o *Output) collectSamples() map[*metrics.Metric]aggregatedSamples {
	samplesContainers := o.GetBufferedSamples()
	if len(samplesContainers) < 1 {
		return nil
	}

	// TODO: we expect to do something more complex here
	// so a more efficient mapping is expected

	series := make(map[*metrics.Metric]aggregatedSamples)
	for _, sampleContainer := range samplesContainers {
		samples := sampleContainer.GetSamples()
		for _, sample := range samples {
			aggr, ok := series[sample.Metric]
			if !ok {
				aggr = aggregatedSamples{
					Samples: make(map[metrics.TimeSeries][]metrics.Sample),
				}
				series[sample.Metric] = aggr
			}
			aggr.AddSample(sample)
		}
	}
	return series
}

func (o *Output) mapMetricProto(m *metrics.Metric, as aggregatedSamples) *pbcloud.Metric {
	var mtype pbcloud.MetricType
	switch m.Type {
	case metrics.Counter:
		mtype = pbcloud.MetricType_COUNTER
	case metrics.Gauge:
		mtype = pbcloud.MetricType_GAUGE
	case metrics.Rate:
		mtype = pbcloud.MetricType_RATE
	case metrics.Trend:
		mtype = pbcloud.MetricType_TREND
	}
	return &pbcloud.Metric{
		Name:       m.Name,
		Type:       mtype,
		TimeSeries: as.MapAsProto(),
	}
}

type aggregatedSamples struct {
	Samples map[metrics.TimeSeries][]metrics.Sample
}

func (as *aggregatedSamples) AddSample(s metrics.Sample) {
	ss := as.Samples[s.TimeSeries]
	as.Samples[s.TimeSeries] = append(ss, s)
}

func (as *aggregatedSamples) MapAsProto() []*pbcloud.TimeSeries {
	if len(as.Samples) < 1 {
		return nil
	}
	pbseries := make([]*pbcloud.TimeSeries, 0, len(as.Samples))
	for ts, samples := range as.Samples {
		pb := pbcloud.TimeSeries{}
		// TODO: optimize removing Map
		// and using https://github.com/grafana/k6/issues/2764
		for ktag, vtag := range ts.Tags.Map() {
			pb.Labels = append(pb.Labels, &pbcloud.Label{Name: ktag, Value: vtag})
		}

		// TODO: extend with other missing types
		switch ts.Metric.Type {
		case metrics.Counter:
			counterSamples := &pbcloud.CounterSamples{}
			for _, counterSample := range samples {
				counterSamples.Values = append(counterSamples.Values, &pbcloud.CounterValue{
					Time:  timestamppb.New(counterSample.Time),
					Value: counterSample.Value,
				})
			}
			pb.Samples = &pbcloud.TimeSeries_CounterSamples{
				CounterSamples: counterSamples,
			}
		case metrics.Gauge:
			gaugeSamples := &pbcloud.GaugeSamples{}
			for _, gaugeSample := range samples {
				gaugeSamples.Values = append(gaugeSamples.Values, &pbcloud.GaugeValue{
					Time: timestamppb.New(gaugeSample.Time),
					Last: gaugeSample.Value,
					Min:  gaugeSample.Value,
					Max:  gaugeSample.Value,
					Avg:  gaugeSample.Value,
				})
			}
			pb.Samples = &pbcloud.TimeSeries_GaugeSamples{
				GaugeSamples: gaugeSamples,
			}
		case metrics.Rate:
			rateSamples := &pbcloud.RateSamples{}
			for _, rateSample := range samples {
				nonzero := uint32(0)
				if rateSample.Value != 0 {
					nonzero = 1
				}
				rateSamples.Values = append(rateSamples.Values, &pbcloud.RateValue{
					Time:         timestamppb.New(rateSample.Time),
					NonzeroCount: nonzero,
					TotalCount:   1,
				})
			}
			pb.Samples = &pbcloud.TimeSeries_RateSamples{
				RateSamples: rateSamples,
			}
		case metrics.Trend:
			// TODO: implement the HDR histogram mapping
		}
		pbseries = append(pbseries, &pb)
	}
	return pbseries
}
