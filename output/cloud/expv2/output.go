// Package expv2 contains a Cloud output using a Protobuf
// binary format for encoding payloads.
package expv2

import (
	"context"
	"time"

	"github.com/mstoykov/atlas"
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

	// TODO: if the metric refactor (#2905) will introduce
	// a sequential ID for metrics
	// then we could reuse the same strategy here
	activeSeries map[*metrics.Metric]aggregatedSamples
}

// New creates a new cloud output.
func New(logger logrus.FieldLogger, conf cloudapi.Config) (*Output, error) {
	mc, err := NewMetricsClient(logger, conf.Host.String, conf.Token.String)
	if err != nil {
		return nil, err
	}
	return &Output{
		config:       conf,
		client:       mc,
		logger:       logger.WithFields(logrus.Fields{"output": "cloudv2"}),
		activeSeries: make(map[*metrics.Metric]aggregatedSamples),
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

	if hasOne := o.collectSamples(); !hasOne {
		return
	}

	// TODO: in case an aggregation period will be added then
	// it continue only if the aggregation time frame passed

	metricSet := make([]*pbcloud.Metric, 0, len(o.activeSeries))
	for m, aggr := range o.activeSeries {
		if len(aggr.Samples) < 1 {
			// If a bucket (a metric) has been added
			// then the assumption is to collect at least once in a flush interval.
			continue
		}
		metricSet = append(metricSet, o.mapMetricProto(m, aggr))
		aggr.Clean()
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

func (o *Output) collectSamples() (updates bool) {
	samplesContainers := o.GetBufferedSamples()
	if len(samplesContainers) < 1 {
		return false
	}

	var (
		aggr aggregatedSamples
		ok   bool
	)
	for _, sampleContainer := range samplesContainers {
		samples := sampleContainer.GetSamples()
		for i := 0; i < len(samples); i++ {
			aggr, ok = o.activeSeries[samples[i].Metric]
			if !ok {
				aggr = aggregatedSamples{
					Samples: make(map[metrics.TimeSeries][]*metrics.Sample),
				}
				o.activeSeries[samples[i].Metric] = aggr
			}
			aggr.AddSample(&samples[i])
		}
	}

	return true
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

	// TODO: based on the fact that this mapping is a pointer
	// and it is escaped on the heap evaluate if it makes
	// sense to allocate just once reusing a cached version
	return &pbcloud.Metric{
		Name:       m.Name,
		Type:       mtype,
		TimeSeries: as.MapAsProto(o.referenceID),
	}
}

type aggregatedSamples struct {
	Samples map[metrics.TimeSeries][]*metrics.Sample
}

func (as *aggregatedSamples) AddSample(s *metrics.Sample) {
	tss, ok := as.Samples[s.TimeSeries]
	if !ok {
		// TODO: optimize the slice allocation
		// A simple 1st step: Reuse the last seen len?
		as.Samples[s.TimeSeries] = []*metrics.Sample{s}
		return
	}
	as.Samples[s.TimeSeries] = append(tss, s)
}

func (as *aggregatedSamples) Clean() {
	// TODO: evaluate if it makes sense
	// to keep the most frequent used keys

	// the compiler optimizes this
	for k := range as.Samples {
		delete(as.Samples, k)
	}
}

func (as *aggregatedSamples) MapAsProto(refID string) []*pbcloud.TimeSeries {
	if len(as.Samples) < 1 {
		return nil
	}
	pbseries := make([]*pbcloud.TimeSeries, 0, len(as.Samples))
	for ts, samples := range as.Samples {
		if ts.Metric.Type == metrics.Trend {
			// skip trend metrics
			continue
		}

		pb := pbcloud.TimeSeries{}
		// TODO: optimize removing Map
		// and using https://github.com/grafana/k6/issues/2764
		pb.Labels = make([]*pbcloud.Label, 0, ((*atlas.Node)(ts.Tags)).Len())
		pb.Labels = append(pb.Labels, &pbcloud.Label{Name: "__name__", Value: ts.Metric.Name})
		pb.Labels = append(pb.Labels, &pbcloud.Label{Name: "test_run_id", Value: refID})
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
