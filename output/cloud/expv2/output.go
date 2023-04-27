package expv2

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"go.k6.io/k6/cloudapi"
	"go.k6.io/k6/errext"
	"go.k6.io/k6/errext/exitcodes"
	"go.k6.io/k6/lib"
	"go.k6.io/k6/lib/consts"
	"go.k6.io/k6/metrics"
	"go.k6.io/k6/output"
	"go.k6.io/k6/output/cloud/expv2/pbcloud"

	"github.com/mstoykov/atlas"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gopkg.in/guregu/null.v3"
)

// TestName is the default k6 Cloud test name
const TestName = "k6 test"

// Output sends result data to the k6 Cloud service.
type Output struct {
	output.SampleBuffer

	logger logrus.FieldLogger
	opts   lib.Options
	config cloudapi.Config

	referenceID   string
	executionPlan []lib.ExecutionStep
	duration      int64 // in seconds
	thresholds    map[string][]*metrics.Threshold

	metricsClient *MetricsClient
	testsClient   *cloudapi.Client

	periodicFlusher *output.PeriodicFlusher

	// TODO: if the metric refactor (#2905) will introduce
	// a sequential ID for metrics
	// then we could reuse the same strategy here
	activeSeries map[*metrics.Metric]aggregatedSamples

	testStopFunc       func(error)
	stopSendingMetrics chan struct{}
}

// New creates a new Cloud output version 2.
func New(params output.Params) (*Output, error) {
	conf, err := cloudapi.GetConsolidatedConfig(
		params.JSONConfig, params.Environment, params.ConfigArgument, params.ScriptOptions.External)
	if err != nil {
		return nil, err
	}

	if err := validateRequiredSystemTags(params.ScriptOptions.SystemTags); err != nil {
		return nil, err
	}

	logger := params.Logger.WithFields(logrus.Fields{"output": "cloud"})

	if !conf.Name.Valid || conf.Name.String == "" {
		scriptPath := params.ScriptPath.String()
		if scriptPath == "" {
			// Script from stdin without a name, likely from stdin
			return nil, errors.New("script name not set, please specify K6_CLOUD_NAME or options.ext.loadimpact.name")
		}

		conf.Name = null.StringFrom(filepath.Base(scriptPath))
	}
	if conf.Name.String == "-" {
		conf.Name = null.StringFrom(TestName)
	}

	duration, testEnds := lib.GetEndOffset(params.ExecutionPlan)
	if !testEnds {
		return nil, errors.New("tests with unspecified duration are not allowed when outputting data to k6 cloud")
	}

	// TODO: evaluate to introduce concurrent flush
	//
	//if !(conf.MetricPushConcurrency.Int64 > 0) {
	//return nil, fmt.Errorf("metrics push concurrency must be a positive number but is %d",
	//conf.MetricPushConcurrency.Int64)
	//}

	metricsClient, err := NewMetricsClient(logger, conf.Host.String, conf.Token.String)
	if err != nil {
		return nil, err
	}

	return &Output{
		logger: logger,
		config: conf,
		opts:   params.ScriptOptions,
		testsClient: cloudapi.NewClient(
			logger, conf.Token.String, conf.Host.String, consts.Version, conf.Timeout.TimeDuration()),
		metricsClient:      metricsClient,
		executionPlan:      params.ExecutionPlan,
		duration:           int64(duration / time.Second),
		activeSeries:       make(map[*metrics.Metric]aggregatedSamples),
		stopSendingMetrics: make(chan struct{}),
	}, nil
}

// validateRequiredSystemTags checks if all required tags are present.
func validateRequiredSystemTags(scriptTags *metrics.SystemTagSet) error {
	missingRequiredTags := []string{}
	requiredTags := metrics.SystemTagSet(metrics.TagName |
		metrics.TagMethod |
		metrics.TagStatus |
		metrics.TagError |
		metrics.TagCheck |
		metrics.TagGroup)
	for _, tag := range metrics.SystemTagValues() {
		if requiredTags.Has(tag) && !scriptTags.Has(tag) {
			missingRequiredTags = append(missingRequiredTags, tag.String())
		}
	}
	if len(missingRequiredTags) > 0 {
		return fmt.Errorf(
			"the cloud output needs the following system tags enabled: %s",
			strings.Join(missingRequiredTags, ", "),
		)
	}
	return nil
}

// Start calls the k6 Cloud API to initialize the test run, and then starts the
// goroutine that would listen for metric samples and send them to the cloud.
func (out *Output) Start() error {
	if out.config.PushRefID.Valid {
		out.referenceID = out.config.PushRefID.String
		out.logger.WithField("referenceId", out.referenceID).Debug("directly pushing metrics without init")
		out.startBackgroundProcesses()
		return nil
	}

	thresholds := make(map[string][]string)

	for name, t := range out.thresholds {
		for _, threshold := range t {
			thresholds[name] = append(thresholds[name], threshold.Source)
		}
	}
	maxVUs := lib.GetMaxPossibleVUs(out.executionPlan)

	testRun := &cloudapi.TestRun{
		Name:       out.config.Name.String,
		ProjectID:  out.config.ProjectID.Int64,
		VUsMax:     int64(maxVUs),
		Thresholds: thresholds,
		Duration:   out.duration,
	}

	response, err := out.testsClient.CreateTestRun(testRun)
	if err != nil {
		return err
	}
	out.referenceID = response.ReferenceID

	if response.ConfigOverride != nil {
		out.logger.WithFields(logrus.Fields{
			"override": response.ConfigOverride,
		}).Debug("overriding config options")
		out.config = out.config.Apply(*response.ConfigOverride)
	}

	out.startBackgroundProcesses()

	out.logger.WithFields(logrus.Fields{
		"name":        out.config.Name,
		"projectId":   out.config.ProjectID,
		"duration":    out.duration,
		"referenceId": out.referenceID,
	}).Debug("Started!")
	return nil
}

func (o *Output) startBackgroundProcesses() error {
	// TODO: Implement a new time series based cloud aggregation
	// https://github.com/grafana/k6/issues/1700

	pf, err := output.NewPeriodicFlusher(o.config.MetricPushInterval.TimeDuration(), o.flushMetrics)
	if err != nil {
		return err
	}

	o.periodicFlusher = pf
	return nil
}

// Stop gracefully stops all metric emission from the output: when all metric
// samples are emitted, it makes a cloud API call to finish the test run.
//
// Deprecated: use StopWithTestError() instead.
func (out *Output) Stop() error {
	return out.StopWithTestError(nil)
}

// StopWithTestError gracefully stops all metric emission from the output: when
// all metric samples are emitted, it makes a cloud API call to finish the test
// run. If testErr was specified, it extracts the RunStatus from it.
func (o *Output) StopWithTestError(testErr error) error {
	o.logger.Debug("Stopping the cloud output...")
	o.periodicFlusher.Stop()

	o.logger.Debug("Metric emission stopped, calling cloud API...")
	err := o.testFinished(testErr)
	if err != nil {
		o.logger.WithFields(logrus.Fields{"error": err}).Warn("Failed to send test finished to the cloud")
	} else {
		o.logger.Debug("Cloud output successfully stopped!")
	}
	return err
}

// Description returns the URL with the test run results.
func (out *Output) Description() string {
	return fmt.Sprintf("cloud (%s)", cloudapi.URLForResults(out.referenceID, out.config))
}

// getRunStatus determines the run status of the test based on the error.
func (out *Output) getRunStatus(testErr error) cloudapi.RunStatus {
	if testErr == nil {
		return cloudapi.RunStatusFinished
	}

	var err errext.HasAbortReason
	if errors.As(testErr, &err) {
		abortReason := err.AbortReason()
		switch abortReason {
		case errext.AbortedByUser:
			return cloudapi.RunStatusAbortedUser
		case errext.AbortedByThreshold:
			return cloudapi.RunStatusAbortedThreshold
		case errext.AbortedByScriptError:
			return cloudapi.RunStatusAbortedScriptError
		case errext.AbortedByScriptAbort:
			return cloudapi.RunStatusAbortedUser // TODO: have a better value than this?
		case errext.AbortedByTimeout:
			return cloudapi.RunStatusAbortedLimit
		case errext.AbortedByOutput:
			return cloudapi.RunStatusAbortedSystem
		case errext.AbortedByThresholdsAfterTestEnd:
			// The test run finished normally, it wasn't prematurely aborted by
			// anything while running, but the thresholds failed at the end and
			// k6 will return an error and a non-zero exit code to the user.
			//
			// However, failures are tracked somewhat differently by the k6
			// cloud compared to k6 OSS. It doesn't have a single pass/fail
			// variable with multiple failure states, like k6's exit codes.
			// Instead, it has two variables, result_status and run_status.
			//
			// The status of the thresholds is tracked by the binary
			// result_status variable, which signifies whether the thresholds
			// passed or failed (failure also called "tainted" in some places of
			// the API here). The run_status signifies whether the test run
			// finished normally and has a few fixed failures values.
			//
			// So, this specific k6 error will be communicated to the cloud only
			// via result_status, while the run_status will appear normal.
			return cloudapi.RunStatusFinished
		}
	}

	// By default, the catch-all error is "aborted by system", but let's log that
	out.logger.WithError(testErr).Debug("unknown test error classified as 'aborted by system'")
	return cloudapi.RunStatusAbortedSystem
}

// SetThresholds receives the thresholds before the output is Start()-ed.
func (out *Output) SetThresholds(scriptThresholds map[string]metrics.Thresholds) {
	thresholds := make(map[string][]*metrics.Threshold)
	for name, t := range scriptThresholds {
		thresholds[name] = append(thresholds[name], t.Thresholds...)
	}
	out.thresholds = thresholds
}

// SetTestRunStopCallback receives the function that stops the engine on error
func (out *Output) SetTestRunStopCallback(stopFunc func(error)) {
	out.testStopFunc = stopFunc
}

func (out *Output) shouldStopSendingMetrics(err error) bool {
	if err == nil {
		return false
	}

	if errResp, ok := err.(cloudapi.ErrorResponse); ok && errResp.Response != nil {
		return errResp.Response.StatusCode == http.StatusForbidden && errResp.Code == 4
	}

	return false
}

func (out *Output) testFinished(testErr error) error {
	// TODO: why do we need to check the config also?
	if out.referenceID == "" || out.config.PushRefID.Valid {
		return nil
	}

	testTainted := false
	thresholdResults := make(cloudapi.ThresholdResult)
	for name, thresholds := range out.thresholds {
		thresholdResults[name] = make(map[string]bool)
		for _, t := range thresholds {
			thresholdResults[name][t.Source] = t.LastFailed
			if t.LastFailed {
				testTainted = true
			}
		}
	}

	runStatus := out.getRunStatus(testErr)
	out.logger.WithFields(logrus.Fields{
		"ref":        out.referenceID,
		"tainted":    testTainted,
		"run_status": runStatus,
	}).Debug("Sending test finished")

	return out.testsClient.TestFinished(out.referenceID, thresholdResults, testTainted, runStatus)
}

func (o *Output) flushMetrics() {
	select {
	case <-o.stopSendingMetrics:
		return
	default:
	}

	if o.referenceID == "" {
		// TODO: should it warn?
		return
	}

	samplesContainers := o.GetBufferedSamples()
	if len(samplesContainers) < 1 {
		return
	}

	start := time.Now()
	o.collectSamples(samplesContainers)

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
	err := o.metricsClient.Push(ctx, o.referenceID, &pbcloud.MetricSet{Metrics: metricSet})
	if err != nil {
		o.logger.WithError(err).Error("Failed to push metrics to the cloud")

		if o.shouldStopSendingMetrics(err) {
			o.logger.WithError(err).Warn("Interrupt sending metrics to cloud due to an error")
			serr := errext.WithAbortReasonIfNone(
				errext.WithExitCodeIfNone(err, exitcodes.ExternalAbort),
				errext.AbortedByOutput,
			)
			if o.config.StopOnError.Bool {
				o.testStopFunc(serr)
			}
			close(o.stopSendingMetrics)
		}
		return
	}

	o.logger.WithField("t", time.Since(start)).Debug("Successfully flushed buffered samples to the cloud")
}

// collectSamples drain the buffer and collect all the samples
func (o *Output) collectSamples(containers []metrics.SampleContainer) {
	var (
		aggr aggregatedSamples
		ok   bool
	)
	for _, sampleContainer := range containers {
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
}

func (o *Output) mapMetricProto(m *metrics.Metric, as aggregatedSamples) *pbcloud.Metric {
	var mtype pbcloud.MetricType
	switch m.Type {
	case metrics.Counter:
		mtype = pbcloud.MetricType_METRIC_TYPE_COUNTER
	case metrics.Gauge:
		mtype = pbcloud.MetricType_METRIC_TYPE_GAUGE
	case metrics.Rate:
		mtype = pbcloud.MetricType_METRIC_TYPE_RATE
	case metrics.Trend:
		mtype = pbcloud.MetricType_METRIC_TYPE_TREND
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
					Time:  timestamppb.New(gaugeSample.Time),
					Last:  gaugeSample.Value,
					Min:   gaugeSample.Value,
					Max:   gaugeSample.Value,
					Avg:   gaugeSample.Value,
					Count: 1,
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
