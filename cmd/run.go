package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"go.k6.io/k6/api"
	"go.k6.io/k6/cmd/state"
	"go.k6.io/k6/errext"
	"go.k6.io/k6/errext/exitcodes"
	"go.k6.io/k6/execution"
	"go.k6.io/k6/execution/local"
	"go.k6.io/k6/js/common"
	"go.k6.io/k6/lib"
	"go.k6.io/k6/lib/consts"
	"go.k6.io/k6/metrics"
	"go.k6.io/k6/metrics/engine"
	"go.k6.io/k6/output"
	"go.k6.io/k6/ui/pb"
)

// cmdsRunAndAgent handles the `k6 run` and `k6 agent` sub-commands
type cmdsRunAndAgent struct {
	gs *state.GlobalState

	// TODO: figure out something more elegant?
	loadConfiguredTests func(cmd *cobra.Command, args []string) ([]*loadedAndConfiguredTest, execution.Controller, error)
	metricsEngineHook   func(*engine.MetricsEngine) func()
	testEndHook         func(err error)
}

func (c *cmdsRunAndAgent) run(cmd *cobra.Command, args []string) (err error) {
	defer func() {
		c.gs.Logger.Debugf("Everything has finished, exiting k6 with error '%s'!", err)
		if c.testEndHook != nil {
			c.testEndHook(err)
		}
	}()
	printBanner(c.gs)

	// TODO: hadnle contexts, Ctrl+C, REST API and a lot of other things here

	tests, controller, err := c.loadConfiguredTests(cmd, args)
	if err != nil {
		return err
	}

	execution.SignalAndWait(controller, "test-suite-start")
	defer execution.SignalAndWait(controller, "test-suite-done")
	for i, test := range tests {
		testName := fmt.Sprintf("%d", i) // TODO: something better but still unique
		testController := execution.GetNamespacedController(testName, controller)

		err := c.runTest(cmd, test, testController)
		if err != nil {
			return err
		}
	}
	return nil
}

//nolint:funlen,gocognit,gocyclo,cyclop
func (c *cmdsRunAndAgent) runTest(
	cmd *cobra.Command, test *loadedAndConfiguredTest, controller execution.Controller,
) (err error) {
	var logger logrus.FieldLogger = c.gs.Logger
	globalCtx, globalCancel := context.WithCancel(c.gs.Ctx)
	defer globalCancel()

	// lingerCtx is cancelled by Ctrl+C, and is used to wait for that event when
	// k6 was started with the --linger option.
	lingerCtx, lingerCancel := context.WithCancel(globalCtx)
	defer lingerCancel()
	execution.SignalAndWait(controller, "test-start")
	defer execution.SignalAndWait(controller, "test-done")

	// runCtx is used for the test run execution and is created with the special
	// execution.NewTestRunContext() function so that it can be aborted even
	// from sub-contexts while also attaching a reason for the abort.
	runCtx, runAbort := execution.NewTestRunContext(lingerCtx, logger)

	if test.keyLogger != nil {
		defer func() {
			if klErr := test.keyLogger.Close(); klErr != nil {
				logger.WithError(klErr).Warn("Error while closing the SSLKEYLOGFILE")
			}
		}()
	}

	// Write the full consolidated *and derived* options back to the Runner.
	conf := test.derivedConfig
	testRunState, err := test.buildTestRunState(conf.Options)
	if err != nil {
		return err
	}

	// Create a local execution scheduler wrapping the runner.
	logger.Debug("Initializing the execution scheduler...")
	execScheduler, err := execution.NewScheduler(testRunState, controller)
	if err != nil {
		return err
	}

	progressBarWG := &sync.WaitGroup{}
	progressBarWG.Add(1)
	defer progressBarWG.Wait()

	// This is manually triggered after the Engine's Run() has completed,
	// and things like a single Ctrl+C don't affect it. We use it to make
	// sure that the progressbars finish updating with the latest execution
	// state one last time, after the test run has finished.
	progressCtx, progressCancel := context.WithCancel(globalCtx)
	defer progressCancel()
	initBar := execScheduler.GetInitProgressBar()
	go func() {
		defer progressBarWG.Done()
		pbs := []*pb.ProgressBar{initBar}
		for _, s := range execScheduler.GetExecutors() {
			pbs = append(pbs, s.GetProgress())
		}
		showProgress(progressCtx, c.gs, pbs, logger)
	}()

	// Create all outputs.
	executionPlan := execScheduler.GetExecutionPlan()
	outputs, err := createOutputs(c.gs, test, executionPlan)
	if err != nil {
		return err
	}

	metricsEngine, err := engine.NewMetricsEngine(testRunState.Registry, logger)
	if err != nil {
		return err
	}

	// We'll need to pipe metrics to the MetricsEngine and process them if any
	// of these are enabled: thresholds, end-of-test summary, engine hook
	shouldProcessMetrics := (!testRunState.RuntimeOptions.NoSummary.Bool ||
		!testRunState.RuntimeOptions.NoThresholds.Bool || c.metricsEngineHook != nil)
	var metricsIngester *engine.OutputIngester
	if shouldProcessMetrics {
		err = metricsEngine.InitSubMetricsAndThresholds(conf.Options, testRunState.RuntimeOptions.NoThresholds.Bool)
		if err != nil {
			return err
		}
		// We'll need to pipe metrics to the MetricsEngine if either the
		// thresholds or the end-of-test summary are enabled.
		metricsIngester = metricsEngine.CreateIngester()
		outputs = append(outputs, metricsIngester)
	}

	executionState := execScheduler.GetState()
	if !testRunState.RuntimeOptions.NoSummary.Bool {
		defer func() {
			logger.Debug("Generating the end-of-test summary...")
			summaryResult, hsErr := test.initRunner.HandleSummary(globalCtx, &lib.Summary{
				Metrics:         metricsEngine.ObservedMetrics,
				RootGroup:       testRunState.Runner.GetDefaultGroup(),
				TestRunDuration: executionState.GetCurrentTestRunDuration(),
				NoColor:         c.gs.Flags.NoColor,
				UIState: lib.UIState{
					IsStdOutTTY: c.gs.Stdout.IsTTY,
					IsStdErrTTY: c.gs.Stderr.IsTTY,
				},
			})
			if hsErr == nil {
				hsErr = handleSummaryResult(c.gs.FS, c.gs.Stdout, c.gs.Stderr, summaryResult)
			}
			if hsErr != nil {
				logger.WithError(hsErr).Error("failed to handle the end-of-test summary")
			}
		}()
	}

	// Create and start the outputs. We do it quite early to get any output URLs
	// or other details below. It also allows us to ensure when they have
	// flushed their samples and when they have stopped in the defer statements.
	initBar.Modify(pb.WithConstProgress(0, "Starting outputs"))
	outputManager := output.NewManager(outputs, logger, func(err error) {
		if err != nil {
			logger.WithError(err).Error("Received error to stop from output")
		}
		// TODO: attach run status and exit code?
		runAbort(err)
	})
	samples := make(chan metrics.SampleContainer, test.derivedConfig.MetricSamplesBufferSize.Int64)
	waitOutputsFlushed, stopOutputs, err := outputManager.Start(samples)
	if err != nil {
		return err
	}
	defer func() {
		logger.Debug("Stopping outputs...")
		// We call waitOutputsFlushed() below because the threshold calculations
		// need all of the metrics to be sent to the MetricsEngine before we can
		// calculate them one last time. We need the threshold calculated here,
		// since they may change the run status for the outputs.
		stopOutputs(err)
	}()

	if c.metricsEngineHook != nil {
		hookFinalize := c.metricsEngineHook(metricsEngine)
		defer hookFinalize()
	}

	if !testRunState.RuntimeOptions.NoThresholds.Bool {
		getCurrentTestDuration := executionState.GetCurrentTestRunDuration
		finalizeThresholds := metricsEngine.StartThresholdCalculations(metricsIngester, getCurrentTestDuration, runAbort)
		defer func() {
			// This gets called after the Samples channel has been closed and
			// the OutputManager has flushed all of the cached samples to
			// outputs (including MetricsEngine's ingester). So we are sure
			// there won't be any more metrics being sent.
			logger.Debug("Finalizing thresholds...")
			breachedThresholds := finalizeThresholds()
			if len(breachedThresholds) > 0 {
				tErr := errext.WithAbortReasonIfNone(
					errext.WithExitCodeIfNone(
						fmt.Errorf("thresholds on metrics '%s' have been breached", strings.Join(breachedThresholds, ", ")),
						exitcodes.ThresholdsHaveFailed,
					), errext.AbortedByThresholdsAfterTestEnd)

				if err == nil {
					err = tErr
				} else {
					logger.WithError(tErr).Debug("Breached thresholds, but test already exited with another error")
				}
			}
		}()
	}

	defer func() {
		logger.Debug("Waiting for metric processing to finish...")
		close(samples)
		waitOutputsFlushed()
		logger.Debug("Metrics processing finished!")
	}()

	// Spin up the REST API server, if not disabled.
	if c.gs.Flags.Address != "" { //nolint:nestif
		initBar.Modify(pb.WithConstProgress(0, "Init API server"))

		apiWG := &sync.WaitGroup{}
		apiWG.Add(2)
		defer apiWG.Wait()

		srvCtx, srvCancel := context.WithCancel(globalCtx)
		defer srvCancel()

		srv := api.GetServer(runCtx, c.gs.Flags.Address, testRunState, samples, metricsEngine, execScheduler)
		go func() {
			defer apiWG.Done()
			logger.Debugf("Starting the REST API server on %s", c.gs.Flags.Address)
			if aerr := srv.ListenAndServe(); aerr != nil && !errors.Is(aerr, http.ErrServerClosed) {
				// Only exit k6 if the user has explicitly set the REST API address
				if cmd.Flags().Lookup("address").Changed {
					logger.WithError(aerr).Error("Error from API server")
					c.gs.OSExit(int(exitcodes.CannotStartRESTAPI))
				} else {
					logger.WithError(aerr).Warn("Error from API server")
				}
			}
		}()
		go func() {
			defer apiWG.Done()
			<-srvCtx.Done()
			shutdCtx, shutdCancel := context.WithTimeout(globalCtx, 1*time.Second)
			defer shutdCancel()
			if aerr := srv.Shutdown(shutdCtx); aerr != nil {
				logger.WithError(aerr).Debug("REST API server did not shut down correctly")
			}
		}()
	}

	printExecutionDescription(
		c.gs, "local", test.sourceRootPath, "", conf, executionState.ExecutionTuple, executionPlan, outputs,
	)

	// Trap Interrupts, SIGINTs and SIGTERMs.
	// TODO: move upwards, right after runCtx is created
	gracefulStop := func(sig os.Signal) {
		logger.WithField("sig", sig).Debug("Stopping k6 in response to signal...")
		// first abort the test run this way, to propagate the error
		runAbort(errext.WithAbortReasonIfNone(
			errext.WithExitCodeIfNone(
				fmt.Errorf("test run was aborted because k6 received a '%s' signal", sig), exitcodes.ExternalAbort,
			), errext.AbortedByUser,
		))
		lingerCancel() // cancel this context as well, since the user did Ctrl+C
	}
	onHardStop := func(sig os.Signal) {
		logger.WithField("sig", sig).Error("Aborting k6 in response to signal")
		globalCancel() // not that it matters, given that os.Exit() will be called right after
	}
	stopSignalHandling := handleTestAbortSignals(c.gs, gracefulStop, onHardStop)
	defer stopSignalHandling()

	if conf.Linger.Bool {
		defer func() {
			msg := "The test is done, but --linger was enabled, so k6 is waiting for Ctrl+C to continue..."
			select {
			case <-lingerCtx.Done():
				// do nothing, we were interrupted by Ctrl+C already
			default:
				logger.Debug(msg)
				if !c.gs.Flags.Quiet {
					printToStdout(c.gs, msg)
				}
				<-lingerCtx.Done()
				logger.Debug("Ctrl+C received, exiting...")
			}
		}()
	}

	// Initialize VUs and start the test! However, we won't immediately return
	// if there was an error, we still have things to do.
	err = execScheduler.Run(globalCtx, runCtx, samples)

	// Init has passed successfully, so unless disabled, make sure we send a
	// usage report after the context is done.
	if !conf.NoUsageReport.Bool {
		reportDone := make(chan struct{})
		go func() {
			<-runCtx.Done()
			_ = reportUsage(execScheduler)
			close(reportDone)
		}()
		defer func() {
			select {
			case <-reportDone:
			case <-time.After(3 * time.Second):
			}
		}()
	}

	// Check what the execScheduler.Run() error is.
	if err != nil {
		err = common.UnwrapGojaInterruptedError(err)
		logger.WithError(err).Debug("Test finished with an error")
		return err
	}

	// Warn if no iterations could be completed.
	if executionState.GetFullIterationCount() == 0 {
		logger.Warn("No script iterations fully finished, consider making the test duration longer")
	}

	logger.Debug("Test finished cleanly")

	return nil
}

func (c *cmdsRunAndAgent) flagSet() *pflag.FlagSet {
	flags := pflag.NewFlagSet("", pflag.ContinueOnError)
	flags.SortFlags = false
	flags.AddFlagSet(optionFlagSet())
	flags.AddFlagSet(runtimeOptionFlagSet(true))
	flags.AddFlagSet(configFlagSet())
	return flags
}

func getCmdRun(gs *state.GlobalState) *cobra.Command {
	c := &cmdsRunAndAgent{
		gs: gs,
		loadConfiguredTests: func(cmd *cobra.Command, args []string) (
			[]*loadedAndConfiguredTest, execution.Controller, error,
		) {
			tests, err := loadAndConfigureLocalTests(gs, cmd, args, getConfig)
			return tests, local.NewController(), err
		},
	}

	runCmd := &cobra.Command{
		Use:   "run",
		Short: "Start a load test",
		Long: `Start a load test.

This also exposes a REST API to interact with it. Various k6 subcommands offer
a commandline interface for interacting with it.`,
		Example: `
  # Run a single VU, once.
  k6 run script.js

  # Run a single VU, 10 times.
  k6 run -i 10 script.js

  # Run 5 VUs, splitting 10 iterations between them.
  k6 run -u 5 -i 10 script.js

  # Run 5 VUs for 10s.
  k6 run -u 5 -d 10s script.js

  # Ramp VUs from 0 to 100 over 10s, stay there for 60s, then 10s down to 0.
  k6 run -u 0 -s 10s:100 -s 60s -s 10s:0

  # Send metrics to an influxdb server
  k6 run -o influxdb=http://1.2.3.4:8086/k6`[1:],
		RunE: c.run,
	}

	runCmd.Flags().SortFlags = false
	runCmd.Flags().AddFlagSet(c.flagSet())

	return runCmd
}

func reportUsage(execScheduler *execution.Scheduler) error {
	execState := execScheduler.GetState()
	executorConfigs := execScheduler.GetExecutorConfigs()

	executors := make(map[string]int)
	for _, ec := range executorConfigs {
		executors[ec.GetType()]++
	}

	body, err := json.Marshal(map[string]interface{}{
		"k6_version": consts.Version,
		"executors":  executors,
		"vus_max":    execState.GetInitializedVUsCount(),
		"iterations": execState.GetFullIterationCount(),
		"duration":   execState.GetCurrentTestRunDuration().String(),
		"goos":       runtime.GOOS,
		"goarch":     runtime.GOARCH,
	})
	if err != nil {
		return err
	}
	res, err := http.Post("https://reports.k6.io/", "application/json", bytes.NewBuffer(body)) //nolint:noctx
	defer func() {
		if err == nil {
			_ = res.Body.Close()
		}
	}()

	return err
}

func handleSummaryResult(fs afero.Fs, stdOut, stdErr io.Writer, result map[string]io.Reader) error {
	var errs []error

	getWriter := func(path string) (io.Writer, error) {
		switch path {
		case "stdout":
			return stdOut, nil
		case "stderr":
			return stdErr, nil
		default:
			return fs.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o666)
		}
	}

	for path, value := range result {
		if writer, err := getWriter(path); err != nil {
			errs = append(errs, fmt.Errorf("could not open '%s': %w", path, err))
		} else if n, err := io.Copy(writer, value); err != nil {
			errs = append(errs, fmt.Errorf("error saving summary to '%s' after %d bytes: %w", path, n, err))
		}
	}

	return consolidateErrorMessage(errs, "Could not save some summary information:")
}
