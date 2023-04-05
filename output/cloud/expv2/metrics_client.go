package expv2

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/golang/snappy"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"

	"go.k6.io/k6/lib/consts"
	"go.k6.io/k6/output/cloud/expv2/pbcloud"
)

type httpDoer interface {
	Do(*http.Request) (*http.Response, error)
}

// MetricsClient is a wrapper around the cloudapi.Client that is also capable of pushing
type MetricsClient struct {
	httpClient httpDoer
	logger     logrus.FieldLogger
	token      string
	host       string
	userAgent  string

	pushBufferPool sync.Pool
}

// NewMetricsClient creates and initializes a new MetricsClient.
func NewMetricsClient(logger logrus.FieldLogger, host string, token string) (*MetricsClient, error) {
	if host == "" {
		return nil, fmt.Errorf("host is required")
	}
	if token == "" {
		return nil, fmt.Errorf("token is required")
	}
	return &MetricsClient{
		httpClient: &http.Client{Timeout: 5 * time.Second},
		logger:     logger,
		host:       host,
		token:      token,
		userAgent:  fmt.Sprintf("k6cloud/v%s", consts.Version),
		pushBufferPool: sync.Pool{
			New: func() interface{} {
				return &bytes.Buffer{}
			},
		},
	}, nil
}

// Push pushes the provided metric samples for the given referenceID
func (mc *MetricsClient) Push(ctx context.Context, referenceID string, samples *pbcloud.MetricSet) error {
	if referenceID == "" {
		return fmt.Errorf("a Reference ID of the test run is required")
	}
	start := time.Now()
	url := fmt.Sprintf("%s/v2/metrics/%s", mc.host, referenceID)

	b, err := newRequestBody(samples)
	if err != nil {
		return err
	}

	buf, _ := mc.pushBufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer mc.pushBufferPool.Put(buf)

	_, err = buf.Write(b)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, buf)
	if err != nil {
		return err
	}

	req.Header.Set("User-Agent", mc.userAgent)
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "snappy")
	req.Header.Set("K6-Metrics-Protocol-Version", "2.0")
	req.Header.Set("Authorization", fmt.Sprintf("Token %s", mc.token))

	resp, err := mc.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to push metrics: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to push metrics: push metrics response got an unexpected status code: %s", resp.Status)
	}
	mc.logger.WithField("t", time.Since(start)).WithField("size", len(b)).
		Debug("Pushed part to cloud")
	return nil
}

func newRequestBody(data *pbcloud.MetricSet) ([]byte, error) {
	b, err := proto.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("encoding series as protobuf write request failed: %w", err)
	}
	if snappy.MaxEncodedLen(len(b)) < 0 {
		return nil, fmt.Errorf("the protobuf message is too large to be handled by Snappy encoder; "+
			"size: %d, limit: %d", len(b), 0xffffffff)
	}
	return snappy.Encode(nil, b), nil
}
