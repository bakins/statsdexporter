package statsdexporter_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/common/expfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bakins/statsdexporter"
)

func TestServer(t *testing.T) {
	s, err := statsdexporter.New()
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	go func() {
		err := s.Run(ctx)
		assert.NoError(t, err)
	}()

	addr, err := s.WaitForAddress(ctx)
	require.NoError(t, err)

	client, err := net.Dial(addr.Network(), addr.String())
	require.NoError(t, err)

	// see https://docs.datadoghq.com/developers/dogstatsd/datagram_shell/?tab=metrics
	_, err = client.Write([]byte("users.online:1|c|#country:china\n"))
	require.NoError(t, err)
	_, err = client.Write([]byte("users.online:1|c|#country:mexico\n"))
	require.NoError(t, err)

	// chessy but makes sure it flushes
	time.Sleep(time.Second)

	cancel()

	r := httptest.NewRequest(http.MethodGet, "http://localhost/metrics", nil)
	w := httptest.NewRecorder()

	s.ServeHTTP(w, r)

	res := w.Result()
	require.Equal(t, http.StatusOK, res.StatusCode)

	parser := &expfmt.TextParser{}
	mfs, err := parser.TextToMetricFamilies(res.Body)
	require.NoError(t, err)

	mf := mfs["users_online"]
	require.NotEmpty(t, mf)
	require.Len(t, mf.Metric, 2)
}
