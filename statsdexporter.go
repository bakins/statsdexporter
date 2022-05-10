// Package statsdexporter is a helper for embedded statsd_exporter in other projects.
package statsdexporter

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/statsd_exporter/pkg/address"
	"github.com/prometheus/statsd_exporter/pkg/mapper"
	"github.com/prometheus/statsd_exporter/pkg/mappercache/lru"
	"github.com/prometheus/statsd_exporter/pkg/mappercache/randomreplacement"
)

// Option is used when creating a new exporter.
type Option interface {
	apply(*config) error
}

type config struct {
	network              string
	address              string
	unixSocketMode       os.FileMode
	mappingConfig        string
	readBuffer           uint
	cacheSize            uint
	cacheType            string
	eventQueueSize       uint
	eventFlushThreshold  uint
	eventFlushInterval   time.Duration
	dogstatsdTagsEnabled bool
	influxdbTagsEnabled  bool
	libratoTagsEnabled   bool
	signalFXTagsEnabled  bool
	registerer           prometheus.Registerer
	gatherer             prometheus.Gatherer
	logger               log.Logger
}

func defaultConfig() config {
	return config{
		network:             "udp",
		address:             "127.0.0.1:0",
		unixSocketMode:      0o755,
		mappingConfig:       "",
		readBuffer:          0,
		cacheSize:           1000,
		cacheType:           "lru",
		eventQueueSize:      10000,
		eventFlushThreshold: 1000,
		eventFlushInterval:  200 * time.Millisecond,
		registerer:          prometheus.DefaultRegisterer,
		gatherer:            prometheus.DefaultGatherer,
		logger:              log.NewNopLogger(),
	}
}

type optionFunc func(*config) error

func (f optionFunc) apply(c *config) error {
	return f(c)
}

// WithAddress sets the address for the listener. Default
// is a random UDP port on localhost.
func WithAddress(network string, addr string) optionFunc {
	return func(c *config) error {
		// we wind up parsing addresses twice, but it's fine.
		switch network {
		case "tcp":
			if _, err := address.TCPAddrFromString(addr); err != nil {
				return fmt.Errorf("invalid TCP listen address %w", err)
			}

		case "udp":
			if _, err := address.UDPAddrFromString(addr); err != nil {
				return fmt.Errorf("invalid UDP listen address %w", err)
			}
		case "unixgram":
			// nothing to do
		default:
			return fmt.Errorf("unsupported network %q", network)
		}

		c.network = network
		c.address = addr

		return nil
	}
}

// WithUnixSocketMode sets the file mode when using a unix socket.
// Default is 0o755
func WithUnixSocketMode(mode os.FileMode) optionFunc {
	return func(c *config) error {
		c.unixSocketMode = mode

		return nil
	}
}

// WithMappingConfig sets the metric mapping configuration file name.
func WithMappingConfig(configFile string) optionFunc {
	return func(c *config) error {
		c.mappingConfig = configFile

		return nil
	}
}

// WithReadBuffer sets the read buffer size (in bytes) of the operating system's
// transmit read buffer associated with the UDP or Unixgram connection. Please
// make sure the kernel parameters net.core.rmem_max is set to a value greater
// than the value specified.
func WithReadBuffer(size uint) optionFunc {
	return func(c *config) error {
		c.readBuffer = size
		return nil
	}
}

// WithCacheSize sets the maximum size of your metric mapping cache.
//
// Default is 1000.
func WithCacheSize(size uint) optionFunc {
	return func(c *config) error {
		c.cacheSize = size
		return nil
	}
}

// WithCacheType sets metric mapping cache type. Valid options are "lru" and
// "random".
//
// Default is "lru".
func WithCacheType(cacheType string) optionFunc {
	return func(c *config) error {
		switch cacheType {
		case "lru", "random":
		default:
			return fmt.Errorf("unsupported cache type %q", cacheType)
		}
		c.cacheType = cacheType

		return nil
	}
}

// WithEventQueueSize sets the size of internal queue for processing events.
//
// Default is 10000.
func WithEventQueueSize(size uint) optionFunc {
	return func(c *config) error {
		c.eventQueueSize = size
		return nil
	}
}

// WithEventFlushThreshold sets the number of events to hold in queue before flushing.
//
// Default is 1000.
func WithEventFlushThreshold(size uint) optionFunc {
	return func(c *config) error {
		c.eventFlushThreshold = size
		return nil
	}
}

// WithEventFlushInterval sets the maximum time between event queue flushes.
//
// Default is 200ms
func WithEventFlushInterval(interval time.Duration) optionFunc {
	return func(c *config) error {
		if interval < 0 {
			return errors.New("interval must be positive")
		}

		c.eventFlushInterval = interval

		return nil
	}
}

// WithLogger sets the logger to use. A nop is used by default.
func WithLogger(logger log.Logger) optionFunc {
	return func(c *config) error {
		c.logger = logger
		return nil
	}
}

// Server is a statsd server
type Server struct {
	config   config
	mapper   *mapper.MetricMapper
	handler  http.Handler
	listener net.Listener
}

func New(options ...Option) (*Server, error) {
	config := defaultConfig()

	for _, o := range options {
		if err := o.apply(&config); err != nil {
			return nil, err
		}
	}

	mapper := &mapper.MetricMapper{
		Registerer:    config.registerer,
		MappingsCount: mappingsCount,
		Logger:        config.logger,
	}

	cache, err := getCache(config.cacheSize, config.cacheType, mapper.Registerer)
	if err != nil {
		return nil, err
	}

	mapper.UseCache(cache)

	if config.mappingConfig != "" {
		if err := mapper.InitFromFile(config.mappingConfig); err != nil {
			return nil, fmt.Errorf("failed to load config %q %w", config.mappingConfig, err)
		}
	}
	s := Server{
		handler: promhttp.HandlerFor(config.gatherer, promhttp.HandlerOpts{}),
		mapper:  mapper,
		config:  config,
	}

	return &s, nil
}

func getCache(cacheSize int, cacheType string, registerer prometheus.Registerer) (mapper.MetricMapperCache, error) {
	var cache mapper.MetricMapperCache
	var err error
	if cacheSize == 0 {
		return nil, nil
	} else {
		switch cacheType {
		case "lru":
			cache, err = lru.NewMetricMapperLRUCache(registerer, cacheSize)
		case "random":
			cache, err = randomreplacement.NewMetricMapperRRCache(registerer, cacheSize)
		default:
			err = fmt.Errorf("unsupported cache type %q", cacheType)
		}

		if err != nil {
			return nil, err
		}
	}

	return cache, nil
}

// WaitForAddress is useful when using a random port.
func (s *Server) WaitForAddress(ctx context.Context) (net.Addr, error) {
	t := time.NewTicker(time.Millisecond * 100)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-t.C:
			// this is technically a race
			if s.listener == nil {
				continue
			}

			return s.listener.Addr(), nil
		}
	}
}

// Reload the mapping config, if set
func (s *Server) Reload() error {
	if s.config.mappingConfig == "" {
		return nil
	}

	err := s.mapper.InitFromFile(s.config.mappingConfig)
	if err != nil {
		configLoads.WithLabelValues("failure").Inc()
		return err
	}
	configLoads.WithLabelValues("success").Inc()

	return nil
}

// ServeHTTP can be used as an http Handler for /metrics
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

var (
	eventStats = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "statsd_exporter_events_total",
			Help: "The total number of StatsD events seen.",
		},
		[]string{"type"},
	)
	eventsFlushed = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "statsd_exporter_event_queue_flushed_total",
			Help: "Number of times events were flushed to exporter",
		},
	)
	eventsUnmapped = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "statsd_exporter_events_unmapped_total",
			Help: "The total number of StatsD events no mapping was found for.",
		})
	udpPackets = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "statsd_exporter_udp_packets_total",
			Help: "The total number of StatsD packets received over UDP.",
		},
	)
	tcpConnections = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "statsd_exporter_tcp_connections_total",
			Help: "The total number of TCP connections handled.",
		},
	)
	tcpErrors = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "statsd_exporter_tcp_connection_errors_total",
			Help: "The number of errors encountered reading from TCP.",
		},
	)
	tcpLineTooLong = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "statsd_exporter_tcp_too_long_lines_total",
			Help: "The number of lines discarded due to being too long.",
		},
	)
	unixgramPackets = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "statsd_exporter_unixgram_packets_total",
			Help: "The total number of StatsD packets received over Unixgram.",
		},
	)
	linesReceived = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "statsd_exporter_lines_total",
			Help: "The total number of StatsD lines received.",
		},
	)
	samplesReceived = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "statsd_exporter_samples_total",
			Help: "The total number of StatsD samples received.",
		},
	)
	sampleErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "statsd_exporter_sample_errors_total",
			Help: "The total number of errors parsing StatsD samples.",
		},
		[]string{"reason"},
	)
	tagsReceived = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "statsd_exporter_tags_total",
			Help: "The total number of DogStatsD tags processed.",
		},
	)
	tagErrors = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "statsd_exporter_tag_errors_total",
			Help: "The number of errors parsing DogStatsD tags.",
		},
	)
	configLoads = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "statsd_exporter_config_reloads_total",
			Help: "The number of configuration reloads.",
		},
		[]string{"outcome"},
	)
	mappingsCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "statsd_exporter_loaded_mappings",
		Help: "The current number of configured metric mappings.",
	})
	conflictingEventStats = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "statsd_exporter_events_conflict_total",
			Help: "The total number of StatsD events with conflicting names.",
		},
		[]string{"type"},
	)
	errorEventStats = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "statsd_exporter_events_error_total",
			Help: "The total number of StatsD events discarded due to errors.",
		},
		[]string{"reason"},
	)
	eventsActions = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "statsd_exporter_events_actions_total",
			Help: "The total number of StatsD events by action.",
		},
		[]string{"action"},
	)
	metricsCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "statsd_exporter_metrics_total",
			Help: "The total number of metrics.",
		},
		[]string{"type"},
	)
)
