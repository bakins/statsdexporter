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
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/statsd_exporter/pkg/address"
	"github.com/prometheus/statsd_exporter/pkg/event"
	"github.com/prometheus/statsd_exporter/pkg/exporter"
	"github.com/prometheus/statsd_exporter/pkg/line"
	"github.com/prometheus/statsd_exporter/pkg/listener"
	"github.com/prometheus/statsd_exporter/pkg/mapper"
	"github.com/prometheus/statsd_exporter/pkg/mappercache/lru"
	"github.com/prometheus/statsd_exporter/pkg/mappercache/randomreplacement"
)

// Option is used when creating a new exporter.
type Option interface {
	apply(*config) error
}

type config struct {
	gatherer             prometheus.Gatherer
	registerer           prometheus.Registerer
	logger               log.Logger
	cacheType            string
	address              string
	mappingConfig        string
	network              string
	eventQueueSize       uint
	cacheSize            uint
	eventFlushInterval   time.Duration
	eventFlushThreshold  uint
	readBuffer           uint
	unixSocketMode       os.FileMode
	influxdbTagsEnabled  bool
	libratoTagsEnabled   bool
	signalFXTagsEnabled  bool
	dogstatsdTagsEnabled bool
}

func defaultConfig() config {
	return config{
		network:              "udp",
		address:              "127.0.0.1:0",
		unixSocketMode:       0o755,
		mappingConfig:        "",
		readBuffer:           0,
		cacheSize:            1000,
		cacheType:            "lru",
		eventQueueSize:       10000,
		eventFlushThreshold:  1000,
		eventFlushInterval:   200 * time.Millisecond,
		dogstatsdTagsEnabled: true,
		influxdbTagsEnabled:  true,
		libratoTagsEnabled:   true,
		signalFXTagsEnabled:  true,
		registerer:           prometheus.DefaultRegisterer,
		gatherer:             prometheus.DefaultGatherer,
		logger:               log.NewNopLogger(),
	}
}

type optionFunc func(*config) error

func (f optionFunc) apply(c *config) error {
	return f(c)
}

// WithAddress sets the address for the listener. Default
// is a random UDP port on localhost.
func WithAddress(network string, addr string) Option {
	return optionFunc(func(c *config) error {
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
	})
}

// WithUnixSocketMode sets the file mode when using a unix socket.
// Default is 0o755
func WithUnixSocketMode(mode os.FileMode) Option {
	return optionFunc(func(c *config) error {
		c.unixSocketMode = mode

		return nil
	})
}

// WithMappingConfig sets the metric mapping configuration file name.
func WithMappingConfig(configFile string) Option {
	return optionFunc(func(c *config) error {
		c.mappingConfig = configFile

		return nil
	})
}

// WithReadBuffer sets the read buffer size (in bytes) of the operating system's
// transmit read buffer associated with the UDP or Unixgram connection. Please
// make sure the kernel parameters net.core.rmem_max is set to a value greater
// than the value specified.
func WithReadBuffer(size uint) Option {
	return optionFunc(func(c *config) error {
		c.readBuffer = size
		return nil
	})
}

// WithCacheSize sets the maximum size of your metric mapping cache.
//
// Default is 1000.
func WithCacheSize(size uint) Option {
	return optionFunc(func(c *config) error {
		c.cacheSize = size
		return nil
	})
}

// WithCacheType sets metric mapping cache type. Valid options are "lru" and
// "random".
//
// Default is "lru".
func WithCacheType(cacheType string) Option {
	return optionFunc(func(c *config) error {
		switch cacheType {
		case "lru", "random":
		default:
			return fmt.Errorf("unsupported cache type %q", cacheType)
		}
		c.cacheType = cacheType

		return nil
	})
}

// WithEventQueueSize sets the size of internal queue for processing events.
//
// Default is 10000.
func WithEventQueueSize(size uint) Option {
	return optionFunc(func(c *config) error {
		c.eventQueueSize = size
		return nil
	})
}

// WithEventFlushThreshold sets the number of events to hold in queue before flushing.
//
// Default is 1000.
func WithEventFlushThreshold(size uint) Option {
	return optionFunc(func(c *config) error {
		c.eventFlushThreshold = size
		return nil
	})
}

// WithEventFlushInterval sets the maximum time between event queue flushes.
//
// Default is 200ms
func WithEventFlushInterval(interval time.Duration) Option {
	return optionFunc(func(c *config) error {
		if interval < 0 {
			return errors.New("interval must be positive")
		}

		c.eventFlushInterval = interval

		return nil
	})
}

// WithDogstatsdTagsEnabled sets whether to parse DogStatsd style tags.
//
// Default is true.
func WithDogstatsdTagsEnabled(enabled bool) Option {
	return optionFunc(func(c *config) error {
		c.dogstatsdTagsEnabled = enabled
		return nil
	})
}

// WithInfluxdbTagsEnabled sets whether to parse InfluxDB style tags.
//
// Default is true.
func WithInfluxdbTagsEnabled(enabled bool) Option {
	return optionFunc(func(c *config) error {
		c.influxdbTagsEnabled = enabled
		return nil
	})
}

// WithInfluxdbTagsEnabled sets whether to parse Librato style tags.
//
// Default is true.
func WithLibratoTagsEnabled(enabled bool) Option {
	return optionFunc(func(c *config) error {
		c.libratoTagsEnabled = enabled
		return nil
	})
}

// WithInfluxdbTagsEnabled sets whether to parse SignalFX style tags.
//
// Default is true.
func WithSignalFXTagsEnabled(enabled bool) Option {
	return optionFunc(func(c *config) error {
		c.signalFXTagsEnabled = enabled
		return nil
	})
}

// WithRegisterer sets the Registerer to sue for registering metrics.
//
// Default is prometheus.DefaultRegisterer.
func WithRegisterer(registerer prometheus.Registerer) Option {
	return optionFunc(func(c *config) error {
		c.registerer = registerer
		return nil
	})
}

// WithGatherer sets the Gatherer to sue for registering metrics.
// The gatherer and register should use the same underlying prometheus
// registry for metrics to work as expected.
//
// Default is prometheus.DefaultGathererer.
func WithRegWithGathereristerer(gatherer prometheus.Gatherer) Option {
	return optionFunc(func(c *config) error {
		c.gatherer = gatherer
		return nil
	})
}

// WithLogger sets the logger to use. A nop is used by default.
func WithLogger(logger log.Logger) Option {
	return optionFunc(func(c *config) error {
		c.logger = logger
		return nil
	})
}

// Server is a statsd server
type Server struct {
	handler http.Handler
	address net.Addr
	mapper  *mapper.MetricMapper
	config  config
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

func getCache(cacheSize uint, cacheType string, registerer prometheus.Registerer) (mapper.MetricMapperCache, error) {
	if cacheSize == 0 {
		return nil, nil
	}

	switch cacheType {
	case "lru":
		return lru.NewMetricMapperLRUCache(registerer, int(cacheSize))
	case "random":
		return randomreplacement.NewMetricMapperRRCache(registerer, int(cacheSize))
	default:
		return nil, fmt.Errorf("unsupported cache type %q", cacheType)
	}
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
			if s.address == nil {
				continue
			}

			return s.address, nil
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

// Run until the context is cancelled.
func (s *Server) Run(ctx context.Context) error {
	// wrap context so we can control closing events, etc
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	events := make(chan event.Events, s.config.eventQueueSize)

	go func() {
		<-ctx.Done()
		close(events)
	}()

	eventQueue := event.NewEventQueue(
		events,
		int(s.config.eventFlushThreshold),
		s.config.eventFlushInterval,
		eventsFlushed,
	)

	logger := s.config.logger

	exporter := exporter.NewExporter(
		s.config.registerer,
		s.mapper,
		logger,
		eventsActions,
		eventsUnmapped,
		errorEventStats,
		eventStats,
		conflictingEventStats,
		metricsCount,
	)

	go exporter.Listen(events)

	parser := line.NewParser()
	if s.config.dogstatsdTagsEnabled {
		parser.EnableDogstatsdParsing()
	}
	if s.config.influxdbTagsEnabled {
		parser.EnableInfluxdbParsing()
	}
	if s.config.libratoTagsEnabled {
		parser.EnableLibratoParsing()
	}
	if s.config.signalFXTagsEnabled {
		parser.EnableSignalFXParsing()
	}

	switch s.config.network {
	case "udp":
		udpListenAddr, err := address.UDPAddrFromString(s.config.address)
		if err != nil {
			return fmt.Errorf("invalid UDP listen address %w", err)
		}

		uconn, err := net.ListenUDP("udp", udpListenAddr)
		if err != nil {
			return fmt.Errorf("failed to start UDP listener %w", err)
		}

		defer uconn.Close()

		if s.config.readBuffer > 0 {
			if err := uconn.SetReadBuffer(int(s.config.readBuffer)); err != nil {
				return fmt.Errorf("error setting UDP read buffer %w", err)
			}
		}

		ul := &listener.StatsDUDPListener{
			Conn:            uconn,
			EventHandler:    eventQueue,
			Logger:          logger,
			LineParser:      parser,
			UDPPackets:      udpPackets,
			LinesReceived:   linesReceived,
			EventsFlushed:   eventsFlushed,
			SampleErrors:    *sampleErrors,
			SamplesReceived: samplesReceived,
			TagErrors:       tagErrors,
			TagsReceived:    tagsReceived,
		}

		s.address = uconn.LocalAddr()

		ul.Listen()

		return nil
	default:
		// should never happen
		return fmt.Errorf("unsupported network %q", s.config.network)
	}
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
	tcpLineTooLong = prometheus.NewCounter(
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

	// Metrics can be used to register metrics about the exporter itself
	Metrics = []prometheus.Collector{
		eventStats,
		eventsFlushed,
		eventsUnmapped,
		udpPackets,
		tcpConnections,
		tcpErrors,
		tcpLineTooLong,
		unixgramPackets,
		linesReceived,
		samplesReceived,
		sampleErrors,
		tagsReceived,
		tagErrors,
		conflictingEventStats,
		errorEventStats,
		eventsActions,
		metricsCount,
	}
)
