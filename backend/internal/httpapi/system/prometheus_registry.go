package system

import (
	"bytes"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/common/expfmt"
)

type pgxPoolCollector struct {
	pool *pgxpool.Pool
	name string

	maxConns          *prometheus.Desc
	totalConns        *prometheus.Desc
	idleConns         *prometheus.Desc
	acquiredConns     *prometheus.Desc
	constructingConns *prometheus.Desc
	acquireCount      *prometheus.Desc
	emptyAcquire      *prometheus.Desc
	canceledAcquire   *prometheus.Desc
}

func newPgxPoolCollector(pool *pgxpool.Pool, name string) *pgxPoolCollector {
	labels := prometheus.Labels{"db_pool": name}
	return &pgxPoolCollector{
		pool: pool,
		name: name,
		maxConns: prometheus.NewDesc(
			"pgxpool_max_conns",
			"Maximum number of connections allowed for this pgx pool.",
			nil, labels,
		),
		totalConns: prometheus.NewDesc(
			"pgxpool_total_conns",
			"Total number of connections currently existing in this pgx pool.",
			nil, labels,
		),
		idleConns: prometheus.NewDesc(
			"pgxpool_idle_conns",
			"Number of idle connections currently in this pgx pool.",
			nil, labels,
		),
		acquiredConns: prometheus.NewDesc(
			"pgxpool_acquired_conns",
			"Number of currently acquired/in-use connections from this pgx pool.",
			nil, labels,
		),
		constructingConns: prometheus.NewDesc(
			"pgxpool_constructing_conns",
			"Number of connections with construction in progress in this pgx pool.",
			nil, labels,
		),
		acquireCount: prometheus.NewDesc(
			"pgxpool_acquire_count_total",
			"Cumulative count of successful connection acquisitions from this pgx pool.",
			nil, labels,
		),
		emptyAcquire: prometheus.NewDesc(
			"pgxpool_empty_acquire_count_total",
			"Cumulative count of successful connection acquisitions that waited for a connection.",
			nil, labels,
		),
		canceledAcquire: prometheus.NewDesc(
			"pgxpool_canceled_acquire_count_total",
			"Cumulative count of connection acquisitions canceled before a connection became available.",
			nil, labels,
		),
	}
}

func (c *pgxPoolCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.maxConns
	ch <- c.totalConns
	ch <- c.idleConns
	ch <- c.acquiredConns
	ch <- c.constructingConns
	ch <- c.acquireCount
	ch <- c.emptyAcquire
	ch <- c.canceledAcquire
}

func (c *pgxPoolCollector) Collect(ch chan<- prometheus.Metric) {
	if c.pool == nil {
		return
	}
	stat := c.pool.Stat()
	ch <- prometheus.MustNewConstMetric(c.maxConns, prometheus.GaugeValue, float64(stat.MaxConns()))
	ch <- prometheus.MustNewConstMetric(c.totalConns, prometheus.GaugeValue, float64(stat.TotalConns()))
	ch <- prometheus.MustNewConstMetric(c.idleConns, prometheus.GaugeValue, float64(stat.IdleConns()))
	ch <- prometheus.MustNewConstMetric(c.acquiredConns, prometheus.GaugeValue, float64(stat.AcquiredConns()))
	ch <- prometheus.MustNewConstMetric(c.constructingConns, prometheus.GaugeValue, float64(stat.ConstructingConns()))
	ch <- prometheus.MustNewConstMetric(c.acquireCount, prometheus.CounterValue, float64(stat.AcquireCount()))
	ch <- prometheus.MustNewConstMetric(c.emptyAcquire, prometheus.CounterValue, float64(stat.EmptyAcquireCount()))
	ch <- prometheus.MustNewConstMetric(c.canceledAcquire, prometheus.CounterValue, float64(stat.CanceledAcquireCount()))
}

// newRuntimeMetricsRegistry builds a dedicated Prometheus registry for native pgx
// connection-pool stats and Go/process runtime metrics.
func newRuntimeMetricsRegistry(interactive, background *pgxpool.Pool) *prometheus.Registry {
	registry := prometheus.NewRegistry()

	if interactive != nil {
		registry.MustRegister(newPgxPoolCollector(interactive, "interactive"))
	}
	if background != nil {
		registry.MustRegister(newPgxPoolCollector(background, "background"))
	}
	registry.MustRegister(collectors.NewGoCollector())
	registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	return registry
}

// renderRegistry serializes a registry to the standard Prometheus text
// exposition format.
func renderRegistry(registry *prometheus.Registry) (string, error) {
	families, err := registry.Gather()
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer
	encoder := expfmt.NewEncoder(&buf, expfmt.NewFormat(expfmt.TypeTextPlain))
	for _, family := range families {
		if err := encoder.Encode(family); err != nil {
			return "", err
		}
	}
	return buf.String(), nil
}
