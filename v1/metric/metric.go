package metric

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	BackendConnUsage = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "machinery",
			Subsystem: "backend",
			Name:      "get_conn",
			Help:      "usage of backend connection.",
		},
		[]string{"function", "detail"},
	)

	BrokerConnUsage = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "machinery",
			Subsystem: "broker",
			Name:      "get_conn",
			Help:      "usage of broker connection",
		},
		[]string{"function", "detail"},
	)
)

func init() {
	prometheus.MustRegister(BackendConnUsage)
	prometheus.MustRegister(BrokerConnUsage)
}
