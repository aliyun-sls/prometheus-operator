package sls

import (
	"context"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	monitoringclient "github.com/prometheus-operator/prometheus-operator/pkg/client/versioned"
	"github.com/prometheus-operator/prometheus-operator/pkg/informers"
	"github.com/prometheus-operator/prometheus-operator/pkg/k8sutil"
	"github.com/prometheus-operator/prometheus-operator/pkg/operator"
	prompkg "github.com/prometheus-operator/prometheus-operator/pkg/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"time"
)

const (
	resyncPeriod = 5 * time.Minute
)

type Operator struct {
	kclient  kubernetes.Interface
	logger   log.Logger
	accessor *operator.Accessor

	smonInfs *informers.ForResource
	pmonInfs *informers.ForResource

	metrics         *operator.Metrics
	reconciliations *operator.ReconciliationTracker

	nodeAddressLookupErrors prometheus.Counter
	nodeEndpointSyncs       prometheus.Counter
	nodeEndpointSyncErrors  prometheus.Counter

	config    operator.Config
	generator *prompkg.ConfigGenerator
	smcb      func(monitor *monitoringv1.SLSServiceMonitor, category cache.DeltaType)
	pmcb      func(monitor *monitoringv1.SLSPodMonitor, category cache.DeltaType)
}

type ServiceMonitorCallback func(monitor *monitoringv1.SLSServiceMonitor, category cache.DeltaType)
type PodMonitorCallback func(monitor *monitoringv1.SLSPodMonitor, category cache.DeltaType)

func NewWithCallback(ctx context.Context, conf operator.Config, logger log.Logger, client *kubernetes.Clientset, smcb ServiceMonitorCallback, pmcb PodMonitorCallback) (*Operator, error) {
	o, err := doNew(nil, client, conf, logger)
	if err != nil {
		return nil, err
	}
	o.smcb = smcb
	o.pmcb = pmcb
	return o, nil
}

// New creates a new controller.
func New(ctx context.Context, conf operator.Config, logger log.Logger, r prometheus.Registerer) (*Operator, error) {
	return doNew(r, nil, conf, logger)
}

func doNew(r prometheus.Registerer, client *kubernetes.Clientset, conf operator.Config, logger log.Logger) (*Operator, error) {
	cfg, err := k8sutil.NewClusterConfig(conf.Host, conf.TLSInsecure, &conf.TLSConfig)
	if err != nil {
		return nil, errors.Wrap(err, "instantiating cluster config failed")
	}
	if client == nil {
		client, err = kubernetes.NewForConfig(cfg)
		if err != nil {
			return nil, errors.Wrap(err, "instantiating kubernetes client failed")
		}
	}
	mclient, err := monitoringclient.NewForConfig(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "instantiating monitoring client failed")
	}

	if _, err := labels.Parse(conf.PromSelector); err != nil {
		return nil, errors.Wrap(err, "can not parse prometheus selector value")
	}

	cg, err := prompkg.NewSLSConfigGenerator(logger)
	if err != nil {
		return nil, err
	}

	c := &Operator{
		kclient:   client,
		logger:    logger,
		accessor:  operator.NewAccessor(logger),
		config:    conf,
		generator: cg,
	}

	if r != nil {
		c.reconciliations = &operator.ReconciliationTracker{}
		// All the metrics exposed by the controller get the controller="prometheus" label.
		r = prometheus.WrapRegistererWith(prometheus.Labels{"controller": "prometheus"}, r)
		c.metrics = operator.NewMetrics(r)
		c.nodeAddressLookupErrors = prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_operator_node_address_lookup_errors_total",
			Help: "Number of times a node IP address could not be determined",
		})
		c.nodeEndpointSyncs = prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_operator_node_syncs_total",
			Help: "Number of node endpoints synchronisations",
		})
		c.nodeEndpointSyncErrors = prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_operator_node_syncs_failed_total",
			Help: "Number of node endpoints synchronisation failures",
		})
		c.metrics.MustRegister(
			c.nodeAddressLookupErrors,
			c.nodeEndpointSyncs,
			c.nodeEndpointSyncErrors,
			c.reconciliations,
		)
	}

	c.smonInfs, err = informers.NewInformersForResource(
		informers.NewMonitoringInformerFactories(
			c.config.Namespaces.AllowList,
			c.config.Namespaces.DenyList,
			mclient,
			resyncPeriod,
			nil,
		),
		monitoringv1.SchemeGroupVersion.WithResource(monitoringv1.SLSServiceMonitorName),
	)
	if err != nil {
		return nil, errors.Wrap(err, "error creating servicemonitor informers")
	}

	c.pmonInfs, err = informers.NewInformersForResource(
		informers.NewMonitoringInformerFactories(
			c.config.Namespaces.AllowList,
			c.config.Namespaces.DenyList,
			mclient,
			resyncPeriod,
			nil,
		),
		monitoringv1.SchemeGroupVersion.WithResource(monitoringv1.SLSPodMonitorName),
	)
	if err != nil {
		return nil, errors.Wrap(err, "error creating podmonitor informers")
	}
	return c, nil

}

func (c *Operator) Run(ctx context.Context) error {
	errChan := make(chan error)
	go func() {
		v, err := c.kclient.Discovery().ServerVersion()
		if err != nil {
			errChan <- errors.Wrap(err, "communicating with server failed")
			return
		}
		level.Info(c.logger).Log("msg", "connection established", "cluster-version", v)
		errChan <- nil
	}()
	select {
	case err := <-errChan:
		if err != nil {
			return err
		}
		level.Info(c.logger).Log("msg", "CRD API endpoints ready")
	case <-ctx.Done():
		return nil
	}
	go c.smonInfs.Start(ctx.Done())
	go c.pmonInfs.Start(ctx.Done())
	if err := c.waitForCacheSync(ctx); err != nil {
		return err
	}

	c.addHandlers()

	c.metrics.Ready().Set(1)
	<-ctx.Done()
	return nil
}

// waitForCacheSync waits for the informers' caches to be synced.
func (c *Operator) waitForCacheSync(ctx context.Context) error {
	for _, infs := range []struct {
		name                 string
		informersForResource *informers.ForResource
	}{
		{"ServiceMonitor", c.smonInfs},
		{"PodMonitor", c.pmonInfs},
	} {
		for _, inf := range infs.informersForResource.GetInformers() {
			if !operator.WaitForNamedCacheSync(ctx, "prometheus", log.With(c.logger, "informer", infs.name), inf.Informer()) {
				return errors.Errorf("failed to sync cache for %s informer", infs.name)
			}
		}
	}
	level.Info(c.logger).Log("msg", "successfully synced all caches")
	return nil
}

// addHandlers adds the eventhandlers to the informers.
func (c *Operator) addHandlers() {
	c.smonInfs.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.handleSmonAdd,
		DeleteFunc: c.handleSmonDelete,
		UpdateFunc: c.handleSmonUpdate,
	})

	c.pmonInfs.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.handlePmonAdd,
		DeleteFunc: c.handlePmonDelete,
		UpdateFunc: c.handlePmonUpdate,
	})
}

func (c *Operator) handleSmonAdd(obj interface{}) {
	_, ok := c.accessor.ObjectMetadata(obj)
	if ok {
		level.Info(c.logger).Log("msg", "ServiceMonitor added")
		if c.metrics != nil {
			c.metrics.TriggerByCounter(monitoringv1.ServiceMonitorsKind, operator.AddEvent).Inc()
		}
		c.smcb(obj.(*monitoringv1.SLSServiceMonitor), cache.Added)
	}
}

func (c *Operator) handleSmonUpdate(old, cur interface{}) {
	if old.(*monitoringv1.ServiceMonitor).ResourceVersion == cur.(*monitoringv1.ServiceMonitor).ResourceVersion {
		return
	}

	_, ok := c.accessor.ObjectMetadata(cur)
	if ok {
		level.Debug(c.logger).Log("msg", "ServiceMonitor updated")
		if c.metrics != nil {
			c.metrics.TriggerByCounter(monitoringv1.ServiceMonitorsKind, operator.UpdateEvent).Inc()
		}
		c.smcb(cur.(*monitoringv1.SLSServiceMonitor), cache.Updated)
	}
}

func (c *Operator) handleSmonDelete(obj interface{}) {
	_, ok := c.accessor.ObjectMetadata(obj)
	if ok {
		level.Info(c.logger).Log("msg", "ServiceMonitor delete")
		if c.metrics != nil {
			c.metrics.TriggerByCounter(monitoringv1.ServiceMonitorsKind, operator.DeleteEvent).Inc()
		}
		c.smcb(obj.(*monitoringv1.SLSServiceMonitor), cache.Deleted)
	}
}

func (c *Operator) handlePmonAdd(obj interface{}) {
	_, ok := c.accessor.ObjectMetadata(obj)
	if ok {
		level.Debug(c.logger).Log("msg", "PodMonitor added")
		if c.metrics != nil {
			c.metrics.TriggerByCounter(monitoringv1.PodMonitorsKind, operator.AddEvent).Inc()
		}
		c.pmcb(obj.(*monitoringv1.SLSPodMonitor), cache.Added)
	}
}

func (c *Operator) handlePmonUpdate(old, cur interface{}) {
	if old.(*monitoringv1.PodMonitor).ResourceVersion == cur.(*monitoringv1.PodMonitor).ResourceVersion {
		return
	}
	_, ok := c.accessor.ObjectMetadata(cur)
	if ok {
		level.Debug(c.logger).Log("msg", "PodMonitor updated")
		if c.metrics != nil {
			c.metrics.TriggerByCounter(monitoringv1.PodMonitorsKind, operator.UpdateEvent).Inc()
		}
		c.pmcb(cur.(*monitoringv1.SLSPodMonitor), cache.Updated)
	}
}

func (c *Operator) handlePmonDelete(obj interface{}) {
	_, ok := c.accessor.ObjectMetadata(obj)
	if ok {
		level.Debug(c.logger).Log("msg", "PodMonitor delete")
		if c.metrics != nil {
			c.metrics.TriggerByCounter(monitoringv1.PodMonitorsKind, operator.DeleteEvent).Inc()
		}
		c.pmcb(obj.(*monitoringv1.SLSPodMonitor), cache.Deleted)
	}
}
