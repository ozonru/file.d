package fd

import (
	"encoding/json"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"runtime/debug"

	"github.com/bitly/go-simplejson"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gitlab.ozon.ru/sre/file-d/logger"
	"gitlab.ozon.ru/sre/file-d/pipeline"
)

type FileD struct {
	config    *Config
	httpAddr  string
	registry  *prometheus.Registry
	plugins   *PluginRegistry
	Pipelines []*pipeline.Pipeline
	server    *http.Server
}

func New(config *Config, httpAddr string) *FileD {
	return &FileD{
		config:    config,
		httpAddr:  httpAddr,
		plugins:   DefaultPluginRegistry,
		Pipelines: make([]*pipeline.Pipeline, 0, 0),
	}
}

func (f *FileD) SetConfig(config *Config) {
	f.config = config
}

func (f *FileD) Start() {
	logger.Infof("starting file-d")

	f.createRegistry()
	f.startHTTP()
	f.startPipelines()
}

func (f *FileD) createRegistry() {
	f.registry = prometheus.NewRegistry()
	f.registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	f.registry.MustRegister(prometheus.NewGoCollector())

	prometheus.DefaultGatherer = f.registry
	prometheus.DefaultRegisterer = f.registry
}

func (f *FileD) startPipelines() {
	f.Pipelines = f.Pipelines[:0]
	for name, config := range f.config.Pipelines {
		f.addPipeline(name, config)
	}
	for _, p := range f.Pipelines {
		p.Start()
	}
}

func (f *FileD) addPipeline(name string, config *PipelineConfig) {
	mux := http.DefaultServeMux
	settings := extractPipelineParams(config.Raw.Get("settings"))

	p := pipeline.New(name, settings, f.registry, mux)
	err := f.setupInput(p, config)
	if err != nil {
		logger.Fatalf("can't create pipeline %q: %s", name, err.Error())
	}

	f.setupActions(p, config)

	err = f.setupOutput(p, config)
	if err != nil {
		logger.Fatalf("can't create pipeline %q: %s", name, err.Error())
	}

	f.Pipelines = append(f.Pipelines, p)
}

func (f *FileD) setupInput(p *pipeline.Pipeline, pipelineConfig *PipelineConfig) error {
	info, err := f.getStaticInfo(pipelineConfig, pipeline.PluginKindInput)
	if err != nil {
		return err
	}

	p.SetInput(&pipeline.InputPluginInfo{
		PluginStaticInfo:  info,
		PluginRuntimeInfo: f.instantiatePlugin(info),
	})

	return nil
}

func (f *FileD) setupActions(p *pipeline.Pipeline, pipelineConfig *PipelineConfig) {
	actions := pipelineConfig.Raw.Get("actions")
	for index := range actions.MustArray() {
		actionJSON := actions.GetIndex(index)
		if actionJSON.MustMap() == nil {
			logger.Fatalf("empty action #%d for pipeline %q", index, p.Name)
		}

		t := actionJSON.Get("type").MustString()
		if t == "" {
			logger.Fatalf("action #%d doesn't provide type for pipeline %q", index, p.Name)
		}
		f.setupAction(p, index, t, actionJSON)
	}
}

func (f *FileD) setupAction(p *pipeline.Pipeline, index int, t string, actionJSON *simplejson.Json) {
	logger.Infof("creating action with type %q for pipeline %q", t, p.Name)
	info := f.plugins.GetActionByType(t)

	matchMode, err := extractMatchMode(actionJSON)
	if err != nil {
		logger.Fatalf("can't extract match mode for action %d/%s in pipeline %q: %s", index, t, p.Name, err.Error())
	}
	conditions, err := extractConditions(actionJSON.Get("match_fields"))
	if err != nil {
		logger.Fatalf("can't extract conditions for action %d/%s in pipeline %q: %s", index, t, p.Name, err.Error())
	}
	metricName, metricLabels := extractMetrics(actionJSON)
	configJSON := makeActionJSON(actionJSON)

	_, config := info.Factory()
	err = json.Unmarshal(configJSON, config)
	if err != nil {
		logger.Fatalf("can't unmarshal config for %s action in pipeline %q: %s", info.Type, p.Name, err.Error())
	}

	err = Parse(config)
	if err != nil {
		logger.Fatalf("wrong config for %q action in pipeline %q: %s", info.Type, p.Name, err.Error())
	}

	infoCopy := *info
	infoCopy.Config = config

	p.AddAction(&pipeline.ActionPluginStaticInfo{
		PluginStaticInfo: &infoCopy,
		MatchConditions:  conditions,
		MatchMode:        matchMode,
		MetricName:       metricName,
		MetricLabels:     metricLabels,
	})
}

func (f *FileD) setupOutput(p *pipeline.Pipeline, pipelineConfig *PipelineConfig) error {
	info, err := f.getStaticInfo(pipelineConfig, pipeline.PluginKindOutput)
	if err != nil {
		return err
	}

	p.SetOutput(&pipeline.OutputPluginInfo{
		PluginStaticInfo:  info,
		PluginRuntimeInfo: f.instantiatePlugin(info),
	})

	return nil
}

func (f *FileD) instantiatePlugin(info *pipeline.PluginStaticInfo) *pipeline.PluginRuntimeInfo {
	plugin, _ := info.Factory()
	return &pipeline.PluginRuntimeInfo{
		Plugin: plugin,
		ID:     "",
	}
}

func (f *FileD) getStaticInfo(pipelineConfig *PipelineConfig, pluginKind pipeline.PluginKind) (*pipeline.PluginStaticInfo, error) {
	configJSON := pipelineConfig.Raw.Get(string(pluginKind))
	if configJSON.MustMap() == nil {
		return nil, fmt.Errorf("no %s plugin provided", pluginKind)
	}
	t := configJSON.Get("type").MustString()
	if t == "" {
		return nil, fmt.Errorf("%s doesn't have type", pluginKind)
	}

	logger.Infof("creating %s with type %q", pluginKind, t)
	info := f.plugins.Get(pluginKind, t)
	configJson, err := configJSON.Encode()
	if err != nil {
		logger.Panicf("can't create config json for %s", t)
	}

	_, config := info.Factory()
	err = json.Unmarshal(configJson, config)
	if err != nil {
		return nil, fmt.Errorf("can't unmarshal config for %s", pluginKind)
	}

	err = Parse(config)
	if err != nil {
		logger.Fatalf("wrong config for %q plugin %q: %s", pluginKind, t, err.Error())
	}

	infoCopy := *info
	infoCopy.Config = config

	return &infoCopy, nil
}

func (f *FileD) Stop() {
	logger.Infof("stopping pipelines=%d", len(f.Pipelines))
	_ = f.server.Shutdown(nil)
	for _, p := range f.Pipelines {
		p.Stop()
	}
}

func (f *FileD) startHTTP() {
	if f.httpAddr == "off" {
		return
	}

	mux := http.DefaultServeMux

	mux.HandleFunc("/live", f.serveLiveReady)
	mux.HandleFunc("/ready", f.serveLiveReady)
	mux.HandleFunc("/freeosmem", f.serveFreeOsMem)
	mux.Handle("/metrics", promhttp.Handler())

	f.server = &http.Server{Addr: f.httpAddr, Handler: mux}
	go f.listenHTTP()
}

func (f *FileD) listenHTTP() {
	err := f.server.ListenAndServe()
	if err != nil {
		logger.Fatalf("http listening error address=%q: %s", f.httpAddr, err.Error())
	}
}

func (f *FileD) serveFreeOsMem(_ http.ResponseWriter, _ *http.Request) {
	debug.FreeOSMemory()
	logger.Infof("free OS memory OK")
}

func (f *FileD) serveLiveReady(_ http.ResponseWriter, _ *http.Request) {
	logger.Infof("live/ready OK")
}

func (f *FileD) servePipelines(_ http.ResponseWriter, _ *http.Request) {
	logger.Infof("pipelines OK")
}
