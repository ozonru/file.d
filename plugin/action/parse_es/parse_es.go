package parse_es

import (
	"github.com/ozonru/file.d/fd"
	"github.com/ozonru/file.d/pipeline"
	"go.uber.org/zap"
)

/*{ introduction
Parses HTTP input using Elasticsearch `/_bulk` API format. It converts sources defining by create/index actions to the events. Update/delete actions are ignored.
> Check out for details: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
}*/
type Plugin struct {
	logger      *zap.SugaredLogger
	config      *Config
	passNext    bool
	discardNext bool
}

type Config struct {
}

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "parse_es",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(_ pipeline.AnyConfig, params *pipeline.ActionPluginParams) {
	p.logger = params.Logger
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	if event.IsTimeoutKind() {
		p.logger.Errorf("timeout while parsing elasticsearch event stream")
		return pipeline.ActionDiscard
	}

	root := event.Root

	if p.passNext && p.discardNext {
		p.logger.Panicf("wrong state")
	}

	if p.passNext {
		p.passNext = false
		return pipeline.ActionPass
	}

	if p.discardNext {
		p.discardNext = false
		return pipeline.ActionCollapse
	}

	if root.Dig("delete") != nil {
		return pipeline.ActionCollapse
	}

	if root.Dig("update") != nil {
		p.discardNext = true
		return pipeline.ActionCollapse
	}

	if root.Dig("index") != nil {
		p.passNext = true
		return pipeline.ActionCollapse
	}

	if root.Dig("create") != nil {
		p.passNext = true
		return pipeline.ActionCollapse
	}

	p.logger.Fatalf("wrong ES input format, expected action, got: %s", root.EncodeToString())

	return pipeline.ActionDiscard
}
