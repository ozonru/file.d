package parse_es

import (
	"gitlab.ozon.ru/sre/filed/filed"
	"gitlab.ozon.ru/sre/filed/logger"
	"gitlab.ozon.ru/sre/filed/pipeline"
)

type Config struct {
}

type Plugin struct {
	config      *Config
	passNext    bool
	discardNext bool
}

func init() {
	filed.DefaultPluginRegistry.RegisterAction(&pipeline.PluginInfo{
		Type:    "parse_es",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.ActionPluginParams) {

}

func (p *Plugin) Stop() {
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	root := event.Root

	if p.passNext && p.discardNext {
		logger.Panicf("wrong state")
	}

	if p.passNext {
		p.passNext = false
		return pipeline.ActionPass
	}

	if p.discardNext {
		p.discardNext = false
		return pipeline.ActionDiscard
	}

	if root.Dig("delete") != nil {
		return pipeline.ActionDiscard
	}

	if root.Dig("update") != nil {
		p.discardNext = true
		return pipeline.ActionDiscard
	}

	if root.Dig("index") != nil {
		p.passNext = true
		return pipeline.ActionDiscard
	}

	if root.Dig("create") != nil {
		p.passNext = true
		return pipeline.ActionDiscard
	}

	logger.Fatalf("wrong ES input format, expected action, got: %s", root.EncodeToString())

	return pipeline.ActionDiscard
}
