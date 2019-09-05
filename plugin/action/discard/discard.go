package discard

import (
	"gitlab.ozon.ru/sre/filed/filed"
	"gitlab.ozon.ru/sre/filed/pipeline"
)

type Plugin struct {
	config     *Config
	controller pipeline.ActionController
}

type Config struct {
}

func init() {
	filed.DefaultPluginRegistry.RegisterAction(&pipeline.PluginInfo{
		Type:    "discard",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, controller pipeline.ActionController) {
	p.controller = controller
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Do(event *pipeline.Event) {
	p.controller.Drop(event)
}
