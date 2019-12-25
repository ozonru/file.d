package discard

import (
	"gitlab.ozon.ru/sre/filed/filed"
	"gitlab.ozon.ru/sre/filed/pipeline"
)

type Plugin struct {
}

type Config struct {
}

func init() {
	filed.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "discard",
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
	return pipeline.ActionDiscard
}
