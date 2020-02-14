package fake

import (
	"github.com/ozonru/file.d/fd"
	"github.com/ozonru/file.d/pipeline"
)

/*{ introduction
Provides API to test pipelines and other plugins.
}*/
type Plugin struct {
	controller pipeline.InputPluginController
	commitFn   func(event *pipeline.Event)
	inFn       func()
}

type Config struct {
}

func init() {
	fd.DefaultPluginRegistry.RegisterInput(&pipeline.PluginStaticInfo{
		Type:    "fake",
		Factory: Factory,
	})
}

func Factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(_ pipeline.AnyConfig, params *pipeline.InputPluginParams) {
	p.controller = params.Controller
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Commit(event *pipeline.Event) {
	if p.commitFn != nil {
		p.commitFn(event)
	}
}

//! fn-list
//^ fn-list

//> Sends test event into pipeline.
func (p *Plugin) In(sourceID pipeline.SourceID, sourceName string, offset int64, bytes []byte) { //*
	if p.inFn != nil {
		p.inFn()
	}
	p.controller.In(sourceID, sourceName, offset, bytes, false)
}

//> Sets up a hook to make sure test event have been successfully committed.
func (p *Plugin) SetCommitFn(fn func(event *pipeline.Event)) { //*
	p.commitFn = fn
}

//> Sets up a hook to make sure test event have been passed to plugin.
func (p *Plugin) SetInFn(fn func()) { //*
	p.inFn = fn
}
