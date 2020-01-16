package k8s

import (
	"strings"

	"gitlab.ozon.ru/sre/file-d/fd"
	"gitlab.ozon.ru/sre/file-d/logger"
	"gitlab.ozon.ru/sre/file-d/pipeline"
	"go.uber.org/atomic"
)

// Plugin adds k8s meta info to docker logs and also joins split docker logs into one event
// source docker log file name format should be: [pod-name]_[namespace]_[container-name]-[container id].log
// example: /docker-logs/advanced-logs-checker-1566485760-trtrq_sre_duty-bot-4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0.log
type Plugin struct {
	config  *Config
	logBuff []byte
	logSize int
}

const (
	defaultMaxEventSize = 1000000
	predictionLookahead = 128 * 1024
)

type Config struct {
	MaxEventSize    int    `json:"max_event_size"`
	LabelsWhitelist string `json:"labels_whitelist"`
	OnlyNode        bool   `json:"only_node"`
	labelsWhitelist map[string]bool
}

var (
	startCounter atomic.Int32
)

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "k8s",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, _ *pipeline.ActionPluginParams) {
	p.config = config.(*Config)

	startCounter := startCounter.Inc()
	if startCounter == 1 {
		if p.config.MaxEventSize == 0 {
			p.config.MaxEventSize = defaultMaxEventSize
		}

		p.config.labelsWhitelist = make(map[string]bool)

		if p.config.LabelsWhitelist != "" {
			parts := strings.Split(p.config.LabelsWhitelist, ",")
			for _, part := range parts {
				cleanPart := strings.TrimSpace(part)
				p.config.labelsWhitelist[cleanPart] = true
			}
		}

		enableGatherer()
	}

	p.logBuff = append(p.logBuff, '"')
}

func (p *Plugin) Stop() {
	startCounter := startCounter.Dec()
	if startCounter == 0 {
		disableGatherer()
	}
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	// todo: do same logic as in join plugin here, to send not full logs
	if event.IsTimeoutKind() {
		logger.Errorf("can't read next sequential event for k8s pod stream")
		p.logBuff = p.logBuff[:1]
		return pipeline.ActionDiscard
	}

	event.Root.AddFieldNoAlloc(event.Root, "k8s_node").MutateToString(node)
	if p.config.OnlyNode {
		return pipeline.ActionPass
	}
	// don't need to unescape/escape log fields cause concatenation of escaped strings is escaped string
	logFragment := event.Root.Dig("log").AsEscapedString()
	if logFragment == "" {
		logger.Fatalf("wrong docker log format, it doesn't contain log field: %s", event.Root.EncodeToString())
		panic("")
	}

	// docker splits long logs by 16kb chunks, so let's join them
	// look ahead to ensure we won't throw events longer than MaxEventSize
	// lookahead value is much more than 16Kb because json may be escaped
	p.logSize += event.Size
	predictedLen := p.logSize + predictionLookahead
	isMaxReached := predictedLen > p.config.MaxEventSize
	logFragmentLen := len(logFragment)
	if logFragment[logFragmentLen-3:logFragmentLen-1] != `\n` && !isMaxReached {
		p.logBuff = append(p.logBuff, logFragment[1:logFragmentLen-1]...)
		return pipeline.ActionCollapse
	}

	ns, pod, container, _, success, podMeta := getMeta(event.SourceName)

	if isMaxReached {
		logger.Warnf("too long k8s event found, it'll be split, ns=%s pod=%s container=%s consider increase max_event_size, max_event_size=%d, predicted event size=%d", ns, pod, container, p.config.MaxEventSize, predictedLen)
	}

	event.Root.AddFieldNoAlloc(event.Root, "k8s_namespace").MutateToString(string(ns))
	event.Root.AddFieldNoAlloc(event.Root, "k8s_pod").MutateToString(string(pod))
	event.Root.AddFieldNoAlloc(event.Root, "k8s_container").MutateToString(string(container))

	if success {
		if ns != namespace(podMeta.Namespace) {
			logger.Panicf("k8s plugin inconsistency: source=%s, file namespace=%s, meta namespace=%s, event=%s", event.SourceName, ns, podMeta.Namespace, event.Root.EncodeToString())
		}
		if pod != podName(podMeta.Name) {
			logger.Panicf("k8s plugin inconsistency: source=%s, file pod=%s, meta pod=%s, x=%s, event=%s", event.SourceName, pod, podMeta.Name, event.Root.EncodeToString())
		}

		for labelName, labelValue := range podMeta.Labels {
			if len(p.config.labelsWhitelist) != 0 {
				_, has := p.config.labelsWhitelist[labelName]

				if !has {
					continue
				}
			}

			l := len(event.Buf)
			event.Buf = append(event.Buf, "k8s_label_"...)
			event.Buf = append(event.Buf, labelName...)
			event.Root.AddFieldNoAlloc(event.Root, pipeline.ByteToStringUnsafe(event.Buf[l:])).MutateToString(labelValue)
		}
	}

	if len(p.logBuff) > 1 {
		p.logBuff = append(p.logBuff, logFragment[1:logFragmentLen-1]...)
		p.logBuff = append(p.logBuff, '"')

		l := len(event.Buf)
		event.Buf = append(event.Buf, p.logBuff...)
		event.Root.AddFieldNoAlloc(event.Root, "log").MutateToEscapedString(pipeline.ByteToStringUnsafe(event.Buf[l:]))

		p.logBuff = p.logBuff[:1]
	}
	p.logSize = 0

	return pipeline.ActionPass
}
