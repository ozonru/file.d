package pipeline

import (
	"strings"

	"gitlab.ozon.ru/sre/filed/logger"
	"go.uber.org/atomic"
)

type ActionResult int

const (
	// pass event to the next action in a pipeline
	ActionPass ActionResult = 0
	// skip further processing of event and request next event from the same stream and source as current
	// plugin may receive event with eventKindTimeout if it takes to long to read next event from same stream
	ActionCollapse ActionResult = 2
	// skip further processing of event and request next event from any stream and source
	ActionDiscard ActionResult = 1
	// hold event in a plugin and request next event from the same stream and source as current.
	// same as ActionCollapse but held event should be manually committed or returned into pipeline.
	// check out Commit()/Propagate() functions in InputPluginController.
	// plugin may receive event with eventKindTimeout if it takes to long to read next event from same stream
	ActionHold ActionResult = 3
)

type eventStatus string

const (
	eventStatusReceived   eventStatus = "received"
	eventStatusNotMatched eventStatus = "not_matched"
	eventStatusPassed     eventStatus = "passed"
	eventStatusDiscarded  eventStatus = "discarded"
	eventStatusCollapse   eventStatus = "collapsed"
	eventStatusHold       eventStatus = "held"
)

// processor is a goroutine which doing pipeline actions
type processor struct {
	id            int
	streamer      *streamer
	metricsHolder *metricsHolder
	output        OutputPlugin
	finalize      finalizeFn

	activeCounter *atomic.Int32

	actions          []ActionPlugin
	actionInfos      []*ActionPluginStaticInfo
	busyActions      []bool
	busyActionsTotal int

	heartbeatCh   chan *stream
	metricsValues []string
}

func NewProcessor(id int, metricsHolder *metricsHolder, activeCounter *atomic.Int32, output OutputPlugin, streamer *streamer, finalizeFn finalizeFn) *processor {
	processor := &processor{
		id:            id,
		streamer:      streamer,
		metricsHolder: metricsHolder,
		output:        output,
		finalize:      finalizeFn,

		activeCounter: activeCounter,

		metricsValues: make([]string, 0, 0),
	}

	return processor
}

func (p *processor) start(params *ActionPluginParams) {
	for i, action := range p.actions {
		action.Start(p.actionInfos[i].PluginStaticInfo.Config, params)
	}

	go p.process()
}

func (p *processor) process() {
	for {
		st := p.streamer.joinStream()
		if st == nil {
			return
		}

		p.activeCounter.Inc()
		p.dischargeStream(st)
		p.activeCounter.Dec()
	}
}

func (p *processor) dischargeStream(st *stream) {
	for {
		event := st.instantGet()
		// if event is nil then stream is over, so let's attach to a new stream
		if event == nil {
			return
		}
		if !p.processSequence(event) {
			return
		}
	}
}

func (p *processor) processSequence(event *Event) bool {
	isSuccess := false
	isPassed := false
	isSuccess, isPassed, event = p.processEvent(event)

	if isPassed {
		if event.IsUnlockKind() {
			return false
		}

		event.stage = eventStageOutput
		p.output.Out(event)
	}

	return isSuccess
}

func (p *processor) processEvent(event *Event) (isSuccess bool, isPassed bool, e *Event) {
	for {
		if event.IsUnlockKind() {
			return true, true, event
		}
		stream := event.stream

		if p.doActions(event) {
			return true, true, event
		}

		// no busy actions, so return
		if p.busyActionsTotal == 0 {
			return true, false, nil
		}

		// there is busy action, waiting for next sequential event
		action := event.action
		event = stream.blockGet()
		if event.IsTimeoutKind() {
			// pass timeout directly to plugin which requested next sequential event
			event.action = action
		}
	}
}

func (p *processor) doActions(event *Event) (isPassed bool) {
	l := len(p.actions)
	for index := event.action; index < l; index++ {
		action := p.actions[index]
		event.action = index
		p.countEvent(event, index, eventStatusReceived)

		if !p.isMatch(index, event) {
			p.countEvent(event, index, eventStatusNotMatched)
			continue
		}

		switch action.Do(event) {
		case ActionPass:
			p.countEvent(event, index, eventStatusPassed)
			p.tryResetBusy(index)
		case ActionDiscard:
			p.countEvent(event, index, eventStatusDiscarded)
			p.tryResetBusy(index)
			// can't notify input here, because previous events may delay and we'll get offset sequence corruption
			p.finalize(event, false, true)
			return false
		case ActionCollapse:
			p.countEvent(event, index, eventStatusCollapse)
			p.tryMarkBusy(index)
			// can't notify input here, because previous events may delay and we'll get offset sequence corruption
			p.finalize(event, false, true)
			return false
		case ActionHold:
			p.countEvent(event, index, eventStatusHold)
			p.tryMarkBusy(index)

			p.finalize(event, false, false)
			return false
		}
	}

	return true
}

func (p *processor) tryMarkBusy(index int) {
	if p.busyActions[index] {
		return
	}
	p.busyActions[index] = true
	p.busyActionsTotal++

	if p.busyActionsTotal > len(p.actions) {
		logger.Panicf("blocked actions too big")
	}
}

func (p *processor) tryResetBusy(index int) {
	if !p.busyActions[index] {
		return
	}
	p.busyActions[index] = false
	p.busyActionsTotal--

	if p.busyActionsTotal < 0 {
		logger.Panicf("blocked action count less than zero")
	}
}

func (p *processor) countEvent(event *Event, actionIndex int, status eventStatus) {
	p.metricsValues = p.metricsHolder.countEvent(event, actionIndex, status, p.metricsValues)
}

func (p *processor) isMatch(index int, event *Event) bool {
	if event.IsTimeoutKind() {
		return true
	}

	if p.busyActions[index] {
		return true
	}

	info := p.actionInfos[index]
	conds := info.MatchConditions
	mode := info.MatchMode

	if mode == MatchModeOr {
		return p.isMatchOr(conds, event)
	} else {
		return p.isMatchAnd(conds, event)
	}
}

func (p *processor) isMatchOr(conds MatchConditions, event *Event) bool {
	for _, cond := range conds {
		value := event.Root.Dig(cond.Field).AsString()
		if value == "" {
			continue
		}

		match := false
		if cond.Regexp != nil {
			match = cond.Regexp.MatchString(value)
		} else {
			match = strings.TrimFunc(value, TrimSpaceFunc) == cond.Value
		}

		if match {
			return true
		}
	}

	return false
}

func (p *processor) isMatchAnd(conds MatchConditions, event *Event) bool {
	for _, cond := range conds {
		value := event.Root.Dig(cond.Field).AsString()
		if value == "" {
			return false
		}

		match := false
		if cond.Regexp != nil {
			match = cond.Regexp.MatchString(value)
		} else {
			match = strings.TrimFunc(value, TrimSpaceFunc) == cond.Value
		}

		if !match {
			return false
		}
	}

	return true
}

func (p *processor) stop() {
	p.streamer.unblockProcessor()

	for _, action := range p.actions {
		action.Stop()
	}
}

func (p *processor) AddActionPlugin(info *ActionPluginInfo) {
	p.actions = append(p.actions, info.Plugin.(ActionPlugin))
	p.actionInfos = append(p.actionInfos, info.ActionPluginStaticInfo)
	p.busyActions = append(p.busyActions, false)
}

func (p *processor) Commit(event *Event) {
	p.finalize(event, false, true)
}

func (p *processor) Propagate(event *Event) {
	event.action++
	p.processSequence(event)
}
