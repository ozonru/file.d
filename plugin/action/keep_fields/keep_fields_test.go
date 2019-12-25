package keep_fields

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"gitlab.ozon.ru/sre/filed/pipeline"
	"gitlab.ozon.ru/sre/filed/test"
)

func TestKeepFields(t *testing.T) {
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, nil, pipeline.MatchModeAnd, nil))
	wg := &sync.WaitGroup{}
	wg.Add(3)

	outEvents := make([]*pipeline.Event, 0, 0)
	output.SetOutFn(func(e *pipeline.Event) {
		outEvents = append(outEvents, e)
		wg.Done()
	})

	input.In(0, "test.log", 0, 0, []byte(`{"field_1":"value_1","a":"b"}`))
	input.In(0, "test.log", 0, 0, []byte(`{"field_2":"value_2","b":"c"}`))
	input.In(0, "test.log", 0, 0, []byte(`{"field_3":"value_3","a":"b"}`))

	wg.Wait()
	p.Stop()

	assert.Equal(t, 3, len(outEvents), "wrong out events count")
	assert.Equal(t, `{"field_1":"value_1"}`, outEvents[0].Root.EncodeToString(), "wrong event")
	assert.Equal(t, `{"field_2":"value_2"}`, outEvents[1].Root.EncodeToString(), "wrong event")
	assert.Equal(t, `{}`, outEvents[2].Root.EncodeToString(), "wrong event")
}
