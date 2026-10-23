package service

import (
	"context"
	"testing"
	"time"

	"github.com/ReCasaOS/CasaOS-Common/utils/logger"
	"github.com/ReCasaOS/CasaOS-MessageBus/model"
	"github.com/ReCasaOS/CasaOS-MessageBus/repository"
	"go.uber.org/goleak"
	"gotest.tools/assert"
)

func TestEventTypeService(t *testing.T) {
	logger.LogInitConsoleOnly() // Publish logs while the loop is not up yet
	defer goleak.VerifyNone(t)

	// new repository
	repository, err := repository.NewDatabaseRepositoryInMemory()
	assert.NilError(t, err)
	defer repository.Close()

	// new typeService
	typeService := NewEventTypeService(&repository)
	wsService := NewEventServiceWS(typeService)

	// new context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go wsService.Start(&ctx)

	sourceID := "Foo"
	eventNames := []string{"Bar", "Baz"}

	// register event type
	for _, name := range eventNames {
		_, err = typeService.RegisterEventType(model.EventType{
			SourceID:         sourceID,
			Name:             name,
			PropertyTypeList: []model.PropertyType{{Name: "Property1"}, {Name: "Property2"}},
		})
	}

	assert.NilError(t, err)

	// get event types
	eventTypes, err := typeService.GetEventTypes()
	assert.NilError(t, err)
	assert.Equal(t, len(eventTypes), 2)

	// get event types by source id
	eventTypes, err = typeService.GetEventTypesBySourceID(sourceID)
	assert.NilError(t, err)
	assert.Equal(t, len(eventTypes), 2)

	// get event type
	for _, name := range eventNames {
		eventType, err := typeService.GetEventType(sourceID, name)
		assert.NilError(t, err)
		assert.Equal(t, eventType.SourceID, sourceID)
		assert.Equal(t, eventType.Name, name)
	}

	// subscribe event type
	channel, err := wsService.Subscribe(sourceID, eventNames)
	assert.NilError(t, err)

	outputChannel := make(chan model.Event)

	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-channel:
				if !ok {
					t.Error("channel closed")
				}
				outputChannel <- event
			}
		}
	}(ctx)

	for _, name := range eventNames {
		expectedEvent := model.Event{
			SourceID: sourceID,
			Name:     name,
			// Properties: []model.Property{
			// 	{Name: "Property1", Value: "Value1"},
			// 	{Name: "Property2", Value: "Value2"},
			// },
		}

		// Start runs in a goroutine; Publish drops until the loop runs → republish
		actualEvent := publishUntilReceived(t, wsService, expectedEvent, outputChannel)
		assert.DeepEqual(t, model.Event{SourceID: actualEvent.SourceID, Name: actualEvent.Name, Properties: actualEvent.Properties}, expectedEvent)
	}
}

func publishUntilReceived(t *testing.T, wsService *EventServiceWS, event model.Event, received <-chan model.Event) model.Event {
	t.Helper()
	deadline := time.After(10 * time.Second)
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		wsService.Publish(event)
		select {
		case actual := <-received:
			return actual
		case <-tick.C:
		case <-deadline:
			t.Fatalf("event %s/%s never arrived", event.SourceID, event.Name)
		}
	}
}
