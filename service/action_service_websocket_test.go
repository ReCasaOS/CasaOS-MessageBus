package service

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ReCasaOS/CasaOS-Common/utils/logger"
	"github.com/ReCasaOS/CasaOS-MessageBus/model"
	"github.com/ReCasaOS/CasaOS-MessageBus/repository"
	"go.uber.org/goleak"
	"gotest.tools/assert"
)

func newActionService(t *testing.T) *ActionServiceWS {
	t.Helper()
	logger.LogInitConsoleOnly()
	repository, err := repository.NewDatabaseRepositoryInMemory()
	assert.NilError(t, err)
	t.Cleanup(func() { repository.Close() })
	typeService := NewActionTypeService(&repository)
	_, err = typeService.RegisterActionType(model.ActionType{SourceID: "app-management", Name: "app:install"})
	assert.NilError(t, err)
	return NewActionServiceWS(typeService)
}

func receiveAction(t *testing.T, channel chan model.Action) model.Action {
	t.Helper()
	select {
	case action := <-channel:
		return action
	case <-time.After(5 * time.Second):
		t.Fatal("no action arrived")
		return model.Action{}
	}
}

// A subscription made before Start must survive Start (see the event service).
func TestActionSubscriptionBeforeStartSurvivesStart(t *testing.T) {
	// a cleanup: runs after the repository's (registered later, run first)
	t.Cleanup(func() { goleak.VerifyNone(t) })
	ws := newActionService(t)

	channel, err := ws.Subscribe("app-management", []string{"app:install"})
	assert.NilError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go ws.Start(&ctx)
	<-ws.Ready()

	within(t, "Trigger after Start", func() {
		ws.Trigger(model.Action{SourceID: "app-management", Name: "app:install"})
	})
	assert.Equal(t, receiveAction(t, channel).Name, "app:install")
}

// A Trigger before Start is dropped; it used to dereference the nil context.
func TestTriggerBeforeStartIsDropped(t *testing.T) {
	ws := newActionService(t)
	within(t, "Trigger before Start", func() {
		ws.Trigger(model.Action{SourceID: "app-management", Name: "app:install"})
	})
}

// Subscribe and Trigger from the routes while Start installs its state, and
// while it dispatches: -race finds an unguarded field.
func TestActionSubscribeAndTriggerRaceWithStart(t *testing.T) {
	// a cleanup: runs after the repository's (registered later, run first)
	t.Cleanup(func() { goleak.VerifyNone(t) })
	ws := newActionService(t)

	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})

	var wg sync.WaitGroup
	channels := make(chan chan model.Action, 8)
	for range 8 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			channel, err := ws.Subscribe("app-management", []string{"app:install"})
			assert.Check(t, err)
			channels <- channel
		}()
		go func() {
			defer wg.Done()
			ws.Trigger(model.Action{SourceID: "app-management", Name: "app:install"})
		}()
	}
	go func() {
		ws.Start(&ctx)
		close(stopped)
	}()
	wg.Wait()
	close(channels)

	// every subscription made concurrently with Start is kept
	<-ws.Ready()
	ws.Trigger(model.Action{SourceID: "app-management", Name: "app:install"})
	for channel := range channels {
		// buffered (1): the last action, or an earlier one if it was still waiting
		assert.Equal(t, receiveAction(t, channel).Name, "app:install")
	}

	cancel()
	<-stopped
}
