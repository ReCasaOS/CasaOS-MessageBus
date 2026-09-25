package service

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ReCasaOS/CasaOS-Common/utils/logger"
	"github.com/ReCasaOS/CasaOS-MessageBus/model"
	"github.com/ReCasaOS/CasaOS-MessageBus/repository"
	"gotest.tools/assert"
)

// within fails the test when f has not returned after five seconds.
func within(t *testing.T, what string, f func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		f()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s did not return: the bus is deadlocked", what)
	}
}

func newEventService(t *testing.T) *EventServiceWS {
	t.Helper()
	logger.LogInitConsoleOnly()
	repository, err := repository.NewDatabaseRepositoryInMemory()
	assert.NilError(t, err)
	t.Cleanup(func() { repository.Close() })
	typeService := NewEventTypeService(&repository)
	_, err = typeService.RegisterEventType(model.EventType{SourceID: "app-management", Name: "backup:error"})
	assert.NilError(t, err)
	return NewEventServiceWS(typeService)
}

// Three websockets on one event, the middle one closing: Unsubscribe took the
// lock once per subscriber it looked at, and deadlocked on the second. Every
// subscription after that hung, the core's alerts among them.
func TestUnsubscribeASubscriberAfterTheFirst(t *testing.T) {
	s := newEventService(t)
	channels := make([]chan model.Event, 3)
	for i := range channels {
		c, err := s.Subscribe("app-management", []string{"backup:error"})
		assert.NilError(t, err)
		channels[i] = c
	}

	within(t, "Unsubscribe", func() {
		assert.NilError(t, s.Unsubscribe("app-management", "backup:error", channels[1]))
	})
	within(t, "Subscribe", func() {
		_, err := s.Subscribe("app-management", []string{"backup:error"})
		assert.NilError(t, err)
	})
	assert.Equal(t, len(s.subscriberChannels["app-management"]["backup:error"]), 3)
}

// Events published together all reach a subscriber: none is dropped because
// the dispatch was busy with another, or the subscriber had one waiting.
func TestEventsPublishedTogetherAllArrive(t *testing.T) {
	s := newEventService(t)
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		s.Start(&ctx)
		close(stopped)
	}()
	t.Cleanup(func() {
		cancel()
		<-stopped
	})
	for started := false; !started; {
		mutex.Lock()
		started = s.stop != nil
		mutex.Unlock()
		time.Sleep(time.Millisecond)
	}

	c, err := s.Subscribe("app-management", []string{"backup:error"})
	assert.NilError(t, err)
	var publishing sync.WaitGroup
	for range 10 {
		publishing.Add(1)
		go func() {
			defer publishing.Done()
			s.Publish(model.Event{SourceID: "app-management", Name: "backup:error"})
		}()
	}
	publishing.Wait()

	for i := range 10 {
		select {
		case <-c:
		case <-time.After(5 * time.Second):
			t.Fatalf("%d of 10 events arrived", i)
		}
	}
}
