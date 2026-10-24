package service

import (
	"context"
	"sync"
	"testing"

	"github.com/ReCasaOS/CasaOS-MessageBus/model"
)

// Publish and Trigger racing the end of Start: the inbound channel is not closed under a sender
// (a send on a closed channel panics, and select may pick it while ctx.Done is ready too).
func TestSendersRacingTheEndOfStartDoNotPanic(t *testing.T) {
	for range 300 {
		events, actions := newEventService(t), newActionService(t)
		ctx, cancel := context.WithCancel(context.Background())
		var stopped sync.WaitGroup
		stopped.Add(2)
		go func() { defer stopped.Done(); events.Start(&ctx) }()
		go func() { defer stopped.Done(); actions.Start(&ctx) }()
		<-events.Ready()
		<-actions.Ready()

		var senders sync.WaitGroup
		go cancel() // the senders and the end of Start at once
		for range 64 {
			senders.Add(2)
			go func() {
				defer senders.Done()
				events.Publish(model.Event{SourceID: "app-management", Name: "backup:error"})
			}()
			go func() {
				defer senders.Done()
				actions.Trigger(model.Action{SourceID: "app-management", Name: "app:install"})
			}()
		}
		senders.Wait()
		stopped.Wait()
	}
}
