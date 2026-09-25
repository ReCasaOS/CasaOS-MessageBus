package service

import (
	"context"
	"sync"
	"time"

	"github.com/ReCasaOS/CasaOS-Common/utils/logger"

	"github.com/ReCasaOS/CasaOS-MessageBus/common"
	"github.com/ReCasaOS/CasaOS-MessageBus/model"
	"go.uber.org/zap"
)

// subscriberBuffer is how many events a subscriber may be behind before it
// misses one: a backup's error right after its begin, or two containers dying
// together, is more than one.
const subscriberBuffer = 64

type EventServiceWS struct {
	typeService *EventTypeService

	ctx  *context.Context
	stop chan struct{}

	inboundChannel     chan model.Event
	subscriberChannels map[string]map[string][]chan model.Event
}

var mutex = &sync.Mutex{}

func (s *EventServiceWS) Publish(event model.Event) {
	if s.inboundChannel == nil {
		logger.Error("error when publishing event via websocket", zap.Error(ErrInboundChannelNotFound))
	}

	if event.Timestamp == 0 {
		event.Timestamp = time.Now().Unix()
	}

	// TODO - ensure properties are valid for event type

	// Waits for the dispatch to take it, never long: the route publishes from a
	// goroutine of its own, and an event dropped here because the dispatch was
	// busy with the one before was an alert never sent.
	select {
	case s.inboundChannel <- event:

	case <-(*s.ctx).Done():
		if err := (*s.ctx).Err(); err != nil {
			logger.Info(err.Error())
		}
		return
	}
}

func (s *EventServiceWS) Subscribe(sourceID string, names []string) (chan model.Event, error) {
	if len(names) == 0 {
		eventTypes, err := s.typeService.GetEventTypesBySourceID(sourceID)
		if err != nil {
			return nil, err
		}

		for _, eventType := range eventTypes {
			names = append(names, eventType.Name)
		}
	}

	for _, name := range names {
		eventType, err := s.typeService.GetEventType(sourceID, name)
		if err != nil {
			return nil, err
		}

		if eventType == nil {
			return nil, ErrEventNameNotFound
		}
	}

	c := func() chan model.Event {
		mutex.Lock()
		defer mutex.Unlock()

		if s.subscriberChannels == nil {
			s.subscriberChannels = make(map[string]map[string][]chan model.Event)
		}

		if s.subscriberChannels[sourceID] == nil {
			s.subscriberChannels[sourceID] = make(map[string][]chan model.Event)
		}

		c := make(chan model.Event, subscriberBuffer)

		for _, name := range names {
			if s.subscriberChannels[sourceID][name] == nil {
				s.subscriberChannels[sourceID][name] = make([]chan model.Event, 0)
			}
			s.subscriberChannels[sourceID][name] = append(s.subscriberChannels[sourceID][name], c)
		}
		return c
	}()

	return c, nil
}

func (s *EventServiceWS) Unsubscribe(sourceID string, name string, c chan model.Event) error {
	// Once, for the whole call. Taken per subscriber looked at, its unlock
	// deferred, it deadlocked the bus on the second: every subscription after
	// that hung once its websocket was open, and its events never came.
	mutex.Lock()
	defer mutex.Unlock()

	if s.subscriberChannels == nil {
		return ErrSubscriberChannelsNotFound
	}

	if s.subscriberChannels[sourceID] == nil {
		return ErrEventSourceIDNotFound
	}

	if s.subscriberChannels[sourceID][name] == nil {
		return ErrEventNameNotFound
	}

	for i, subscriber := range s.subscriberChannels[sourceID][name] {
		if subscriber == c {
			logger.Info("unsubscribing from event type", zap.String("sourceID", sourceID), zap.String("name", name), zap.Int("subscriber", i))
			s.subscriberChannels[sourceID][name] = append(s.subscriberChannels[sourceID][name][:i], s.subscriberChannels[sourceID][name][i+1:]...)
			return nil
		}
	}

	return nil
}

func (s *EventServiceWS) Start(ctx *context.Context) {
	func() {
		mutex.Lock()
		defer mutex.Unlock()

		s.ctx = ctx

		s.inboundChannel = make(chan model.Event)
		s.subscriberChannels = make(map[string]map[string][]chan model.Event)
		s.stop = make(chan struct{})
	}()

	defer func() {
		if s.subscriberChannels != nil {
			for sourceID, source := range s.subscriberChannels {
				for eventName, subscribers := range source {
					for _, subscriber := range subscribers {
						select {
						case _, ok := <-subscriber:
							if ok {
								close(subscriber)
							}
						default:
							continue
						}
					}
					delete(s.subscriberChannels[sourceID], eventName)
				}
				delete(s.subscriberChannels, sourceID)
			}
			s.subscriberChannels = nil
		}

		close(s.inboundChannel)
		s.inboundChannel = nil

		close(s.stop)
		s.stop = nil
	}()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {

		case <-(*s.ctx).Done():
			return

		case event, ok := <-s.inboundChannel:
			if !ok {
				return
			}

			// Under the lock Subscribe and Unsubscribe change the lists with: read
			// while they change, a map can bring the bus down, and a subscriber
			// just gone has its channel closed.
			func() {
				mutex.Lock()
				defer mutex.Unlock()

				if s.subscriberChannels == nil || s.subscriberChannels[event.SourceID] == nil {
					return
				}

				for _, c := range s.subscriberChannels[event.SourceID][event.Name] {
					select {
					case c <- event:
					default: // a subscriber this far behind misses it: the bus waits for none
					}
				}
			}()

		case <-ticker.C:
			heartbeat := model.Event{
				SourceID:  common.MessageBusSourceID,
				Name:      common.MessageBusHeartbeatName,
				Timestamp: time.Now().Unix(),
			}

			func() {
				mutex.Lock()
				defer mutex.Unlock()

				for _, source := range s.subscriberChannels {
					for _, subscribers := range source {
						for _, subscriber := range subscribers {
							select {
							case subscriber <- heartbeat:
							default: // one already waiting does as well
							}
						}
					}
				}
			}()
		}
	}
}

func NewEventServiceWS(eventTypeService *EventTypeService) *EventServiceWS {
	return &EventServiceWS{
		typeService: eventTypeService,
	}
}
