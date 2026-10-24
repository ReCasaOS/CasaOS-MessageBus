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

type ActionServiceWS struct {
	typeService *ActionTypeService

	// mutex guards every field below: Start runs in a goroutine of its own,
	// Trigger and Subscribe come from the routes
	mutex sync.Mutex
	ctx   *context.Context
	stop  chan struct{}

	inboundChannel     chan model.Action
	subscriberChannels map[string]map[string][]chan model.Action

	// ready: closed once Start has installed its state; Trigger delivers from then on
	ready     chan struct{}
	readyOnce sync.Once
}

// Ready is closed once Start runs: actions triggered from then on are delivered.
func (s *ActionServiceWS) Ready() <-chan struct{} {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.readyLocked()
}

func (s *ActionServiceWS) readyLocked() chan struct{} {
	if s.ready == nil {
		s.ready = make(chan struct{})
	}
	return s.ready
}

func (s *ActionServiceWS) Trigger(action model.Action) {
	// Start runs in a goroutine, sets both under the lock; before that: drop (no nil-ctx panic)
	inboundChannel, ctx := func() (chan model.Action, *context.Context) {
		s.mutex.Lock()
		defer s.mutex.Unlock()
		return s.inboundChannel, s.ctx
	}()
	if inboundChannel == nil || ctx == nil {
		logger.Warn("action triggered before the bus started: dropped", zap.String("sourceID", action.SourceID), zap.String("name", action.Name))
		return
	}

	if action.Timestamp == 0 {
		action.Timestamp = time.Now().Unix()
	}

	// TODO - ensure properties are valid for action type

	// Waits for the dispatch to take it, never long (it waits for no
	// subscriber), as EventServiceWS.Publish does: the route triggers from a
	// goroutine of its own.
	select {
	case inboundChannel <- action:

	case <-(*ctx).Done():
		if err := (*ctx).Err(); err != nil {
			logger.Info(err.Error())
		}
		return
	}
}

func (s *ActionServiceWS) Subscribe(sourceID string, names []string) (chan model.Action, error) {
	if len(names) == 0 {
		actionTypes, err := s.typeService.GetActionTypesBySourceID(sourceID)
		if err != nil {
			return nil, err
		}

		for _, actionType := range actionTypes {
			names = append(names, actionType.Name)
		}
	}

	for _, name := range names {
		actionType, err := s.typeService.GetActionType(sourceID, name)
		if err != nil {
			return nil, err
		}

		if actionType == nil {
			return nil, ErrActionNameNotFound
		}
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.subscriberChannels == nil {
		s.subscriberChannels = make(map[string]map[string][]chan model.Action)
	}

	if s.subscriberChannels[sourceID] == nil {
		s.subscriberChannels[sourceID] = make(map[string][]chan model.Action)
	}

	c := make(chan model.Action, subscriberBuffer)

	for _, name := range names {
		if s.subscriberChannels[sourceID][name] == nil {
			s.subscriberChannels[sourceID][name] = make([]chan model.Action, 0)
		}
		s.subscriberChannels[sourceID][name] = append(s.subscriberChannels[sourceID][name], c)
	}

	return c, nil
}

func (s *ActionServiceWS) Unsubscribe(sourceID string, name string, c chan model.Action) error {
	// once, for the whole call: taken per subscriber looked at, its unlock
	// deferred, it deadlocked on the second (see EventServiceWS.Unsubscribe)
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.subscriberChannels == nil {
		return ErrSubscriberChannelsNotFound
	}

	if s.subscriberChannels[sourceID] == nil {
		return ErrActionSourceIDNotFound
	}

	if s.subscriberChannels[sourceID][name] == nil {
		return ErrActionNameNotFound
	}

	for i, subscriber := range s.subscriberChannels[sourceID][name] {
		if subscriber == c {
			s.subscriberChannels[sourceID][name] = append(s.subscriberChannels[sourceID][name][:i], s.subscriberChannels[sourceID][name][i+1:]...)
			return nil
		}
	}

	return nil
}

func (s *ActionServiceWS) Start(ctx *context.Context) {
	inboundChannel := func() chan model.Action {
		s.mutex.Lock()
		defer s.mutex.Unlock()

		s.ctx = ctx

		s.inboundChannel = make(chan model.Action)
		// keep subscriptions made before Start (see EventServiceWS.Start)
		if s.subscriberChannels == nil {
			s.subscriberChannels = make(map[string]map[string][]chan model.Action)
		}
		s.stop = make(chan struct{})
		ready := s.readyLocked()
		s.readyOnce.Do(func() { close(ready) })
		return s.inboundChannel
	}()

	defer func() {
		s.mutex.Lock()
		defer s.mutex.Unlock()

		if s.subscriberChannels != nil {
			for sourceID, source := range s.subscriberChannels {
				for actionName, subscribers := range source {
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
					delete(s.subscriberChannels[sourceID], actionName)
				}
				delete(s.subscriberChannels, sourceID)
			}
			s.subscriberChannels = nil
		}

		// not closed: a Publish/Trigger may hold it; senders select on ctx.Done, which ended the loop
		s.inboundChannel = nil

		close(s.stop)
		s.stop = nil
	}()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {

		case <-(*ctx).Done():
			return

		case action, ok := <-inboundChannel:
			if !ok {
				return
			}

			// under the lock Subscribe and Unsubscribe change the lists with (see
			// EventServiceWS.Start)
			func() {
				s.mutex.Lock()
				defer s.mutex.Unlock()

				if s.subscriberChannels == nil || s.subscriberChannels[action.SourceID] == nil {
					return
				}

				for _, c := range s.subscriberChannels[action.SourceID][action.Name] {
					select {
					case c <- action:
					default: // drop action if no one is listening
					}
				}
			}()

		case <-ticker.C:
			heartbeat := model.Action{
				SourceID:  common.MessageBusSourceID,
				Name:      common.MessageBusHeartbeatName,
				Timestamp: time.Now().Unix(),
			}

			func() {
				s.mutex.Lock()
				defer s.mutex.Unlock()

				for _, source := range s.subscriberChannels {
					for _, subscribers := range source {
						for _, subscriber := range subscribers {
							select {
							case subscriber <- heartbeat:
							default: // drop event if no one is listening
							}
						}
					}
				}
			}()
		}
	}
}

func NewActionServiceWS(actionTypeService *ActionTypeService) *ActionServiceWS {
	return &ActionServiceWS{
		typeService: actionTypeService,
	}
}
