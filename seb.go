package seb

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DefaultRecipientBufferSize = 100
	DefaultBlockedEventTTL     = 5 * time.Second
)

var (
	recipientIDSource atomic.Uint64

	globalBusMu sync.Mutex
	globalBus   *Bus
)

// GlobalBus returns a bus that is global to this package, allowing its use as a static singleton.
// If you need event isolation or different configuration, you will need to construct a new Bus.
func GlobalBus() *Bus {
	globalBusMu.Lock()
	defer globalBusMu.Unlock()
	if globalBus == nil {
		globalBus = New()
	}
	return globalBus
}

// AttachFunc attaches an event recipient func to the global bus
func AttachFunc(id string, fn EventFunc, topicFilters ...any) (string, bool, error) {
	return GlobalBus().AttachFunc(id, fn, topicFilters...)
}

// AttachChannel attaches an event recipient channel to the global bus
func AttachChannel(id string, ch EventChannel, topicFilters ...any) (string, bool, error) {
	return GlobalBus().AttachChannel(id, ch, topicFilters...)
}

// Push pushes an event onto the global bus
func Push(ctx context.Context, topic string, data any) {
	GlobalBus().Push(ctx, topic, data)
}

//
//// PushTo attempts to push an event to a particular recipient
//func PushTo(ctx context.Context, to, topic string, data any) error {
//	return GlobalBus().PushTo(ctx, to, topic, data)
//}

var (
	ErrEventFuncNil           = errors.New("event func is nil")
	ErrEventChanNil           = errors.New("event chan is nil")
	ErrInvalidTopicFilterType = errors.New("invalid topic filter type")

	ErrRecipientNotFound = errors.New("target recipient not found")

	ErrRecipientClosed     = errors.New("recipient is closed")
	ErrRecipientBufferFull = errors.New("recipient event buffer is full")
)

type Reply struct {
	// RecipientID is the ID of the recipient that provided this response
	RecipientID string

	// Data is arbitrary data provided by the recipient
	Data any

	// Err is arbitrary error provided by the recipient
	Err error
}

type ReplyFunc func(data any, err error)

// Event describes a specific event with associated data that gets pushed to any registered recipients at the
// time of push
type Event struct {
	// Ctx is the context that was provided at time of event push
	Ctx context.Context

	// Originated is the time at which this event was created
	Originated time.Time

	// Topic is the topic this event was pushed to
	Topic string

	// Data is arbitrary data accompanying this event
	Data any
}

func newEvent(ctx context.Context, topic string, data any) *Event {
	n := Event{
		Ctx:        ctx,
		Originated: time.Now(),
		Topic:      topic,
		Data:       data,
	}
	return &n
}

// EventFunc can be provided to a Bus to be called per Event
type EventFunc func(*Event)

// EventChannel can be provided to a Bus to have new Events pushed to it
type EventChannel chan *Event

type recipientConfig struct {
	buffSize int
}

type recipient struct {
	id string
	fn EventFunc
	wg sync.WaitGroup
	in chan *Event
}

func newRecipient(id string, fn EventFunc, cfg recipientConfig) *recipient {
	w := &recipient{
		id: id,
		fn: fn,
		in: make(chan *Event, cfg.buffSize),
	}
	go w.process()
	return w
}

func (r *recipient) process() {
	for ev := range r.in {
		r.fn(ev)
	}
}

func (r *recipient) push(ev *Event) {
	defer r.wg.Done()
	select {
	case r.in <- ev:
	default:
	}
}

type BusConfig struct {
	// BlockedEventTTL denotes the maximum amount of time a given recipient will be allowed to block before
	// an event before the event is dropped for that recipient.  This only impacts the event for the blocking recipient
	BlockedEventTTL time.Duration

	// RecipientBufferSize will be the size of the input buffer created for your recipient.
	RecipientBufferSize int
}

type BusOpt func(*BusConfig)

func WithBlockedEventTTL(d time.Duration) BusOpt {
	return func(cfg *BusConfig) {
		cfg.BlockedEventTTL = d
	}
}

func WithRecipientBufferSize(n int) BusOpt {
	return func(cfg *BusConfig) {
		cfg.RecipientBufferSize = n
	}
}

type Bus struct {
	mu sync.RWMutex

	rcfg recipientConfig

	// recipients is a map of recipient_id => recipient
	recipients map[string]*recipient
}

// New creates a new Bus for immediate use
func New(opts ...BusOpt) *Bus {
	cfg := BusConfig{
		BlockedEventTTL:     DefaultBlockedEventTTL,
		RecipientBufferSize: DefaultRecipientBufferSize,
	}

	for _, fn := range opts {
		fn(&cfg)
	}

	b := Bus{
		rcfg: recipientConfig{
			buffSize: cfg.RecipientBufferSize,
		},
		recipients: make(map[string]*recipient),
	}

	return &b
}

// Push will immediately send a new event to all currently registered recipients, blocking until completed.
func (b *Bus) Push(ctx context.Context, topic string, data any) {
	ev := newEvent(ctx, topic, data)

	b.mu.RLock()
	defer b.mu.RUnlock()

	// fire push event to each recipient in a separate routine.
	for id := range b.recipients {
		b.recipients[id].wg.Add(1)
		b.recipients[id].push(ev)
	}
}

// PushAsync pushes an event to all recipients without blocking the caller.  You amy optionally provide errc if you
// wish know about any / all errors that occurred during the push.  Otherwise, set errc to nil.
func (b *Bus) PushAsync(ctx context.Context, topic string, data any) {
	ev := newEvent(ctx, topic, data)

	b.mu.RLock()
	defer b.mu.RUnlock()

	for id := range b.recipients {
		b.recipients[id].wg.Add(1)
		go b.recipients[id].push(ev)
	}
}

//// PushTo attempts to push an even to a specific recipient, blocking until completed.
//func (b *Bus) PushTo(ctx context.Context, to, topic string, data any) error {
//	var (
//		errc <-chan error
//		errs []error
//		err  error
//	)
//
//	if to == "" {
//		errc = b.pushBlock(ev)
//	} else {
//		errc = b.doPushTo(to, ev)
//	}
//
//	for err = range errc {
//		if err != nil {
//			errs = append(errs, err)
//		}
//	}
//
//	return errors.Join(errs...)
//}
//
//// PushToAsync attempts to push an event to a specific recipient without blocking the caller.
//func (b *Bus) PushToAsync(ctx context.Context, to, topic string, data any, errc chan<- error) {
//	ev := newEvent(ctx, b.rand.Int63(), topic, data)
//	if errc == nil {
//		go func() {
//			for range b.doPushTo(to, ev) {
//			}
//		}()
//	} else {
//		go func() {
//			for err := range b.doPushTo(to, ev) {
//				errc <- err
//			}
//		}()
//	}
//}

// AttachFunc immediately adds the provided fn to the list of recipients for new events.
//
// You may optionally provide a list of filters to only allow specific messages to be received by this func.  A filter
// may be a string of the exact topic you wish to receive events from, or a *regexp.Regexp instance to use when
// matching.
func (b *Bus) AttachFunc(id string, fn EventFunc, topicFilters ...any) (string, bool, error) {
	var (
		replaced bool
		err      error
	)

	// prevent nil handlers
	if fn == nil {
		return "", false, ErrEventFuncNil
	}

	// if filters are provided, handle before acquiring lock
	if len(topicFilters) > 0 {
		fn, err = buildFilteredFunc(topicFilters, fn)
		if err != nil {
			return "", false, err
		}
	}

	// if no specific id is provided, create one
	if id == "" {
		id = strconv.FormatUint(recipientIDSource.Add(1), 10)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// if handler with ID is already registered, close it and make note
	if _, replaced = b.recipients[id]; replaced {
		b.closeRecipient(id)
	}

	// create new recipient
	b.recipients[id] = newRecipient(id, fn, b.rcfg)

	return id, replaced, nil
}

// AttachChannel immediately adds the provided channel to the list of recipients for new
// events.
func (b *Bus) AttachChannel(id string, ch EventChannel, topicFilters ...any) (string, bool, error) {
	if ch == nil {
		return "", false, ErrEventChanNil
	}
	return b.AttachFunc(id, func(ev *Event) { ch <- ev }, topicFilters...)
}

// Detach immediately removes the provided recipient from receiving any new events, returning true if a
// recipient was found with the provided id
func (b *Bus) Detach(id string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.recipients[id]; ok {
		b.closeRecipient(id)
		return ok
	}

	return false
}

// DetachAll immediately clears all attached recipients, returning the count of those previously
// attached.
func (b *Bus) DetachAll() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	// count how many are in there right now
	cnt := len(b.recipients)

	// close all current in separate goroutine
	for id := range b.recipients {
		b.closeRecipient(id)
	}

	b.recipients = make(map[string]*recipient)

	return cnt
}

func (b *Bus) closeRecipient(id string) {
	go func(r *recipient) {
		r.wg.Wait()
		close(r.in)
	}(b.recipients[id])
	delete(b.recipients, id)
}

func buildFilteredFunc(topicFilters []any, fn EventFunc) (EventFunc, error) {
	var (
		st []string
		rt []*regexp.Regexp
	)
	for i := range topicFilters {
		if s, ok := topicFilters[i].(string); ok {
			st = append(st, s)
		} else if r, ok := topicFilters[i].(*regexp.Regexp); ok {
			rt = append(rt, r)
		} else {
			return nil, fmt.Errorf("%w: expected %T or %T, saw %T", ErrInvalidTopicFilterType, "", (*regexp.Regexp)(nil), topicFilters[i])
		}
	}

	return func(ev *Event) {
		for i := range st {
			if st[i] == ev.Topic {
				fn(ev)
				return
			}
		}
		for i := range rt {
			if rt[i].MatchString(ev.Topic) {
				fn(ev)
				return
			}
		}
	}, nil
}
