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

var recipientIDSource atomic.Uint64

var (
	ErrEventFuncNil           = errors.New("event func is nil")
	ErrEventChanNil           = errors.New("event chan is nil")
	ErrInvalidTopicFilterType = errors.New("invalid topic filter type")

	ErrRecipientNotFound = errors.New("target recipient not found")
)

// Event describes a specific event with associated data that gets pushed to any registered recipients at the
// time of push
type Event[T any] struct {
	// Ctx is the context that was provided at time of event push
	Ctx context.Context

	// Originated is the time at which this event was created
	Originated time.Time

	// Topic is the topic this event was pushed to
	Topic string

	// Data is arbitrary data accompanying this event
	Data T
}

func newEvent[T any](ctx context.Context, topic string, data T) *Event[T] {
	n := Event[T]{
		Ctx:        ctx,
		Originated: time.Now(),
		Topic:      topic,
		Data:       data,
	}
	return &n
}

// EventFunc can be provided to a Bus to be called per Event
type EventFunc[T any] func(*Event[T])

// EventChannel can be provided to a Bus to have new Events pushed to it
type EventChannel[T any] chan *Event[T]

type recipientConfig struct {
	buffSize int
}

type recipient[T any] struct {
	id string
	fn EventFunc[T]
	wg sync.WaitGroup
	in chan *Event[T]
}

func newRecipient[T any](id string, fn EventFunc[T], cfg recipientConfig) *recipient[T] {
	w := &recipient[T]{
		id: id,
		fn: fn,
		in: make(chan *Event[T], cfg.buffSize),
	}
	go w.process()
	return w
}

func (r *recipient[T]) process() {
	for ev := range r.in {
		r.fn(ev)
	}
}

func (r *recipient[T]) push(ev *Event[T]) {
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

type Bus[T any] struct {
	mu sync.RWMutex

	rcfg recipientConfig

	// recipients is a map of recipient_id => recipient
	recipients map[string]*recipient[T]
}

// New creates a new Bus for immediate use
func New[T any](opts ...BusOpt) *Bus[T] {
	cfg := BusConfig{
		BlockedEventTTL:     DefaultBlockedEventTTL,
		RecipientBufferSize: DefaultRecipientBufferSize,
	}

	for _, fn := range opts {
		fn(&cfg)
	}

	b := Bus[T]{
		rcfg: recipientConfig{
			buffSize: cfg.RecipientBufferSize,
		},
		recipients: make(map[string]*recipient[T]),
	}

	return &b
}

// Push will immediately send a new event to all currently registered recipients, blocking until completed.
func (b *Bus[T]) Push(ctx context.Context, topic string, data T) {
	ev := newEvent(ctx, topic, data)

	b.mu.RLock()
	defer b.mu.RUnlock()

	// fire push event to each recipient in a separate routine.
	for id := range b.recipients {
		b.recipients[id].wg.Add(1)
		b.recipients[id].push(ev)
	}
}

// PushTo attempts to push an even to a specific recipient, blocking until completed.
func (b *Bus[T]) PushTo(ctx context.Context, to, topic string, data T) error {
	const errf = "%w: %s"

	b.mu.RLock()
	defer b.mu.RUnlock()

	r, ok := b.recipients[to]
	if !ok {
		return fmt.Errorf(errf, ErrRecipientNotFound, to)
	}

	r.wg.Add(1)
	go r.push(newEvent(ctx, topic, data))

	return nil
}

// AttachFunc immediately adds the provided fn to the list of recipients for new events.
//
// You may optionally provide a list of filters to only allow specific messages to be received by this func.  A filter
// may be a string of the exact topic you wish to receive events from, or a *regexp.Regexp instance to use when
// matching.
func (b *Bus[T]) AttachFunc(id string, fn EventFunc[T], topicFilters ...any) (string, bool, error) {
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
func (b *Bus[T]) AttachChannel(id string, ch EventChannel[T], topicFilters ...any) (string, bool, error) {
	if ch == nil {
		return "", false, ErrEventChanNil
	}
	return b.AttachFunc(id, func(ev *Event[T]) { ch <- ev }, topicFilters...)
}

// Detach immediately removes the provided recipient from receiving any new events, returning true if a
// recipient was found with the provided id
func (b *Bus[T]) Detach(id string) bool {
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
func (b *Bus[T]) DetachAll() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	// count how many are in there right now
	cnt := len(b.recipients)

	// close all current in separate goroutine
	for id := range b.recipients {
		b.closeRecipient(id)
	}

	b.recipients = make(map[string]*recipient[T])

	return cnt
}

func (b *Bus[T]) closeRecipient(id string) {
	go func(r *recipient[T]) {
		r.wg.Wait()
		close(r.in)
	}(b.recipients[id])
	delete(b.recipients, id)
}

func buildFilteredFunc[T any](topicFilters []any, fn EventFunc[T]) (EventFunc[T], error) {
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

	return func(ev *Event[T]) {
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
