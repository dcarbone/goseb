package seb

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"regexp"
	"strconv"
	"sync"
	"time"
)

const (
	DefaultRecipientBufferSize = 100
	DefaultBlockedEventTTL     = 5 * time.Second
)

var (
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
func Push(ctx context.Context, topic string, data any) error {
	return GlobalBus().Push(ctx, topic, data)
}

// PushTo attempts to push an event to a particular recipient
func PushTo(ctx context.Context, to, topic string, data any) error {
	return GlobalBus().PushTo(ctx, to, topic, data)
}

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

	// ID is the randomly generated ID of this event
	ID int64

	// Originated is the unix nano timestamp of when this event was created
	Originated int64

	// Topic is the topic this event was pushed to
	Topic string

	// Data is arbitrary data accompanying this event
	Data any
}

func newEvent(ctx context.Context, id int64, topic string, data any) *Event {
	n := Event{
		Ctx:        ctx,
		ID:         id,
		Originated: time.Now().UnixNano(),
		Topic:      topic,
		Data:       data,
	}
	return &n
}

// EventFunc can be provided to a Bus to be called per Event
type EventFunc func(Event)

// EventChannel can be provided to a Bus to have new Events pushed to it
type EventChannel chan Event

type lockableRandSource struct {
	mu  sync.Mutex
	src rand.Source
}

func newLockableRandSource() *lockableRandSource {
	s := new(lockableRandSource)
	s.src = rand.NewSource(time.Now().UnixNano())
	return s
}

func (s *lockableRandSource) Int63() int64 {
	s.mu.Lock()
	v := s.src.Int63()
	s.mu.Unlock()
	return v
}

type recipientConfig struct {
	buffSize int
	blockTTL time.Duration
}

type recipient struct {
	mu       sync.Mutex
	id       string
	closed   bool
	blockTTL time.Duration
	in       chan Event
	out      chan Event
	fn       EventFunc
}

func newRecipient(id string, fn EventFunc, cfg recipientConfig) *recipient {
	w := &recipient{
		id:       id,
		blockTTL: cfg.blockTTL,
		in:       make(chan Event, cfg.buffSize),
		out:      make(chan Event),
		fn:       fn,
	}
	go w.send()
	go w.process()
	return w
}

func (r *recipient) close() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return
	}

	r.closed = true

	close(r.in)
	close(r.out)
	if len(r.in) > 0 {
		for range r.in {
		}
	}
}

func (r *recipient) send() {
	var wait *time.Timer

	defer func() {
		if wait != nil && !wait.Stop() && len(wait.C) > 0 {
			<-wait.C
		}
	}()

	for ev := range r.in {
		// acquire lock to prevent closure while attempting to push a message
		r.mu.Lock()

		// test if recipient was closed between message push request and now.
		if r.closed {
			r.mu.Unlock()
			continue
		}

		// if the block ttl is > 0...
		if r.blockTTL > 0 {

			// construct or reset block ttl timer
			if wait == nil {
				wait = time.NewTimer(r.blockTTL)
			} else {
				if !wait.Stop() && len(wait.C) > 0 {
					<-wait.C
				}
				wait.Reset(r.blockTTL)
			}

			// attempt to send message.  if recipient blocks for more than configured
			select {
			case r.out <- ev:
			case <-ev.Ctx.Done():
			case <-wait.C:
			}
		} else {
			// attempt to push event to recipient
			select {
			case r.out <- ev:
			case <-ev.Ctx.Done():
			}
		}

		r.mu.Unlock()
	}
}

func (r *recipient) process() {
	// r.out is an unbuffered channel.  it blocks until any preceding event has been handled by the registered
	// handler.  it is closed once the push() loop breaks.
	for n := range r.out {
		r.fn(n)
	}
}

func (r *recipient) push(ev Event) error {
	// hold a lock for the duration of the push attempt to ensure that, at a minimum, the message is added to the
	// channel before it can be closed.
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return fmt.Errorf("%w: %q", ErrRecipientClosed, r.id)
	}

	// attempt to push message to ingest chan.  if chan is full or event context expires, drop on floor
	select {
	case <-ev.Ctx.Done():
		return fmt.Errorf("cannot push topic %q event %d to recipient %q: %w", ev.Topic, ev.ID, r.id, ev.Ctx.Err())
	case r.in <- ev:
		return nil
	default:
		return fmt.Errorf("cannot push topic %q event %d to recipient %q: %w", ev.Topic, ev.ID, r.id, ErrRecipientBufferFull)
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

	rand *lockableRandSource

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
		rand: newLockableRandSource(),
		rcfg: recipientConfig{
			blockTTL: cfg.BlockedEventTTL,
			buffSize: cfg.RecipientBufferSize,
		},
		recipients: make(map[string]*recipient),
	}

	return &b
}

// Push will immediately send a new event to all currently registered recipients, blocking until completed.
func (b *Bus) Push(ctx context.Context, topic string, data any) error {
	return b.PushTo(ctx, "", topic, data)
}

// PushAsync pushes an event to all recipients without blocking the caller.  You amy optionally provide errc if you
// wish know about any / all errors that occurred during the push.  Otherwise, set errc to nil.
func (b *Bus) PushAsync(ctx context.Context, topic string, data any, errc chan<- error) {
	ev := newEvent(ctx, b.rand.Int63(), topic, data)
	if errc == nil {
		go func() {
			for range b.doPush(ev) {
			}
		}()
	} else {
		go func() {
			for err := range b.doPush(ev) {
				errc <- err
			}
		}()
	}
}

// PushTo attempts to push an even to a specific recipient, blocking until completed.
func (b *Bus) PushTo(ctx context.Context, to, topic string, data any) error {
	var (
		errc <-chan error
		errs []error
		err  error

		ev = newEvent(ctx, b.rand.Int63(), topic, data)
	)

	if to == "" {
		errc = b.doPush(ev)
	} else {
		errc = b.doPushTo(to, ev)
	}

	for err = range errc {
		if err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

// PushToAsync attempts to push an event to a specific recipient without blocking the caller.
func (b *Bus) PushToAsync(ctx context.Context, to, topic string, data any, errc chan<- error) {
	ev := newEvent(ctx, b.rand.Int63(), topic, data)
	if errc == nil {
		go func() {
			for range b.doPushTo(to, ev) {
			}
		}()
	} else {
		go func() {
			for err := range b.doPushTo(to, ev) {
				errc <- err
			}
		}()
	}
}

// AttachFunc immediately adds the provided fn to the list of recipients for new events.
//
// You may optionally provide a list of filters to only allow specific messages to be received by this func.  A filter
// may be a string of the exact topic you wish to receive events from, or a *regexp.Regexp instance to use when
// matching.
func (b *Bus) AttachFunc(id string, fn EventFunc, topicFilters ...any) (string, bool, error) {
	var (
		r        *recipient
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
		id = strconv.FormatInt(b.rand.Int63(), 10)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// if handler with ID is already registered, close it and make note
	if r, replaced = b.recipients[id]; replaced {
		r.close()
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
	return b.AttachFunc(id, func(event Event) { ch <- event }, topicFilters...)
}

// Detach immediately removes the provided recipient from receiving any new events, returning true if a
// recipient was found with the provided id
func (b *Bus) Detach(id string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if w, ok := b.recipients[id]; ok {
		go w.close()
		delete(b.recipients, id)
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
	for _, w := range b.recipients {
		go w.close()
	}

	b.recipients = make(map[string]*recipient)

	return cnt
}

// doPush immediately calls each handler with the new event
func (b *Bus) doPush(ev *Event) <-chan error {
	var (
		rl    int
		wg    sync.WaitGroup
		errCh chan error
	)

	b.mu.RLock()

	// set up wait group and error collecting chan
	rl = len(b.recipients)
	wg.Add(rl)
	errCh = make(chan error, rl)

	// fire push event to each recipient in a separate routine.
	for r := range b.recipients {
		go func(w *recipient) {
			defer wg.Done()
			errCh <- w.push(*ev)
		}(b.recipients[r])
	}

	b.mu.RUnlock()

	// wait around for all responses to be collected
	go func() {
		wg.Wait()
		close(errCh)
	}()

	return errCh
}

func (b *Bus) doPushTo(to string, ev *Event) <-chan error {
	errc := make(chan error, 1)

	b.mu.RLock()

	if r, ok := b.recipients[to]; ok {
		b.mu.RUnlock()
		go func() {
			defer close(errc)
			errc <- r.push(*ev)
		}()
	} else {
		b.mu.RUnlock()
		errc <- fmt.Errorf("%w: %s", ErrRecipientNotFound, to)
		close(errc)
	}

	return errc
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

	return func(event Event) {
		for i := range st {
			if st[i] == event.Topic {
				fn(event)
				return
			}
		}
		for i := range rt {
			if rt[i].MatchString(event.Topic) {
				fn(event)
				return
			}
		}
	}, nil
}
