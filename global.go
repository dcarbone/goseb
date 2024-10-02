package seb

import (
	"context"
)

var (
	globalBus *Bus[any]
)

func init() {
	globalBus = New[any]()
}

// AttachFunc attaches an event recipient func to the global bus
func AttachFunc(id string, fn EventFunc[any], topicFilters ...any) (string, bool, error) {
	return globalBus.AttachFunc(id, fn, topicFilters...)
}

// AttachChannel attaches an event recipient channel to the global bus
func AttachChannel(id string, ch EventChannel[any], topicFilters ...any) (string, bool, error) {
	return globalBus.AttachChannel(id, ch, topicFilters...)
}

// Push pushes an event onto the global bus
func Push[T any](ctx context.Context, topic string, data T) {
	globalBus.Push(ctx, topic, data)
}

// PushTo attempts to push an event to a particular recipient
func PushTo[T any](ctx context.Context, to, topic string, data T) error {
	return globalBus.PushTo(ctx, to, topic, data)
}
