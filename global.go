package seb

import (
	"context"
)

var (
	anyBus *Bus[any]
)

func init() {
	anyBus = New[any]()
}

// AttachFunc attaches an event recipient func to the global bus
func AttachFunc(id string, fn EventFunc[any], topicFilters ...any) (string, bool, error) {
	return anyBus.AttachFunc(id, fn, topicFilters...)
}

// AttachChannel attaches an event recipient channel to the global bus
func AttachChannel(id string, ch EventChannel[any], topicFilters ...any) (string, bool, error) {
	return anyBus.AttachChannel(id, ch, topicFilters...)
}

// Push pushes an event onto the global bus
func Push[T any](ctx context.Context, topic string, data T) {
	anyBus.Push(ctx, topic, data)
}

// PushTo attempts to push an event to a particular recipient
func PushTo[T any](ctx context.Context, to, topic string, data T) error {
	return anyBus.PushTo(ctx, to, topic, data)
}
