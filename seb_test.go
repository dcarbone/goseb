package seb_test

import (
	"context"
	"testing"
	"time"

	"github.com/dcarbone/seb/v3"
)

func TestBus(t *testing.T) {
	var (
		receivedEvent seb.Event

		done = make(chan struct{})
	)

	bus := seb.New()
	_, err := bus.AttachFunc(func(ev seb.Event) {
		receivedEvent = ev
		close(done)
	})
	if err != nil {

	}

	err = bus.Push(context.Background(), "test-topic", true)
	if err != nil {
		t.Logf("Error while pushing event: %v", err)
		t.Fail()
		return
	}

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Log("No event received, assume stuck")
		t.Fail()
	}

	if t.Failed() {
		return
	}

	if receivedEvent.Topic != "test-topic" {
		t.Logf("Expected received event to have topic \"test-topic\", saw %q", receivedEvent.Topic)
		t.Fail()
	}
	if b, _ := receivedEvent.Data.(bool); b != true {
		t.Logf("Expected received event to have data=%[1]T(%[1]v), saw %[2]v(%[2]T)", true, receivedEvent.Data)
		t.Fail()
	}
}
