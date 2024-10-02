package seb_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dcarbone/seb/v4"
)

func TestBus(t *testing.T) {
	const (
		testTopic = "test-topic"
	)
	var (
		receivedEvent *seb.Event

		done = make(chan struct{})
	)

	bus := seb.New()
	_, _, err := bus.AttachFunc("", func(ev *seb.Event) {
		receivedEvent = ev
		close(done)
	})
	if err != nil {
		t.Errorf("Error pushing message: %v", err)
		return
	}

	bus.Push(context.Background(), testTopic, true)

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Error("No event received, assume stuck")
		return
	}

	if receivedEvent.Topic != "test-topic" {
		t.Errorf("Expected received event to have topic %q, saw %q", testTopic, receivedEvent.Topic)
	}
	if b, _ := receivedEvent.Data.(bool); b != true {
		t.Errorf("Expected received event to have data=%[1]T(%[1]v), saw %[2]v(%[2]T)", true, receivedEvent.Data)
	}
}

func BenchmarkBus_Blocking(b *testing.B) {
	const testTopic = "test-topic"

	var (
		data = struct{}{}
	)

	for i := 10; i <= 10000; i = i * 10 {
		bus := seb.New()

		for n := 0; n < i; n++ {
			_, _, err := bus.AttachFunc("", func(ev *seb.Event) {})
			if err != nil {
				b.Errorf("Error adding recipient %d: %v", n, err)
				return
			}
		}

		b.Run(fmt.Sprintf("%d-recipients", i), func(b *testing.B) {
			for m := 0; m < b.N; m++ {
				bus.Push(context.Background(), testTopic, data)
			}
		})

		bus.DetachAll()
	}
}
