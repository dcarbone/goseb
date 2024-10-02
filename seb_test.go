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
		receivedEvent *seb.Event[bool]

		done = make(chan struct{})
	)

	bus := seb.New[bool]()
	_, _, err := bus.AttachFunc("", func(ev *seb.Event[bool]) {
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

	if receivedEvent.Topic != testTopic {
		t.Errorf("Expected received event to have topic %q, saw %q", testTopic, receivedEvent.Topic)
	}
}

func BenchmarkBus_Push_Unfiltered(b *testing.B) {
	const testTopic = "test-topic"

	var (
		data = true
	)

	for i := 10; i <= 10000; i = i * 10 {
		b.Run(fmt.Sprintf("%d-recipients", i), func(b *testing.B) {
			bus := seb.New[bool]()
			b.Cleanup(func() { bus.DetachAll() })

			for n := 0; n < i; n++ {
				_, _, err := bus.AttachFunc("", func(_ *seb.Event[bool]) {})
				if err != nil {
					b.Errorf("Error adding recipient %d: %v", n, err)
					return
				}
			}

			b.ResetTimer()
			for m := 0; m < b.N; m++ {
				bus.Push(context.Background(), testTopic, data)
			}
		})
	}
}
