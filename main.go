package main

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type waiter struct {
	ch chan string
}

type queue struct {
	messages []string
	waiters  []*waiter
}

type broker struct {
	mu     sync.Mutex
	queues map[string]*queue
}

// создает брокер с мапой очередей
func newBroker() *broker {
	return &broker{queues: make(map[string]*queue)}
}

// обрабатывает HTTP-запросы и распределяет их по PUT/GET
func (b *broker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	queueName := strings.TrimPrefix(r.URL.Path, "/")

	switch r.Method {
	case http.MethodPut:
		values, ok := r.URL.Query()["v"]
		if !ok {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		b.putMessage(queueName, values[0])
	case http.MethodGet:
		msg, ok, err := b.getMessage(queueName, r)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte(msg))
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// хелпер, который создает очередь, если ее еще не было, если была, то просто возвращает
func (b *broker) getOrCreateQueue(name string) *queue {
	q := b.queues[name]
	if q == nil {
		q = &queue{}
		b.queues[name] = q
	}
	return q
}

// добавляет сообщение в очередь или отдает его первому ожидающему получателю, сохраняя FIFO
func (b *broker) putMessage(name, msg string) {
	b.mu.Lock()
	queue := b.getOrCreateQueue(name)
	if len(queue.waiters) > 0 {
		waiting := queue.waiters[0]       // первый ожидающий
		queue.waiters = queue.waiters[1:] // первый ожидающий убирается из очереди
		waiting.ch <- msg
	} else {
		queue.messages = append(queue.messages, msg)
	}
	b.mu.Unlock()
}

// забирает сообщение из очереди или ждет его, если передали таймаут
func (b *broker) getMessage(name string, r *http.Request) (string, bool, error) {
	timeout, wait, err := parseTimeout(r)
	if err != nil {
		return "", false, err
	}

	b.mu.Lock()
	q := b.getOrCreateQueue(name)
	if len(q.messages) > 0 {
		msg := q.messages[0]
		q.messages = q.messages[1:]
		b.mu.Unlock()
		return msg, true, nil
	}
	if !wait {
		b.mu.Unlock()
		return "", false, nil
	}

	w := &waiter{ch: make(chan string, 1)}
	q.waiters = append(q.waiters, w)
	b.mu.Unlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case msg := <-w.ch:
		return msg, true, nil
	case <-timer.C:
		if b.removeWaiter(name, w) {
			return "", false, nil
		}
		return <-w.ch, true, nil
	case <-r.Context().Done():
		if !b.removeWaiter(name, w) {
			<-w.ch
		}
		return "", false, nil
	}
}

// хелпер, который достает таймаут из запроса и переводит его в длительность ожидания
func parseTimeout(r *http.Request) (time.Duration, bool, error) {
	values, ok := r.URL.Query()["timeout"]
	if !ok {
		return 0, false, nil
	}
	n, err := strconv.Atoi(values[0])
	if err != nil {
		return 0, false, err
	}
	if n < 0 {
		return 0, false, fmt.Errorf("timeout must be non-negative")
	}
	return time.Duration(n) * time.Second, n > 0, nil
}

// убирает ожидающий GET запрос из очереди ожидания, если он больше не ждет
func (b *broker) removeWaiter(name string, w *waiter) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	q := b.queues[name]
	if q == nil {
		return false
	}
	for i, current := range q.waiters {
		if current == w {
			q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
			return true
		}
	}
	return false
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: queue-broker <port>")
		os.Exit(1)
	}

	port := strings.TrimPrefix(os.Args[1], ":")
	if err := http.ListenAndServe(":"+port, newBroker()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
