package bitcask

import (
	"sync"
	"time"
)

type adaptiveSyncLoop struct {
	db       *DB
	maxDelay time.Duration
	wake     chan struct{}
	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once

	mu      sync.Mutex
	lastErr error
}

func newAdaptiveSyncLoop(db *DB, maxDelay time.Duration) *adaptiveSyncLoop {
	if maxDelay <= 0 {
		maxDelay = time.Second
	}
	loop := &adaptiveSyncLoop{
		db:       db,
		maxDelay: maxDelay,
		wake:     make(chan struct{}, 1),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	go loop.run()
	return loop
}

func (l *adaptiveSyncLoop) notify() {
	select {
	case l.wake <- struct{}{}:
	default:
	}
}

func (l *adaptiveSyncLoop) run() {
	defer close(l.done)
	var timer *time.Timer
	var timerC <-chan time.Time
	for {
		select {
		case <-l.wake:
			if timer == nil {
				timer = time.NewTimer(l.maxDelay)
				timerC = timer.C
			}
		case <-timerC:
			l.flush()
			timer = nil
			timerC = nil
		case <-l.stop:
			if timer != nil {
				timer.Stop()
			}
			l.flush()
			return
		}
	}
}

func (l *adaptiveSyncLoop) flush() {
	l.db.mu.Lock()
	if l.db.dirtyCommits == 0 {
		l.db.mu.Unlock()
		return
	}
	err := l.db.syncDurabilityResources()
	l.db.mu.Unlock()
	if err != nil {
		l.mu.Lock()
		l.lastErr = err
		l.mu.Unlock()
	}
}

func (l *adaptiveSyncLoop) close() error {
	l.stopOnce.Do(func() { close(l.stop) })
	<-l.done
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastErr
}
