package bitcask

import (
	"sync"
	"time"
)

type groupCommitRequest struct {
	sequence uint64
	result   chan error
}

// groupCommitCoordinator batches concurrent SyncPolicyGroupCommit waiters into
// shared durability epochs (size or delay capped). See docs/tracks/durability-pipeline.md.
type groupCommitCoordinator struct {
	db        *DB
	maxDelay  time.Duration
	maxWrites int

	mu              sync.Mutex
	pending         []*groupCommitRequest
	timer           *time.Timer
	syncing         bool
	closing         bool
	closed          bool
	closeErr        error
	cond            *sync.Cond
	nextSequence    uint64
	nextEpoch       uint64
	durableFrontier uint64
}

func newGroupCommitCoordinator(db *DB, policy SyncPolicyOptions) *groupCommitCoordinator {
	maxDelay := policy.GroupMaxDelay
	if maxDelay <= 0 {
		maxDelay = time.Millisecond
	}
	maxWrites := policy.GroupMaxWrites
	if maxWrites <= 0 {
		maxWrites = 16
	}
	coordinator := &groupCommitCoordinator{db: db, maxDelay: maxDelay, maxWrites: maxWrites}
	coordinator.cond = sync.NewCond(&coordinator.mu)
	return coordinator
}

func (c *groupCommitCoordinator) enqueue() (*groupCommitRequest, error) {
	c.mu.Lock()
	c.nextSequence++
	request := &groupCommitRequest{sequence: c.nextSequence, result: make(chan error, 1)}
	if c.closing || c.closed {
		err := c.closeErr
		if err == nil {
			err = ErrDBClosed
		}
		c.mu.Unlock()
		return nil, err
	}
	for len(c.pending) >= c.maxWrites && !c.closing && !c.closed {
		c.cond.Wait()
	}
	if c.closing || c.closed {
		err := c.closeErr
		if err == nil {
			err = ErrDBClosed
		}
		c.mu.Unlock()
		return nil, err
	}
	c.pending = append(c.pending, request)
	if len(c.pending) == 1 {
		c.timer = time.AfterFunc(c.maxDelay, c.flush)
	}
	flushNow := len(c.pending) == c.maxWrites
	c.mu.Unlock()
	if flushNow {
		go c.flush()
	}
	return request, nil
}

func (c *groupCommitCoordinator) wait(request *groupCommitRequest) error {
	return <-request.result
}

func (c *groupCommitCoordinator) flush() {
	c.mu.Lock()
	if c.syncing || len(c.pending) == 0 {
		c.mu.Unlock()
		return
	}
	c.syncing = true
	if c.timer != nil {
		c.timer.Stop()
		c.timer = nil
	}
	requests := c.pending
	c.pending = nil
	c.cond.Broadcast()
	c.nextEpoch++
	epochID := c.nextEpoch
	frontier := requests[len(requests)-1].sequence
	c.mu.Unlock()

	c.db.mu.RLock()
	err := c.db.syncDurabilityResources()
	c.db.mu.RUnlock()

	c.mu.Lock()
	c.syncing = false
	if err == nil {
		c.durableFrontier = frontier
		c.db.recordGroupEpoch(epochID, frontier, len(requests))
	} else if c.closeErr == nil {
		c.closeErr = err
	}
	for _, request := range requests {
		request.result <- err
		close(request.result)
	}
	if len(c.pending) > 0 {
		if c.timer != nil {
			c.timer.Stop()
		}
		c.timer = time.AfterFunc(c.maxDelay, c.flush)
	}
	c.cond.Broadcast()
	c.mu.Unlock()
}

func (c *groupCommitCoordinator) close() error {
	c.mu.Lock()
	if c.closed {
		err := c.closeErr
		c.mu.Unlock()
		return err
	}
	c.closing = true
	if c.timer != nil {
		c.timer.Stop()
		c.timer = nil
	}
	c.mu.Unlock()

	for {
		c.flush()
		c.mu.Lock()
		if !c.syncing && len(c.pending) == 0 {
			c.closed = true
			c.cond.Broadcast()
			err := c.closeErr
			c.mu.Unlock()
			return err
		}
		c.cond.Wait()
		c.mu.Unlock()
	}
}
