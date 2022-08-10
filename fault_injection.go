package bitcask

import (
	"bitcask/rwmanager"
	"errors"
	"sync/atomic"
)

var (
	ErrFaultInjectedWrite      = errors.New("fault injected write error")
	ErrFaultInjectedSync       = errors.New("fault injected sync error")
	ErrFaultInjectedShortWrite = errors.New("fault injected short write")
	ErrFaultInjectedRead       = errors.New("fault injected read corruption")
)

type faultInjectionState struct {
	writes int64
	syncs  int64
	reads  int64
}

type faultRWManager struct {
	inner rwmanager.RWManager
	opt   FaultInjectionOptions
	state *faultInjectionState
}

func newFaultRWManager(inner rwmanager.RWManager, opt FaultInjectionOptions, state *faultInjectionState) rwmanager.RWManager {
	if !opt.Enable {
		return inner
	}
	if state == nil {
		state = &faultInjectionState{}
	}
	return &faultRWManager{inner: inner, opt: opt, state: state}
}

func (fm *faultRWManager) WriteAt(b []byte, off int64) (int, error) {
	writeID := atomic.AddInt64(&fm.state.writes, 1)
	if failAfter(fm.opt.WriteFailAfter, writeID) {
		return 0, ErrFaultInjectedWrite
	}
	if failAfter(fm.opt.ShortWriteAfter, writeID) {
		if len(b) == 0 {
			return 0, ErrFaultInjectedShortWrite
		}
		n, err := fm.inner.WriteAt(b[:len(b)/2], off)
		if err != nil {
			return n, err
		}
		return n, ErrFaultInjectedShortWrite
	}
	if fm.opt.CorruptAfterWrite {
		buf := append([]byte(nil), b...)
		if len(buf) > 0 {
			buf[len(buf)-1] ^= 0xff
		}
		return fm.inner.WriteAt(buf, off)
	}
	return fm.inner.WriteAt(b, off)
}

func (fm *faultRWManager) ReadAt(b []byte, off int64) (int, error) {
	n, err := fm.inner.ReadAt(b, off)
	readID := atomic.AddInt64(&fm.state.reads, 1)
	if err == nil && failAfter(fm.opt.ReadCorruptAfter, readID) {
		if n > 0 {
			b[n-1] ^= 0xff
		}
		return n, ErrFaultInjectedRead
	}
	return n, err
}

func (fm *faultRWManager) Sync() error {
	syncID := atomic.AddInt64(&fm.state.syncs, 1)
	if failAfter(fm.opt.SyncFailAfter, syncID) {
		return ErrFaultInjectedSync
	}
	return fm.inner.Sync()
}

func (fm *faultRWManager) Close() error {
	return fm.inner.Close()
}

func failAfter(limit, count int64) bool {
	return limit >= 0 && count > limit
}
