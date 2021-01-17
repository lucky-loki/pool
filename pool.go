package pool

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrValueClosed = errors.New("pool: value is already closed")
	ErrBadValue = errors.New("value: bad value")
    errPoolClosed = errors.New("pool: pool is closed")
)

var valueRequestQueueSize int = 1000000

const (
	maxBadRetries = 2
	defaultMaxIdleValue = 2
)

type Pool struct {
	// Atomic access only. At top of struct to prevent mis-alignment
	// on 32-bit platforms. Of type time.Duration.
	waitDuration int64 // Total time waited for new connections.

	newer newer
	// numClosed is an atomic counter which represents a total number of
	// closed connections.
	numClosed uint64

	mu sync.Mutex
	freeEntry []*entry
	requestWait chan *entry
	waitCount         int // Total number of request block for wait.
	numNew      int    // number of newed and pending open connections
	// Used to signal the need for new connections
	// a goroutine running connectionOpener() reads on this chan and
	// maybeOpenNewConnections sends on the chan (one send per needed connection)
	// It is closed during pool.Close(). The close tells the connectionOpener
	// goroutine to exit.
	newerCh           chan struct{}
	cleanerCh         chan struct{}
	closed            bool
	maxIdle           int                    // zero means defaultMaxIdleConns; negative means 0
	maxNew            int                    // <= 0 means unlimited
	maxLifetime       time.Duration          // maximum amount of time a value may be reused
	maxIdleClosed     int // Total number of connections closed due to idle.
	maxLifetimeClosed int // Total number of connections closed due to max free limit.

	stop func() // stop cancels the value newer
}

type newer interface {
	New(context.Context) (io.Closer, error)
}

// connReuseStrategy determines how (*DB).conn returns database connections.
type valueReuseStrategy uint8

const (
	// alwaysNewValue forces a new connection to the database.
	alwaysNewValue valueReuseStrategy = iota
	// cachedOrNewValue returns a cached connection, if available, else waits
	// for one to become available (if MaxOpenConns has been reached) or
	// creates a new database connection.
	cachedOrNewValue
)


type entry struct {
	pool *Pool
	createdAt time.Time

	sync.Mutex	// guards following
	value io.Closer
	closed bool
	lastErr error

	// guarded by pool.mu
	inUse bool
}

func (e *entry) expired(timeout time.Duration) bool {
	if timeout <= 0 {
		return false
	}
	return e.createdAt.Add(timeout).Before(time.Now())
}

func (e *entry) close() error {
	e.Lock()
	defer e.Unlock()

	if e.closed {
		return errors.New("pool: duplicate entry close")
	}
	e.closed = true
	err := e.value.Close()

	atomic.AddUint64(&e.pool.numClosed, 1)
	return err
}

func (e *entry) release() {
	e.pool.putEntry(e)
}

func NewPool(newer newer) *Pool  {
	ctx, cancel := context.WithCancel(context.Background())
	pool := &Pool{
		newer:    newer,
		newerCh:     make(chan struct{}, valueRequestQueueSize),
		requestWait: make(chan *entry),
		stop:         cancel,
	}

	go pool.valueNewer(ctx)

	return pool
}

// Runs in a separate goroutine, opens new connections when requested.
func (pool *Pool) valueNewer(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-pool.newerCh:
			pool.newNewValue(ctx)
		}
	}
}

// Open one new connection
func (pool *Pool) newNewValue(ctx context.Context) {
	// maybeNewValuesPoolLocked has already executed pool.numNew++ before it sent
	// on pool.newerCh. This function must execute pool.numNew-- if the
	// connection fails or is closed before returning.
	v, err := pool.newer.New(ctx)

	pool.mu.Lock()
	defer pool.mu.Unlock()
	if pool.closed {
		if err == nil {
			v.Close()
		}
		pool.numNew--
		return
	}
	if err != nil {
		pool.numNew--
		pool.maybeNewValuesPoolLocked()
		return
	}
	entry := &entry{
		pool:        pool,
		createdAt: time.Now(),
		value:        v,
	}
	if !pool.putEntryPoolLocked(entry) {
		pool.numNew--
		v.Close()
	}
}


func (pool *Pool) DoContext(ctx context.Context, f func(v interface{}) error) error {
	var err error
	for i:=0; i<maxBadRetries; i++ {
		err = pool.do(ctx, f, cachedOrNewValue)
		if err != ErrBadValue {
			break
		}
	}
	if err == ErrBadValue {
		return pool.do(ctx, f, alwaysNewValue)
	}
	return err
}

func (pool *Pool) Do(f func(v interface{}) error) error {
	return pool.DoContext(context.Background(), f)
}

func (pool *Pool) do(ctx context.Context, f func(v interface{}) error, strategy valueReuseStrategy) error {
	entry, err := pool.get(ctx, strategy)
	if err != nil {
		return err
	}
	return pool.doEntry(ctx, entry, entry.release, f)
}

func (pool *Pool) doEntry(ctx context.Context, entry *entry, release func(), f func(v interface{}) error) error {
	defer release()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	withLock(entry, func() {
		entry.lastErr = f(entry.value)
	})
	return entry.lastErr
}

func (pool *Pool) get(ctx context.Context, strategy valueReuseStrategy) (*entry, error) {
	pool.mu.Lock()

	if pool.closed {
		pool.mu.Unlock()
		return nil, errPoolClosed
	}

	// check if the context is expired
	select {
	case <-ctx.Done():
		pool.mu.Unlock()
		return nil, ctx.Err()
	default:
	}
	lifetime := pool.maxLifetime

	// Prefer a free entry, if possible
	numFree := len(pool.freeEntry)
	if strategy == cachedOrNewValue && numFree > 0 {
		entry := pool.freeEntry[0]	//oldest one
		copy(pool.freeEntry, pool.freeEntry[1:])
		pool.freeEntry = pool.freeEntry[:numFree-1]
		if entry.expired(lifetime) {
			pool.numNew--
			pool.maybeNewValuesPoolLocked()
			pool.mu.Unlock()
			entry.close()
			return nil, ErrBadValue
		}
		entry.inUse = true
		pool.mu.Unlock()
		return entry, nil
	}

	// Out of free entry or we were asked not to use one. If we're not
	// allowed to new any more connections, make a request and wait.
	if pool.maxNew > 0 && pool.numNew >= pool.maxNew {
		// Make the connRequest channel. It's buffered so that the
		// connectionOpener doesn't block while waiting for the req to be read.
		pool.waitCount++
		pool.mu.Unlock()

		waitStart := time.Now()

		select {
		case <-ctx.Done():
			atomic.AddInt64(&pool.waitDuration, int64(time.Since(waitStart)))

			select {
			default:
			case entry, ok := <-pool.requestWait:
				if ok && entry != nil {
					pool.putEntry(entry)
				}
			}
			return nil, ctx.Err()
		case entry, ok := <-pool.requestWait:
			atomic.AddInt64(&pool.waitDuration, int64(time.Since(waitStart)))

			pool.mu.Lock()
			pool.waitCount--
			if !ok {
				pool.mu.Unlock()
				return nil, errPoolClosed
			}
			if entry.expired(lifetime) {
				pool.numNew--
				pool.maybeNewValuesPoolLocked()
				pool.mu.Unlock()
				entry.close()
				return nil, ErrBadValue
			}
			pool.mu.Unlock()
			return entry, nil
		}
	}

	pool.numNew++ // optimistically
	pool.mu.Unlock()
	ci, err := pool.newer.New(ctx)
	if err != nil {
		pool.mu.Lock()
		pool.numNew-- // correct for earlier optimism
		pool.maybeNewValuesPoolLocked()
		pool.mu.Unlock()
		return nil, err
	}
	entry := &entry{
		pool:        pool,
		createdAt: time.Now(),
		value:        ci,
		inUse:     true,
	}
	return entry, nil
}

func (pool *Pool) putEntry(entry *entry) {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	if !entry.inUse {
		panic("pool: entry returned that was never out")
	}

	added := pool.putEntryPoolLocked(entry)
	if !added {
		entry.close()
		pool.numNew--
		pool.maybeNewValuesPoolLocked()
	}
}

func (pool *Pool) putEntryPoolLocked(entry *entry) bool {
	if pool.closed || entry.lastErr == ErrBadValue {
		return false
	}

	if pool.maxNew > 0 && pool.numNew > pool.maxNew {
		return false
	}

	if pool.waitCount > 0 {
		select {
		case pool.requestWait <- entry:
			return true
		default:
		}
	}

	if pool.maxIdleEntryPoolLocked() > len(pool.freeEntry) {
		pool.freeEntry = append(pool.freeEntry, entry)
		pool.startCleanerLocked()
		return true
	}
	pool.maxIdleClosed++
	return false
}

func (pool *Pool) maxIdleEntryPoolLocked() int {
	n := pool.maxIdle
	switch {
	case n == 0:
		return defaultMaxIdleValue
	case n < 0:
		return 0
	default:
		return n
	}
}

// startCleanerLocked starts connectionCleaner if needed.
func (pool *Pool) startCleanerLocked() {
	if pool.maxLifetime > 0 && pool.numNew > 0 && pool.cleanerCh == nil {
		pool.cleanerCh = make(chan struct{}, 1)
		go pool.valueCleaner(pool.maxLifetime)
	}
}

func (pool *Pool) valueCleaner(d time.Duration) {
	const minInterval = time.Second

	if d < minInterval {
		d = minInterval
	}
	t := time.NewTimer(d)

	for {
		select {
		case <-t.C:
		case <-pool.cleanerCh: // maxLifetime was changed or pool was closed.
		}

		pool.mu.Lock()
		d = pool.maxLifetime
		if pool.closed || pool.numNew == 0 || d <= 0 {
			pool.cleanerCh = nil
			pool.mu.Unlock()
			return
		}

		expiredSince := time.Now().Add(-d)
		var closing []*entry
		for i := 0; i < len(pool.freeEntry); i++ {
			c := pool.freeEntry[i]
			if c.createdAt.Before(expiredSince) {
				closing = append(closing, c)
				last := len(pool.freeEntry) - 1
				pool.freeEntry[i] = pool.freeEntry[last]
				pool.freeEntry[last] = nil
				pool.freeEntry = pool.freeEntry[:last]
				i--
			}
		}
		pool.maxLifetimeClosed += len(closing)
		pool.numNew -= len(closing)
		pool.mu.Unlock()

		for _, c := range closing {
			c.close()
		}

		if d < minInterval {
			d = minInterval
		}
		t.Reset(d)
	}
}


func (pool *Pool) Get(ctx context.Context) (*Value, error) {
	var entry *entry
	var err error
	for i := 0; i < maxBadRetries; i++ {
		entry, err = pool.get(ctx, cachedOrNewValue)
		if err != ErrBadValue {
			break
		}
	}
	if err == ErrBadValue {
		entry, err = pool.get(ctx, alwaysNewValue)
	}
	if err != nil {
		return nil, err
	}

	value := &Value{
		pool: pool,
		entry: entry,
	}
	return value, nil
}

func (pool *Pool) maybeNewValuesPoolLocked() {
	if pool.closed {
		return
	}

	numRequests := pool.waitCount
	if pool.maxNew > 0 {
		numCanNew := pool.maxNew - pool.numNew
		if numCanNew < numRequests {
			numRequests = numCanNew
		}
	}
	for i := 0; i < numRequests; i++ {
		pool.numNew++
		pool.newerCh <- struct{}{}
	}
}

func (pool *Pool) Close() error {
	pool.mu.Lock()
	if pool.closed { // Make Pool.Close idempotent
		pool.mu.Unlock()
		return nil
	}
	if pool.cleanerCh != nil {	// stop cleaner
		pool.cleanerCh <- struct{}{}
	}
	fns := make([]func() error, 0, len(pool.freeEntry))
	for _, entry := range pool.freeEntry {
		pool.numNew--
		fns = append(fns, entry.close)
	}
	pool.freeEntry = nil
	pool.closed = true
	close(pool.requestWait)	// release all request waited
	pool.mu.Unlock()

	var err error
	for _, fn := range fns {
		err1 := fn()
		if err1 != nil {
			err = err1
		}
	}
	pool.stop()
	return err
}

// SetMaxIdleValues sets the maximum number of values in the idle
// value pool.
//
// If MaxNewValue is greater than 0 but less than the new MaxIdleValue,
// then the new MaxIdleValue will be reduced to match the MaxNewValue limit.
//
// If n <= 0, no idle values are retained.
//
// The default max idle values is currently 2. This may change in
// a future release.
func (pool *Pool) SetMaxIdleValues(n int) {
	pool.mu.Lock()
	if n > 0 {
		pool.maxIdle = n
	} else {
		// No idle connections.
		pool.maxIdle = -1
	}
	// Make sure maxIdle doesn't exceed maxOpen
	if pool.maxNew > 0 && pool.maxIdleEntryPoolLocked() > pool.maxNew {
		pool.maxIdle = pool.maxNew
	}
	var closing []*entry
	idleCount := len(pool.freeEntry)
	maxIdle := pool.maxIdleEntryPoolLocked()
	if idleCount > maxIdle {
		closing = pool.freeEntry[maxIdle:]
		pool.freeEntry = pool.freeEntry[:maxIdle]
	}
	pool.maxIdleClosed += len(closing)
	pool.mu.Unlock()
	for _, c := range closing {
		c.close()
	}
}

// SetMaxNewValues sets the maximum number of new values to used.
//
// If MaxIdleValue is greater than 0 and the new MaxNewValue is less than
// MaxIdleValue, then maxIdle will be reduced to match the new
// MaxNewValue limit.
//
// If n <= 0, then there is no limit on the number of new value.
// The default is 0 (unlimited).
func (pool *Pool) SetMaxNewValues(n int) {
	pool.mu.Lock()
	pool.maxNew = n
	if n < 0 {
		pool.maxNew = 0
	}
	syncMaxIdle := pool.maxNew > 0 && pool.maxIdleEntryPoolLocked() > pool.maxNew
	pool.mu.Unlock()
	if syncMaxIdle {
		pool.SetMaxIdleValues(n)
	}
}

// SetValueMaxLifetime sets the maximum amount of time a value may be reused.
//
// Expired value may be closed lazily before reuse.
//
// If d <= 0, value are reused forever.
func (pool *Pool) SetValueMaxLifetime(d time.Duration) {
	if d < 0 {
		d = 0
	}
	pool.mu.Lock()
	// wake cleaner up when lifetime is shortened.
	if d > 0 && d < pool.maxLifetime && pool.cleanerCh != nil {
		select {
		case pool.cleanerCh <- struct{}{}:
		default:
		}
	}
	pool.maxLifetime = d
	pool.startCleanerLocked()
	pool.mu.Unlock()
}


type Value struct {
	pool *Pool

	// closemu prevents the value from closing while there
	// is still using. It is held for read during using
	// and exclusively during close.
	closemu sync.RWMutex

	// entry is owned until close, at which point
	// it's returned to the pool.
	entry *entry

	// done transitions from 0 to 1 exactly once, on close.
	// Once done, all operations fail with ErrValueDone.
	// Use atomic operations on value when checking value.
	done int32
}

func (v *Value) Close() error {
	if !atomic.CompareAndSwapInt32(&v.done, 0, 1) {
		return ErrValueClosed
	}

	// Lock around releasing the driver connection
	// to ensure all queries have been stopped before doing so.
	v.closemu.Lock()
	defer v.closemu.Unlock()

	v.entry.release()
	v.entry = nil
	v.pool = nil
	return nil
}

func (v *Value) DoContext(ctx context.Context, f func(v interface{}) error) error {
	if atomic.LoadInt32(&v.done) != 0 {
		return ErrValueClosed
	}
	v.closemu.RLock()
	return v.pool.doEntry(ctx, v.entry, v.closemu.RUnlock, f)
}

func (v *Value) Do(f func(v interface{}) error) error {
	return v.DoContext(context.Background(), f)
}

// withLock runs while holding lk.
func withLock(lk sync.Locker, fn func()) {
	lk.Lock()
	defer lk.Unlock() // in case fn panics
	fn()
}
