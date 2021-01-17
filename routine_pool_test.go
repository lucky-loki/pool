package pool

import (
	"sync"
	"testing"
	"time"
)

func TestRoutinePool_Do(t *testing.T) {
	pool := NewRoutinePool()
	pool.SetMaxOpenRoutines(1000)
	pool.SetMaxIdleRoutine(100)

	wg := &sync.WaitGroup{}
	var failed uint64
	for i := 0; i < 10000; i++ {
		go func() {
			wg.Add(1)
			err := pool.Submit(func() {
				time.Sleep(10*time.Millisecond)
				wg.Done()
			})
			if err == ErrBadValue {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	t.Logf("failed request: %d, routine opened: %d\n", failed, pool.pool.numNew+int(pool.pool.numClosed))
}

