package pool

import (
	"context"
	"io"
	"time"
)

type RoutinePool struct {
	pool *Pool
}

func NewRoutinePool() *RoutinePool {
	r := &routine{}
	pool := &RoutinePool{
		pool: NewPool(r),
	}
	return pool
}

func (rp *RoutinePool) SubmitContext(ctx context.Context, job func()) error {
	return rp.pool.DoContext(ctx, func(v interface{}) error {
		r := v.(*routine)
		return r.SubmitJob(job)
	})
}

func (rp *RoutinePool) Submit(job func()) error {
	return rp.SubmitContext(context.Background(), job)
}

func (rp *RoutinePool) SetMaxOpenRoutines(n int) {
	rp.pool.SetMaxNewValues(n)
}

func (rp *RoutinePool) SetMaxIdleRoutine(n int) {
	rp.pool.SetMaxIdleValues(n)
}

func (rp *RoutinePool) SetRoutineMaxLifetime(d time.Duration) {
	rp.pool.SetValueMaxLifetime(d)
}

type routine struct {
	taskQueue chan func()
}

func (r *routine) New(ctx context.Context) (io.Closer, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	worker := &routine{
		taskQueue: make(chan func(), 1),
	}
	err := worker.Run()
	return worker, err
}

func (r *routine) Run() error {
	go func() {
		for job := range r.taskQueue {
			r.runWithRecover(job)
		}
	}()
	return nil
}

func (r *routine) SubmitJob(job func()) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = ErrBadValue
		}
	}()

	r.taskQueue <- job
	return nil
}

func (r *routine) runWithRecover(job func()) {
	defer func() {
		if p := recover(); p != nil {

		}
	}()

	job()
}

func (r *routine) Close() error {
	close(r.taskQueue)
	return nil
}