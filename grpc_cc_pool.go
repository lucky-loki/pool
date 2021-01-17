package pool

import (
	"context"
	"io"

	"google.golang.org/grpc"
)

type grpcConn struct {
	addr string
	options []grpc.DialOption
}

func (ccConf *grpcConn) New(ctx context.Context) (io.Closer, error) {
	cc, err := grpc.DialContext(ctx, ccConf.addr, ccConf.options...)
	if err != nil {
		return nil, err
	}
	return cc, nil
}

type grpcConnPool struct {
	pool *Pool
}

func NewGrpcConnPool(addr string, options ...grpc.DialOption) *grpcConnPool {
	conf := &grpcConn{
		addr: addr,
		options: options,
	}
	return &grpcConnPool{
		pool: NewPool(conf),
	}
}

// DoContext if f() returns ErrBadValue, will close and remove the connection from pool
func (ccPool *grpcConnPool) DoContext(ctx context.Context, f func(cc *grpc.ClientConn) error) error {
	return ccPool.pool.DoContext(ctx, func(v interface{}) error {
		cc := v.(*grpc.ClientConn)
		err := f(cc)
		return err
	})
}

func (ccPool *grpcConnPool) Do(f func(cc *grpc.ClientConn) error) error {
	return ccPool.DoContext(context.Background(), f)
}