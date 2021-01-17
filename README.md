## Pool

### 1. 简介

通用的池包，支持以下特性：

    1. 支持池自动收缩功能，定期清理掉过期的资源（MaxLifeTime）
    2. 支持运行时修改池配置（MaxIdle、MaxNew、MaxLifeTime）
    3. 池资源不足时，调用请求将阻塞直到上下文结束或者其他请求释放资源

### 使用

### Pool

你可以实现你自己特定池，通过实现`pool.newer`接口，然后使用`NewPool`方法
将你实现的`resource`纳入池的管理。实现例子可以参考`routine_pool.go`和`grpc_cc_pool.go`。

#### Routine Pool

`routine pool`实现。

```go
package main

import (
    "fmt"
    "sync"
    "time"

    "github.com/lucky-loki/pool"
)


func main() {
    routinePool := pool.NewRoutinePool()
    routinePool.SetMaxOpenRoutines(10000)
    routinePool.SetMaxIdleRoutine(100)
    routinePool.SetRoutineMaxLifetime(30*time.Minute)
    
    wg := &sync.WaitGroup{}
    for i := 0; i < 100000; i++ {
        go func() {
            wg.Add(1)
            err := routinePool.Submit(func() {
                time.Sleep(1*time.Second)
                wg.Done()
            })
            if err == pool.ErrBadValue {
                fmt.Println(err)
            }
        }()
    }
    wg.Wait()
}
```

### GrpcClientConn Pool

```go
package main

import (
	"google.golang.org/grpc"

	"github.com/lucky-loki/pool"
)

func main()  {
    ccPool := pool.NewGrpcConnPool("127.0.0.1:8081", grpc.WithInsecure())
    var resp *proto.DoSometingReply
    var err error
    err = ccPool.Do(func(cc *grpc.ClientConn) error {
        client := proto.NewYourClient(cc)
        resp, err = client.DoSometing()
        return err
    })
    // ...
}
```