## Pool

### 简介

通用的池包，支持池自动收缩功能。

### 使用

#### Routine Pool
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