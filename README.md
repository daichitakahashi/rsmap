# rsmap
[![Go Reference](https://pkg.go.dev/badge/github.com/daichitakahashi/rsmap.svg)](https://pkg.go.dev/github.com/daichitakahashi/rsmap)
[![coverage](https://img.shields.io/endpoint?style=flat-square&url=https%3A%2F%2Fdaichitakahashi.github.io%2Frsmap%2Fcoverage.json)](https://daichitakahashi.github.io/rsmap/coverage.html)

`rsmap` is a package for exclusive control between parallelized `go test` processes.

## Why?
When developing applications that rely on a data store, multiple packages may operate on the same data store.

If tests for each package running in parallel or each test case performs data manipulation simultaneously, the stored data may end up in an unintended state, leading to potential test failures.

To avoid this, it is necessary either to run tests for all packages in a single process (go test -p=1) or to implement cross-process mutual exclusion to ensure that operations are exclusive across processes.

The former option increases the time required for test execution. Using dedicated commands/processes to achieve the latter compromises Go's great virtue in portability.

Efficiently executing tests that share a data store while maintaining reliability requires avoiding both of these pitfalls.

## How to use
```go
// ./internal/pkg/users_test.go
var userDB *rsmap.Resource

func TestMain(m *testing.M) {
    var err error
    m, err = rsmap.New("../../.rsmap")
    if err != nil {
        log.Panic(err)
    }
    defer m.Close()

    userDB, err = m.Resource(ctx, "user_db")
    if err != nil {
        log.Panic(err)
    }

    m.Run()
}

func TestListUsers(t *testing.T) {
    err := userDB.Lock(ctx)
    if err != nil {
        t.Fatal(err)
    }
    t.Cleanup(func() {
        _ = userDB.UnlockAny()
    })

    users, err := userRepo.ListUsers(ctx)
    // Test against users.
}
```

```go
// ./users/create_test.go
var userDB *rsmap.Resource

func TestMain(m *testing.M) {
    var err error
    m, err = rsmap.New("../.rsmap")
    if err != nil {
        log.Panic(err)
    }
    defer m.Close()

    userDB, err = m.Resource(ctx, "user_db")
    if err != nil {
        log.Panic(err)
    }

    m.Run()
}

func TestCreateUser(t *testing.T) {
    err := userDB.Lock(ctx)
    if err != nil {
        t.Fatal(err)
    }
    t.Cleanup(func() {
        _ = userDB.UnlockAny()
    })

    users, err := userRepo.CreateUser(ctx, param)
    // Test against user creation.
}
```

## Mechanism
`rsmap.New()` creates a database file ([BoltDB](https://github.com/etcd-io/bbolt)) within the directory specified as an argument. Since only one process can concurrently open a BoltDB database, the process that initially creates/opens the database has the authority to perform read and write operations.

The instance of `rsmap.Map` with permissions to manipulate this database launches a server in the background, serving as the interface for exclusive control. Other processes/instances act as clients, requesting the acquisition or release of locks from the server.

Each test process corresponds to a test-specific binary for a package. This means that the process at the core of exclusive control promptly terminates once all tests for its respective package have completed.

Processes that act as clients continue to wait in the background until the database becomes available. This ensures that when the process responsible for the server terminates, another process immediately takes on the role of the server. Subsequently, all other clients start making requests to the new server.

Without the need for dedicated commands or processes, developers can achieve cross-process exclusive control seamlessly.

## `viewlogs`コマンドによるログの確認
排他制御やサーバー/クライアントの切り替えの信頼性を高めるために注意を払って開発しています。
排他制御の状態を永続化しているデータベースファイルには、発生したイベントが記録されています。

テストが予期しない原因によって失敗した際にこのイベントを確認することで、原因特定の助けとすることができます。
```shell
$ go run github.com/daichitakahashi/rsmap/cmd/viewlogs YOUR_DATABASE_FILE
```

TODO: screenshot

|オプション|短縮系|説明|
|---|---|---|
|`--operation`|`-o`|`server`(サーバーの起動/停止), `init`(リソースの初期化), `acquire`(ロックの獲得/解放)のうち、出力したい情報をカンマ区切りで指定する。デフォルトではすべてを表示する。|
|`--resource`|`-r`|ログを出力するリソースを指定する。デフォルトではすべてのリソースのログを出力する。|
|`--short`|`-s`|ログのコンテクスト（各関数/メソッドをコールされた場所）の出力を省略し、ハッシュのみとする。|
