# rsmap
[![Go Reference](https://pkg.go.dev/badge/github.com/daichitakahashi/rsmap.svg)](https://pkg.go.dev/github.com/daichitakahashi/rsmap)
[![coverage](https://img.shields.io/endpoint?style=flat-square&url=https%3A%2F%2Fdaichitakahashi.github.io%2Frsmap%2Fcoverage.json)](https://daichitakahashi.github.io/rsmap/coverage.html)

簡単な説明。

## 目的
データストアに依存するアプリケーションを開発する際、データストアを操作するパッケージが複数あると、並行して実行されるテストによるデータ操作が衝突してしまうことがある。
これを避けるためには、すべてのパッケージのテストを一つのプロセスだけで実行させるか(`go test -p=1`)、プロセスを横断した排他制御のいずれかが必要である。

前者はテストの実行速度を著しく損なってしまう。後者を実現するために専用のコマンド/プロセスを用意するのは、Goのポータビリティを損なう。
これらのいずれも避けつつ、リソースを共有するテストを信頼性を損なわずに効率よく実行させたい。

## 使い方
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

func TestUsers(t *testing.T) {
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

## 仕組み
`rsmap.New()`は、引数で指定されたディレクトリの中にデータベースファイル([BoltDB](https://github.com/etcd-io/bbolt))を作成します。
BoltDBのデータベースを同時にオープンできるのは一つのプロセスに限定されているため、最初にデータベースを作成/オープンしたプロセスだけが読み込み/書き込みを行うことができます。
そしてこのデータベースを操作する権限を持った`rsmap.Map`は、排他制御を司るサーバーをバックグラウンドで起動させ、他のプロセスはクライアントとしてサーバーにロックの獲得や解放をリクエストします。

TODO: illustration

テストの各プロセスは、パッケージごとのテスト用バイナリに対応します。
これは、排他制御の中心となったプロセスも、自分のパッケージのテストがすべて終了したら速やかに終了することを意味しています。

データベースをオープンできずクライアントとなっていたプロセスは、データベースがオープンできるまでバックグラウンドで待機し続けています。
これにより、サーバーを担っていたプロセスが終了すると、直ちに別のプロセスがサーバーに昇格し、他のすべてのクライアントは新しいサーバーにリクエストするようになります。

開発者は、排他制御の中心となるコマンドやプロセスに注意を払うことなく、テスト間の排他制御を行うことができます。

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
