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

## 仕組み

## ログの確認
