# rsmap
[![Go Reference](https://pkg.go.dev/badge/github.com/daichitakahashi/rsmap.svg)](https://pkg.go.dev/github.com/daichitakahashi/rsmap)
[![coverage](https://img.shields.io/endpoint?style=flat-square&url=https%3A%2F%2Fdaichitakahashi.github.io%2Frsmap%2Fcoverage.json)](https://daichitakahashi.github.io/rsmap/coverage.html)

![](./sequence.drawio.svg)

## On failure of initialization
![](./retry-init.drawio.svg)

## BoltDB Keys

|Bucket|Key|Value|
|---|---|---|
|`info`|`server`|`serverRecord`|
|`init`|`${resource}`|`initRecord`|
|`acquire`|`${resource}`|`acquireRecord`|

```typescript
type serverRecord = {
    logs: {
        event: "launched" | "stopped"
        addr?: "${addr}"
        operator: "${clientID}"
        ts: 31536000
    }
}
```

```typescript
type initRecord = {
    logs: {
        event: "started" | "complete"
        operator: "${clientID}"
        ts: 31536000
    }[]
}
```

```typescript
type acquireRecord = {
    max: 999
    logs: {
        event: "acquired" | "released"
        n?: 1 // 1 for shared, 999 for exclusive
        operator: "${clientID}"
        ts: 31536000
    }[]
}
```
