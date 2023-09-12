# rsmap
[![Go Reference](https://pkg.go.dev/badge/github.com/daichitakahashi/rsmap.svg)](https://pkg.go.dev/github.com/daichitakahashi/rsmap)

![](./sequence.drawio.svg)

## On failure of initialization
![](./retry-init.drawio.svg)

## BoltDB Keys

|Bucket|Key|Value|
|---|---|---|
|`init`|`${resource}`|`initRecord`|
|`acquire`|`${resource}`|`acquireRecord`|

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
        n: 1 // 1 for shared, 999 for exclusive
        operator: "${clientID}"
        ts: 31536000
    }[]
}
```
