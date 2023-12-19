# Changelog

## [v0.0.4](https://github.com/daichitakahashi/rsmap/compare/v0.0.3...v0.0.4) - 2023-12-19
- Implement multiple resource lock by @daichitakahashi in https://github.com/daichitakahashi/rsmap/pull/50

## [v0.0.3](https://github.com/daichitakahashi/rsmap/compare/v0.0.2...v0.0.3) - 2023-12-14
- Get expected coverage by @daichitakahashi in https://github.com/daichitakahashi/rsmap/pull/31
- Update workflows/test.yaml: fix coverage extraction by @daichitakahashi in https://github.com/daichitakahashi/rsmap/pull/33
- More test by @daichitakahashi in https://github.com/daichitakahashi/rsmap/pull/34
- Update test: use relative path as `rsmapDir` by @daichitakahashi in https://github.com/daichitakahashi/rsmap/pull/35
- Update dependabot.yml: add check for GitHub Actions by @daichitakahashi in https://github.com/daichitakahashi/rsmap/pull/36
- Update cmd/viewlogs: show logs of all resources by @daichitakahashi in https://github.com/daichitakahashi/rsmap/pull/38
- Bump actions/upload-pages-artifact from 1 to 2 by @dependabot in https://github.com/daichitakahashi/rsmap/pull/37
- Bump golang.org/x/mod from 0.12.0 to 0.13.0 by @dependabot in https://github.com/daichitakahashi/rsmap/pull/41
- Bump golang.org/x/sync from 0.3.0 to 0.4.0 by @dependabot in https://github.com/daichitakahashi/rsmap/pull/40
- Bump github.com/daichitakahashi/deps from 0.0.3 to 0.4.0 by @daichitakahashi in https://github.com/daichitakahashi/rsmap/pull/42
- Bump github.com/google/go-cmp from 0.5.9 to 0.6.0 by @dependabot in https://github.com/daichitakahashi/rsmap/pull/43
- Bump go.etcd.io/bbolt from 1.3.7 to 1.3.8 by @dependabot in https://github.com/daichitakahashi/rsmap/pull/45
- Bump golang.org/x/mod from 0.13.0 to 0.14.0 by @dependabot in https://github.com/daichitakahashi/rsmap/pull/46
- Bump golang.org/x/sync from 0.4.0 to 0.5.0 by @dependabot in https://github.com/daichitakahashi/rsmap/pull/47
- Bump github.com/fatih/color from 1.15.0 to 1.16.0 by @dependabot in https://github.com/daichitakahashi/rsmap/pull/48
- Bump actions/setup-go from 4 to 5 by @dependabot in https://github.com/daichitakahashi/rsmap/pull/49
- Add documents by @daichitakahashi in https://github.com/daichitakahashi/rsmap/pull/39

## [v0.0.2](https://github.com/daichitakahashi/rsmap/compare/v0.0.1...v0.0.2) - 2023-09-28
- Do not (*http.Server).Shutdown() immediately by @daichitakahashi in https://github.com/daichitakahashi/rsmap/pull/26
- Check if `${rsmapDir}/logs.db` and `${rsmapDir}/addr` already exists as directory by @daichitakahashi in https://github.com/daichitakahashi/rsmap/pull/29
- Update internal/ctl: re-implement channel-based semaphore by @daichitakahashi in https://github.com/daichitakahashi/rsmap/pull/30

## [v0.0.1](https://github.com/daichitakahashi/rsmap/commits/v0.0.1) - 2023-09-23
- Reworking control core by @daichitakahashi in https://github.com/daichitakahashi/rsmap/pull/1
- Rework resource map by @daichitakahashi in https://github.com/daichitakahashi/rsmap/pull/2
- Add `(*Resource).RLock()` and `(*Resource).Lock()` by @daichitakahashi in https://github.com/daichitakahashi/rsmap/pull/3
- Update README.md: add Go Reference badge by @daichitakahashi in https://github.com/daichitakahashi/rsmap/pull/4
- Enhance logs for debugging by @daichitakahashi in https://github.com/daichitakahashi/rsmap/pull/5
- Fix deadlock by @daichitakahashi in https://github.com/daichitakahashi/rsmap/pull/8
- Add "failed" status to init by @daichitakahashi in https://github.com/daichitakahashi/rsmap/pull/10
- Add `logs.Callers` to retain caller context by @daichitakahashi in https://github.com/daichitakahashi/rsmap/pull/11
- Add protobuf schema `internal.proto.logs.v1` by @daichitakahashi in https://github.com/daichitakahashi/rsmap/pull/13
- Update logs: use random uint32 value to create hash by @daichitakahashi in https://github.com/daichitakahashi/rsmap/pull/15
- Improve viewlogs by @daichitakahashi in https://github.com/daichitakahashi/rsmap/pull/16
- Add testing workflow by @daichitakahashi in https://github.com/daichitakahashi/rsmap/pull/19
- Fix data race by @daichitakahashi in https://github.com/daichitakahashi/rsmap/pull/20
- Actions/tagpr by @daichitakahashi in https://github.com/daichitakahashi/rsmap/pull/21
- Bump gotest.tools/v3 from 3.4.0 to 3.5.1 by @dependabot in https://github.com/daichitakahashi/rsmap/pull/22
