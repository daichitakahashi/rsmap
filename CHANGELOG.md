# Changelog

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
