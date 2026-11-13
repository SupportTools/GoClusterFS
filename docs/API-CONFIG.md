# GoClusterFS Configuration Reference

This document provides a comprehensive reference for all configuration options in GoClusterFS.

## Table of Contents

1. [Command-Line Flags](#command-line-flags)
2. [HTTP API Statistics](#http-api-statistics)
3. [HTTP API Health and Shutdown](#http-api-health-and-shutdown)
4. [Code-Level Settings](#code-level-settings)
5. [Tuning Recommendations](#tuning-recommendations)
6. [Environment Variables](#environment-variables)

---

## Command-Line Flags

These are the `goclusterfs-mount` flags:

```bash
./goclusterfs-mount [flags]
```

### Required Flags

| Flag | Type | Description |
|------|------|-------------|
| `-device` | string | Path to block device or image file |
| `-mount` | string | Mount point directory |

### General Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `-format` | bool | `false` | Format the device (destroys all data) |
| `-bytes-per-inode` | uint | `1048576` | With `-format`: one inode per this many bytes of device, 4096 to 16,777,216 inodes |
| `-debug` | bool | `false` | Print `[DEBUG]` log lines |
| `-fuse-debug` | bool | `false` | Log every FUSE request and reply |
| `-version` | bool | `false` | Print the version, commit and commit date, then exit |

### Cache Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `-cache-strict` | bool | `false` | Enable strict cache mode (always validate against disk) |
| `-cache-ttl` | duration | `5s` | Cache entry TTL before revalidation required |
| `-attr-timeout` | duration | `1s` | How long the kernel caches file attributes and directory entries |
| `-cache-inodes` | int | `10000` | Most inodes the inode cache holds, about 480 bytes each (5 MB). `0` turns it off |
| `-cache-extents` | int | `50000` | Most block mappings the extent cache holds, about 120 bytes each (6 MB). `0` turns it off |
| `-cache-negative` | int | `1000` | Most not-found inode lookups the negative cache holds, about 40 bytes each. `0` turns it off |

Each cache splits its limit evenly over 16 shards, so it holds a multiple of 16 entries: `-cache-negative 1000` holds 992, and a limit from 1 to 15 holds 16. A full cache drops the entry used longest ago.

The kernel caches file attributes and directory entries for `-attr-timeout`. For that long after another node changes a file, `stat` and path lookups on this node can return the old size. Opening a file reads its inode from disk and makes the kernel fetch its size again, so a read after `open` sees the other node's completed writes. Reads through a file that is already open use the inode cache and can use the old size and block mappings for up to `-cache-ttl`, including a block that another node has since freed and reused for other data. With broadcast enabled, writes and directory changes invalidate peers' caches sooner; truncate, fallocate and unlink do not.

`-attr-timeout=0` makes `stat` and path lookups always current, but each `stat` and path step then reads the directory from disk under a cluster lock. In one test on a single mount, 2000 `stat` calls on one file took 13.9s at 0 and 0.01s at 1s, and `ls -l` of 300 files took 11.8s and 3.7s.

### Journal Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `-journal` | bool | `true` | Enable write-ahead journal for crash consistency |
| `-skip-recovery` | int | `-1` (off) | Discard the journal of registry slot N instead of replaying it, if the mount takes slot N (dangerous, may lose data) |

### Network Broadcast Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `-broadcast` | bool | `true` | Enable network cache invalidation |
| `-mcast-group` | string | `239.192.74.75` | Multicast group IP address |
| `-mcast-port` | int | `31337` | UDP port for multicast |
| `-unicast-port` | int | `31338` | UDP port for unicast; must differ from `-mcast-port` |
| `-broadcast-secret` | string | (empty) | HMAC secret for message authentication |
| `-broadcast-iface` | string | (auto) | Network interface for multicast |
| `-broadcast-max-peers` | int | `256` | Most nodes the unicast peer table and the lost-message tracking hold. When either is full, the node heard from longest ago is dropped |

### TRIM Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `-trim` | bool | `true` | Enable batched TRIM/BLKDISCARD |
| `-trim-batch` | int | `256` | Number of blocks per TRIM batch |
| `-trim-age` | duration | `5s` | Maximum age before forcing TRIM flush |

### Feature Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `-sfb` | bool | `true` | Ignored. SFB containers are disabled |
| `-snapshots` | bool | `true` | Enable file snapshots and clones through extended attributes |

### HTTP API Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `-api` | bool | `true` | Serve the HTTP API |
| `-api-addr` | string | `127.0.0.1:8080` | Listen address |
| `-api-token-file` | string | (empty) | File holding the bearer token for `/api/v1/...` requests, mode 0600 or stricter. A token also turns on the snapshot, clone and admin endpoints |
| `-api-token` | string | (empty) | The token on the command line. Every local user can read it with `ps`; mount logs a `WARNING` |
| `-api-cors` | string | (empty) | Comma-separated CORS allowed origins, or `*` |
| `-api-rate-limit` | float | `0` (off) | Requests per second per client IP, with bursts of twice that, up to 1,000,000. Over the limit, the API answers 429 |
| `-dashboard` | bool | `true` | Serve the [web dashboard](#web-dashboard) under `/ui/` |

The API serves `GET /health`, `GET /metrics`, `GET /api/v1/status`, `GET /api/v1/stats/...`, the [web dashboard](#web-dashboard) under `/ui/` and, with a token, the [write endpoints](#http-api-write-endpoints). [HTTP API Statistics](#http-api-statistics) lists what `/metrics`, `/api/v1/status` and `/api/v1/stats/...` return. `/health` checks the device, journal, journal recovery and broadcast; see [HTTP API Health and Shutdown](#http-api-health-and-shutdown). With a token set, `/api/v1/...` requests need `Authorization: Bearer <token>`. `/health`, `/metrics` and the dashboard's files under `/ui/` never need the token.

Without a token, the API answers only requests whose `Host` header is `localhost`, a `127.x` address or `[::1]`, so a web page cannot reach it through DNS rebinding, and mount refuses `-api-cors '*'`. List the allowed origins instead. The dashboard at `/ui/` shares the API's origin and needs no CORS.

By default the API only accepts connections from this host. To serve other hosts, set `-api-addr` (for example `:8080` or `10.0.0.5:8080`) and `-api-token-file`. Mount exits before it opens the device when the address is not loopback (`127.0.0.0/8`, `::1` or `localhost`) and the token is empty.

Give the token with `-api-token-file` or the `GOCLUSTERFS_API_TOKEN` environment variable, and set only one of them; mount exits when more than one is set, even with `-api=false`. Mount refuses a token file that group or others can read, that belongs to a user other than the mount's or root, or that is empty or over 4096 bytes. `-api-token` still works, but `ps` and `/proc/<pid>/cmdline` show it to every local user, and with it they could change any file's snapshots through the API. The API uses plain HTTP, so a token sent to another host crosses the network unencrypted: put a TLS proxy in front of the API, or reach it over an SSH tunnel (`ssh -L 8080:127.0.0.1:8080 node`).

The rate limit counts requests per connecting IP address, and per /64 for IPv6. It ignores `X-Forwarded-For`, so behind a proxy every client shares the proxy's limit. `/health`, `/metrics` and `/ui/` are never limited. The limiter tracks at most 65,536 clients at once; while it is full, new clients get 429 until idle clients' buckets refill and are dropped, about 2 seconds (longer below 0.5 requests per second).

Each mount on a host needs its own `-api-addr`. When the port is taken, mount logs a warning and runs without the API.

### HTTP API Write Endpoints

These endpoints change the filesystem or this node's state, and listing snapshots reads filesystem metadata. They only work with a token, also on loopback: the Host check stops web pages, not other local processes. Without a token they answer 403, and mount logs `API: snapshot, clone and admin endpoints are off`. The API refuses cross-origin writes: a CORS preflight for any method but `GET` or `HEAD` fails, and a write whose `Origin` header names another site gets 403. Same-origin pages and non-browser clients can call them.

Snapshots belong to single files. A request names the file by its path in the mount, such as `/dir/file`. The path must be absolute and clean, and may not go through a symlink or out of the mount. Snapshot names are unique in the filesystem. The API creates and clones only names without `:`, `/` or NUL, because `user.snapshot.clone` splits at the first `:`. It deletes any name; escape `/` as `%2F` in the URL.

| Method and path | Body | Does the same as | Success |
|-----------------|------|------------------|---------|
| `GET /api/v1/snapshots[?path=/f]` | | Reads the snapshot registry: every snapshot, or the file's, as `getfattr -n user.snapshots` lists them | 200, `{snapshots: [{id, name, inode, state, size_bytes, blocks, created_at}], total}` |
| `POST /api/v1/snapshots` | `{"path": "/f", "name": "s1"}` | `setfattr -n user.snapshot -v s1 f` | 201 |
| `DELETE /api/v1/snapshots/s1?path=/f` | | `setfattr -n user.snapshot.delete -v s1 f` | 200 |
| `POST /api/v1/snapshots/s1/clone` | `{"path": "/f", "dest": "/copy"}` | `setfattr -n user.snapshot.clone -v s1:/copy f` | 201 |
| `POST /api/v1/clones` | `{"path": "/f", "dest": "/copy"}` | `setfattr -n user.clone -v /copy f` | 201 |
| `POST /api/v1/admin/flush-journal` | | Checkpoints the journal now | 200 |
| `POST /api/v1/admin/force-trim` | | Discards the queued TRIM blocks now | 200 |
| `POST /api/v1/admin/clear-cache` | | Drops this node's inode, extent and negative caches | 200 |

The snapshot and clone endpoints call `setxattr` on the file through the mount, so they take exactly the path `setfattr` takes. Delete marks the snapshot `deleting` and returns; the snapshot worker frees its blocks and removes it from the list shortly after, as with `setfattr -n user.snapshot.delete`. Clones go in the root directory only. Admin endpoints act on this node only.

Errors answer with `{error, code, details}`: 400 for a bad path, name, destination or body; 413 for a body over 64 KB; 404 when the file or snapshot does not exist; 409 when the name exists, the file is busy, or the feature is off (`-snapshots=false`, `-journal=false`, `-trim=false`) or the file has old-style sharing that needs `goclusterfs-fsck -repair`; 507 when the device is full; 503 when the filesystem is no longer mounted; 501 when the kernel lacks `openat2` (before Linux 5.6).

Mount logs each write at INFO with the client address, the action, its arguments and the result. On shutdown, mount stops taking requests, waits for writes in flight to finish, and only then unmounts.

### Web Dashboard

Mount serves a web dashboard at `http://<api-addr>/ui/`, unless `-dashboard=false`. It shows status, capacity, the cache, journal, TRIM and lock counters, and the nodes in the registry. It lists snapshots, and for a file you name it creates, clones and deletes them and clones the file. Its Settings page runs the admin endpoints.

The dashboard's files are static and hold no data, so they need no token and skip the rate limit. The page calls `/api/v1/...` on the same origin with the same rules as any client: reads need the token when one is set, and snapshots always need it. Enter the token on the dashboard's Settings page. The browser keeps it in `sessionStorage`, so closing the tab forgets it. Responses carry a `Content-Security-Policy` that lets the page load and call only its own origin and stops other sites from framing it.

Release archives include the dashboard. `go build` and `make build` alone embed a page that says to run `make dashboard build`; `make dashboard` needs Node 22 and npm, and installs the exact packages in `web/dashboard/package-lock.json` with `npm ci`.

---

## HTTP API Statistics

Mount copies its managers' counters every second. While requests read capacity or cluster data (`/metrics`, `/api/v1/status`, `/api/v1/stats/cluster`, `/api/v1/stats/all`), it also reads the block bitmap and node registry from the device every 15 seconds. Requests get the latest copy, so they never read the device. Capacity and cluster data can be 15 seconds old, and after a quiet period the first request returns the data from before it and starts a new read. `df` reads the device on every call. Counters start at zero when the mount starts and count this node only. Times are RFC 3339 in UTC.

| Endpoint | Returns |
|----------|---------|
| `/api/v1/status` | Version, uptime, mount and device paths, and `capacity`: total, used and free blocks and bytes of the data region, for the whole volume |
| `/api/v1/stats` | `cache` and `journal`, as below |
| `/api/v1/stats/cache` | `inode_cache`, `extent_cache`, `negative_cache` and `lock_hint_cache`, each with `size` (entries), `capacity` (the limit), `hits`, `misses` and `hit_rate` (0 to 1). `lock_hint_cache` is always 0: locks no longer use a hint cache. With broadcast on, `broadcast` has `peers` (unicast peers), `senders` (nodes whose message numbers this node tracks) and `max_peers` |
| `/api/v1/stats/journal` | This node's journal: `state` (`clean` or `dirty`), `head_offset` and `tail_offset` in the ring, `available_space` in bytes, `active_txn_count`, `commit_count`, `bytes_written`, `last_checkpoint_seq`, `batch_sync_count`. 404 with `-journal=false` |
| `/api/v1/stats/trim` | `trim_enabled`, false with `-trim=false` or after the device refused a discard as unsupported (image files always do). With `-trim`, also `trim_operations` (discard calls), `trim_blocks` and `trim_bytes` discarded, `pending_trim_blocks`, `trim_errors` (failed ranges, plus flushes skipped because the freed blocks could not be reserved), `last_trim_at` and `batch_size` |
| `/api/v1/stats/lock` | `active_locks` (locks this node holds), `pending_locks` (lock requests that found the lock taken and are retrying), `hint_cache_hits`, `hint_cache_misses` and `hint_cache_hit_rate`, which are always 0 |
| `/api/v1/stats/cluster` | This node's `node_id` (UUID in hex), `node_address` (hostname), `state`, `last_heartbeat_at` and `joined_at`; `cluster_size` (registered nodes); `healthy_nodes` (active with a heartbeat under 10 seconds old); and `peers`, each with `node_id`, `address` (hostname), `state` and `last_seen`. A state is `active`, `leaving`, `dead`, or `stale` when the heartbeat is over 10 seconds old and no node has declared it dead yet. `role` is always `peer`: there is no leader |
| `/api/v1/stats/sfb` | 404, because SFB containers are disabled |
| `/api/v1/stats/all` | `collected_at` and every section above except `sfb` |

`/metrics` exports the same counters under the `goclusterfs_` prefix: `cache_*` (including `cache_inode_capacity`, `cache_extent_capacity` and `cache_negative_capacity`), `broadcast_peers`, `broadcast_senders` and `broadcast_max_peers` (with broadcast on), `journal_*`, `trim_*`, `lock_*`, `cluster_size`, `cluster_healthy_nodes` and `cluster_is_leader` (always 0). `goclusterfs_journal_state` is 0 for clean, 1 for dirty, 2 for recovering and 3 for corrupt. `goclusterfs_fs_size_bytes` and `goclusterfs_fs_free_bytes` give the data region's capacity. There are no `sfb_*` metrics.

---

## HTTP API Health and Shutdown

`GET /health` runs four checks and reports the worst of them:

| Check | `ok` | `degraded` | `unhealthy` |
|-------|------|------------|-------------|
| `device` | The last device read, write or sync worked | | The last device read, write or sync failed |
| `journal` | Clean or dirty, or off with `-journal=false` | This node's journal is being replayed | This node's journal is corrupt |
| `recovery` | No replay is running | This node replays the journal of a crashed node | |
| `broadcast` | Working, or off with `-broadcast=false` | The last invalidation send failed, or multicast is unavailable and no unicast peers are known | |

`ok` and `degraded` answer 200; `unhealthy` answers 503. Degraded means the mount works with a problem: during a replay, files the crashed node had locked wait until it ends, and with broadcast down, peers see this node's changes only after `-cache-ttl`. Unhealthy means file operations fail. A mount that is not running refuses the connection. A check that is not `ok` has a `message` that says what to do. `gfsctl health` exits 0, 1 or 4 for the three states.

The API starts after the mount has replayed its own journal, so a mount that is still starting refuses the connection too.

On SIGINT or SIGTERM, the mount stops the API before it unmounts, because snapshot and clone requests go through the mount. The API stops accepting connections and gives requests in flight 10 seconds to finish. After that it cancels them and closes their connections; snapshot, clone and admin requests that have started still run to the end, and the API waits for them. After `fusermount3 -u`, the API stops the same way once the unmount is done.

## Code-Level Settings

The settings below have no flag unless the table names one. `goclusterfs-mount` uses these defaults; changing the others means editing the Go code and rebuilding.

### Cache

`internal/cache/config.go`. Mount starts from `DefaultConfig()` and applies the flags.

| Field | Default | Flag | Meaning |
|-------|---------|------|---------|
| `InodeCacheSize` | `10000` | | Cached inodes |
| `InodeTTL` | `5s` | `-cache-ttl` | How long a cached inode is used before it is checked against the disk |
| `ExtentCacheSize` | `50000` | | Cached mappings from (inode, logical block) to physical block. A mapping is only used with the inode version it was looked up with, so any inode write by any node makes it stale |
| `NegativeCacheSize` | `1000` | | Cached "not found" lookups |
| `NegativeTTL` | `1s` | | How long a "not found" result is kept |
| `StrictMode` | `false` | `-cache-strict` | Check every cached inode against the disk before use |

The package also defines `NetworkEnabledConfig`, `HighConsistencyConfig` and `HighPerformanceConfig`. `goclusterfs-mount` does not use them.

### Journal

`internal/journal/journal.go`.

| Field | Default | Meaning |
|-------|---------|---------|
| `CommitMode` | `CommitSync` | `CommitSync` syncs every commit. `CommitBatch` groups commits, up to `MaxBatchTransactions` (32) or `MaxBatchAge` (100 ms). `CommitAsync` never syncs and is for tests only |
| `CheckpointInterval` | `5s` | Time between checkpoints. A checkpoint is also forced when a transaction does not fit |

Each node's journal is 4 MB, set by the on-disk layout.

### TRIM

`internal/trim/trim.go`.

| Field | Default | Flag | Meaning |
|-------|---------|------|---------|
| `Enabled` | `true` | `-trim` | Discard freed blocks. After the device refuses a discard as unsupported, as image files do, TRIM stays off for the rest of the mount |
| `BatchSize` | `256` | `-trim-batch` | Freed blocks queued before a discard |
| `MaxAge` | `5s` | `-trim-age` | Longest a freed block waits before it is discarded |

### Broadcast

`internal/cache/broadcast/transport.go`.

| Field | Default | Flag | Meaning |
|-------|---------|------|---------|
| `Enabled` | `true` | `-broadcast` | Send and receive cache invalidations |
| `MulticastGroup` | `239.192.74.75` | `-mcast-group` | All nodes of a cluster must use the same group. `239.192.0.0/14` is for local administrative use |
| `MulticastPort` | `31337` | `-mcast-port` | UDP port for multicast |
| `UnicastPort` | `31338` | `-unicast-port` | UDP port for unicast; must differ from `MulticastPort` |
| `MulticastTTL` | `1` | | Multicast packets stay in the local subnet |
| `MaxMulticastFailures` | `3` | | After this many multicast sends fail in a row, the node sends only to its unicast peers for 30 seconds, then tries multicast again. A successful send resets the count. `0` turns multicast off |
| `HeartbeatInterval` | `30s` | | How often a node announces itself over UDP, so peers learn its unicast address. Failure detection uses the on-disk heartbeat, not this |
| `Secret` | (empty) | `-broadcast-secret` | HMAC secret; all nodes must use the same one. With broadcast on and no secret, mount logs a `WARNING` at startup. Generate one with `openssl rand -hex 32` |
| `Interface` | (system default) | `-broadcast-iface` | Network interface for multicast, such as `eth0` |

---

## Tuning Recommendations

| Workload | Flags | Notes |
|----------|-------|-------|
| Read-heavy | `-cache-ttl 30s` | Open files can see other nodes' changes up to 30 seconds late |
| Shared files, databases | `-cache-strict -attr-timeout 0` | Every node sees changes as soon as possible; `stat` and `ls` get much slower |
| SSD or NVMe | `-trim-batch 512 -trim-age 10s` | Fewer, larger discards |
| HDD, or storage without discard | `-trim=false` | |
| Small files | | SFB is disabled, so each file with data uses a full 1 MB block |

Cache sizes and journal commit modes have no flags.

```bash
./goclusterfs-mount -device /dev/device -mount /mnt/fs -cache-ttl 30s
```

---

## Environment Variables

| Variable | Read by | Meaning |
|----------|---------|---------|
| `GOCLUSTERFS_API_TOKEN` | `goclusterfs-mount` | The API token, instead of `-api-token-file`. Set only one of them |
| `GFSCTL_HOST`, `GFSCTL_PORT`, `GFSCTL_TOKEN`, `GFSCTL_OUTPUT`, `GFSCTL_TIMEOUT` | `gfsctl` | Defaults for its flags; see [gfsctl](../README.md#gfsctl) |
| `XDG_CONFIG_HOME` | `gfsctl` | Its config file is `$XDG_CONFIG_HOME/gfsctl/`, or `~/.config/gfsctl/` |
| `NO_COLOR` | `gfsctl` | Turns off colored output |

---

## Related Documentation

- [Deployment Guide](DEPLOYMENT.md) - Installation and setup
- [Operations Guide](OPERATIONS.md) - Day-to-day operations
- [Troubleshooting](TROUBLESHOOTING.md) - Problem diagnosis
- [Design Overview](DESIGN.md) - Architecture and internals
