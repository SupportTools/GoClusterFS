# Third-party code

## go-fuse

`go-fuse/` is [github.com/hanwen/go-fuse](https://github.com/hanwen/go-fuse) v2.9.0, commit `3183e469f466607a92dd8c2cb44f966d7d69993b`. It was copied from the Go module cache, and `go.mod` points at it with a `replace` directive.

**Why it is copied:** v2.9.0 can only ask the kernel to forward both flock(2) and POSIX (fcntl) locks, or neither. GoClusterFS serves flock across nodes and leaves fcntl locks to the kernel, so it needs to ask for flock alone.

**Changes from upstream:** none yet.

**Upstream PR:** none yet.

**Vulnerability scans:** `govulncheck` lists this module as `./third_party/go-fuse`, with no version, so it cannot match go-fuse advisories. Check the [Go vulnerability database](https://pkg.go.dev/vuln/list) for `github.com/hanwen/go-fuse/v2` by hand.

**Upgrading:**
1. Copy the new version from the module cache.
2. Apply the changes listed above again.
3. Update this file.
4. Once upstream has an option for flock alone, drop the copy and the `replace` directive.
