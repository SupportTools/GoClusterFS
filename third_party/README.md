# Third-party code

## go-fuse

`go-fuse/` is [github.com/hanwen/go-fuse](https://github.com/hanwen/go-fuse) v2.9.0, commit `3183e469f466607a92dd8c2cb44f966d7d69993b`. It was copied from the Go module cache, and `go.mod` points at it with a `replace` directive.

**Why it is copied:** v2.9.0 can only ask the kernel to forward both flock(2) and POSIX (fcntl) locks, or neither. GoClusterFS serves flock across nodes and leaves fcntl locks to the kernel, so it needs to ask for flock alone.

**Changes from upstream:** one. `fuse.MountOptions.EnableFlockLocks` makes the INIT reply set `CAP_FLOCK_LOCKS` without `CAP_POSIX_LOCKS`, unless `EnableLocks` is also set. Kernels on FUSE protocol 7.17 and later then forward flock(2) as `SETLK`/`SETLKW` with `FUSE_LK_FLOCK` and keep POSIX locks local. Older kernels keep both local.

```diff
--- a/fuse/api.go
+++ b/fuse/api.go
@@ -277,6 +277,11 @@ type MountOptions struct {
 	// When used, you must implement the GetLk/SetLk/SetLkw methods.
 	EnableLocks bool
 
+	// EnableFlockLocks, if set and EnableLocks is not, asks the kernel to
+	// forward only flock(2) locks to FUSE, as SetLk/SetLkw with
+	// FUSE_LK_FLOCK set. The kernel keeps handling POSIX locks itself.
+	EnableFlockLocks bool
+
 	// EnableSymlinkCaching, if set, makes the kernel cache all Readlink return values.
--- a/fuse/opcode.go
+++ b/fuse/opcode.go
@@ -104,6 +104,8 @@ func doInit(server *protocolServer, req *request) {
 
 	if server.opts.EnableLocks {
 		kernelFlags |= CAP_FLOCK_LOCKS | CAP_POSIX_LOCKS
+	} else if server.opts.EnableFlockLocks {
+		kernelFlags |= CAP_FLOCK_LOCKS
 	}
```

**Upstream PR:** none yet.

**Vulnerability scans:** `govulncheck` lists this module as `./third_party/go-fuse`, with no version, so it cannot match go-fuse advisories. Check the [Go vulnerability database](https://pkg.go.dev/vuln/list) for `github.com/hanwen/go-fuse/v2` by hand.

**Upgrading:**
1. Copy the new version from the module cache.
2. Apply the changes listed above again.
3. Update this file.
4. Once upstream has an option for flock alone, drop the copy and the `replace` directive.
