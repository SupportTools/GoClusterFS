# Contributing to GoClusterFS

Thanks for helping. This guide covers how to build, test, and send changes.

Report security problems as described in [SECURITY.md](SECURITY.md), not in issues.

## What you need

GoClusterFS builds and runs on Linux only. The storage layer opens devices with `O_DIRECT`, and the filesystem runs through FUSE. It does not build on macOS or Windows.

- The Go version in `go.mod` (1.26 or later)
- FUSE and access to `/dev/fuse`, for integration tests
- The rest of the [Requirements](README.md#requirements) in the README

## Build

```bash
make build
```

This builds `goclusterfs-mount` in the repository root.

## Test

| Command | What it runs | Host needs |
|---|---|---|
| `make test-unit` | Unit tests in `internal/` | Go |
| `make test-integration` | Single-host FUSE tests in `tests/integration/` | FUSE |
| `make test-multinode-docker` | Multi-node tests in Docker containers | Docker and `docker-compose`; the containers run privileged with `/dev/fuse` |
| `make test-multinode-vm` | Multi-node tests in QEMU VMs, including power failure | x86_64 host with KVM and QEMU, see [Testing](README.md#testing) |
| `make test-parity` | Checks that each test scenario exists in every environment | None |

Run `make test-unit` and `make test-integration` before you open a pull request. Also run the Docker or VM suite when you change multi-node behavior, such as locking, the journal, caches, broadcast, or recovery.

CI runs `go vet`, a `gofmt` check, the unit and integration tests, and the Docker suite on every pull request. Run `make fmt` and `make vet` before you push.

## Adding tests

[`tests/test-manifest.yaml`](tests/test-manifest.yaml) lists every test scenario. It is the single source of truth for test coverage across the Go, Docker, and VM environments.

When you add a test:

1. Add the scenario to `tests/test-manifest.yaml` first.
2. Write the Go integration test.
3. Write the Docker test, or set the `docker` entry to `TODO` and say why in the pull request.
4. Write the VM test, or set the `vm` entry to `TODO` and say why in the pull request.
5. Run `make test-parity`.

The comments at the top of the manifest explain the priority levels and the entry format.

For tests where several nodes write to one file at the same time, create the file at its final size before the writers start, and give each node its own offset. This avoids races in the distributed locking.

## Commits and pull requests

- Use [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) for commit messages and pull request titles, for example `fix(journal): replay batches in order`. Common types here are `feat`, `fix`, `perf`, `test`, `docs`, `ci`, and `chore`.
- Keep each pull request to one concern. Send a refactor, a fix, and a feature as separate pull requests.
- Fill in the pull request template: what changed, why, and which test suites you ran.

## License

By sending a contribution, you agree that it is licensed under the [Apache License 2.0](LICENSE).
