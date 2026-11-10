// Package stats feeds the HTTP API's /api/v1/stats endpoints, /metrics and the
// capacity in /api/v1/status from the mount's managers.
//
// A [Sampler] copies each manager's counters every [CounterInterval]. It reads
// the block bitmap and node registry from the device at most every
// [DeviceInterval], and only while requests read capacity or cluster data.
// API requests only load the latest snapshot, so an unauthenticated /metrics
// scrape never reads the device or waits on a manager's lock.
package stats
