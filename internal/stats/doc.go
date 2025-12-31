// Package stats provides unified statistics collection and aggregation for GoClusterFS.
//
// The stats package serves as an aggregation layer that collects statistics from
// multiple internal components (cache, journal, filesystem, etc.) and presents
// them through a unified interface for API endpoints and monitoring.
//
// # Architecture
//
// Unlike the metrics package which provides raw counters/gauges/histograms,
// the stats package provides high-level system status aggregation:
//
//   - Cache stats: hit rates, sizes, broadcast status
//   - Journal stats: transaction counts, space utilization
//   - Filesystem stats: file counts, capacity, mount state
//   - Lock stats: contention metrics, hint cache performance
//
// # Collection Model
//
// Statistics are collected on-demand using a "pull" model. The [Collector] queries
// registered providers when [Collector.Collect] is called. Results are cached
// with a configurable TTL to avoid expensive repeated queries.
//
// # Provider Interfaces
//
// Components implement provider interfaces to expose their statistics:
//
//   - [CacheStatsProvider]: cache hit/miss rates and sizes
//   - [JournalStatsProvider]: journal state and transaction stats
//   - [FilesystemStatsProvider]: file counts and capacity
//   - [LockStatsProvider]: lock contention metrics
//
// This interface-based approach keeps the stats package decoupled from
// internal implementation details.
//
// # Usage
//
//	// Create collector with 1-second cache TTL
//	cfg := stats.DefaultConfig()
//	collector := stats.NewCollector(cfg)
//
//	// Register providers
//	collector.SetCacheProvider(cacheManager)
//	collector.SetJournalProvider(journal)
//
//	// Collect statistics (cached for TTL)
//	snapshot := collector.Collect()
//	fmt.Printf("Cache hit rate: %.2f%%\n", snapshot.Cache.InodeHitRate*100)
package stats
