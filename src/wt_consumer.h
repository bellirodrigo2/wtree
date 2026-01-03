#ifndef WT_CONSUMER_H
#define WT_CONSUMER_H

/**
 * @file wt_consumer.h
 * @brief Consumer thread for wt_queue → wtree3 integration
 *
 * This module implements a dedicated consumer thread that:
 * - Waits efficiently for queue items (no busy-loop, 0% CPU when idle)
 * - Swaps entire buffer for maximum throughput
 * - Writes to wtree3 in batches (optimized LMDB commits)
 * - Monitors health metrics (depth, latency, errors)
 * - Handles errors gracefully (retry, dead letter queue, logging)
 * - Shuts down cleanly without data loss
 *
 * USAGE:
 *   1. wt_consumer_t *c = wtc_create(queue, wtree, config)
 *   2. wtc_start(c)  // Spawns background thread
 *   3. ... (producers enqueue to queue) ...
 *   4. wtc_stop(c)   // Graceful shutdown
 *   5. wtc_destroy(c)
 */

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include "wt_queue.h"
#include "wtree3.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================
 * Monitoring Metrics
 * ============================================================ */

/**
 * @brief Consumer health metrics (thread-safe to read)
 */
typedef struct {
    /* Throughput metrics */
    uint64_t total_items_processed;   /* Lifetime total */
    uint64_t total_batches_processed; /* Number of swaps */
    uint64_t items_per_second;        /* Current rate (updated every second) */

    /* Latency metrics (in milliseconds) */
    uint64_t avg_batch_latency_ms;    /* Average time to process one batch */
    uint64_t max_batch_latency_ms;    /* Worst case latency */
    uint64_t p95_batch_latency_ms;    /* 95th percentile */

    /* Queue health */
    uint64_t current_queue_depth;     /* Items waiting in queue */
    double queue_utilization;         /* depth/capacity (0.0 - 1.0) */

    /* Error tracking */
    uint64_t total_errors;            /* Cumulative errors */
    uint64_t consecutive_errors;      /* Current error streak */
    uint64_t items_in_dlq;            /* Dead letter queue size */

    /* Thread state */
    bool is_running;                  /* Consumer thread active */
    bool is_healthy;                  /* No critical errors */
    uint64_t uptime_seconds;          /* Time since start */
} wtc_metrics_t;

/* ============================================================
 * Configuration
 * ============================================================ */

/**
 * @brief Error handling strategy
 */
typedef enum {
    WTC_ERROR_FAIL_FAST,    /* Stop consumer on first error */
    WTC_ERROR_RETRY,        /* Retry failed items with backoff */
    WTC_ERROR_DLQ,          /* Move failed items to dead letter queue */
    WTC_ERROR_LOG_CONTINUE  /* Log error and continue (data loss) */
} wtc_error_strategy_t;

/**
 * @brief Consumer configuration
 */
typedef struct {
    /* Error handling */
    wtc_error_strategy_t error_strategy;
    int max_retries;              /* For RETRY strategy (default: 3) */
    int retry_backoff_ms;         /* Exponential backoff base (default: 100ms) */

    /* Performance tuning */
    size_t max_batch_size;        /* Max items per batch (0 = unlimited) */
    int commit_interval_ms;       /* Force commit every N ms (default: 1000) */

    /* Monitoring */
    bool enable_latency_tracking; /* Track P95 latency (slight overhead) */
    int metrics_update_interval_s;/* How often to update metrics (default: 1) */

    /* Logging callback (optional) */
    void (*log_fn)(const char *level, const char *message, void *arg);
    void *log_arg;
} wtc_config_t;

/**
 * @brief Get default configuration
 */
wtc_config_t wtc_default_config(void);

/* ============================================================
 * Consumer API
 * ============================================================ */

typedef struct wt_consumer wt_consumer_t;

/**
 * @brief Create consumer (does not start thread yet)
 *
 * @param queue     MPSC queue to consume from
 * @param wtree     wtree3 instance to write to
 * @param config    Configuration (NULL = defaults)
 * @return Consumer instance or NULL on failure
 */
wt_consumer_t *wtc_create(wtq_t *queue, wtree3_tree_t *wtree, const wtc_config_t *config);

/**
 * @brief Start consumer thread
 *
 * Spawns background thread that runs consumer loop.
 * Thread will block on wtq_wait_nonempty() until items arrive.
 *
 * @param consumer  Consumer instance
 * @return true on success, false on failure
 */
bool wtc_start(wt_consumer_t *consumer);

/**
 * @brief Stop consumer thread gracefully
 *
 * Signals queue flush, waits for consumer to drain remaining items,
 * then joins thread. May take up to commit_interval_ms to complete.
 *
 * @param consumer  Consumer instance
 */
void wtc_stop(wt_consumer_t *consumer);

/**
 * @brief Destroy consumer (must call wtc_stop first)
 *
 * @param consumer  Consumer instance
 */
void wtc_destroy(wt_consumer_t *consumer);

/**
 * @brief Get current metrics (thread-safe snapshot)
 *
 * @param consumer  Consumer instance
 * @param metrics   Output metrics structure
 */
void wtc_get_metrics(wt_consumer_t *consumer, wtc_metrics_t *metrics);

/**
 * @brief Check if consumer is healthy
 *
 * Returns false if:
 * - Thread crashed or stopped unexpectedly
 * - Consecutive errors > threshold
 * - Queue depth critically high (near capacity)
 *
 * @param consumer  Consumer instance
 * @return true if healthy, false otherwise
 */
bool wtc_is_healthy(wt_consumer_t *consumer);

/**
 * @brief Get dead letter queue (failed items)
 *
 * Only available if error_strategy == WTC_ERROR_DLQ.
 * Returns array of failed items for manual inspection/retry.
 *
 * @param consumer  Consumer instance
 * @param out_count Output: number of items in DLQ
 * @return Array of failed items (caller must free), or NULL if empty
 */
void **wtc_get_dlq(wt_consumer_t *consumer, size_t *out_count);

#ifdef __cplusplus
}
#endif

#endif /* WT_CONSUMER_H */
