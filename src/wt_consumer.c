/*
 * wt_consumer.c - Consumer thread implementation (wt_queue → wtree3)
 */

#include "wt_consumer.h"
#include "wt_sync.h"
#include "wt_atomic.h"
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stdio.h>

#ifdef _WIN32
    #include <windows.h>
    #include <process.h>
    #define THREAD_RETURN unsigned int __stdcall
    #define THREAD_HANDLE HANDLE
#else
    #include <pthread.h>
    #include <unistd.h>
    #define THREAD_RETURN void*
    #define THREAD_HANDLE pthread_t
#endif

/* ============================================================
 * Internal Structures
 * ============================================================ */

/* Ring buffer entry (internal to wt_queue.c, duplicated here for swap processing) */
typedef struct {
    const void *key;
    size_t key_len;
    const void *value;
    size_t val_len;
} wtq_entry_t;

/* Dead letter queue item */
typedef struct dlq_item {
    void *key;
    void *value;
    int retry_count;
    time_t last_attempt;
    struct dlq_item *next;
} dlq_item_t;

/* Consumer state */
struct wt_consumer {
    /* Dependencies */
    wtq_t *queue;
    wtree3_tree_t *wtree;
    wtc_config_t config;

    /* Thread management */
    THREAD_HANDLE thread;
    wt_atomic_bool_t running;
    wt_atomic_bool_t should_stop;

    /* Metrics (protected by metrics_lock) */
    wt_mutex_t metrics_lock;
    wtc_metrics_t metrics;
    time_t start_time;
    time_t last_metrics_update;

    /* Latency tracking (ring buffer for P95 calculation) */
    uint64_t *latency_samples;
    size_t latency_samples_size;
    size_t latency_samples_idx;

    /* Dead letter queue */
    wt_mutex_t dlq_lock;
    dlq_item_t *dlq_head;
    size_t dlq_count;

    /* Error tracking */
    wt_atomic_uint64_t consecutive_errors;
    wt_atomic_uint64_t total_errors;
};

/* ============================================================
 * Logging Helper
 * ============================================================ */

static void log_message(wt_consumer_t *c, const char *level, const char *fmt, ...) {
    if (!c->config.log_fn) return;

    char buffer[512];
    va_list args;
    va_start(args, fmt);
    vsnprintf(buffer, sizeof(buffer), fmt, args);
    va_end(args);

    c->config.log_fn(level, buffer, c->config.log_arg);
}

/* ============================================================
 * Configuration
 * ============================================================ */

wtc_config_t wtc_default_config(void) {
    wtc_config_t config = {0};
    config.error_strategy = WTC_ERROR_RETRY;
    config.max_retries = 3;
    config.retry_backoff_ms = 100;
    config.max_batch_size = 0;  /* Unlimited */
    config.commit_interval_ms = 1000;
    config.enable_latency_tracking = true;
    config.metrics_update_interval_s = 1;
    config.log_fn = NULL;
    config.log_arg = NULL;
    return config;
}

/* ============================================================
 * Dead Letter Queue
 * ============================================================ */

static void dlq_add(wt_consumer_t *c, void *key, void *value, int retry_count) {
    dlq_item_t *item = malloc(sizeof(dlq_item_t));
    if (!item) return;

    item->key = key;
    item->value = value;
    item->retry_count = retry_count;
    item->last_attempt = time(NULL);
    item->next = NULL;

    wt_mutex_lock(&c->dlq_lock);
    item->next = c->dlq_head;
    c->dlq_head = item;
    c->dlq_count++;
    wt_mutex_unlock(&c->dlq_lock);
}

static void dlq_clear(wt_consumer_t *c) {
    wt_mutex_lock(&c->dlq_lock);
    dlq_item_t *item = c->dlq_head;
    while (item) {
        dlq_item_t *next = item->next;
        wtq_release(c->queue, item->key, item->value);
        free(item);
        item = next;
    }
    c->dlq_head = NULL;
    c->dlq_count = 0;
    wt_mutex_unlock(&c->dlq_lock);
}

/* ============================================================
 * Metrics Tracking
 * ============================================================ */

static void update_latency_sample(wt_consumer_t *c, uint64_t latency_ms) {
    if (!c->config.enable_latency_tracking) return;

    c->latency_samples[c->latency_samples_idx] = latency_ms;
    c->latency_samples_idx = (c->latency_samples_idx + 1) % c->latency_samples_size;
}

static uint64_t calculate_p95_latency(wt_consumer_t *c) {
    if (!c->config.enable_latency_tracking) return 0;

    /* Simple P95: sort and take 95th percentile */
    uint64_t *sorted = malloc(c->latency_samples_size * sizeof(uint64_t));
    if (!sorted) return 0;

    memcpy(sorted, c->latency_samples, c->latency_samples_size * sizeof(uint64_t));

    /* Bubble sort (good enough for small arrays) */
    for (size_t i = 0; i < c->latency_samples_size - 1; i++) {
        for (size_t j = 0; j < c->latency_samples_size - i - 1; j++) {
            if (sorted[j] > sorted[j + 1]) {
                uint64_t tmp = sorted[j];
                sorted[j] = sorted[j + 1];
                sorted[j + 1] = tmp;
            }
        }
    }

    size_t p95_idx = (size_t)(c->latency_samples_size * 0.95);
    uint64_t p95 = sorted[p95_idx];
    free(sorted);
    return p95;
}

static void update_metrics(wt_consumer_t *c) {
    time_t now = time(NULL);
    time_t elapsed = now - c->last_metrics_update;

    if (elapsed < c->config.metrics_update_interval_s) {
        return;  /* Too soon to update */
    }

    wt_mutex_lock(&c->metrics_lock);

    /* Update throughput (items/sec) */
    if (elapsed > 0) {
        uint64_t delta_items = c->metrics.total_items_processed;
        c->metrics.items_per_second = delta_items / elapsed;
    }

    /* Update queue health */
    c->metrics.current_queue_depth = wtq_depth(c->queue);
    /* Note: queue capacity is opaque, would need to expose wtq_capacity() */
    c->metrics.queue_utilization = 0.0;  /* Placeholder */

    /* Update uptime */
    c->metrics.uptime_seconds = (uint64_t)(now - c->start_time);

    /* Update P95 latency */
    if (c->config.enable_latency_tracking) {
        c->metrics.p95_batch_latency_ms = calculate_p95_latency(c);
    }

    /* Update error tracking */
    c->metrics.consecutive_errors = wt_atomic_load_uint64(&c->consecutive_errors);
    c->metrics.total_errors = wt_atomic_load_uint64(&c->total_errors);
    c->metrics.items_in_dlq = c->dlq_count;

    /* Health check */
    c->metrics.is_running = wt_atomic_load_bool(&c->running);
    c->metrics.is_healthy = (c->metrics.consecutive_errors < 10);

    c->last_metrics_update = now;

    wt_mutex_unlock(&c->metrics_lock);
}

/* ============================================================
 * Error Handling
 * ============================================================ */

static bool handle_write_error(wt_consumer_t *c, void *key, void *value, int retry_count, gerror_t *err) {
    wt_atomic_fetch_add_uint64(&c->total_errors, 1);
    wt_atomic_fetch_add_uint64(&c->consecutive_errors, 1);

    log_message(c, "ERROR", "wtree3 write failed: %s (retry %d/%d)",
                gerror_get_message(err), retry_count, c->config.max_retries);

    switch (c->config.error_strategy) {
        case WTC_ERROR_FAIL_FAST:
            log_message(c, "FATAL", "Consumer stopping due to error (FAIL_FAST)");
            return false;  /* Stop consumer */

        case WTC_ERROR_RETRY:
            if (retry_count < c->config.max_retries) {
                /* Exponential backoff */
                int backoff_ms = c->config.retry_backoff_ms * (1 << retry_count);
                log_message(c, "WARN", "Retrying after %dms backoff", backoff_ms);
#ifdef _WIN32
                Sleep(backoff_ms);
#else
                usleep(backoff_ms * 1000);
#endif
                return true;  /* Retry */
            } else {
                log_message(c, "ERROR", "Max retries exceeded, moving to DLQ");
                dlq_add(c, key, value, retry_count);
                return true;  /* Continue with next item */
            }

        case WTC_ERROR_DLQ:
            dlq_add(c, key, value, retry_count);
            log_message(c, "WARN", "Item moved to dead letter queue");
            return true;  /* Continue */

        case WTC_ERROR_LOG_CONTINUE:
            log_message(c, "WARN", "Skipping failed item (data loss)");
            wtq_release(c->queue, key, value);
            return true;  /* Continue, item lost */
    }

    return false;
}

/* ============================================================
 * Consumer Loop
 * ============================================================ */

static bool process_batch(wt_consumer_t *c, wtq_buffer_t *buf) {
    if (buf->count == 0) return true;

    wtq_entry_t *entries = (wtq_entry_t*)buf->entries;
    gerror_t err = {0};

    /* Start transaction */
    wtree3_txn_t *txn = wt3_txn_begin(c->wtree, false, &err);
    if (!txn) {
        log_message(c, "ERROR", "Failed to begin transaction: %s", gerror_get_message(&err));
        return false;
    }

    uint64_t batch_start = 0;
#ifdef _WIN32
    batch_start = GetTickCount64();
#else
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    batch_start = ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
#endif

    /* Process all items in batch */
    size_t processed = 0;
    for (size_t i = 0; i < buf->count; i++) {
        size_t idx = (buf->head_offset + i) & (buf->capacity - 1);
        const void *key = entries[idx].key;
        const void *value = entries[idx].value;
        size_t key_len = entries[idx].key_len;
        size_t val_len = entries[idx].val_len;

        /* Attempt write with retries */
        int retry = 0;
        bool success = false;

        while (retry <= c->config.max_retries) {
            if (wt3_put(txn, key, key_len, value, val_len, &err)) {
                success = true;
                break;
            }

            /* Handle error */
            if (!handle_write_error(c, (void*)key, (void*)value, retry, &err)) {
                wt3_txn_abort(txn);
                return false;  /* Fatal error, stop consumer */
            }

            retry++;
        }

        if (success) {
            wtq_release(c->queue, (void*)key, (void*)value);
            processed++;
            wt_atomic_store_uint64(&c->consecutive_errors, 0);  /* Reset error streak */
        }
    }

    /* Commit transaction */
    if (!wt3_txn_commit(txn, &err)) {
        log_message(c, "ERROR", "Failed to commit transaction: %s", gerror_get_message(&err));
        return false;
    }

    /* Update metrics */
    uint64_t batch_end = 0;
#ifdef _WIN32
    batch_end = GetTickCount64();
#else
    clock_gettime(CLOCK_MONOTONIC, &ts);
    batch_end = ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
#endif

    uint64_t batch_latency = batch_end - batch_start;

    wt_mutex_lock(&c->metrics_lock);
    c->metrics.total_items_processed += processed;
    c->metrics.total_batches_processed++;
    c->metrics.avg_batch_latency_ms =
        (c->metrics.avg_batch_latency_ms * (c->metrics.total_batches_processed - 1) + batch_latency)
        / c->metrics.total_batches_processed;
    if (batch_latency > c->metrics.max_batch_latency_ms) {
        c->metrics.max_batch_latency_ms = batch_latency;
    }
    wt_mutex_unlock(&c->metrics_lock);

    update_latency_sample(c, batch_latency);

    log_message(c, "INFO", "Batch processed: %zu items in %llums", processed,
                (unsigned long long)batch_latency);

    return true;
}

static THREAD_RETURN consumer_thread(void *arg) {
    wt_consumer_t *c = (wt_consumer_t*)arg;

    log_message(c, "INFO", "Consumer thread started");
    wt_atomic_store_bool(&c->running, true);

    while (!wt_atomic_load_bool(&c->should_stop)) {
        /* Wait for items (blocks efficiently, 0% CPU when empty) */
        if (!wtq_wait_nonempty(c->queue)) {
            /* Queue was flushed, drain remaining items and exit */
            log_message(c, "INFO", "Queue flushed, draining remaining items");
            break;
        }

        /* Swap entire buffer (instant, releases producer lock) */
        wtq_buffer_t buf = wtq_swap_buffer(c->queue, 0);

        if (buf.count == 0) {
            wtq_buffer_free(c->queue, &buf);
            continue;  /* Spurious wake-up */
        }

        /* Process batch offline (no queue lock held) */
        if (!process_batch(c, &buf)) {
            /* Fatal error */
            wtq_buffer_free(c->queue, &buf);
            log_message(c, "FATAL", "Consumer stopping due to fatal error");
            break;
        }

        wtq_buffer_free(c->queue, &buf);

        /* Update metrics periodically */
        update_metrics(c);
    }

    wt_atomic_store_bool(&c->running, false);
    log_message(c, "INFO", "Consumer thread stopped");

#ifdef _WIN32
    return 0;
#else
    return NULL;
#endif
}

/* ============================================================
 * Public API
 * ============================================================ */

wt_consumer_t *wtc_create(wtq_t *queue, wtree3_tree_t *wtree, const wtc_config_t *config) {
    if (!queue || !wtree) return NULL;

    wt_consumer_t *c = calloc(1, sizeof(wt_consumer_t));
    if (!c) return NULL;

    c->queue = queue;
    c->wtree = wtree;
    c->config = config ? *config : wtc_default_config();

    wt_mutex_init(&c->metrics_lock);
    wt_mutex_init(&c->dlq_lock);
    wt_atomic_init_bool(&c->running, false);
    wt_atomic_init_bool(&c->should_stop, false);
    wt_atomic_init_uint64(&c->consecutive_errors, 0);
    wt_atomic_init_uint64(&c->total_errors, 0);

    c->start_time = time(NULL);
    c->last_metrics_update = c->start_time;

    /* Allocate latency tracking buffer (last 100 samples) */
    if (c->config.enable_latency_tracking) {
        c->latency_samples_size = 100;
        c->latency_samples = calloc(c->latency_samples_size, sizeof(uint64_t));
        if (!c->latency_samples) {
            free(c);
            return NULL;
        }
    }

    return c;
}

bool wtc_start(wt_consumer_t *c) {
    if (!c || wt_atomic_load_bool(&c->running)) return false;

#ifdef _WIN32
    c->thread = (HANDLE)_beginthreadex(NULL, 0, consumer_thread, c, 0, NULL);
    return c->thread != NULL;
#else
    return pthread_create(&c->thread, NULL, consumer_thread, c) == 0;
#endif
}

void wtc_stop(wt_consumer_t *c) {
    if (!c) return;

    log_message(c, "INFO", "Stopping consumer...");
    wt_atomic_store_bool(&c->should_stop, true);

    /* Flush queue to wake consumer */
    wtq_flush(c->queue);

    /* Wait for consumer to finish */
#ifdef _WIN32
    if (c->thread) {
        WaitForSingleObject(c->thread, INFINITE);
        CloseHandle(c->thread);
        c->thread = NULL;
    }
#else
    if (c->thread) {
        pthread_join(c->thread, NULL);
        c->thread = 0;
    }
#endif

    log_message(c, "INFO", "Consumer stopped gracefully");
}

void wtc_destroy(wt_consumer_t *c) {
    if (!c) return;

    /* Ensure stopped */
    if (wt_atomic_load_bool(&c->running)) {
        wtc_stop(c);
    }

    /* Cleanup */
    dlq_clear(c);
    wt_mutex_destroy(&c->metrics_lock);
    wt_mutex_destroy(&c->dlq_lock);
    free(c->latency_samples);
    free(c);
}

void wtc_get_metrics(wt_consumer_t *c, wtc_metrics_t *metrics) {
    if (!c || !metrics) return;

    wt_mutex_lock(&c->metrics_lock);
    *metrics = c->metrics;
    wt_mutex_unlock(&c->metrics_lock);
}

bool wtc_is_healthy(wt_consumer_t *c) {
    if (!c) return false;

    wtc_metrics_t m;
    wtc_get_metrics(c, &m);

    return m.is_running && m.is_healthy && m.consecutive_errors < 10;
}

void **wtc_get_dlq(wt_consumer_t *c, size_t *out_count) {
    if (!c || !out_count) return NULL;

    wt_mutex_lock(&c->dlq_lock);

    *out_count = c->dlq_count;
    if (c->dlq_count == 0) {
        wt_mutex_unlock(&c->dlq_lock);
        return NULL;
    }

    void **items = malloc(c->dlq_count * 2 * sizeof(void*));  /* key + value per item */
    if (!items) {
        wt_mutex_unlock(&c->dlq_lock);
        return NULL;
    }

    size_t i = 0;
    dlq_item_t *item = c->dlq_head;
    while (item) {
        items[i * 2] = item->key;
        items[i * 2 + 1] = item->value;
        item = item->next;
        i++;
    }

    wt_mutex_unlock(&c->dlq_lock);
    return items;
}
