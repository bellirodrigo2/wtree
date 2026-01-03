/*
 * consumer_example.c - Complete example of wtree3 + wt_queue + wt_consumer integration
 *
 * This demonstrates the full architecture:
 * - Multiple producer threads enqueuing items
 * - Single consumer thread writing to wtree3 in batches
 * - Health monitoring and metrics
 * - Graceful shutdown
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifdef _WIN32
    #include <windows.h>
    #include <process.h>
    #define THREAD_RETURN unsigned int __stdcall
    #define THREAD_HANDLE HANDLE
    #define sleep_ms(ms) Sleep(ms)
#else
    #include <pthread.h>
    #include <unistd.h>
    #define THREAD_RETURN void*
    #define THREAD_HANDLE pthread_t
    #define sleep_ms(ms) usleep((ms) * 1000)
#endif

#include "wtree3.h"
#include "wt_queue.h"
#include "wt_consumer.h"

/* ============================================================
 * Configuration
 * ============================================================ */

#define NUM_PRODUCERS 4
#define ITEMS_PER_PRODUCER 10000
#define QUEUE_CAPACITY 1000

/* ============================================================
 * Logging Callback
 * ============================================================ */

static void log_callback(const char *level, const char *message, void *arg) {
    (void)arg;
    time_t now = time(NULL);
    struct tm *tm_info = localtime(&now);
    char timestamp[32];
    strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", tm_info);

    printf("[%s] [%s] %s\n", timestamp, level, message);
    fflush(stdout);
}

/* ============================================================
 * Producer Thread
 * ============================================================ */

typedef struct {
    wtq_t *queue;
    int producer_id;
    int items_to_produce;
} producer_args_t;

static THREAD_RETURN producer_thread(void *arg) {
    producer_args_t *args = (producer_args_t*)arg;

    printf("Producer %d started (will produce %d items)\n", args->producer_id, args->items_to_produce);

    for (int i = 0; i < args->items_to_produce; i++) {
        /* Allocate key/value */
        char *key = malloc(64);
        char *value = malloc(256);

        snprintf(key, 64, "producer%d_item%d", args->producer_id, i);
        snprintf(value, 256, "data from producer %d, item %d, timestamp %ld",
                 args->producer_id, i, time(NULL));

        /* Enqueue (with retry on full) */
        while (!wtq_enqueue(args->queue, key, strlen(key), value, strlen(value))) {
            printf("Producer %d: queue full, waiting...\n", args->producer_id);
            sleep_ms(100);
        }

        /* Simulate variable production rate */
        if (i % 100 == 0) {
            sleep_ms(10);  /* Small delay every 100 items */
        }
    }

    printf("Producer %d finished\n", args->producer_id);

#ifdef _WIN32
    return 0;
#else
    return NULL;
#endif
}

/* ============================================================
 * Health Monitor Thread
 * ============================================================ */

typedef struct {
    wt_consumer_t *consumer;
    volatile bool *running;
} monitor_args_t;

static THREAD_RETURN monitor_thread(void *arg) {
    monitor_args_t *args = (monitor_args_t*)arg;

    printf("Health monitor started\n");

    while (*args->running) {
        sleep_ms(2000);  /* Check every 2 seconds */

        wtc_metrics_t metrics;
        wtc_get_metrics(args->consumer, &metrics);

        printf("\n=== HEALTH METRICS ===\n");
        printf("  Status: %s\n", metrics.is_healthy ? "HEALTHY" : "DEGRADED");
        printf("  Uptime: %llu seconds\n", (unsigned long long)metrics.uptime_seconds);
        printf("  Total items processed: %llu\n", (unsigned long long)metrics.total_items_processed);
        printf("  Total batches: %llu\n", (unsigned long long)metrics.total_batches_processed);
        printf("  Throughput: %llu items/sec\n", (unsigned long long)metrics.items_per_second);
        printf("  Queue depth: %llu\n", (unsigned long long)metrics.current_queue_depth);
        printf("  Avg batch latency: %llu ms\n", (unsigned long long)metrics.avg_batch_latency_ms);
        printf("  Max batch latency: %llu ms\n", (unsigned long long)metrics.max_batch_latency_ms);
        printf("  P95 batch latency: %llu ms\n", (unsigned long long)metrics.p95_batch_latency_ms);
        printf("  Total errors: %llu\n", (unsigned long long)metrics.total_errors);
        printf("  Consecutive errors: %llu\n", (unsigned long long)metrics.consecutive_errors);
        printf("  Items in DLQ: %llu\n", (unsigned long long)metrics.items_in_dlq);
        printf("======================\n\n");

        /* Alert if unhealthy */
        if (!wtc_is_healthy(args->consumer)) {
            printf("⚠️  WARNING: Consumer is unhealthy!\n");
        }
    }

    printf("Health monitor stopped\n");

#ifdef _WIN32
    return 0;
#else
    return NULL;
#endif
}

/* ============================================================
 * Main
 * ============================================================ */

int main(void) {
    printf("=== wtree3 + wt_queue + wt_consumer Integration Example ===\n\n");

    gerror_t err = {0};

    /* 1. Create wtree3 database */
    printf("Creating wtree3 database...\n");
    wt3_t *wtree = wt3_open("test_consumer.db", 100 * 1024 * 1024, &err);  /* 100MB */
    if (!wtree) {
        fprintf(stderr, "Failed to open wtree3: %s\n", gerror_get_message(&err));
        return 1;
    }

    /* 2. Create queue (MPSC) */
    printf("Creating MPSC queue (capacity: %d)...\n", QUEUE_CAPACITY);
    wtq_t *queue = wtq_create(QUEUE_CAPACITY,
                              free, NULL,  /* free_key */
                              free, NULL,  /* free_value */
                              NULL, NULL,  /* on_full callback */
                              NULL, NULL); /* on_batch_flush */
    if (!queue) {
        fprintf(stderr, "Failed to create queue\n");
        wt3_close(wtree);
        return 1;
    }

    /* 3. Configure consumer */
    printf("Configuring consumer...\n");
    wtc_config_t config = wtc_default_config();
    config.error_strategy = WTC_ERROR_RETRY;
    config.max_retries = 3;
    config.retry_backoff_ms = 100;
    config.enable_latency_tracking = true;
    config.metrics_update_interval_s = 1;
    config.log_fn = log_callback;
    config.log_arg = NULL;

    /* 4. Create and start consumer */
    printf("Starting consumer thread...\n");
    wt_consumer_t *consumer = wtc_create(queue, wtree, &config);
    if (!consumer) {
        fprintf(stderr, "Failed to create consumer\n");
        wtq_destroy(queue);
        wt3_close(wtree);
        return 1;
    }

    if (!wtc_start(consumer)) {
        fprintf(stderr, "Failed to start consumer thread\n");
        wtc_destroy(consumer);
        wtq_destroy(queue);
        wt3_close(wtree);
        return 1;
    }

    /* 5. Start health monitor */
    printf("Starting health monitor...\n");
    volatile bool monitor_running = true;
    monitor_args_t monitor_args = { consumer, &monitor_running };

    THREAD_HANDLE monitor_handle;
#ifdef _WIN32
    monitor_handle = (HANDLE)_beginthreadex(NULL, 0, monitor_thread, &monitor_args, 0, NULL);
#else
    pthread_create(&monitor_handle, NULL, monitor_thread, &monitor_args);
#endif

    /* 6. Start producer threads */
    printf("Starting %d producer threads...\n\n", NUM_PRODUCERS);
    THREAD_HANDLE producers[NUM_PRODUCERS];
    producer_args_t producer_args[NUM_PRODUCERS];

    for (int i = 0; i < NUM_PRODUCERS; i++) {
        producer_args[i].queue = queue;
        producer_args[i].producer_id = i;
        producer_args[i].items_to_produce = ITEMS_PER_PRODUCER;

#ifdef _WIN32
        producers[i] = (HANDLE)_beginthreadex(NULL, 0, producer_thread, &producer_args[i], 0, NULL);
#else
        pthread_create(&producers[i], NULL, producer_thread, &producer_args[i]);
#endif
    }

    /* 7. Wait for producers to finish */
    printf("Waiting for producers to finish...\n");
    for (int i = 0; i < NUM_PRODUCERS; i++) {
#ifdef _WIN32
        WaitForSingleObject(producers[i], INFINITE);
        CloseHandle(producers[i]);
#else
        pthread_join(producers[i], NULL);
#endif
    }

    printf("\nAll producers finished! Total items enqueued: %d\n",
           NUM_PRODUCERS * ITEMS_PER_PRODUCER);

    /* 8. Wait for consumer to drain queue */
    printf("Waiting for consumer to drain queue...\n");
    wtq_drain(queue);
    printf("Queue drained!\n");

    /* 9. Stop health monitor */
    monitor_running = false;
#ifdef _WIN32
    WaitForSingleObject(monitor_handle, INFINITE);
    CloseHandle(monitor_handle);
#else
    pthread_join(monitor_handle, NULL);
#endif

    /* 10. Stop consumer */
    printf("\nStopping consumer...\n");
    wtc_stop(consumer);

    /* 11. Final metrics */
    printf("\n=== FINAL METRICS ===\n");
    wtc_metrics_t final_metrics;
    wtc_get_metrics(consumer, &final_metrics);
    printf("  Total items processed: %llu\n", (unsigned long long)final_metrics.total_items_processed);
    printf("  Total batches: %llu\n", (unsigned long long)final_metrics.total_batches_processed);
    printf("  Avg batch latency: %llu ms\n", (unsigned long long)final_metrics.avg_batch_latency_ms);
    printf("  Total errors: %llu\n", (unsigned long long)final_metrics.total_errors);
    printf("=====================\n\n");

    /* 12. Verify in database */
    printf("Verifying data in wtree3...\n");
    wt3_txn_t *txn = wt3_txn_begin(wtree, true, &err);
    if (txn) {
        /* Check a random key */
        char test_key[64];
        snprintf(test_key, sizeof(test_key), "producer0_item100");

        void *value_out = NULL;
        size_t value_len = 0;
        if (wt3_get(txn, test_key, strlen(test_key), &value_out, &value_len, &err)) {
            printf("✓ Sample key found: %s = %.*s\n", test_key, (int)value_len, (char*)value_out);
            free(value_out);
        }
        wt3_txn_commit(txn, &err);
    }

    /* 13. Cleanup */
    printf("\nCleaning up...\n");
    wtc_destroy(consumer);
    wtq_destroy(queue);
    wt3_close(wtree);

    printf("\n✓ Example completed successfully!\n");
    return 0;
}
