/*
 * test_wt_queue_stress.c - Stress tests and edge cases for wt_queue
 */

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <setjmp.h>
#include <cmocka.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stdatomic.h>

#ifdef _WIN32
    #include <windows.h>
    #include <process.h>
    #define THREAD_RETURN DWORD WINAPI
    #define THREAD_HANDLE HANDLE
#else
    #include <pthread.h>
    #include <unistd.h>
    #define THREAD_RETURN void*
    #define THREAD_HANDLE pthread_t
#endif

#include "wt_queue.h"

/* Internal structure for buffer swap testing */
typedef struct {
    const void *key;
    size_t key_len;
    const void *value;
    size_t val_len;
} wtq_entry_t;

/* ============================================================
 * Helper: Emulate dequeue using swap (for test compatibility)
 * ============================================================ */

static wtq_buffer_t test_swap_state = {0};
static size_t test_swap_index = 0;

static bool wtq_dequeue(wtq_t *q, void **out_key, void **out_value) {
    if (!q || !out_key || !out_value) return false;

    /* If we've consumed all items from previous swap, get new buffer */
    if (test_swap_index >= test_swap_state.count) {
        /* Free old buffer if exists */
        if (test_swap_state.entries) {
            wtq_buffer_free(q, &test_swap_state);
            test_swap_state.entries = NULL;
            test_swap_state.count = 0;
            test_swap_index = 0;
        }

        /* Swap for new buffer */
        test_swap_state = wtq_swap_buffer(q, 0);
        if (test_swap_state.count == 0) {
            return false;  /* Queue is empty */
        }
        test_swap_index = 0;
    }

    /* Return next item from swapped buffer */
    wtq_entry_t *entries = (wtq_entry_t*)test_swap_state.entries;
    size_t idx = (test_swap_state.head_offset + test_swap_index) & (test_swap_state.capacity - 1);
    *out_key = (void*)entries[idx].key;
    *out_value = (void*)entries[idx].value;
    test_swap_index++;

    return true;
}

/* ============================================================
 * Helper Counters
 * ============================================================ */

static atomic_int total_enqueued = 0;
static atomic_int total_dequeued = 0;
static atomic_int total_freed = 0;

static void stress_free_key(const void *ptr, void *arg) {
    (void)arg;
    atomic_fetch_add(&total_freed, 1);
    free((void*)ptr);
}

static void stress_free_value(const void *ptr, void *arg) {
    (void)arg;
    atomic_fetch_add(&total_freed, 1);
    free((void*)ptr);
}

/* ============================================================
 * Edge Case: Rapid enqueue/dequeue cycles
 * ============================================================ */

static void test_rapid_enqueue_dequeue_cycles(void **state) {
    (void)state;

    wtq_t *q = wtq_create(100, stress_free_key, NULL, stress_free_value, NULL,
                          NULL, NULL, NULL, NULL);
    assert_non_null(q);

    /* Perform 1000 cycles of enqueue-dequeue */
    for (int cycle = 0; cycle < 1000; cycle++) {
        char *key = malloc(32);
        char *value = malloc(32);
        snprintf(key, 32, "cycle_%d", cycle);
        snprintf(value, 32, "value_%d", cycle);

        assert_true(wtq_enqueue(q, key, strlen(key), value, strlen(value)));

        void *out_key, *out_value;
        assert_true(wtq_dequeue(q, &out_key, &out_value));

        assert_ptr_equal(key, out_key);
        assert_ptr_equal(value, out_value);

        wtq_release(q, out_key, out_value);
    }

    assert_int_equal(0, wtq_depth(q));

    wtq_destroy(q);
}

/* ============================================================
 * Edge Case: Fill and drain multiple times
 * ============================================================ */

static void test_fill_and_drain_cycles(void **state) {
    (void)state;

    wtq_t *q = wtq_create(50, stress_free_key, NULL, stress_free_value, NULL,
                          NULL, NULL, NULL, NULL);
    assert_non_null(q);

    for (int cycle = 0; cycle < 10; cycle++) {
        /* Fill to capacity */
        for (int i = 0; i < 50; i++) {
            char *key = malloc(32);
            char *value = malloc(32);
            snprintf(key, 32, "c%d_k%d", cycle, i);
            snprintf(value, 32, "c%d_v%d", cycle, i);
            assert_true(wtq_enqueue(q, key, strlen(key), value, strlen(value)));
        }

        assert_int_equal(50, wtq_depth(q));

        /* Drain completely */
        void *key, *value;
        int drained = 0;
        while (wtq_dequeue(q, &key, &value)) {
            wtq_release(q, key, value);
            drained++;
        }

        assert_int_equal(50, drained);
        assert_int_equal(0, wtq_depth(q));
    }

    wtq_destroy(q);
}

/* ============================================================
 * Edge Case: Buffer swap with high volume
 * ============================================================ */

static void test_batch_dequeue_varying_sizes(void **state) {
    (void)state;

    wtq_t *q = wtq_create(200, stress_free_key, NULL, stress_free_value, NULL,
                          NULL, NULL, NULL, NULL);
    assert_non_null(q);

    /* Enqueue 100 items */
    for (int i = 0; i < 100; i++) {
        char *key = malloc(32);
        char *value = malloc(32);
        snprintf(key, 32, "key_%d", i);
        snprintf(value, 32, "val_%d", i);
        wtq_enqueue(q, key, strlen(key), value, strlen(value));
    }

    /* Swap entire buffer */
    wtq_buffer_t buf = wtq_swap_buffer(q, 0);
    assert_int_equal(100, buf.count);
    assert_int_equal(0, wtq_depth(q));

    /* Process all entries */
    wtq_entry_t *entries = (wtq_entry_t*)buf.entries;
    for (size_t i = 0; i < buf.count; i++) {
        size_t idx = (buf.head_offset + i) & (buf.capacity - 1);
        wtq_release(q, (void*)entries[idx].key, (void*)entries[idx].value);
    }

    wtq_buffer_free(q, &buf);
    wtq_destroy(q);
}

/* ============================================================
 * Stress Test: High volume single-threaded
 * ============================================================ */

static void test_high_volume_single_threaded(void **state) {
    (void)state;

    const int COUNT = 100000;

    wtq_t *q = wtq_create(COUNT + 100, stress_free_key, NULL, stress_free_value, NULL,
                          NULL, NULL, NULL, NULL);
    assert_non_null(q);

    /* Enqueue 100k items */
    for (int i = 0; i < COUNT; i++) {
        char *key = malloc(32);
        char *value = malloc(64);
        snprintf(key, 32, "stress_key_%d", i);
        snprintf(value, 64, "stress_value_%d_%d", i, i * 2);
        wtq_enqueue(q, key, strlen(key), value, strlen(value));
    }

    assert_int_equal(COUNT, wtq_depth(q));

    /* Dequeue all */
    void *key, *value;
    int dequeued = 0;
    while (wtq_dequeue(q, &key, &value)) {
        wtq_release(q, key, value);
        dequeued++;
    }

    assert_int_equal(COUNT, dequeued);
    assert_int_equal(0, wtq_depth(q));

    wtq_destroy(q);
}

/* ============================================================
 * Stress Test: Multiple producers, single consumer
 * ============================================================ */

typedef struct {
    wtq_t *q;
    int producer_id;
    int items_per_producer;
} producer_arg_t;

typedef struct {
    wtq_t *q;
    int expected_total;
} consumer_arg_t;

static THREAD_RETURN producer_stress(void *arg) {
    producer_arg_t *pa = (producer_arg_t*)arg;

    for (int i = 0; i < pa->items_per_producer; i++) {
        char *key = malloc(64);
        char *value = malloc(128);
        snprintf(key, 64, "prod_%d_key_%d", pa->producer_id, i);
        snprintf(value, 128, "prod_%d_value_%d", pa->producer_id, i);

        /* Retry on full */
        while (!wtq_enqueue(pa->q, key, strlen(key), value, strlen(value))) {
#ifdef _WIN32
            Sleep(1);
#else
            usleep(100);
#endif
        }

        atomic_fetch_add(&total_enqueued, 1);
    }

#ifdef _WIN32
    return 0;
#else
    return NULL;
#endif
}

static THREAD_RETURN consumer_stress(void *arg) {
    consumer_arg_t *ca = (consumer_arg_t*)arg;
    int consumed = 0;

    while (consumed < ca->expected_total) {
        void *key, *value;

        if (wtq_wait_nonempty(ca->q)) {
            while (wtq_dequeue(ca->q, &key, &value)) {
                wtq_release(ca->q, key, value);
                consumed++;
                atomic_fetch_add(&total_dequeued, 1);

                if (consumed >= ca->expected_total) break;
            }
        }
    }

#ifdef _WIN32
    return 0;
#else
    return NULL;
#endif
}

static void test_mpsc_stress_many_producers(void **state) {
    (void)state;

    /* Reset counters */
    atomic_store(&total_enqueued, 0);
    atomic_store(&total_dequeued, 0);

    const int NUM_PRODUCERS = 8;
    const int ITEMS_PER_PRODUCER = 10000;
    const int TOTAL_ITEMS = NUM_PRODUCERS * ITEMS_PER_PRODUCER;

    wtq_t *q = wtq_create(1000, stress_free_key, NULL, stress_free_value, NULL,
                          NULL, NULL, NULL, NULL);
    assert_non_null(q);

    /* Start consumer first */
    consumer_arg_t ca = {.q = q, .expected_total = TOTAL_ITEMS};

#ifdef _WIN32
    HANDLE consumer_thread = CreateThread(NULL, 0, consumer_stress, &ca, 0, NULL);
#else
    pthread_t consumer_thread;
    pthread_create(&consumer_thread, NULL, consumer_stress, &ca);
#endif

    /* Start producers */
    THREAD_HANDLE producers[NUM_PRODUCERS];
    producer_arg_t producer_args[NUM_PRODUCERS];

    for (int i = 0; i < NUM_PRODUCERS; i++) {
        producer_args[i].q = q;
        producer_args[i].producer_id = i;
        producer_args[i].items_per_producer = ITEMS_PER_PRODUCER;

#ifdef _WIN32
        producers[i] = CreateThread(NULL, 0, producer_stress, &producer_args[i], 0, NULL);
#else
        pthread_create(&producers[i], NULL, producer_stress, &producer_args[i]);
#endif
    }

    /* Wait for all producers to finish */
    for (int i = 0; i < NUM_PRODUCERS; i++) {
#ifdef _WIN32
        WaitForSingleObject(producers[i], INFINITE);
        CloseHandle(producers[i]);
#else
        pthread_join(producers[i], NULL);
#endif
    }

    /* Signal consumer to finish */
    wtq_flush(q);

    /* Wait for consumer */
#ifdef _WIN32
    WaitForSingleObject(consumer_thread, INFINITE);
    CloseHandle(consumer_thread);
#else
    pthread_join(consumer_thread, NULL);
#endif

    /* Verify counts */
    assert_int_equal(TOTAL_ITEMS, atomic_load(&total_enqueued));
    assert_int_equal(TOTAL_ITEMS, atomic_load(&total_dequeued));

    wtq_destroy(q);
}

/* ============================================================
 * Edge Case: Interleaved flush and enqueue attempts
 * ============================================================ */

static void test_interleaved_flush_enqueue(void **state) {
    (void)state;

    wtq_t *q = wtq_create(100, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
    assert_non_null(q);

    /* Enqueue some items */
    for (int i = 0; i < 10; i++) {
        char *key = malloc(16);
        char *value = malloc(16);
        snprintf(key, 16, "key%d", i);
        snprintf(value, 16, "val%d", i);
        assert_true(wtq_enqueue(q, key, strlen(key), value, strlen(value)));
    }

    /* Flush */
    wtq_flush(q);

    /* Try to enqueue after flush - should fail */
    char *key = strdup("after_flush");
    char *value = strdup("should_fail");
    assert_false(wtq_enqueue(q, key, strlen(key), value, strlen(value)));

    /* Cleanup failed enqueue */
    free(key);
    free(value);

    /* Drain existing items */
    void *out_key, *out_value;
    int drained = 0;
    while (wtq_dequeue(q, &out_key, &out_value)) {
        free(out_key);
        free(out_value);
        drained++;
    }

    assert_int_equal(10, drained);

    wtq_destroy(q);
}

/* ============================================================
 * Main
 * ============================================================ */

int main(void) {
    const struct CMUnitTest tests[] = {
        /* Edge cases */
        cmocka_unit_test(test_rapid_enqueue_dequeue_cycles),
        cmocka_unit_test(test_fill_and_drain_cycles),
        cmocka_unit_test(test_batch_dequeue_varying_sizes),
        cmocka_unit_test(test_interleaved_flush_enqueue),

        /* Stress tests */
        cmocka_unit_test(test_high_volume_single_threaded),
        cmocka_unit_test(test_mpsc_stress_many_producers),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
