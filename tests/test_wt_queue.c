/*
 * test_wt_queue.c - Comprehensive tests for wt_queue module
 */

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <setjmp.h>
#include <cmocka.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
    #include <windows.h>
    #include <process.h>
#else
    #include <pthread.h>
    #include <unistd.h>
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
 * Test Helpers
 * ============================================================ */

/* Helper: Emulate single-item dequeue using swap (for test compatibility) */
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

static int free_key_called = 0;
static int free_value_called = 0;
static int on_full_called = 0;
static int on_batch_flush_called = 0;

static void test_free_key(const void *ptr, void *arg) {
    (void)arg;
    free_key_called++;
    free((void*)ptr);
}

static void test_free_value(const void *ptr, void *arg) {
    (void)arg;
    free_value_called++;
    free((void*)ptr);
}

static void test_on_full(void *arg) {
    (void)arg;
    on_full_called++;
}

static void test_on_batch_flush(void *arg) {
    (void)arg;
    on_batch_flush_called++;
}

static void reset_counters(void) {
    free_key_called = 0;
    free_value_called = 0;
    on_full_called = 0;
    on_batch_flush_called = 0;
}

/* ============================================================
 * Basic Creation and Destruction Tests
 * ============================================================ */

static void test_create_destroy_basic(void **state) {
    (void)state;
    reset_counters();

    wtq_t *q = wtq_create(10, test_free_key, NULL, test_free_value, NULL,
                          test_on_full, NULL, test_on_batch_flush, NULL);
    assert_non_null(q);

    wtq_destroy(q);
}

static void test_create_zero_capacity(void **state) {
    (void)state;
    reset_counters();

    wtq_t *q = wtq_create(0, test_free_key, NULL, test_free_value, NULL,
                          NULL, NULL, NULL, NULL);
    assert_null(q);  /* Should fail */
}

static void test_create_null_callbacks(void **state) {
    (void)state;
    reset_counters();

    /* NULL callbacks should be allowed */
    wtq_t *q = wtq_create(10, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
    assert_non_null(q);

    wtq_destroy(q);
}

static void test_destroy_null(void **state) {
    (void)state;
    reset_counters();

    /* Should not crash */
    wtq_destroy(NULL);
}

/* ============================================================
 * Enqueue/Dequeue Tests
 * ============================================================ */

static void test_enqueue_dequeue_single(void **state) {
    (void)state;
    reset_counters();

    wtq_t *q = wtq_create(10, test_free_key, NULL, test_free_value, NULL,
                          NULL, NULL, NULL, NULL);
    assert_non_null(q);

    char *key = strdup("key1");
    char *value = strdup("value1");

    bool ok = wtq_enqueue(q, key, strlen(key), value, strlen(value));
    assert_true(ok);
    assert_int_equal(1, wtq_depth(q));

    void *out_key, *out_value;
    ok = wtq_dequeue(q, &out_key, &out_value);
    assert_true(ok);
    assert_ptr_equal(key, out_key);
    assert_ptr_equal(value, out_value);
    assert_int_equal(0, wtq_depth(q));

    /* Release should call free callbacks */
    wtq_release(q, out_key, out_value);
    assert_int_equal(1, free_key_called);
    assert_int_equal(1, free_value_called);

    wtq_destroy(q);
}

static void test_enqueue_null_queue(void **state) {
    (void)state;
    reset_counters();

    bool ok = wtq_enqueue(NULL, "key", 3, "value", 5);
    assert_false(ok);
}

static void test_enqueue_null_key(void **state) {
    (void)state;
    reset_counters();

    wtq_t *q = wtq_create(10, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
    assert_non_null(q);

    bool ok = wtq_enqueue(q, NULL, 3, "value", 5);
    assert_false(ok);

    wtq_destroy(q);
}

static void test_dequeue_null_queue(void **state) {
    (void)state;
    reset_counters();

    void *key, *value;
    bool ok = wtq_dequeue(NULL, &key, &value);
    assert_false(ok);
}

static void test_dequeue_null_outputs(void **state) {
    (void)state;
    reset_counters();

    wtq_t *q = wtq_create(10, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
    assert_non_null(q);

    void *key, *value;

    /* NULL out_key */
    bool ok = wtq_dequeue(q, NULL, &value);
    assert_false(ok);

    /* NULL out_value */
    ok = wtq_dequeue(q, &key, NULL);
    assert_false(ok);

    wtq_destroy(q);
}

static void test_dequeue_empty(void **state) {
    (void)state;
    reset_counters();

    wtq_t *q = wtq_create(10, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
    assert_non_null(q);

    void *key, *value;
    bool ok = wtq_dequeue(q, &key, &value);
    assert_false(ok);  /* Empty queue */

    wtq_destroy(q);
}

static void test_enqueue_multiple(void **state) {
    (void)state;
    reset_counters();

    wtq_t *q = wtq_create(10, test_free_key, NULL, test_free_value, NULL,
                          NULL, NULL, NULL, NULL);
    assert_non_null(q);

    /* Enqueue 5 items */
    for (int i = 0; i < 5; i++) {
        char *key = malloc(16);
        char *value = malloc(16);
        snprintf(key, 16, "key%d", i);
        snprintf(value, 16, "value%d", i);

        bool ok = wtq_enqueue(q, key, strlen(key), value, strlen(value));
        assert_true(ok);
    }

    assert_int_equal(5, wtq_depth(q));

    /* Dequeue all */
    for (int i = 0; i < 5; i++) {
        void *key, *value;
        bool ok = wtq_dequeue(q, &key, &value);
        assert_true(ok);

        wtq_release(q, key, value);
    }

    assert_int_equal(0, wtq_depth(q));
    assert_int_equal(5, free_key_called);
    assert_int_equal(5, free_value_called);

    wtq_destroy(q);
}

/* ============================================================
 * Queue Full Tests
 * ============================================================ */

static void test_enqueue_full(void **state) {
    (void)state;
    reset_counters();

    /* Small queue */
    wtq_t *q = wtq_create(2, NULL, NULL, NULL, NULL,
                          test_on_full, NULL, NULL, NULL);
    assert_non_null(q);

    /* Fill queue (capacity is rounded to power of 2, so 2 items) */
    char *key1 = strdup("key1");
    char *val1 = strdup("val1");
    bool ok = wtq_enqueue(q, key1, 4, val1, 4);
    assert_true(ok);

    char *key2 = strdup("key2");
    char *val2 = strdup("val2");
    ok = wtq_enqueue(q, key2, 4, val2, 4);
    assert_true(ok);

    /* Try to enqueue when full - should fail and trigger callback */
    char *key3 = strdup("key3");
    char *val3 = strdup("val3");
    ok = wtq_enqueue(q, key3, 4, val3, 4);
    assert_false(ok);
    assert_int_equal(1, on_full_called);

    /* Cleanup failed enqueue */
    free(key3);
    free(val3);

    /* Dequeue to free space */
    void *out_key, *out_value;
    wtq_dequeue(q, &out_key, &out_value);
    free(out_key);
    free(out_value);

    wtq_dequeue(q, &out_key, &out_value);
    free(out_key);
    free(out_value);

    wtq_destroy(q);
}

/* ============================================================
 * Release Tests
 * ============================================================ */

static void test_release_null_queue(void **state) {
    (void)state;
    reset_counters();

    /* Should not crash */
    wtq_release(NULL, (void*)"key", (void*)"value");
}

static void test_release_null_pointers(void **state) {
    (void)state;
    reset_counters();

    wtq_t *q = wtq_create(10, test_free_key, NULL, test_free_value, NULL,
                          NULL, NULL, NULL, NULL);
    assert_non_null(q);

    /* NULL pointers should be handled safely */
    wtq_release(q, NULL, NULL);
    assert_int_equal(0, free_key_called);
    assert_int_equal(0, free_value_called);

    wtq_destroy(q);
}

static void test_release_null_callbacks(void **state) {
    (void)state;
    reset_counters();

    /* Queue with NULL callbacks */
    wtq_t *q = wtq_create(10, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
    assert_non_null(q);

    char *key = strdup("key");
    char *value = strdup("value");

    wtq_enqueue(q, key, 3, value, 5);

    void *out_key, *out_value;
    wtq_dequeue(q, &out_key, &out_value);

    /* Release with NULL callbacks - should not crash */
    wtq_release(q, out_key, out_value);

    /* Manual cleanup since callbacks are NULL */
    free(out_key);
    free(out_value);

    wtq_destroy(q);
}

/* ============================================================
 * Flush and Drain Tests
 * ============================================================ */

static void test_flush_basic(void **state) {
    (void)state;
    reset_counters();

    wtq_t *q = wtq_create(10, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
    assert_non_null(q);

    char *key = strdup("key");
    char *value = strdup("value");
    wtq_enqueue(q, key, 3, value, 5);

    /* Flush should prevent new enqueues */
    wtq_flush(q);

    char *key2 = strdup("key2");
    char *value2 = strdup("value2");
    bool ok = wtq_enqueue(q, key2, 4, value2, 6);
    assert_false(ok);  /* Should fail after flush */

    /* Cleanup failed enqueue */
    free(key2);
    free(value2);

    /* Cleanup queue */
    void *out_key, *out_value;
    wtq_dequeue(q, &out_key, &out_value);
    free(out_key);
    free(out_value);

    wtq_destroy(q);
}

static void test_flush_null(void **state) {
    (void)state;
    reset_counters();

    /* Should not crash */
    wtq_flush(NULL);
}

static void test_drain_empty(void **state) {
    (void)state;
    reset_counters();

    wtq_t *q = wtq_create(10, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
    assert_non_null(q);

    /* Drain on empty queue should return immediately */
    wtq_drain(q);

    wtq_destroy(q);
}

static void test_drain_null(void **state) {
    (void)state;
    reset_counters();

    /* Should not crash */
    wtq_drain(NULL);
}

/* ============================================================
 * Depth Tests
 * ============================================================ */

static void test_depth_null(void **state) {
    (void)state;
    reset_counters();

    uint64_t depth = wtq_depth(NULL);
    assert_int_equal(0, depth);
}

static void test_depth_tracking(void **state) {
    (void)state;
    reset_counters();

    wtq_t *q = wtq_create(10, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
    assert_non_null(q);

    assert_int_equal(0, wtq_depth(q));

    /* Enqueue 3 */
    for (int i = 0; i < 3; i++) {
        char *key = malloc(16);
        char *value = malloc(16);
        snprintf(key, 16, "key%d", i);
        snprintf(value, 16, "value%d", i);
        wtq_enqueue(q, key, strlen(key), value, strlen(value));
    }

    assert_int_equal(3, wtq_depth(q));

    /* Swap buffer (swap-only design: depth becomes 0 after swap) */
    wtq_buffer_t buf = wtq_swap_buffer(q, 0);
    assert_int_equal(3, buf.count);
    assert_int_equal(0, wtq_depth(q));  /* Queue is now empty after swap */

    /* Free swapped buffer */
    wtq_entry_t *entries = (wtq_entry_t*)buf.entries;
    for (size_t i = 0; i < buf.count; i++) {
        size_t idx = (buf.head_offset + i) & (buf.capacity - 1);
        free((void*)entries[idx].key);
        free((void*)entries[idx].value);
    }
    wtq_buffer_free(q, &buf);

    assert_int_equal(0, wtq_depth(q));

    wtq_destroy(q);
}

/* ============================================================
 * Wait/Signal Tests (single-threaded validation)
 * ============================================================ */

static void test_wait_nonempty_null(void **state) {
    (void)state;
    reset_counters();

    bool ok = wtq_wait_nonempty(NULL);
    assert_false(ok);
}

static void test_wait_nonempty_with_items(void **state) {
    (void)state;
    reset_counters();

    wtq_t *q = wtq_create(10, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
    assert_non_null(q);

    char *key = strdup("key");
    char *value = strdup("value");
    wtq_enqueue(q, key, 3, value, 5);

    /* Should return immediately since queue has items */
    bool ok = wtq_wait_nonempty(q);
    assert_true(ok);

    /* Cleanup */
    void *out_key, *out_value;
    wtq_dequeue(q, &out_key, &out_value);
    free(out_key);
    free(out_value);

    wtq_destroy(q);
}

static void test_wait_nonempty_flushed(void **state) {
    (void)state;
    reset_counters();

    wtq_t *q = wtq_create(10, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
    assert_non_null(q);

    /* Flush empty queue */
    wtq_flush(q);

    /* Wait should return false */
    bool ok = wtq_wait_nonempty(q);
    assert_false(ok);

    wtq_destroy(q);
}

/* ============================================================
 * Power-of-2 Capacity Tests
 * ============================================================ */

static void test_capacity_power_of_2(void **state) {
    (void)state;
    reset_counters();

    /* Request capacity 5, should round to 8 */
    wtq_t *q = wtq_create(5, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
    assert_non_null(q);

    /* Should be able to enqueue at least 5 items */
    for (int i = 0; i < 5; i++) {
        char *key = malloc(16);
        char *value = malloc(16);
        snprintf(key, 16, "key%d", i);
        snprintf(value, 16, "value%d", i);
        bool ok = wtq_enqueue(q, key, strlen(key), value, strlen(value));
        assert_true(ok);
    }

    /* Cleanup */
    void *key, *value;
    while (wtq_dequeue(q, &key, &value)) {
        free(key);
        free(value);
    }

    wtq_destroy(q);
}

static void test_destroy_with_pending_entries(void **state) {
    (void)state;
    reset_counters();

    /* Queue with NULL free callbacks to avoid double-free */
    wtq_t *q = wtq_create(10, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
    assert_non_null(q);

    /* Enqueue items but don't dequeue */
    for (int i = 0; i < 3; i++) {
        char *key = malloc(16);
        char *value = malloc(16);
        snprintf(key, 16, "key%d", i);
        snprintf(value, 16, "value%d", i);
        bool ok = wtq_enqueue(q, key, strlen(key), value, strlen(value));
        assert_true(ok);
    }

    assert_int_equal(3, wtq_depth(q));

    /* Destroy with pending entries - should cleanup internal structures */
    wtq_destroy(q);

    /* Note: Since we used NULL callbacks, we leak the allocated key/value memory here.
     * This is intentional for this test to cover the destroy cleanup path.
     * In real code, you should either dequeue everything or use proper free callbacks. */
}

/* ============================================================
 * Buffer Swap Tests
 * ============================================================ */

static void test_swap_buffer_basic(void **state) {
    (void)state;
    reset_counters();

    wtq_t *q = wtq_create(16, test_free_key, NULL, test_free_value, NULL, NULL, NULL, NULL, NULL);
    assert_non_null(q);

    /* Enqueue some items */
    for (int i = 0; i < 10; i++) {
        char *k = malloc(16);
        char *v = malloc(16);
        snprintf(k, 16, "key%d", i);
        snprintf(v, 16, "val%d", i);
        assert_true(wtq_enqueue(q, k, strlen(k), v, strlen(v)));
    }

    /* Swap buffer */
    wtq_buffer_t buf = wtq_swap_buffer(q, 0);
    assert_non_null(buf.entries);
    assert_int_equal(10, buf.count);
    assert_int_equal(16, buf.capacity);

    /* Queue should be empty now */
    assert_int_equal(0, wtq_depth(q));

    /* Process entries from swapped buffer */
    wtq_entry_t *entries = (wtq_entry_t*)buf.entries;
    for (size_t i = 0; i < buf.count; i++) {
        size_t idx = (buf.head_offset + i) & (buf.capacity - 1);
        char *k = (char*)entries[idx].key;
        char *v = (char*)entries[idx].value;

        char expected_key[16], expected_val[16];
        snprintf(expected_key, 16, "key%zu", i);
        snprintf(expected_val, 16, "val%zu", i);

        assert_string_equal(expected_key, k);
        assert_string_equal(expected_val, v);

        /* Release entries */
        wtq_release(q, k, v);
    }

    /* Free swapped buffer */
    wtq_buffer_free(q, &buf);
    assert_null(buf.entries);

    assert_int_equal(10, free_key_called);
    assert_int_equal(10, free_value_called);

    wtq_destroy(q);
}

static void test_swap_buffer_empty(void **state) {
    (void)state;
    reset_counters();

    wtq_t *q = wtq_create(16, test_free_key, NULL, test_free_value, NULL, NULL, NULL, NULL, NULL);
    assert_non_null(q);

    /* Swap empty buffer */
    wtq_buffer_t buf = wtq_swap_buffer(q, 0);
    assert_non_null(buf.entries);
    assert_int_equal(0, buf.count);

    wtq_buffer_free(q, &buf);
    wtq_destroy(q);
}

static void test_swap_buffer_resize(void **state) {
    (void)state;
    reset_counters();

    wtq_t *q = wtq_create(16, test_free_key, NULL, test_free_value, NULL, NULL, NULL, NULL, NULL);
    assert_non_null(q);

    /* Enqueue items */
    for (int i = 0; i < 8; i++) {
        char *k = malloc(16);
        char *v = malloc(16);
        snprintf(k, 16, "key%d", i);
        snprintf(v, 16, "val%d", i);
        wtq_enqueue(q, k, strlen(k), v, strlen(v));
    }

    /* Swap with larger capacity */
    wtq_buffer_t buf = wtq_swap_buffer(q, 64);
    assert_non_null(buf.entries);
    assert_int_equal(8, buf.count);
    assert_int_equal(16, buf.capacity);  /* Old buffer capacity */

    /* Queue now has new capacity of 64 */
    /* Enqueue more items to verify new capacity */
    for (int i = 0; i < 50; i++) {
        char *k = malloc(16);
        char *v = malloc(16);
        snprintf(k, 16, "newkey%d", i);
        snprintf(v, 16, "newval%d", i);
        assert_true(wtq_enqueue(q, k, strlen(k), v, strlen(v)));
    }
    assert_int_equal(50, wtq_depth(q));

    /* Process old buffer */
    wtq_entry_t *entries = (wtq_entry_t*)buf.entries;
    for (size_t i = 0; i < buf.count; i++) {
        size_t idx = (buf.head_offset + i) & (buf.capacity - 1);
        wtq_release(q, (void*)entries[idx].key, (void*)entries[idx].value);
    }
    wtq_buffer_free(q, &buf);

    /* Clean up queue */
    void *k, *v;
    while (wtq_dequeue(q, &k, &v)) {
        wtq_release(q, k, v);
    }

    wtq_destroy(q);
}

static void test_swap_buffer_concurrent_enqueue(void **state) {
    (void)state;
    reset_counters();

    wtq_t *q = wtq_create(32, test_free_key, NULL, test_free_value, NULL, NULL, NULL, NULL, NULL);
    assert_non_null(q);

    /* Enqueue initial batch */
    for (int i = 0; i < 10; i++) {
        char *k = malloc(16);
        char *v = malloc(16);
        snprintf(k, 16, "batch1_%d", i);
        snprintf(v, 16, "val1_%d", i);
        wtq_enqueue(q, k, strlen(k), v, strlen(v));
    }

    /* Swap buffer (consumer grabs batch 1) */
    wtq_buffer_t buf = wtq_swap_buffer(q, 0);
    assert_int_equal(10, buf.count);

    /* Producer immediately continues with new buffer */
    for (int i = 0; i < 10; i++) {
        char *k = malloc(16);
        char *v = malloc(16);
        snprintf(k, 16, "batch2_%d", i);
        snprintf(v, 16, "val2_%d", i);
        assert_true(wtq_enqueue(q, k, strlen(k), v, strlen(v)));
    }

    /* Verify queue has new batch */
    assert_int_equal(10, wtq_depth(q));

    /* Process old buffer offline */
    wtq_entry_t *entries = (wtq_entry_t*)buf.entries;
    for (size_t i = 0; i < buf.count; i++) {
        size_t idx = (buf.head_offset + i) & (buf.capacity - 1);
        wtq_release(q, (void*)entries[idx].key, (void*)entries[idx].value);
    }
    wtq_buffer_free(q, &buf);

    /* Clean up second batch */
    void *k, *v;
    while (wtq_dequeue(q, &k, &v)) {
        wtq_release(q, k, v);
    }

    wtq_destroy(q);
}

static void test_swap_buffer_multiple_swaps(void **state) {
    (void)state;
    reset_counters();

    wtq_t *q = wtq_create(16, test_free_key, NULL, test_free_value, NULL, NULL, NULL, NULL, NULL);
    assert_non_null(q);

    /* Perform multiple swap cycles */
    for (int cycle = 0; cycle < 5; cycle++) {
        /* Enqueue batch */
        for (int i = 0; i < 8; i++) {
            char *k = malloc(32);
            char *v = malloc(32);
            snprintf(k, 32, "cycle%d_key%d", cycle, i);
            snprintf(v, 32, "cycle%d_val%d", cycle, i);
            wtq_enqueue(q, k, strlen(k), v, strlen(v));
        }

        /* Swap */
        wtq_buffer_t buf = wtq_swap_buffer(q, 0);
        assert_int_equal(8, buf.count);

        /* Process */
        wtq_entry_t *entries = (wtq_entry_t*)buf.entries;
        for (size_t i = 0; i < buf.count; i++) {
            size_t idx = (buf.head_offset + i) & (buf.capacity - 1);
            wtq_release(q, (void*)entries[idx].key, (void*)entries[idx].value);
        }
        wtq_buffer_free(q, &buf);

        /* Queue should be empty after each cycle */
        assert_int_equal(0, wtq_depth(q));
    }

    wtq_destroy(q);
}

/* ============================================================
 * Multithreaded Tests
 * ============================================================ */

#ifdef _WIN32
typedef struct {
    wtq_t *q;
    int count;
} thread_arg_t;

static DWORD WINAPI producer_thread(LPVOID arg) {
    thread_arg_t *ta = (thread_arg_t*)arg;

    for (int i = 0; i < ta->count; i++) {
        char *key = malloc(16);
        char *value = malloc(16);
        snprintf(key, 16, "key%d", i);
        snprintf(value, 16, "value%d", i);

        while (!wtq_enqueue(ta->q, key, strlen(key), value, strlen(value))) {
            Sleep(1);  /* Retry if full */
        }
    }

    return 0;
}

static DWORD WINAPI consumer_thread(LPVOID arg) {
    thread_arg_t *ta = (thread_arg_t*)arg;
    int consumed = 0;

    while (consumed < ta->count) {
        void *key, *value;

        if (wtq_wait_nonempty(ta->q)) {
            while (wtq_dequeue(ta->q, &key, &value)) {
                wtq_release(ta->q, key, value);
                consumed++;
                if (consumed >= ta->count) break;
            }
        }
    }

    return 0;
}
#else
typedef struct {
    wtq_t *q;
    int count;
} thread_arg_t;

static void *producer_thread(void *arg) {
    thread_arg_t *ta = (thread_arg_t*)arg;

    for (int i = 0; i < ta->count; i++) {
        char *key = malloc(16);
        char *value = malloc(16);
        snprintf(key, 16, "key%d", i);
        snprintf(value, 16, "value%d", i);

        while (!wtq_enqueue(ta->q, key, strlen(key), value, strlen(value))) {
            usleep(1000);  /* Retry if full */
        }
    }

    return NULL;
}

static void *consumer_thread(void *arg) {
    thread_arg_t *ta = (thread_arg_t*)arg;
    int consumed = 0;

    while (consumed < ta->count) {
        void *key, *value;

        if (wtq_wait_nonempty(ta->q)) {
            while (wtq_dequeue(ta->q, &key, &value)) {
                wtq_release(ta->q, key, value);
                consumed++;
                if (consumed >= ta->count) break;
            }
        }
    }

    return NULL;
}
#endif

static void test_mpsc_basic(void **state) {
    (void)state;
    reset_counters();

    wtq_t *q = wtq_create(100, test_free_key, NULL, test_free_value, NULL,
                          NULL, NULL, NULL, NULL);
    assert_non_null(q);

    thread_arg_t ta = {.q = q, .count = 50};

#ifdef _WIN32
    HANDLE producer = CreateThread(NULL, 0, producer_thread, &ta, 0, NULL);
    HANDLE consumer = CreateThread(NULL, 0, consumer_thread, &ta, 0, NULL);

    WaitForSingleObject(producer, INFINITE);
    wtq_flush(q);  /* Signal consumer to exit */
    WaitForSingleObject(consumer, INFINITE);

    CloseHandle(producer);
    CloseHandle(consumer);
#else
    pthread_t prod, cons;

    pthread_create(&cons, NULL, consumer_thread, &ta);
    pthread_create(&prod, NULL, producer_thread, &ta);

    pthread_join(prod, NULL);
    wtq_flush(q);  /* Signal consumer to exit */
    pthread_join(cons, NULL);
#endif

    assert_int_equal(50, free_key_called);
    assert_int_equal(50, free_value_called);

    wtq_destroy(q);
}

/* ============================================================
 * Main
 * ============================================================ */

int main(void) {
    const struct CMUnitTest tests[] = {
        /* Creation/destruction */
        cmocka_unit_test(test_create_destroy_basic),
        cmocka_unit_test(test_create_zero_capacity),
        cmocka_unit_test(test_create_null_callbacks),
        cmocka_unit_test(test_destroy_null),

        /* Enqueue/dequeue */
        cmocka_unit_test(test_enqueue_dequeue_single),
        cmocka_unit_test(test_enqueue_null_queue),
        cmocka_unit_test(test_enqueue_null_key),
        cmocka_unit_test(test_dequeue_null_queue),
        cmocka_unit_test(test_dequeue_null_outputs),
        cmocka_unit_test(test_dequeue_empty),
        cmocka_unit_test(test_enqueue_multiple),

        /* Queue full */
        cmocka_unit_test(test_enqueue_full),

        /* Release */
        cmocka_unit_test(test_release_null_queue),
        cmocka_unit_test(test_release_null_pointers),
        cmocka_unit_test(test_release_null_callbacks),

        /* Flush/drain */
        cmocka_unit_test(test_flush_basic),
        cmocka_unit_test(test_flush_null),
        cmocka_unit_test(test_drain_empty),
        cmocka_unit_test(test_drain_null),

        /* Depth */
        cmocka_unit_test(test_depth_null),
        cmocka_unit_test(test_depth_tracking),

        /* Wait */
        cmocka_unit_test(test_wait_nonempty_null),
        cmocka_unit_test(test_wait_nonempty_with_items),
        cmocka_unit_test(test_wait_nonempty_flushed),

        /* Capacity */
        cmocka_unit_test(test_capacity_power_of_2),
        cmocka_unit_test(test_destroy_with_pending_entries),

        /* Buffer swap (zero-copy buffer exchange) */
        cmocka_unit_test(test_swap_buffer_basic),
        cmocka_unit_test(test_swap_buffer_empty),
        cmocka_unit_test(test_swap_buffer_resize),
        cmocka_unit_test(test_swap_buffer_concurrent_enqueue),
        cmocka_unit_test(test_swap_buffer_multiple_swaps),

        /* Multithreaded */
        cmocka_unit_test(test_mpsc_basic),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
