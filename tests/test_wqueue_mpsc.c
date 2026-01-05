/*
 * test_wqueue_mpsc.c - Multithreaded MPSC tests for wqueue with consumer thread
 */

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <setjmp.h>
#include <cmocka.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>

#include "wqueue.h"
#include "wbuffer.h"
#include "wt_sync.h"

/* Test entry structure */
typedef struct {
    int producer_id;
    int sequence_num;
    char data[64];
} test_entry_t;

/* ============================================================
 * Atomic Counters for Multithreaded Tests
 * ============================================================ */

static atomic_int total_produced = 0;
static atomic_int total_consumed = 0;
static atomic_int total_freed = 0;
static atomic_int consumer_errors = 0;

static void reset_counters(void) {
    atomic_store(&total_produced, 0);
    atomic_store(&total_consumed, 0);
    atomic_store(&total_freed, 0);
    atomic_store(&consumer_errors, 0);
}

/* ============================================================
 * Test Callbacks
 * ============================================================ */

static void test_free_entry(void *entry_ptr, void *arg) {
    (void)arg;
    atomic_fetch_add(&total_freed, 1);
    free(entry_ptr);
}

/* Consumer callback - processes entries successfully */
static int test_consumer_success(void *ctx, const void *entry) {
    (void)ctx;

    /* Extract entry pointer from buffer storage */
    void *entry_ptr = *(void**)entry;
    test_entry_t *e = (test_entry_t*)entry_ptr;

    /* Simulate some work */
    (void)e->producer_id;
    (void)e->sequence_num;

    atomic_fetch_add(&total_consumed, 1);

    /* Free entry on success */
    free(entry_ptr);

    return 0;  /* Success */
}

/* Error handler - always discard (no retry logic) */
static int test_error_handler_discard(void *ctx, const void *entry) {
    (void)ctx;

    void *entry_ptr = *(void**)entry;
    test_entry_t *e = (test_entry_t*)entry_ptr;

    /* Count error and free entry */
    atomic_fetch_add(&consumer_errors, 1);
    free(entry_ptr);

    return 0;  /* Always discard */
}

/* Consumer callback - simulates random failures */
static int test_consumer_with_failures(void *ctx, const void *entry) {
    (void)ctx;

    void *entry_ptr = *(void**)entry;
    test_entry_t *e = (test_entry_t*)entry_ptr;

    /* Fail every 10th item */
    if (e->sequence_num % 10 == 0) {
        return -1;  /* Failure - will be passed to error handler */
    }

    atomic_fetch_add(&total_consumed, 1);
    free(entry_ptr);
    return 0;  /* Success */
}

/* ============================================================
 * Producer Thread Function
 * ============================================================ */

typedef struct {
    wqueue_t *q;
    int producer_id;
    int items_count;
} producer_arg_t;

static WT_THREAD_FUNC(producer_thread) {
    producer_arg_t *pa = (producer_arg_t*)arg;

    for (int i = 0; i < pa->items_count; i++) {
        test_entry_t *entry = malloc(sizeof(test_entry_t));
        entry->producer_id = pa->producer_id;
        entry->sequence_num = i;
        snprintf(entry->data, sizeof(entry->data), "prod_%d_seq_%d",
                 pa->producer_id, i);

        /* Retry on full */
        while (!wqueue_push(pa->q, entry)) {
#ifdef _WIN32
            Sleep(1);
#else
            usleep(100);
#endif
        }

        atomic_fetch_add(&total_produced, 1);
    }

    WT_THREAD_RETURN(0);
}

/* ============================================================
 * Test: Basic consumer thread
 * ============================================================ */

static void test_consumer_thread_basic(void **state) {
    (void)state;
    reset_counters();

    const int ITEM_COUNT = 100;

    wqueue_t *q = wqueue_create(sizeof(void*), 200, test_free_entry, NULL);

    /* Start consumer thread */
    int ret = wqueue_start_consumer_thread(q, NULL, test_consumer_success,
                                            NULL, test_error_handler_discard);
    assert_int_equal(0, ret);

    /* Push items */
    for (int i = 0; i < ITEM_COUNT; i++) {
        test_entry_t *entry = malloc(sizeof(test_entry_t));
        entry->producer_id = 0;
        entry->sequence_num = i;
        snprintf(entry->data, sizeof(entry->data), "entry_%d", i);
        assert_true(wqueue_push(q, entry));
    }

    /* Wait a bit for consumer to process */
#ifdef _WIN32
    Sleep(500);
#else
    usleep(500000);
#endif

    /* Stop consumer thread */
    wqueue_stop_consumer_thread(q);

    /* Verify all items were consumed */
    assert_int_equal(ITEM_COUNT, atomic_load(&total_consumed));

    wqueue_destroy(q);
}

/* ============================================================
 * Test: MPSC with multiple producers and consumer thread
 * ============================================================ */

static void test_mpsc_multiple_producers(void **state) {
    (void)state;
    reset_counters();

    const int NUM_PRODUCERS = 4;
    const int ITEMS_PER_PRODUCER = 1000;
    const int TOTAL_ITEMS = NUM_PRODUCERS * ITEMS_PER_PRODUCER;

    wqueue_t *q = wqueue_create(sizeof(void*), 500, test_free_entry, NULL);

    /* Start consumer thread */
    int ret = wqueue_start_consumer_thread(q, NULL, test_consumer_success,
                                            NULL, test_error_handler_discard);
    assert_int_equal(0, ret);

    /* Start producers */
    wt_thread_t producers[NUM_PRODUCERS];
    producer_arg_t producer_args[NUM_PRODUCERS];

    for (int i = 0; i < NUM_PRODUCERS; i++) {
        producer_args[i].q = q;
        producer_args[i].producer_id = i;
        producer_args[i].items_count = ITEMS_PER_PRODUCER;

        wt_thread_create(&producers[i], producer_thread, &producer_args[i]);
    }

    /* Wait for all producers to finish */
    for (int i = 0; i < NUM_PRODUCERS; i++) {
        wt_thread_join(&producers[i]);
    }

    /* Wait for consumer to process items */
#ifdef _WIN32
    Sleep(500);
#else
    usleep(500000);
#endif

    /* Stop consumer thread */
    wqueue_stop_consumer_thread(q);

    /* Verify counts */
    assert_int_equal(TOTAL_ITEMS, atomic_load(&total_produced));
    assert_int_equal(TOTAL_ITEMS, atomic_load(&total_consumed));

    wqueue_destroy(q);
}

/* ============================================================
 * Test: Consumer thread with high volume
 * ============================================================ */

static void test_consumer_thread_high_volume(void **state) {
    (void)state;
    reset_counters();

    const int ITEM_COUNT = 50000;

    wqueue_t *q = wqueue_create(sizeof(void*), 1000, test_free_entry, NULL);

    /* Start consumer thread */
    int ret = wqueue_start_consumer_thread(q, NULL, test_consumer_success,
                                            NULL, test_error_handler_discard);
    assert_int_equal(0, ret);

    /* Single producer pushing many items */
    for (int i = 0; i < ITEM_COUNT; i++) {
        test_entry_t *entry = malloc(sizeof(test_entry_t));
        entry->producer_id = 0;
        entry->sequence_num = i;

        /* Retry on full */
        while (!wqueue_push(q, entry)) {
#ifdef _WIN32
            Sleep(1);
#else
            usleep(10);
#endif
        }
    }

    /* Wait for consumer to process items */
#ifdef _WIN32
    Sleep(500);
#else
    usleep(500000);
#endif

    /* Stop consumer thread */
    wqueue_stop_consumer_thread(q);

    /* Verify all consumed */
    assert_int_equal(ITEM_COUNT, atomic_load(&total_consumed));

    wqueue_destroy(q);
}

/* ============================================================
 * Test: Consumer thread with error handling (no retry)
 * ============================================================ */

static void test_consumer_thread_errors_no_retry(void **state) {
    (void)state;
    reset_counters();

    const int ITEM_COUNT = 100;

    wqueue_t *q = wqueue_create(sizeof(void*), 200, test_free_entry, NULL);

    /* Start consumer with failure simulation */
    int ret = wqueue_start_consumer_thread(q, NULL, test_consumer_with_failures,
                                            NULL, test_error_handler_discard);
    assert_int_equal(0, ret);

    /* Push items */
    for (int i = 0; i < ITEM_COUNT; i++) {
        test_entry_t *entry = malloc(sizeof(test_entry_t));
        entry->producer_id = 0;
        entry->sequence_num = i;
        assert_true(wqueue_push(q, entry));
    }

    /* Wait for processing */
#ifdef _WIN32
    Sleep(500);
#else
    usleep(500000);
#endif

    /* Stop consumer thread */
    wqueue_stop_consumer_thread(q);

    /* Verify: 10% should have failed (every 10th item) */
    int expected_failures = ITEM_COUNT / 10;
    assert_int_equal(expected_failures, atomic_load(&consumer_errors));

    /* Successful items should be consumed */
    int expected_success = ITEM_COUNT - expected_failures;
    assert_int_equal(expected_success, atomic_load(&total_consumed));

    wqueue_destroy(q);
}


/* ============================================================
 * Test: Stop and restart consumer thread
 * ============================================================ */

static void test_consumer_thread_restart(void **state) {
    (void)state;
    reset_counters();

    const int BATCH1 = 50;
    const int BATCH2 = 50;

    wqueue_t *q = wqueue_create(sizeof(void*), 200, test_free_entry, NULL);

    /* Start consumer thread - first run */
    int ret = wqueue_start_consumer_thread(q, NULL, test_consumer_success,
                                            NULL, test_error_handler_discard);
    assert_int_equal(0, ret);

    /* Push batch 1 */
    for (int i = 0; i < BATCH1; i++) {
        test_entry_t *entry = malloc(sizeof(test_entry_t));
        entry->producer_id = 0;
        entry->sequence_num = i;
        assert_true(wqueue_push(q, entry));
    }

    /* Wait for processing */
#ifdef _WIN32
    Sleep(200);
#else
    usleep(200000);
#endif

    /* Stop consumer thread */
    wqueue_stop_consumer_thread(q);

    int consumed_after_batch1 = atomic_load(&total_consumed);
    assert_true(consumed_after_batch1 > 0);

    /* Start consumer thread again - second run */
    ret = wqueue_start_consumer_thread(q, NULL, test_consumer_success,
                                        NULL, test_error_handler_discard);
    assert_int_equal(0, ret);

    /* Push batch 2 */
    for (int i = 0; i < BATCH2; i++) {
        test_entry_t *entry = malloc(sizeof(test_entry_t));
        entry->producer_id = 1;
        entry->sequence_num = i;
        assert_true(wqueue_push(q, entry));
    }

    /* Wait for processing */
#ifdef _WIN32
    Sleep(200);
#else
    usleep(200000);
#endif

    /* Stop consumer thread again */
    wqueue_stop_consumer_thread(q);

    /* Total consumed should be batch1 + batch2 */
    int total = atomic_load(&total_consumed);
    assert_true(total >= BATCH1 + BATCH2);

    wqueue_destroy(q);
}

/* ============================================================
 * Test: Consumer thread already running
 * ============================================================ */

static void test_consumer_thread_already_running(void **state) {
    (void)state;
    reset_counters();

    wqueue_t *q = wqueue_create(sizeof(void*), 100, test_free_entry, NULL);

    /* Start consumer thread */
    int ret = wqueue_start_consumer_thread(q, NULL, test_consumer_success,
                                            NULL, test_error_handler_discard);
    assert_int_equal(0, ret);

    /* Try to start again - should fail */
    ret = wqueue_start_consumer_thread(q, NULL, test_consumer_success,
                                        NULL, test_error_handler_discard);
    assert_int_equal(-1, ret);

    /* Stop consumer thread */
    wqueue_stop_consumer_thread(q);

    wqueue_destroy(q);
}

/* ============================================================
 * Test: Stress test with rapid push/consume cycles
 * ============================================================ */

static void test_consumer_thread_stress(void **state) {
    (void)state;
    reset_counters();

    const int NUM_PRODUCERS = 8;
    const int ITEMS_PER_PRODUCER = 5000;
    const int TOTAL_ITEMS = NUM_PRODUCERS * ITEMS_PER_PRODUCER;

    wqueue_t *q = wqueue_create(sizeof(void*), 500, test_free_entry, NULL);

    /* Start consumer thread */
    int ret = wqueue_start_consumer_thread(q, NULL, test_consumer_success,
                                            NULL, test_error_handler_discard);
    assert_int_equal(0, ret);

    /* Start multiple producers */
    wt_thread_t producers[NUM_PRODUCERS];
    producer_arg_t producer_args[NUM_PRODUCERS];

    for (int i = 0; i < NUM_PRODUCERS; i++) {
        producer_args[i].q = q;
        producer_args[i].producer_id = i;
        producer_args[i].items_count = ITEMS_PER_PRODUCER;

        wt_thread_create(&producers[i], producer_thread, &producer_args[i]);
    }

    /* Wait for all producers */
    for (int i = 0; i < NUM_PRODUCERS; i++) {
        wt_thread_join(&producers[i]);
    }

    /* Wait for consumer to process items */
#ifdef _WIN32
    Sleep(500);
#else
    usleep(500000);
#endif

    /* Stop consumer thread */
    wqueue_stop_consumer_thread(q);

    /* Verify all items produced and consumed */
    assert_int_equal(TOTAL_ITEMS, atomic_load(&total_produced));
    assert_int_equal(TOTAL_ITEMS, atomic_load(&total_consumed));

    wqueue_destroy(q);
}

/* ============================================================
 * Main
 * ============================================================ */

int main(void) {
    const struct CMUnitTest tests[] = {
        /* Basic consumer thread tests */
        cmocka_unit_test(test_consumer_thread_basic),
        cmocka_unit_test(test_consumer_thread_already_running),
        cmocka_unit_test(test_consumer_thread_restart),

        /* MPSC tests with consumer thread */
        cmocka_unit_test(test_mpsc_multiple_producers),
        cmocka_unit_test(test_consumer_thread_high_volume),

        /* Error handling tests */
        cmocka_unit_test(test_consumer_thread_errors_no_retry),

        /* Stress tests */
        cmocka_unit_test(test_consumer_thread_stress),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
