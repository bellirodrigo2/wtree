/*
 * test_wbuffer.c - Comprehensive tests for wbuffer module
 */
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <setjmp.h>
#include <cmocka.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "wbuffer.h"
/* ============================================================
 * Test Data Structures
 * ============================================================ */
typedef struct {
    int id;
    char name[32];
} test_entry_t;
/* ============================================================
 * Test Helpers
 * ============================================================ */
static int consumer_call_count = 0;
static int error_handler_call_count = 0;
static int consumer_fail_at = -1;  /* -1 means never fail */
static int error_handler_should_retry = 0;  /* 0 = don't retry, 1 = retry */
static int test_consumer(void *ctx, const void *entry) {
    int *total = (int*)ctx;
    const test_entry_t *e = (const test_entry_t*)entry;
    consumer_call_count++;
    if (consumer_fail_at >= 0 && consumer_call_count - 1 == consumer_fail_at) {
        return -1;  /* Simulate failure */
    }
    if (total) {
        *total += e->id;
    }
    return 0;  /* Success */
}
static int test_error_handler(void *ctx, const void *entry) {
    int *error_count = (int*)ctx;
    error_handler_call_count++;
    if (error_count) {
        (*error_count)++;
    }
    return error_handler_should_retry;
}
static void reset_counters(void) {
    consumer_call_count = 0;
    error_handler_call_count = 0;
    consumer_fail_at = -1;
    error_handler_should_retry = 0;
}
/* ============================================================
 * Initialization and Destruction Tests
 * ============================================================ */
static void test_init_destroy_basic(void **state) {
    (void)state;
    reset_counters();
    wbuffer_t *buf = wbuffer_create(sizeof(test_entry_t), 10);
    assert_non_null(buf);
    assert_true(wbuffer_is_empty(buf));
    assert_false(wbuffer_is_full(buf));
    assert_true(wbuffer_entry_size(buf) == sizeof(test_entry_t));
    wbuffer_free(buf);
}
static void test_init_large_capacity(void **state) {
    (void)state;
    reset_counters();
    size_t n = 1000;
    wbuffer_t *buf = wbuffer_create(sizeof(test_entry_t), n);
    assert_non_null(buf);
    assert_true(wbuffer_is_empty(buf));
    assert_true(wbuffer_capacity(buf) == n);
    wbuffer_free(buf);
}
static void test_init_small_entry(void **state) {
    (void)state;
    reset_counters();
    wbuffer_t *buf = wbuffer_create(sizeof(int), 5);
    assert_non_null(buf);
    wbuffer_free(buf);
}
/* ============================================================
 * Push Tests
 * ============================================================ */
static void test_push_single(void **state) {
    (void)state;
    reset_counters();
    wbuffer_t *buf = wbuffer_create(sizeof(test_entry_t), 10);
    assert_non_null(buf);
    test_entry_t entry = {.id = 42, .name = "test"};
    int result = wbuffer_push(buf, &entry);
    assert_int_equal(0, result);
    assert_false(wbuffer_is_empty(buf));
    wbuffer_free(buf);
}
static void test_push_multiple(void **state) {
    (void)state;
    reset_counters();
    wbuffer_t *buf = wbuffer_create(sizeof(test_entry_t), 10);
    assert_non_null(buf);
    for (int i = 0; i < 5; i++) {
        test_entry_t entry = {.id = i, .name = ""};
        snprintf(entry.name, sizeof(entry.name), "entry%d", i);
        int result = wbuffer_push(buf, &entry);
        assert_int_equal(0, result);
    }
    assert_false(wbuffer_is_empty(buf));
    wbuffer_free(buf);
}
static void test_push_until_full(void **state) {
    (void)state;
    reset_counters();
    wbuffer_t *buf = wbuffer_create(sizeof(test_entry_t), 3);
    assert_non_null(buf);
    test_entry_t entry = {.id = 1, .name = "test"};
    /* Fill buffer */
    assert_int_equal(0, wbuffer_push(buf, &entry));
    assert_int_equal(0, wbuffer_push(buf, &entry));
    assert_int_equal(0, wbuffer_push(buf, &entry));
    assert_true(wbuffer_is_full(buf));
    /* Try to push when full */
    assert_int_equal(-1, wbuffer_push(buf, &entry));
    wbuffer_free(buf);
}
/* ============================================================
 * Consume Tests - Success Cases
 * ============================================================ */
static void test_consume_empty_buffer(void **state) {
    (void)state;
    reset_counters();
    wbuffer_t *buf = wbuffer_create(sizeof(test_entry_t), 10);
    int total = 0;
    int error_count = 0;
    int result = wbuffer_consume(buf, &total, test_consumer,
                                  &error_count, test_error_handler);
    assert_int_equal(0, result);
    assert_int_equal(0, consumer_call_count);
    assert_int_equal(0, error_handler_call_count);
    assert_int_equal(0, total);
    wbuffer_free(buf);
}
static void test_consume_all_success(void **state) {
    (void)state;
    reset_counters();
    wbuffer_t *buf = wbuffer_create(sizeof(test_entry_t), 10);
    /* Push 5 entries */
    for (int i = 1; i <= 5; i++) {
        test_entry_t entry = {.id = i, .name = ""};
        snprintf(entry.name, sizeof(entry.name), "entry%d", i);
        wbuffer_push(buf, &entry);
    }
    int total = 0;
    int error_count = 0;
    int result = wbuffer_consume(buf, &total, test_consumer,
                                  &error_count, test_error_handler);
    /* All consumed successfully, buffer should be empty */
    assert_int_equal(0, result);
    assert_int_equal(5, consumer_call_count);
    assert_int_equal(0, error_handler_call_count);
    assert_int_equal(15, total);  /* 1+2+3+4+5 = 15 */
    assert_true(wbuffer_is_empty(buf));
    wbuffer_free(buf);
}
static void test_consume_single_entry(void **state) {
    (void)state;
    reset_counters();
    wbuffer_t *buf = wbuffer_create(sizeof(test_entry_t), 10);
    test_entry_t entry = {.id = 100, .name = "single"};
    wbuffer_push(buf, &entry);
    int total = 0;
    int error_count = 0;
    int result = wbuffer_consume(buf, &total, test_consumer,
                                  &error_count, test_error_handler);
    assert_int_equal(0, result);
    assert_int_equal(1, consumer_call_count);
    assert_int_equal(0, error_handler_call_count);
    assert_int_equal(100, total);
    assert_true(wbuffer_is_empty(buf));
    wbuffer_free(buf);
}
/* ============================================================
 * Consume Tests - Error Handling Without Retry
 * ============================================================ */
static void test_consume_one_failure_no_retry(void **state) {
    (void)state;
    reset_counters();
    wbuffer_t *buf = wbuffer_create(sizeof(test_entry_t), 10);
    /* Push 3 entries */
    for (int i = 1; i <= 3; i++) {
        test_entry_t entry = {.id = i, .name = ""};
        wbuffer_push(buf, &entry);
    }
    /* Make second entry fail */
    consumer_fail_at = 1;  /* Fail on index 1 (second item) */
    error_handler_should_retry = 0;  /* Don't retry */
    int total = 0;
    int error_count = 0;
    int result = wbuffer_consume(buf, &total, test_consumer,
                                  &error_count, test_error_handler);
    /* Consumer called 3 times, error handler called once */
    assert_int_equal(0, result);
    assert_int_equal(3, consumer_call_count);
    assert_int_equal(1, error_handler_call_count);
    assert_int_equal(1, error_count);
    assert_int_equal(4, total);  /* 1 + (2 failed) + 3 = 1 + 3 = 4 */
    /* Buffer should be empty (failed entry not retried) */
    assert_true(wbuffer_is_empty(buf));
    wbuffer_free(buf);
}
static void test_consume_all_fail_no_retry(void **state) {
    (void)state;
    reset_counters();
    wbuffer_t *buf = wbuffer_create(sizeof(test_entry_t), 10);
    /* Push 3 entries */
    for (int i = 1; i <= 3; i++) {
        test_entry_t entry = {.id = i, .name = ""};
        wbuffer_push(buf, &entry);
    }
    /* Make first entry fail (only first one fails) */
    consumer_fail_at = 0;
    error_handler_should_retry = 0;
    int total = 0;
    int error_count = 0;
    wbuffer_consume(buf, &total, test_consumer,
                    &error_count, test_error_handler);
    /* All entries processed, but only first fails */
    assert_int_equal(3, consumer_call_count);
    assert_int_equal(1, error_handler_call_count);  /* Only first entry failed */
    assert_int_equal(1, error_count);
    assert_int_equal(5, total);  /* 2 + 3 = 5 (entry 1 failed) */
    /* Buffer empty (no retries) */
    assert_true(wbuffer_is_empty(buf));
    wbuffer_free(buf);
}
/* ============================================================
 * Consume Tests - Error Handling With Retry
 * ============================================================ */
static void test_consume_one_failure_with_retry(void **state) {
    (void)state;
    reset_counters();
    wbuffer_t *buf = wbuffer_create(sizeof(test_entry_t), 10);
    /* Push 3 entries */
    for (int i = 1; i <= 3; i++) {
        test_entry_t entry = {.id = i, .name = ""};
        wbuffer_push(buf, &entry);
    }
    /* Make second entry fail and request retry */
    consumer_fail_at = 1;
    error_handler_should_retry = 1;  /* Retry failed entries */
    int total = 0;
    int error_count = 0;
    int result = wbuffer_consume(buf, &total, test_consumer,
                                  &error_count, test_error_handler);
    /* Returns count of failed entries that need retry */
    assert_int_equal(1, result);
    assert_int_equal(3, consumer_call_count);
    assert_int_equal(1, error_handler_call_count);
    assert_int_equal(1, error_count);
    /* Buffer should contain 1 entry (the failed one) */
    assert_false(wbuffer_is_empty(buf));
    wbuffer_free(buf);
}
static void test_consume_multiple_failures_with_retry(void **state) {
    (void)state;
    reset_counters();
    wbuffer_t *buf = wbuffer_create(sizeof(test_entry_t), 10);
    /* Push 5 entries */
    for (int i = 1; i <= 5; i++) {
        test_entry_t entry = {.id = i, .name = ""};
        wbuffer_push(buf, &entry);
    }
    /* Make first entry fail and request retry */
    consumer_fail_at = 0;
    error_handler_should_retry = 1;
    int total = 0;
    int error_count = 0;
    int result = wbuffer_consume(buf, &total, test_consumer,
                                  &error_count, test_error_handler);
    /* First entry should be retried */
    assert_int_equal(1, result);
    assert_int_equal(5, consumer_call_count);
    assert_int_equal(1, error_handler_call_count);
    /* Buffer should contain 1 entry (the failed one) */
    assert_false(wbuffer_is_empty(buf));
    assert_int_equal(1, wbuffer_count(buf));
    /* Try consuming again - now succeed */
    reset_counters();
    consumer_fail_at = -1;  /* Don't fail */
    error_handler_should_retry = 0;
    total = 0;
    result = wbuffer_consume(buf, &total, test_consumer,
                             &error_count, test_error_handler);
    /* The retried entry consumed successfully this time */
    assert_int_equal(0, result);
    assert_int_equal(1, consumer_call_count);
    assert_int_equal(0, error_handler_call_count);
    assert_int_equal(1, total);  /* Entry with id=1 */
    assert_true(wbuffer_is_empty(buf));
    wbuffer_free(buf);
}
static int alternating_consumer_call_num = 0;
static int alternating_consumer(void *ctx, const void *entry) {
    (void)ctx;
    (void)entry;
    int idx = alternating_consumer_call_num++;
    if (idx % 2 == 1) {
        return -1;  /* Fail on indices 1, 3, 5 */
    }
    return 0;  /* Succeed on indices 0, 2, 4 */
}
static void test_consume_mixed_success_and_retry(void **state) {
    (void)state;
    reset_counters();
    wbuffer_t *buf = wbuffer_create(sizeof(test_entry_t), 10);
    /* Push 6 entries */
    for (int i = 1; i <= 6; i++) {
        test_entry_t entry = {.id = i * 10, .name = ""};
        wbuffer_push(buf, &entry);
    }
    /* Simulate pattern: succeed, fail, succeed, fail, succeed, fail */
    alternating_consumer_call_num = 0;
    error_handler_should_retry = 1;
    int error_count = 0;
    int result = wbuffer_consume(buf, NULL, alternating_consumer,
                                  &error_count, test_error_handler);
    /* 3 should fail and be retried (indices 1, 3, 5) */
    assert_int_equal(3, result);
    assert_int_equal(3, error_handler_call_count);
    wbuffer_free(buf);
}
/* ============================================================
 * Consume Tests - Sequential Retry Order
 * ============================================================ */
static void test_consume_retry_order_preserved(void **state) {
    (void)state;
    reset_counters();
    wbuffer_t *buf = wbuffer_create(sizeof(test_entry_t), 10);
    /* Push entries with specific IDs */
    int ids[] = {10, 20, 30, 40, 50};
    for (int i = 0; i < 5; i++) {
        test_entry_t entry = {.id = ids[i], .name = ""};
        wbuffer_push(buf, &entry);
    }
    /* Make entries 1 and 3 (20 and 40) fail */
    consumer_fail_at = 1;
    error_handler_should_retry = 1;
    int total = 0;
    wbuffer_consume(buf, &total, test_consumer, NULL, test_error_handler);
    /* After first consume: entry at index 1 failed */
    /* Reset and consume again to verify remaining entries */
    reset_counters();
    consumer_fail_at = -1;
    total = 0;
    wbuffer_consume(buf, &total, test_consumer, NULL, test_error_handler);
    /* The failed entry (id=20) should be consumed */
    assert_int_equal(1, consumer_call_count);
    wbuffer_free(buf);
}
/* ============================================================
 * Integration Tests
 * ============================================================ */
static void test_push_consume_cycle(void **state) {
    (void)state;
    reset_counters();
    wbuffer_t *buf = wbuffer_create(sizeof(test_entry_t), 5);
    assert_non_null(buf);
    /* Cycle 1: Push and consume */
    for (int i = 0; i < 3; i++) {
        test_entry_t entry = {.id = i, .name = ""};
        wbuffer_push(buf, &entry);
    }
    int total = 0;
    wbuffer_consume(buf, &total, test_consumer, NULL, test_error_handler);
    assert_true(wbuffer_is_empty(buf));
    /* Cycle 2: Push and consume again */
    reset_counters();
    for (int i = 10; i < 13; i++) {
        test_entry_t entry = {.id = i, .name = ""};
        wbuffer_push(buf, &entry);
    }
    total = 0;
    wbuffer_consume(buf, &total, test_consumer, NULL, test_error_handler);
    assert_int_equal(3, consumer_call_count);
    assert_true(wbuffer_is_empty(buf));
    wbuffer_free(buf);
}
static void test_full_buffer_consume_and_refill(void **state) {
    (void)state;
    reset_counters();
    wbuffer_t *buf = wbuffer_create(sizeof(test_entry_t), 3);
    assert_non_null(buf);
    /* Fill buffer completely */
    for (int i = 0; i < 3; i++) {
        test_entry_t entry = {.id = i, .name = ""};
        wbuffer_push(buf, &entry);
    }
    assert_true(wbuffer_is_full(buf));
    /* Consume all */
    wbuffer_consume(buf, NULL, test_consumer, NULL, test_error_handler);
    assert_true(wbuffer_is_empty(buf));
    /* Refill */
    for (int i = 100; i < 103; i++) {
        test_entry_t entry = {.id = i, .name = ""};
        int result = wbuffer_push(buf, &entry);
        assert_int_equal(0, result);
    }
    assert_true(wbuffer_is_full(buf));
    wbuffer_free(buf);
}
/* ============================================================
 * Edge Cases
 * ============================================================ */
static int int_consumer(void *ctx, const void *entry) {
    int *sum = (int*)ctx;
    const int *val = (const int*)entry;
    if (sum) *sum += *val;
    return 0;
}
static int always_fail_consumer(void *ctx, const void *entry) {
    (void)ctx;
    (void)entry;
    return -1;
}
static int always_retry_handler(void *ctx, const void *entry) {
    (void)ctx;
    (void)entry;
    return 1;
}
static void test_consume_with_different_entry_sizes(void **state) {
    (void)state;
    reset_counters();

    /* Test with int */
    wbuffer_t *buf_int = wbuffer_create(sizeof(int), 5);
    assert_non_null(buf_int);

    for (int i = 0; i < 5; i++) {
        int val = i * 100;
        wbuffer_push(buf_int, &val);
    }

    int sum = 0;
    wbuffer_consume(buf_int, &sum, int_consumer, NULL, test_error_handler);
    assert_int_equal(1000, sum);  /* 0+100+200+300+400 */

    wbuffer_free(buf_int);

    /* Test with large struct */
    typedef struct {
        char data[256];
        int value;
    } large_entry_t;

    wbuffer_t *buf_large = wbuffer_create(sizeof(large_entry_t), 3);
    assert_non_null(buf_large);

    for (int i = 0; i < 3; i++) {
        large_entry_t entry = {.value = i};
        snprintf(entry.data, sizeof(entry.data), "large_%d", i);
        wbuffer_push(buf_large, &entry);
    }

    assert_int_equal(3, wbuffer_consume(buf_large, NULL,
        always_fail_consumer, NULL, always_retry_handler));

    wbuffer_free(buf_large);
}
/* ============================================================
 * Main
 * ============================================================ */
int main(void) {
    const struct CMUnitTest tests[] = {
        /* Initialization */
        cmocka_unit_test(test_init_destroy_basic),
        cmocka_unit_test(test_init_large_capacity),
        cmocka_unit_test(test_init_small_entry),
        /* Push */
        cmocka_unit_test(test_push_single),
        cmocka_unit_test(test_push_multiple),
        cmocka_unit_test(test_push_until_full),
        /* Consume - Success */
        cmocka_unit_test(test_consume_empty_buffer),
        cmocka_unit_test(test_consume_all_success),
        cmocka_unit_test(test_consume_single_entry),
        /* Consume - Errors without retry */
        cmocka_unit_test(test_consume_one_failure_no_retry),
        cmocka_unit_test(test_consume_all_fail_no_retry),
        /* Consume - Errors with retry */
        cmocka_unit_test(test_consume_one_failure_with_retry),
        cmocka_unit_test(test_consume_multiple_failures_with_retry),
        cmocka_unit_test(test_consume_mixed_success_and_retry),
        /* Retry order */
        cmocka_unit_test(test_consume_retry_order_preserved),
        /* Integration */
        cmocka_unit_test(test_push_consume_cycle),
        cmocka_unit_test(test_full_buffer_consume_and_refill),
        /* Edge cases */
        cmocka_unit_test(test_consume_with_different_entry_sizes),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
