/*
 * test_wqueue.c - Comprehensive tests for wqueue module
 */

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <setjmp.h>
#include <cmocka.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "wqueue.h"

/* Test entry structure */
typedef struct {
    int id;
    char data[64];
} test_entry_t;

/* ============================================================
 * Test Helpers
 * ============================================================ */

static int on_full_called = 0;

static void test_on_full(void *arg) {
    (void)arg;
    on_full_called++;
}

static void test_free_entry(void *entry_ptr, void *arg) {
    (void)arg;
    free(entry_ptr);
}

static void reset_counters(void) {
    on_full_called = 0;
}

/* ============================================================
 * Basic Creation and Destruction Tests
 * ============================================================ */

static void test_create_destroy_basic(void **state) {
    (void)state;
    reset_counters();

    wqueue_t *q = wqueue_create(sizeof(void*), 10, test_free_entry, NULL);
    assert_non_null(q);

    wqueue_destroy(q);
}

static void test_create_invalid_params(void **state) {
    (void)state;
    reset_counters();

    /* Zero entry size */
    wqueue_t *q1 = wqueue_create(0, 10, NULL, NULL);
    assert_null(q1);

    /* Zero capacity */
    wqueue_t *q2 = wqueue_create(sizeof(void*), 0, NULL, NULL);
    assert_null(q2);
}

static void test_destroy_null(void **state) {
    (void)state;
    reset_counters();

    /* Should not crash */
    wqueue_destroy(NULL);
}

/* ============================================================
 * Push Tests
 * ============================================================ */

static void test_push_single(void **state) {
    (void)state;
    reset_counters();

    wqueue_t *q = wqueue_create(sizeof(void*), 10, test_free_entry, NULL);
    assert_non_null(q);

    test_entry_t *entry = malloc(sizeof(test_entry_t));
    entry->id = 1;
    snprintf(entry->data, sizeof(entry->data), "test_data_1");

    bool ok = wqueue_push(q, entry);
    assert_true(ok);
    assert_int_equal(1, wqueue_depth(q));

    wqueue_destroy(q);  /* Queue owns entry and will free it */
}

static void test_push_null_queue(void **state) {
    (void)state;
    reset_counters();

    test_entry_t *entry = malloc(sizeof(test_entry_t));
    bool ok = wqueue_push(NULL, entry);
    assert_false(ok);
    free(entry);
}

static void test_push_null_entry(void **state) {
    (void)state;
    reset_counters();

    wqueue_t *q = wqueue_create(sizeof(void*), 10, test_free_entry, NULL);

    bool ok = wqueue_push(q, NULL);
    assert_false(ok);

    wqueue_destroy(q);
}

static void test_push_multiple(void **state) {
    (void)state;
    reset_counters();

    wqueue_t *q = wqueue_create(sizeof(void*), 10, test_free_entry, NULL);

    /* Push 5 items */
    for (int i = 0; i < 5; i++) {
        test_entry_t *entry = malloc(sizeof(test_entry_t));
        entry->id = i;
        snprintf(entry->data, sizeof(entry->data), "data_%d", i);

        bool ok = wqueue_push(q, entry);
        assert_true(ok);
    }

    assert_int_equal(5, wqueue_depth(q));

    wqueue_destroy(q);  /* Queue owns all entries and will free them */
}

/* ============================================================
 * Queue Full Tests
 * ============================================================ */

static void test_push_full(void **state) {
    (void)state;
    reset_counters();

    /* Small queue */
    wqueue_t *q = wqueue_create(sizeof(void*), 2, test_free_entry, NULL);

    /* Fill queue */
    test_entry_t *e1 = malloc(sizeof(test_entry_t));
    test_entry_t *e2 = malloc(sizeof(test_entry_t));
    assert_true(wqueue_push(q, e1));
    assert_true(wqueue_push(q, e2));

    /* Try to push when full - should fail */
    test_entry_t *e3 = malloc(sizeof(test_entry_t));
    bool ok = wqueue_push(q, e3);
    assert_false(ok);

    /* e3 was rejected, so we need to free it manually */
    free(e3);

    wqueue_destroy(q);  /* Queue owns e1 and e2, will free them */
}


/* ============================================================
 * Depth Tests
 * ============================================================ */

static void test_depth_null(void **state) {
    (void)state;
    reset_counters();

    uint64_t depth = wqueue_depth(NULL);
    assert_int_equal(0, depth);
}

static void test_depth_tracking(void **state) {
    (void)state;
    reset_counters();

    wqueue_t *q = wqueue_create(sizeof(void*), 10, test_free_entry, NULL);

    assert_int_equal(0, wqueue_depth(q));

    /* Push 3 entries */
    for (int i = 0; i < 3; i++) {
        test_entry_t *entry = malloc(sizeof(test_entry_t));
        entry->id = i;
        wqueue_push(q, entry);
    }

    assert_int_equal(3, wqueue_depth(q));

    wqueue_destroy(q);  /* Queue owns all entries and will free them */
}


/* ============================================================
 * Main
 * ============================================================ */

int main(void) {
    const struct CMUnitTest tests[] = {
        /* Creation/destruction */
        cmocka_unit_test(test_create_destroy_basic),
        cmocka_unit_test(test_create_invalid_params),
        cmocka_unit_test(test_destroy_null),

        /* Push */
        cmocka_unit_test(test_push_single),
        cmocka_unit_test(test_push_null_queue),
        cmocka_unit_test(test_push_null_entry),
        cmocka_unit_test(test_push_multiple),

        /* Queue full */
        cmocka_unit_test(test_push_full),

        /* Depth */
        cmocka_unit_test(test_depth_null),
        cmocka_unit_test(test_depth_tracking),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
