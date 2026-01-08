/*
 * test_wvector.c - Comprehensive tests for wvector (generic dynamic array)
 */

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <setjmp.h>
#include <cmocka.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "wvector.h"

/* ============================================================
 * Test Data Structures
 * ============================================================ */

typedef struct {
    int id;
    char name[32];
} test_item_t;

static int cleanup_count = 0;

static void test_cleanup(void *element) {
    free(element);
    cleanup_count++;
}

static int compare_ints(const void *a, const void *b) {
    int ia = *(int*)a;
    int ib = *(int*)b;
    return ia == ib ? 0 : (ia < ib ? -1 : 1);
}

static int compare_items(const void *a, const void *b) {
    const test_item_t *item_a = (const test_item_t *)a;
    const test_item_t *item_b = (const test_item_t *)b;
    return item_a->id == item_b->id ? 0 : (item_a->id < item_b->id ? -1 : 1);
}

/* ============================================================
 * Lifecycle Tests
 * ============================================================ */

static void test_create_destroy(void **state) {
    (void)state;

    /* Create with default capacity */
    wvector_t *vec = wvector_create(0, NULL);
    assert_non_null(vec);
    assert_int_equal(wvector_size(vec), 0);
    assert_true(wvector_is_empty(vec));
    wvector_destroy(vec);

    /* Create with specific capacity */
    vec = wvector_create(100, NULL);
    assert_non_null(vec);
    assert_int_equal(wvector_size(vec), 0);
    wvector_destroy(vec);

    /* Destroy NULL is safe */
    wvector_destroy(NULL);
}

static void test_create_with_cleanup(void **state) {
    (void)state;

    cleanup_count = 0;
    wvector_t *vec = wvector_create(0, test_cleanup);
    assert_non_null(vec);

    /* Add some elements */
    for (int i = 0; i < 5; i++) {
        int *val = malloc(sizeof(int));
        *val = i;
        assert_true(wvector_push(vec, val));
    }

    /* Destroy should call cleanup for all elements */
    wvector_destroy(vec);
    assert_int_equal(cleanup_count, 5);
}

/* ============================================================
 * Basic Operations Tests
 * ============================================================ */

static void test_push_pop(void **state) {
    (void)state;

    wvector_t *vec = wvector_create(0, NULL);
    assert_non_null(vec);

    /* Push elements */
    int a = 10, b = 20, c = 30;
    assert_true(wvector_push(vec, &a));
    assert_int_equal(wvector_size(vec), 1);
    assert_false(wvector_is_empty(vec));

    assert_true(wvector_push(vec, &b));
    assert_int_equal(wvector_size(vec), 2);

    assert_true(wvector_push(vec, &c));
    assert_int_equal(wvector_size(vec), 3);

    /* Pop elements (LIFO) */
    int *val = wvector_pop(vec);
    assert_non_null(val);
    assert_int_equal(*val, 30);
    assert_int_equal(wvector_size(vec), 2);

    val = wvector_pop(vec);
    assert_int_equal(*val, 20);
    assert_int_equal(wvector_size(vec), 1);

    val = wvector_pop(vec);
    assert_int_equal(*val, 10);
    assert_int_equal(wvector_size(vec), 0);
    assert_true(wvector_is_empty(vec));

    /* Pop from empty vector */
    val = wvector_pop(vec);
    assert_null(val);

    wvector_destroy(vec);
}

static void test_get_set(void **state) {
    (void)state;

    wvector_t *vec = wvector_create(0, NULL);
    int a = 10, b = 20, c = 30, d = 40;

    wvector_push(vec, &a);
    wvector_push(vec, &b);
    wvector_push(vec, &c);

    /* Get elements */
    assert_int_equal(*(int*)wvector_get(vec, 0), 10);
    assert_int_equal(*(int*)wvector_get(vec, 1), 20);
    assert_int_equal(*(int*)wvector_get(vec, 2), 30);

    /* Get out of bounds */
    assert_null(wvector_get(vec, 3));
    assert_null(wvector_get(vec, 100));

    /* Set element */
    assert_true(wvector_set(vec, 1, &d));
    assert_int_equal(*(int*)wvector_get(vec, 1), 40);

    /* Set out of bounds */
    assert_false(wvector_set(vec, 10, &d));

    wvector_destroy(vec);
}

static void test_clear(void **state) {
    (void)state;

    cleanup_count = 0;
    wvector_t *vec = wvector_create(0, test_cleanup);

    /* Add elements */
    for (int i = 0; i < 10; i++) {
        int *val = malloc(sizeof(int));
        *val = i;
        wvector_push(vec, val);
    }
    assert_int_equal(wvector_size(vec), 10);

    /* Clear should call cleanup and reset size */
    wvector_clear(vec);
    assert_int_equal(wvector_size(vec), 0);
    assert_true(wvector_is_empty(vec));
    assert_int_equal(cleanup_count, 10);

    /* Can still use after clear */
    int *a = malloc(sizeof(int));
    *a = 100;
    assert_true(wvector_push(vec, a));
    assert_int_equal(wvector_size(vec), 1);

    wvector_destroy(vec);
}

/* ============================================================
 * Growth Tests
 * ============================================================ */

static void test_capacity_growth(void **state) {
    (void)state;

    wvector_t *vec = wvector_create(2, NULL);
    int values[100];

    /* Push beyond initial capacity */
    for (int i = 0; i < 100; i++) {
        values[i] = i;
        assert_true(wvector_push(vec, &values[i]));
    }

    assert_int_equal(wvector_size(vec), 100);

    /* Verify all elements */
    for (int i = 0; i < 100; i++) {
        assert_int_equal(*(int*)wvector_get(vec, i), i);
    }

    wvector_destroy(vec);
}

/* ============================================================
 * Search Operations Tests
 * ============================================================ */

static void test_find(void **state) {
    (void)state;

    wvector_t *vec = wvector_create(0, NULL);
    int a = 10, b = 20, c = 30, d = 40, search;

    wvector_push(vec, &a);
    wvector_push(vec, &b);
    wvector_push(vec, &c);
    wvector_push(vec, &d);

    /* Find existing element */
    search = 20;
    int *found = wvector_find(vec, &search, compare_ints);
    assert_non_null(found);
    assert_int_equal(*found, 20);

    /* Find non-existing element */
    search = 999;
    found = wvector_find(vec, &search, compare_ints);
    assert_null(found);

    /* Find with NULL vector */
    found = wvector_find(NULL, &search, compare_ints);
    assert_null(found);

    /* Find with NULL comparator */
    found = wvector_find(vec, &search, NULL);
    assert_null(found);

    wvector_destroy(vec);
}

static void test_find_index(void **state) {
    (void)state;

    wvector_t *vec = wvector_create(0, NULL);
    int a = 10, b = 20, c = 30, search;
    size_t index;

    wvector_push(vec, &a);
    wvector_push(vec, &b);
    wvector_push(vec, &c);

    /* Find index of existing element */
    search = 20;
    assert_true(wvector_find_index(vec, &search, compare_ints, &index));
    assert_int_equal(index, 1);

    /* Find first element */
    search = 10;
    assert_true(wvector_find_index(vec, &search, compare_ints, &index));
    assert_int_equal(index, 0);

    /* Find last element */
    search = 30;
    assert_true(wvector_find_index(vec, &search, compare_ints, &index));
    assert_int_equal(index, 2);

    /* Find non-existing element */
    search = 999;
    assert_false(wvector_find_index(vec, &search, compare_ints, &index));

    /* NULL output parameter is ok */
    search = 20;
    assert_true(wvector_find_index(vec, &search, compare_ints, NULL));

    wvector_destroy(vec);
}

static void test_remove(void **state) {
    (void)state;

    cleanup_count = 0;
    wvector_t *vec = wvector_create(0, test_cleanup);

    /* Add elements */
    for (int i = 0; i < 5; i++) {
        int *val = malloc(sizeof(int));
        *val = i * 10;
        wvector_push(vec, val);
    }

    /* Remove middle element */
    int search = 20;
    assert_true(wvector_remove(vec, &search, compare_ints));
    assert_int_equal(wvector_size(vec), 4);
    assert_int_equal(cleanup_count, 1);

    /* Verify order after removal */
    assert_int_equal(*(int*)wvector_get(vec, 0), 0);
    assert_int_equal(*(int*)wvector_get(vec, 1), 10);
    assert_int_equal(*(int*)wvector_get(vec, 2), 30);  /* Was at index 3 */
    assert_int_equal(*(int*)wvector_get(vec, 3), 40);  /* Was at index 4 */

    /* Remove first element */
    search = 0;
    assert_true(wvector_remove(vec, &search, compare_ints));
    assert_int_equal(wvector_size(vec), 3);
    assert_int_equal(*(int*)wvector_get(vec, 0), 10);

    /* Remove last element */
    search = 40;
    assert_true(wvector_remove(vec, &search, compare_ints));
    assert_int_equal(wvector_size(vec), 2);

    /* Remove non-existing element */
    search = 999;
    assert_false(wvector_remove(vec, &search, compare_ints));
    assert_int_equal(wvector_size(vec), 2);

    wvector_destroy(vec);
}

/* ============================================================
 * Iteration Tests
 * ============================================================ */

static int foreach_sum = 0;

static bool sum_foreach(void *element, size_t index, void *user_data) {
    (void)index;
    (void)user_data;
    foreach_sum += *(int*)element;
    return true;  /* Continue */
}

static bool stop_at_20(void *element, size_t index, void *user_data) {
    (void)user_data;
    int val = *(int*)element;
    if (val == 20) {
        *(size_t*)user_data = index;
        return false;  /* Stop */
    }
    return true;
}

static void test_foreach(void **state) {
    (void)state;

    wvector_t *vec = wvector_create(0, NULL);
    int a = 10, b = 20, c = 30, d = 40;

    wvector_push(vec, &a);
    wvector_push(vec, &b);
    wvector_push(vec, &c);
    wvector_push(vec, &d);

    /* Sum all elements */
    foreach_sum = 0;
    wvector_foreach(vec, sum_foreach, NULL);
    assert_int_equal(foreach_sum, 100);

    /* Stop early */
    size_t stop_index = 999;
    wvector_foreach(vec, stop_at_20, &stop_index);
    assert_int_equal(stop_index, 1);

    /* Foreach with NULL vector */
    wvector_foreach(NULL, sum_foreach, NULL);

    /* Foreach with NULL function */
    wvector_foreach(vec, NULL, NULL);

    wvector_destroy(vec);
}

/* ============================================================
 * Edge Cases Tests
 * ============================================================ */

static void test_null_parameters(void **state) {
    (void)state;

    /* Functions with NULL vector */
    assert_false(wvector_push(NULL, (void*)1));
    assert_null(wvector_pop(NULL));
    assert_null(wvector_get(NULL, 0));
    assert_false(wvector_set(NULL, 0, (void*)1));
    assert_int_equal(wvector_size(NULL), 0);
    assert_true(wvector_is_empty(NULL));
    wvector_clear(NULL);  /* Should not crash */
}

static void test_set_with_cleanup(void **state) {
    (void)state;

    cleanup_count = 0;
    wvector_t *vec = wvector_create(0, test_cleanup);

    /* Add elements */
    int *a = malloc(sizeof(int));
    int *b = malloc(sizeof(int));
    int *c = malloc(sizeof(int));
    *a = 10;
    *b = 20;
    *c = 30;

    wvector_push(vec, a);
    wvector_push(vec, b);

    /* Set should cleanup old element */
    wvector_set(vec, 0, c);
    assert_int_equal(cleanup_count, 1);
    assert_int_equal(*(int*)wvector_get(vec, 0), 30);

    wvector_destroy(vec);
}

static void test_complex_data_type(void **state) {
    (void)state;

    wvector_t *vec = wvector_create(0, free);

    /* Add complex items */
    for (int i = 0; i < 10; i++) {
        test_item_t *item = malloc(sizeof(test_item_t));
        item->id = i;
        snprintf(item->name, sizeof(item->name), "Item_%d", i);
        wvector_push(vec, item);
    }

    /* Search for item */
    test_item_t search = {.id = 5};
    test_item_t *found = wvector_find(vec, &search, compare_items);
    assert_non_null(found);
    assert_int_equal(found->id, 5);
    assert_string_equal(found->name, "Item_5");

    /* Remove item */
    assert_true(wvector_remove(vec, &search, compare_items));
    assert_int_equal(wvector_size(vec), 9);

    wvector_destroy(vec);
}

static void test_empty_operations(void **state) {
    (void)state;

    wvector_t *vec = wvector_create(0, NULL);

    /* Operations on empty vector */
    assert_null(wvector_pop(vec));
    assert_null(wvector_get(vec, 0));
    assert_false(wvector_set(vec, 0, (void*)1));
    assert_null(wvector_find(vec, (void*)1, compare_ints));
    assert_false(wvector_find_index(vec, (void*)1, compare_ints, NULL));
    assert_false(wvector_remove(vec, (void*)1, compare_ints));
    wvector_clear(vec);  /* Should not crash */

    wvector_destroy(vec);
}

/* ============================================================
 * Test Runner
 * ============================================================ */

int main(void) {
    const struct CMUnitTest tests[] = {
        /* Lifecycle */
        cmocka_unit_test(test_create_destroy),
        cmocka_unit_test(test_create_with_cleanup),

        /* Basic operations */
        cmocka_unit_test(test_push_pop),
        cmocka_unit_test(test_get_set),
        cmocka_unit_test(test_clear),

        /* Growth */
        cmocka_unit_test(test_capacity_growth),

        /* Search operations */
        cmocka_unit_test(test_find),
        cmocka_unit_test(test_find_index),
        cmocka_unit_test(test_remove),

        /* Iteration */
        cmocka_unit_test(test_foreach),

        /* Edge cases */
        cmocka_unit_test(test_null_parameters),
        cmocka_unit_test(test_set_with_cleanup),
        cmocka_unit_test(test_complex_data_type),
        cmocka_unit_test(test_empty_operations),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
