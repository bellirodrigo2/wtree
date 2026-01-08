/*
 * test_wtree3_extractor_registry.c - Comprehensive tests for extractor registry
 */

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <setjmp.h>
#include <cmocka.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "wtree3.h"
#include "wtree3_extractor_registry.h"

/* ============================================================
 * Mock Extractor Functions
 * ============================================================ */

static bool extractor_v1_flags0(const void *value, size_t value_len,
                                 void *user_data,
                                 void **out_key, size_t *out_len) {
    (void)value_len;
    (void)user_data;
    char *key = malloc(5);
    strcpy(key, "v1f0");
    *out_key = key;
    *out_len = 4;
    return true;
}

static bool extractor_v1_flags1(const void *value, size_t value_len,
                                 void *user_data,
                                 void **out_key, size_t *out_len) {
    (void)value_len;
    (void)user_data;
    char *key = malloc(5);
    strcpy(key, "v1f1");
    *out_key = key;
    *out_len = 4;
    return true;
}

static bool extractor_v2_flags0(const void *value, size_t value_len,
                                 void *user_data,
                                 void **out_key, size_t *out_len) {
    (void)value_len;
    (void)user_data;
    char *key = malloc(5);
    strcpy(key, "v2f0");
    *out_key = key;
    *out_len = 4;
    return true;
}

/* ============================================================
 * Helper Functions
 * ============================================================ */

static uint64_t make_id(uint32_t version, uint32_t flags) {
    return ((uint64_t)version << 32) | flags;
}

/* ============================================================
 * Lifecycle Tests
 * ============================================================ */

static void test_create_destroy(void **state) {
    (void)state;

    /* Create registry */
    wtree3_extractor_registry_t *reg = wtree3_extractor_registry_create();
    assert_non_null(reg);
    assert_int_equal(wtree3_extractor_registry_count(reg), 0);

    /* Destroy */
    wtree3_extractor_registry_destroy(reg);

    /* Destroy NULL is safe */
    wtree3_extractor_registry_destroy(NULL);
}

/* ============================================================
 * Basic Operations Tests
 * ============================================================ */

static void test_set_and_get(void **state) {
    (void)state;

    wtree3_extractor_registry_t *reg = wtree3_extractor_registry_create();
    uint64_t id1 = make_id(1, 0);
    uint64_t id2 = make_id(1, 1);

    /* Set extractor */
    assert_true(wtree3_extractor_registry_set(reg, id1, extractor_v1_flags0));
    assert_int_equal(wtree3_extractor_registry_count(reg), 1);

    /* Get extractor */
    wtree3_index_key_fn fn = wtree3_extractor_registry_get(reg, id1);
    assert_non_null(fn);
    assert_ptr_equal(fn, extractor_v1_flags0);

    /* Set another extractor */
    assert_true(wtree3_extractor_registry_set(reg, id2, extractor_v1_flags1));
    assert_int_equal(wtree3_extractor_registry_count(reg), 2);

    /* Get both */
    fn = wtree3_extractor_registry_get(reg, id1);
    assert_ptr_equal(fn, extractor_v1_flags0);

    fn = wtree3_extractor_registry_get(reg, id2);
    assert_ptr_equal(fn, extractor_v1_flags1);

    wtree3_extractor_registry_destroy(reg);
}

static void test_has(void **state) {
    (void)state;

    wtree3_extractor_registry_t *reg = wtree3_extractor_registry_create();
    uint64_t id1 = make_id(1, 0);
    uint64_t id2 = make_id(2, 0);

    /* Initially not present */
    assert_false(wtree3_extractor_registry_has(reg, id1));

    /* After set */
    wtree3_extractor_registry_set(reg, id1, extractor_v1_flags0);
    assert_true(wtree3_extractor_registry_has(reg, id1));
    assert_false(wtree3_extractor_registry_has(reg, id2));

    wtree3_extractor_registry_destroy(reg);
}

static void test_duplicate_registration(void **state) {
    (void)state;

    wtree3_extractor_registry_t *reg = wtree3_extractor_registry_create();
    uint64_t id = make_id(1, 0);

    /* First registration succeeds */
    assert_true(wtree3_extractor_registry_set(reg, id, extractor_v1_flags0));
    assert_int_equal(wtree3_extractor_registry_count(reg), 1);

    /* Duplicate registration fails */
    assert_false(wtree3_extractor_registry_set(reg, id, extractor_v1_flags1));
    assert_int_equal(wtree3_extractor_registry_count(reg), 1);

    /* Original extractor is still registered */
    wtree3_index_key_fn fn = wtree3_extractor_registry_get(reg, id);
    assert_ptr_equal(fn, extractor_v1_flags0);

    wtree3_extractor_registry_destroy(reg);
}

/* ============================================================
 * Version and Flags Tests
 * ============================================================ */

static void test_multiple_versions(void **state) {
    (void)state;

    wtree3_extractor_registry_t *reg = wtree3_extractor_registry_create();

    /* Register multiple versions with same flags */
    uint64_t id_v1 = make_id(1, 0);
    uint64_t id_v2 = make_id(2, 0);
    uint64_t id_v3 = make_id(3, 0);

    assert_true(wtree3_extractor_registry_set(reg, id_v1, extractor_v1_flags0));
    assert_true(wtree3_extractor_registry_set(reg, id_v2, extractor_v2_flags0));
    assert_true(wtree3_extractor_registry_set(reg, id_v3, extractor_v1_flags1));

    /* Each version can be retrieved independently */
    assert_ptr_equal(wtree3_extractor_registry_get(reg, id_v1), extractor_v1_flags0);
    assert_ptr_equal(wtree3_extractor_registry_get(reg, id_v2), extractor_v2_flags0);
    assert_ptr_equal(wtree3_extractor_registry_get(reg, id_v3), extractor_v1_flags1);

    wtree3_extractor_registry_destroy(reg);
}

static void test_multiple_flags(void **state) {
    (void)state;

    wtree3_extractor_registry_t *reg = wtree3_extractor_registry_create();

    /* Register all 4 flag combinations for version 1.0 */
    uint64_t id_00 = make_id(WTREE3_VERSION(1, 0), 0x00);  /* non-unique, non-sparse */
    uint64_t id_01 = make_id(WTREE3_VERSION(1, 0), 0x01);  /* unique, non-sparse */
    uint64_t id_02 = make_id(WTREE3_VERSION(1, 0), 0x02);  /* non-unique, sparse */
    uint64_t id_03 = make_id(WTREE3_VERSION(1, 0), 0x03);  /* unique, sparse */

    assert_true(wtree3_extractor_registry_set(reg, id_00, extractor_v1_flags0));
    assert_true(wtree3_extractor_registry_set(reg, id_01, extractor_v1_flags0));
    assert_true(wtree3_extractor_registry_set(reg, id_02, extractor_v1_flags0));
    assert_true(wtree3_extractor_registry_set(reg, id_03, extractor_v1_flags0));

    assert_int_equal(wtree3_extractor_registry_count(reg), 4);

    /* All can be retrieved */
    assert_non_null(wtree3_extractor_registry_get(reg, id_00));
    assert_non_null(wtree3_extractor_registry_get(reg, id_01));
    assert_non_null(wtree3_extractor_registry_get(reg, id_02));
    assert_non_null(wtree3_extractor_registry_get(reg, id_03));

    wtree3_extractor_registry_destroy(reg);
}

static void test_version_macro(void **state) {
    (void)state;

    /* Test WTREE3_VERSION macro */
    uint32_t v1_0 = WTREE3_VERSION(1, 0);
    uint32_t v1_5 = WTREE3_VERSION(1, 5);
    uint32_t v2_0 = WTREE3_VERSION(2, 0);

    /* Versions should be different */
    assert_int_not_equal(v1_0, v1_5);
    assert_int_not_equal(v1_0, v2_0);
    assert_int_not_equal(v1_5, v2_0);

    /* Upper 16 bits = major, lower 16 bits = minor */
    assert_int_equal(v1_0 >> 16, 1);
    assert_int_equal(v1_0 & 0xFFFF, 0);
    assert_int_equal(v1_5 >> 16, 1);
    assert_int_equal(v1_5 & 0xFFFF, 5);
    assert_int_equal(v2_0 >> 16, 2);
    assert_int_equal(v2_0 & 0xFFFF, 0);
}

/* ============================================================
 * Large Scale Tests
 * ============================================================ */

static void test_many_extractors(void **state) {
    (void)state;

    wtree3_extractor_registry_t *reg = wtree3_extractor_registry_create();

    /* Register many extractors (different versions and flags) */
    const int NUM_VERSIONS = 10;
    const int NUM_FLAGS = 4;

    for (int v = 0; v < NUM_VERSIONS; v++) {
        for (int f = 0; f < NUM_FLAGS; f++) {
            uint64_t id = make_id(WTREE3_VERSION(v, 0), f);
            assert_true(wtree3_extractor_registry_set(reg, id, extractor_v1_flags0));
        }
    }

    assert_int_equal(wtree3_extractor_registry_count(reg), NUM_VERSIONS * NUM_FLAGS);

    /* Verify all can be retrieved */
    for (int v = 0; v < NUM_VERSIONS; v++) {
        for (int f = 0; f < NUM_FLAGS; f++) {
            uint64_t id = make_id(WTREE3_VERSION(v, 0), f);
            assert_true(wtree3_extractor_registry_has(reg, id));
            assert_non_null(wtree3_extractor_registry_get(reg, id));
        }
    }

    wtree3_extractor_registry_destroy(reg);
}

/* ============================================================
 * NULL Parameter Tests
 * ============================================================ */

static void test_null_parameters(void **state) {
    (void)state;

    wtree3_extractor_registry_t *reg = wtree3_extractor_registry_create();
    uint64_t id = make_id(1, 0);

    /* Set with NULL registry */
    assert_false(wtree3_extractor_registry_set(NULL, id, extractor_v1_flags0));

    /* Set with NULL function */
    assert_false(wtree3_extractor_registry_set(reg, id, NULL));

    /* Get with NULL registry */
    assert_null(wtree3_extractor_registry_get(NULL, id));

    /* Has with NULL registry */
    assert_false(wtree3_extractor_registry_has(NULL, id));

    /* Count with NULL registry */
    assert_int_equal(wtree3_extractor_registry_count(NULL), 0);

    wtree3_extractor_registry_destroy(reg);
}

/* ============================================================
 * Non-existent Entry Tests
 * ============================================================ */

static void test_get_nonexistent(void **state) {
    (void)state;

    wtree3_extractor_registry_t *reg = wtree3_extractor_registry_create();

    /* Get from empty registry */
    uint64_t id1 = make_id(1, 0);
    assert_null(wtree3_extractor_registry_get(reg, id1));

    /* Add one entry */
    wtree3_extractor_registry_set(reg, id1, extractor_v1_flags0);

    /* Get different ID */
    uint64_t id2 = make_id(2, 0);
    assert_null(wtree3_extractor_registry_get(reg, id2));

    wtree3_extractor_registry_destroy(reg);
}

/* ============================================================
 * Functional Tests
 * ============================================================ */

static void test_extractor_execution(void **state) {
    (void)state;

    wtree3_extractor_registry_t *reg = wtree3_extractor_registry_create();
    uint64_t id = make_id(1, 0);

    /* Register extractor */
    wtree3_extractor_registry_set(reg, id, extractor_v1_flags0);

    /* Get and execute extractor */
    wtree3_index_key_fn fn = wtree3_extractor_registry_get(reg, id);
    assert_non_null(fn);

    void *key = NULL;
    size_t key_len = 0;
    bool result = fn("test_value", 10, NULL, &key, &key_len);

    assert_true(result);
    assert_non_null(key);
    assert_int_equal(key_len, 4);
    assert_string_equal((char*)key, "v1f0");

    free(key);
    wtree3_extractor_registry_destroy(reg);
}

static void test_different_extractors_same_version(void **state) {
    (void)state;

    wtree3_extractor_registry_t *reg = wtree3_extractor_registry_create();

    /* Register different extractors for different flags of same version */
    uint64_t id0 = make_id(WTREE3_VERSION(1, 0), 0x00);
    uint64_t id1 = make_id(WTREE3_VERSION(1, 0), 0x01);

    wtree3_extractor_registry_set(reg, id0, extractor_v1_flags0);
    wtree3_extractor_registry_set(reg, id1, extractor_v1_flags1);

    /* Execute both and verify different results */
    wtree3_index_key_fn fn0 = wtree3_extractor_registry_get(reg, id0);
    wtree3_index_key_fn fn1 = wtree3_extractor_registry_get(reg, id1);

    void *key0 = NULL, *key1 = NULL;
    size_t len0 = 0, len1 = 0;

    fn0("value", 5, NULL, &key0, &len0);
    fn1("value", 5, NULL, &key1, &len1);

    assert_string_equal((char*)key0, "v1f0");
    assert_string_equal((char*)key1, "v1f1");

    free(key0);
    free(key1);
    wtree3_extractor_registry_destroy(reg);
}

/* ============================================================
 * Edge Cases
 * ============================================================ */

static void test_extreme_version_numbers(void **state) {
    (void)state;

    wtree3_extractor_registry_t *reg = wtree3_extractor_registry_create();

    /* Test with extreme version numbers */
    uint64_t id_max = make_id(0xFFFFFFFF, 0xFF);
    uint64_t id_zero = make_id(0, 0);

    assert_true(wtree3_extractor_registry_set(reg, id_max, extractor_v1_flags0));
    assert_true(wtree3_extractor_registry_set(reg, id_zero, extractor_v1_flags1));

    assert_non_null(wtree3_extractor_registry_get(reg, id_max));
    assert_non_null(wtree3_extractor_registry_get(reg, id_zero));

    wtree3_extractor_registry_destroy(reg);
}

static void test_count_accuracy(void **state) {
    (void)state;

    wtree3_extractor_registry_t *reg = wtree3_extractor_registry_create();

    assert_int_equal(wtree3_extractor_registry_count(reg), 0);

    /* Add entries one by one and verify count */
    for (int i = 1; i <= 20; i++) {
        uint64_t id = make_id(i, 0);
        wtree3_extractor_registry_set(reg, id, extractor_v1_flags0);
        assert_int_equal(wtree3_extractor_registry_count(reg), i);
    }

    /* Try to add duplicate - count shouldn't change */
    uint64_t dup_id = make_id(5, 0);
    wtree3_extractor_registry_set(reg, dup_id, extractor_v1_flags1);
    assert_int_equal(wtree3_extractor_registry_count(reg), 20);

    wtree3_extractor_registry_destroy(reg);
}

/* ============================================================
 * Test Runner
 * ============================================================ */

int main(void) {
    const struct CMUnitTest tests[] = {
        /* Lifecycle */
        cmocka_unit_test(test_create_destroy),

        /* Basic operations */
        cmocka_unit_test(test_set_and_get),
        cmocka_unit_test(test_has),
        cmocka_unit_test(test_duplicate_registration),

        /* Version and flags */
        cmocka_unit_test(test_multiple_versions),
        cmocka_unit_test(test_multiple_flags),
        cmocka_unit_test(test_version_macro),

        /* Large scale */
        cmocka_unit_test(test_many_extractors),

        /* NULL parameters */
        cmocka_unit_test(test_null_parameters),

        /* Non-existent entries */
        cmocka_unit_test(test_get_nonexistent),

        /* Functional tests */
        cmocka_unit_test(test_extractor_execution),
        cmocka_unit_test(test_different_extractors_same_version),

        /* Edge cases */
        cmocka_unit_test(test_extreme_version_numbers),
        cmocka_unit_test(test_count_accuracy),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
