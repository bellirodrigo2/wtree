/*
 * test_wtree3_tier2.c - Comprehensive tests for Tier 2 operations
 *
 * Tests specialized bulk operations including:
 * - Conditional bulk delete (delete_if)
 * - Range collection with predicates (collect_range)
 * - Batch existence checks (exists_many)
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
    #include <direct.h>
    #include <process.h>
    #include <io.h>
    #define mkdir(path, mode) _mkdir(path)
    #define getpid() _getpid()
    #define unlink _unlink
    #define rmdir _rmdir
#else
    #include <sys/stat.h>
    #include <unistd.h>
#endif

#include "wtree3.h"

/* Extractor ID for test extractors */
#define TEST_EXTRACTOR_ID WTREE3_VERSION(1, 1)

/* Test database path */
static char test_db_path[256];
static wtree3_db_t *test_db = NULL;

/* ============================================================
 * Test Fixtures
 * ============================================================ */

static int setup_db(void **state) {
    (void)state;

#ifdef _WIN32
    snprintf(test_db_path, sizeof(test_db_path), "%s\\test_wtree3_tier2_%d",
             getenv("TEMP"), getpid());
#else
    snprintf(test_db_path, sizeof(test_db_path), "/tmp/test_wtree3_tier2_%d", getpid());
#endif
    mkdir(test_db_path, 0755);

    gerror_t error = {0};
    test_db = wtree3_db_open(test_db_path, 64 * 1024 * 1024, 128, WTREE3_VERSION(1, 0), 0, &error);
    if (!test_db) {
        fprintf(stderr, "Failed to create test database: %s\n", error.message);
        return -1;
    }

    return 0;
}

static int teardown_db(void **state) {
    (void)state;

    if (test_db) {
        wtree3_db_close(test_db);
        test_db = NULL;
    }

    char cmd[512];
#ifdef _WIN32
    snprintf(cmd, sizeof(cmd), "rd /s /q \"%s\"", test_db_path);
#else
    snprintf(cmd, sizeof(cmd), "rm -rf %s", test_db_path);
#endif
    (void)system(cmd);

    return 0;
}

/* ============================================================
 * Helper Functions
 * ============================================================ */

/* Populate tree with numbered keys and values */
static void populate_numbered_tree(wtree3_tree_t *tree, int count) {
    gerror_t error = {0};

    for (int i = 1; i <= count; i++) {
        char key[32];
        char value[32];
        snprintf(key, sizeof(key), "key%d", i);
        snprintf(value, sizeof(value), "value%d", i);

        int rc = wtree3_insert_one(tree, key, strlen(key) + 1,
                                   value, strlen(value) + 1, &error);
        assert_int_equal(rc, 0);
    }
}

/* Predicate: delete entries with even-numbered keys */
static bool predicate_even_keys(const void *key, size_t key_len,
                                const void *value, size_t value_len,
                                void *user_data) {
    (void)key_len;
    (void)value;
    (void)value_len;
    (void)user_data;

    const char *key_str = (const char*)key;
    /* Extract number from "keyN" format */
    int num = atoi(key_str + 3);
    return num % 2 == 0;
}

/* Predicate: collect entries with number > threshold */
typedef struct {
    int threshold;
} threshold_ctx_t;

static bool predicate_above_threshold(const void *key, size_t key_len,
                                     const void *value, size_t value_len,
                                     void *user_data) {
    (void)key_len;
    (void)value;
    (void)value_len;

    threshold_ctx_t *ctx = (threshold_ctx_t*)user_data;
    const char *key_str = (const char*)key;
    int num = atoi(key_str + 3);
    return num > ctx->threshold;
}

/* Predicate: always return false (delete nothing) */
static bool predicate_false(const void *key, size_t key_len,
                           const void *value, size_t value_len,
                           void *user_data) {
    (void)key;
    (void)key_len;
    (void)value;
    (void)value_len;
    (void)user_data;
    return false;
}

/* Predicate: always return true (delete/collect all) */
static bool predicate_true(const void *key, size_t key_len,
                          const void *value, size_t value_len,
                          void *user_data) {
    (void)key;
    (void)key_len;
    (void)value;
    (void)value_len;
    (void)user_data;
    return true;
}

/* ============================================================
 * Delete If Tests
 * ============================================================ */

static void test_delete_if_even_keys(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "delete_if_even", 0, 0, &error);
    assert_non_null(tree);

    /* Insert 10 entries */
    populate_numbered_tree(tree, 10);
    assert_int_equal(wtree3_tree_count(tree), 10);

    /* Delete even-numbered keys */
    wtree3_txn_t *txn = wtree3_txn_begin(test_db, true, &error);
    assert_non_null(txn);

    size_t deleted = 0;
    int rc = wtree3_delete_if_txn(txn, tree, NULL, 0, NULL, 0,
                                  predicate_even_keys, NULL, &deleted, &error);
    assert_int_equal(rc, 0);
    assert_int_equal(deleted, 5);  /* key2, key4, key6, key8, key10 */

    rc = wtree3_txn_commit(txn, &error);
    assert_int_equal(rc, 0);

    /* Verify count */
    assert_int_equal(wtree3_tree_count(tree), 5);

    /* Verify odd keys still exist */
    void *value;
    size_t value_len;
    rc = wtree3_get(tree, "key1", 5, &value, &value_len, &error);
    assert_int_equal(rc, 0);
    free(value);

    /* Verify even keys are gone */
    rc = wtree3_get(tree, "key2", 5, &value, &value_len, &error);
    assert_int_equal(rc, WTREE3_NOT_FOUND);

    wtree3_tree_close(tree);
}

static void test_delete_if_range(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "delete_if_range", 0, 0, &error);
    assert_non_null(tree);

    populate_numbered_tree(tree, 10);

    /* Delete even keys only in range key3..key7 */
    wtree3_txn_t *txn = wtree3_txn_begin(test_db, true, &error);
    assert_non_null(txn);

    size_t deleted = 0;
    int rc = wtree3_delete_if_txn(txn, tree,
                                  "key3", 5,
                                  "key7", 5,
                                  predicate_even_keys, NULL, &deleted, &error);
    assert_int_equal(rc, 0);
    assert_int_equal(deleted, 2);  /* key4, key6 */

    rc = wtree3_txn_commit(txn, &error);
    assert_int_equal(rc, 0);

    /* Verify count */
    assert_int_equal(wtree3_tree_count(tree), 8);

    /* key2 and key8 should still exist (outside range) */
    void *value;
    size_t value_len;
    rc = wtree3_get(tree, "key2", 5, &value, &value_len, &error);
    assert_int_equal(rc, 0);
    free(value);

    rc = wtree3_get(tree, "key8", 5, &value, &value_len, &error);
    assert_int_equal(rc, 0);
    free(value);

    /* key4 and key6 should be gone */
    rc = wtree3_get(tree, "key4", 5, &value, &value_len, &error);
    assert_int_equal(rc, WTREE3_NOT_FOUND);

    wtree3_tree_close(tree);
}

static void test_delete_if_empty_result(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "delete_if_empty", 0, 0, &error);
    assert_non_null(tree);

    populate_numbered_tree(tree, 5);

    /* Predicate that matches nothing */
    wtree3_txn_t *txn = wtree3_txn_begin(test_db, true, &error);
    assert_non_null(txn);

    size_t deleted = 0;
    int rc = wtree3_delete_if_txn(txn, tree, NULL, 0, NULL, 0,
                                  predicate_false, NULL, &deleted, &error);
    assert_int_equal(rc, 0);
    assert_int_equal(deleted, 0);

    rc = wtree3_txn_commit(txn, &error);
    assert_int_equal(rc, 0);

    /* All entries should still exist */
    assert_int_equal(wtree3_tree_count(tree), 5);

    wtree3_tree_close(tree);
}

static void test_delete_if_all(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "delete_if_all", 0, 0, &error);
    assert_non_null(tree);

    populate_numbered_tree(tree, 5);

    /* Delete all entries */
    wtree3_txn_t *txn = wtree3_txn_begin(test_db, true, &error);
    assert_non_null(txn);

    size_t deleted = 0;
    int rc = wtree3_delete_if_txn(txn, tree, NULL, 0, NULL, 0,
                                  predicate_true, NULL, &deleted, &error);
    assert_int_equal(rc, 0);
    assert_int_equal(deleted, 5);

    rc = wtree3_txn_commit(txn, &error);
    assert_int_equal(rc, 0);

    /* Tree should be empty */
    assert_int_equal(wtree3_tree_count(tree), 0);

    wtree3_tree_close(tree);
}

/* ============================================================
 * Collect Range Tests
 * ============================================================ */

static void test_collect_range_all(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "collect_all", 0, 0, &error);
    assert_non_null(tree);

    populate_numbered_tree(tree, 5);

    wtree3_txn_t *txn = wtree3_txn_begin(test_db, false, &error);
    assert_non_null(txn);

    /* Collect all entries (no predicate) */
    void **keys = NULL;
    size_t *key_lens = NULL;
    void **values = NULL;
    size_t *value_lens = NULL;
    size_t count = 0;

    int rc = wtree3_collect_range_txn(txn, tree, NULL, 0, NULL, 0,
                                      NULL, NULL,
                                      &keys, &key_lens, &values, &value_lens,
                                      &count, 0, &error);
    assert_int_equal(rc, 0);
    assert_int_equal(count, 5);

    /* Verify collected data */
    for (size_t i = 0; i < count; i++) {
        assert_non_null(keys[i]);
        assert_non_null(values[i]);
        free(keys[i]);
        free(values[i]);
    }
    free(keys);
    free(key_lens);
    free(values);
    free(value_lens);

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
}

static void test_collect_range_with_predicate(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "collect_pred", 0, 0, &error);
    assert_non_null(tree);

    populate_numbered_tree(tree, 10);

    wtree3_txn_t *txn = wtree3_txn_begin(test_db, false, &error);
    assert_non_null(txn);

    /* Collect entries with number > 5 */
    threshold_ctx_t ctx = {.threshold = 5};
    void **keys = NULL;
    size_t *key_lens = NULL;
    void **values = NULL;
    size_t *value_lens = NULL;
    size_t count = 0;

    int rc = wtree3_collect_range_txn(txn, tree, NULL, 0, NULL, 0,
                                      predicate_above_threshold, &ctx,
                                      &keys, &key_lens, &values, &value_lens,
                                      &count, 0, &error);
    assert_int_equal(rc, 0);
    assert_int_equal(count, 5);  /* key6, key7, key8, key9, key10 (note: key10 comes after key1 in lexicographic order) */

    /* Cleanup */
    for (size_t i = 0; i < count; i++) {
        free(keys[i]);
        free(values[i]);
    }
    free(keys);
    free(key_lens);
    free(values);
    free(value_lens);

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
}

static void test_collect_range_with_limit(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "collect_limit", 0, 0, &error);
    assert_non_null(tree);

    populate_numbered_tree(tree, 10);

    wtree3_txn_t *txn = wtree3_txn_begin(test_db, false, &error);
    assert_non_null(txn);

    /* Collect only first 3 entries */
    void **keys = NULL;
    size_t *key_lens = NULL;
    void **values = NULL;
    size_t *value_lens = NULL;
    size_t count = 0;

    int rc = wtree3_collect_range_txn(txn, tree, NULL, 0, NULL, 0,
                                      NULL, NULL,
                                      &keys, &key_lens, &values, &value_lens,
                                      &count, 3, &error);
    assert_int_equal(rc, 0);
    assert_int_equal(count, 3);

    /* Cleanup */
    for (size_t i = 0; i < count; i++) {
        free(keys[i]);
        free(values[i]);
    }
    free(keys);
    free(key_lens);
    free(values);
    free(value_lens);

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
}

static void test_collect_range_partial(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "collect_partial", 0, 0, &error);
    assert_non_null(tree);

    populate_numbered_tree(tree, 10);

    wtree3_txn_t *txn = wtree3_txn_begin(test_db, false, &error);
    assert_non_null(txn);

    /* Collect entries in range key3..key5 */
    void **keys = NULL;
    size_t *key_lens = NULL;
    void **values = NULL;
    size_t *value_lens = NULL;
    size_t count = 0;

    int rc = wtree3_collect_range_txn(txn, tree, "key3", 5, "key5", 5,
                                      NULL, NULL,
                                      &keys, &key_lens, &values, &value_lens,
                                      &count, 0, &error);
    assert_int_equal(rc, 0);
    assert_int_equal(count, 3);  /* key3, key4, key5 */

    /* Cleanup */
    for (size_t i = 0; i < count; i++) {
        free(keys[i]);
        free(values[i]);
    }
    free(keys);
    free(key_lens);
    free(values);
    free(value_lens);

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
}

static void test_collect_range_empty(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "collect_empty", 0, 0, &error);
    assert_non_null(tree);

    /* Don't insert anything */

    wtree3_txn_t *txn = wtree3_txn_begin(test_db, false, &error);
    assert_non_null(txn);

    void **keys = NULL;
    size_t *key_lens = NULL;
    void **values = NULL;
    size_t *value_lens = NULL;
    size_t count = 0;

    int rc = wtree3_collect_range_txn(txn, tree, NULL, 0, NULL, 0,
                                      NULL, NULL,
                                      &keys, &key_lens, &values, &value_lens,
                                      &count, 0, &error);
    assert_int_equal(rc, 0);
    assert_int_equal(count, 0);

    /* Arrays should still be allocated but empty */
    free(keys);
    free(key_lens);
    free(values);
    free(value_lens);

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
}

/* ============================================================
 * Exists Many Tests
 * ============================================================ */

static void test_exists_many_all_exist(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "exists_all", 0, 0, &error);
    assert_non_null(tree);

    populate_numbered_tree(tree, 5);

    wtree3_txn_t *txn = wtree3_txn_begin(test_db, false, &error);
    assert_non_null(txn);

    /* Check existence of all keys */
    wtree3_kv_t keys[] = {
        {.key = "key1", .key_len = 5},
        {.key = "key2", .key_len = 5},
        {.key = "key3", .key_len = 5},
        {.key = "key4", .key_len = 5},
        {.key = "key5", .key_len = 5}
    };

    bool results[5];
    int rc = wtree3_exists_many_txn(txn, tree, keys, 5, results, &error);
    assert_int_equal(rc, 0);

    for (int i = 0; i < 5; i++) {
        assert_true(results[i]);
    }

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
}

static void test_exists_many_mixed(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "exists_mixed", 0, 0, &error);
    assert_non_null(tree);

    /* Insert only odd-numbered keys */
    gerror_t err = {0};
    wtree3_insert_one(tree, "key1", 5, "value1", 7, &err);
    wtree3_insert_one(tree, "key3", 5, "value3", 7, &err);
    wtree3_insert_one(tree, "key5", 5, "value5", 7, &err);

    wtree3_txn_t *txn = wtree3_txn_begin(test_db, false, &error);
    assert_non_null(txn);

    /* Check mixed keys (some exist, some don't) */
    wtree3_kv_t keys[] = {
        {.key = "key1", .key_len = 5},  /* exists */
        {.key = "key2", .key_len = 5},  /* doesn't exist */
        {.key = "key3", .key_len = 5},  /* exists */
        {.key = "key4", .key_len = 5},  /* doesn't exist */
        {.key = "key5", .key_len = 5}   /* exists */
    };

    bool results[5];
    int rc = wtree3_exists_many_txn(txn, tree, keys, 5, results, &error);
    assert_int_equal(rc, 0);

    assert_true(results[0]);
    assert_false(results[1]);
    assert_true(results[2]);
    assert_false(results[3]);
    assert_true(results[4]);

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
}

static void test_exists_many_none_exist(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "exists_none", 0, 0, &error);
    assert_non_null(tree);

    /* Tree is empty */

    wtree3_txn_t *txn = wtree3_txn_begin(test_db, false, &error);
    assert_non_null(txn);

    wtree3_kv_t keys[] = {
        {.key = "key1", .key_len = 5},
        {.key = "key2", .key_len = 5},
        {.key = "key3", .key_len = 5}
    };

    bool results[3];
    int rc = wtree3_exists_many_txn(txn, tree, keys, 3, results, &error);
    assert_int_equal(rc, 0);

    for (int i = 0; i < 3; i++) {
        assert_false(results[i]);
    }

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
}

/* ============================================================
 * Error Cases
 * ============================================================ */

static void test_delete_if_null_params(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "delete_null", 0, 0, &error);
    assert_non_null(tree);

    wtree3_txn_t *txn = wtree3_txn_begin(test_db, true, &error);

    /* NULL predicate should fail */
    int rc = wtree3_delete_if_txn(txn, tree, NULL, 0, NULL, 0,
                                  NULL, NULL, NULL, &error);
    assert_int_equal(rc, WTREE3_EINVAL);

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
}

static void test_delete_if_readonly_txn(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "delete_ro", 0, 0, &error);
    assert_non_null(tree);

    wtree3_txn_t *txn = wtree3_txn_begin(test_db, false, &error);

    /* Read-only transaction should fail */
    int rc = wtree3_delete_if_txn(txn, tree, NULL, 0, NULL, 0,
                                  predicate_true, NULL, NULL, &error);
    assert_int_equal(rc, WTREE3_EINVAL);

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
}

static void test_collect_range_null_outputs(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "collect_null", 0, 0, &error);
    assert_non_null(tree);

    wtree3_txn_t *txn = wtree3_txn_begin(test_db, false, &error);

    /* NULL outputs should fail */
    int rc = wtree3_collect_range_txn(txn, tree, NULL, 0, NULL, 0,
                                      NULL, NULL,
                                      NULL, NULL, NULL, NULL, NULL, 0, &error);
    assert_int_equal(rc, WTREE3_EINVAL);

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
}

/* ============================================================
 * Main Test Suite
 * ============================================================ */

int main(void) {
    const struct CMUnitTest tests[] = {
        /* Delete if */
        cmocka_unit_test_setup_teardown(test_delete_if_even_keys, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_delete_if_range, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_delete_if_empty_result, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_delete_if_all, setup_db, teardown_db),

        /* Collect range */
        cmocka_unit_test_setup_teardown(test_collect_range_all, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_collect_range_with_predicate, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_collect_range_with_limit, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_collect_range_partial, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_collect_range_empty, setup_db, teardown_db),

        /* Exists many */
        cmocka_unit_test_setup_teardown(test_exists_many_all_exist, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_exists_many_mixed, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_exists_many_none_exist, setup_db, teardown_db),

        /* Error cases */
        cmocka_unit_test_setup_teardown(test_delete_if_null_params, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_delete_if_readonly_txn, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_collect_range_null_outputs, setup_db, teardown_db),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
