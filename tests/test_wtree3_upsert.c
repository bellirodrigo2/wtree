/*
 * test_wtree3_upsert.c - Comprehensive tests for upsert functionality
 *
 * Tests upsert operations with and without merge callbacks, including:
 * - Basic upsert (insert when not exists)
 * - Upsert with overwrite (exists, no merge callback)
 * - Upsert with merge callback
 * - Index maintenance during upsert
 * - Transaction-based and auto-transaction upsert
 * - Error cases
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

/* Test database path */
static char test_db_path[256];
static wtree3_db_t *test_db = NULL;

/* ============================================================
 * Test Fixtures
 * ============================================================ */

static int setup_db(void **state) {
    (void)state;

#ifdef _WIN32
    snprintf(test_db_path, sizeof(test_db_path), "%s\\test_wtree3_upsert_%d",
             getenv("TEMP"), getpid());
#else
    snprintf(test_db_path, sizeof(test_db_path), "/tmp/test_wtree3_upsert_%d", getpid());
#endif
    mkdir(test_db_path, 0755);

    gerror_t error = {0};
    test_db = wtree3_db_open(test_db_path, 64 * 1024 * 1024, 128, 0, &error);
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
 * Merge Callbacks
 * ============================================================ */

/* Simple concatenation merge: "old" + "new" */
static void* merge_concat(const void *existing_value, size_t existing_len,
                          const void *new_value, size_t new_len,
                          void *user_data,
                          size_t *out_len) {
    (void)user_data;

    size_t total_len = existing_len + new_len;
    char *result = malloc(total_len);
    if (!result) return NULL;

    memcpy(result, existing_value, existing_len);
    memcpy(result + existing_len, new_value, new_len);

    *out_len = total_len;
    return result;
}

/* Integer addition merge: parse both as int, return sum */
static void* merge_add_int(const void *existing_value, size_t existing_len,
                           const void *new_value, size_t new_len,
                           void *user_data,
                           size_t *out_len) {
    (void)user_data;
    (void)existing_len;
    (void)new_len;

    int existing = *(int*)existing_value;
    int new = *(int*)new_value;
    int sum = existing + new;

    int *result = malloc(sizeof(int));
    if (!result) return NULL;

    *result = sum;
    *out_len = sizeof(int);
    return result;
}

/* Merge that returns NULL (error case) */
static void* merge_failing(const void *existing_value, size_t existing_len,
                           const void *new_value, size_t new_len,
                           void *user_data,
                           size_t *out_len) {
    (void)existing_value;
    (void)existing_len;
    (void)new_value;
    (void)new_len;
    (void)user_data;
    (void)out_len;
    return NULL;
}

/* ============================================================
 * Basic Upsert Tests
 * ============================================================ */

static void test_upsert_insert_new_key(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "upsert_insert", 0, 0, &error);
    assert_non_null(tree);

    /* Upsert a key that doesn't exist - should insert */
    int rc = wtree3_upsert(tree, "key1", 4, "value1", 6, &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Verify it was inserted */
    void *value;
    size_t value_len;
    rc = wtree3_get(tree, "key1", 4, &value, &value_len, &error);
    assert_int_equal(WTREE3_OK, rc);
    assert_int_equal(6, value_len);
    assert_memory_equal("value1", value, 6);
    free(value);

    /* Entry count should be 1 */
    assert_int_equal(1, wtree3_tree_count(tree));

    wtree3_tree_close(tree);
}

static void test_upsert_overwrite_existing_no_merge(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "upsert_overwrite", 0, 0, &error);
    assert_non_null(tree);

    /* Insert initial value */
    int rc = wtree3_insert_one(tree, "key1", 4, "old", 3, &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Upsert without merge callback - should overwrite */
    rc = wtree3_upsert(tree, "key1", 4, "new", 3, &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Verify it was overwritten */
    void *value;
    size_t value_len;
    rc = wtree3_get(tree, "key1", 4, &value, &value_len, &error);
    assert_int_equal(WTREE3_OK, rc);
    assert_int_equal(3, value_len);
    assert_memory_equal("new", value, 3);
    free(value);

    /* Entry count should still be 1 */
    assert_int_equal(1, wtree3_tree_count(tree));

    wtree3_tree_close(tree);
}

static void test_upsert_with_concat_merge(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "upsert_concat", 0, 0, &error);
    assert_non_null(tree);

    /* Set merge callback */
    wtree3_tree_set_merge_fn(tree, merge_concat, NULL);

    /* Insert initial value */
    int rc = wtree3_insert_one(tree, "key1", 4, "hello", 5, &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Upsert with merge - should concatenate */
    rc = wtree3_upsert(tree, "key1", 4, "world", 5, &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Verify merged value */
    void *value;
    size_t value_len;
    rc = wtree3_get(tree, "key1", 4, &value, &value_len, &error);
    assert_int_equal(WTREE3_OK, rc);
    assert_int_equal(10, value_len);
    assert_memory_equal("helloworld", value, 10);
    free(value);

    /* Entry count should still be 1 */
    assert_int_equal(1, wtree3_tree_count(tree));

    wtree3_tree_close(tree);
}

static void test_upsert_with_int_add_merge(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "upsert_int", 0, 0, &error);
    assert_non_null(tree);

    /* Set integer addition merge callback */
    wtree3_tree_set_merge_fn(tree, merge_add_int, NULL);

    /* Insert initial counter */
    int initial = 10;
    int rc = wtree3_insert_one(tree, "counter", 7, &initial, sizeof(int), &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Upsert to add 5 */
    int increment = 5;
    rc = wtree3_upsert(tree, "counter", 7, &increment, sizeof(int), &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Verify sum is 15 */
    void *value;
    size_t value_len;
    rc = wtree3_get(tree, "counter", 7, &value, &value_len, &error);
    assert_int_equal(WTREE3_OK, rc);
    assert_int_equal(sizeof(int), value_len);
    assert_int_equal(15, *(int*)value);
    free(value);

    /* Upsert to add 3 more */
    int increment2 = 3;
    rc = wtree3_upsert(tree, "counter", 7, &increment2, sizeof(int), &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Verify sum is 18 */
    rc = wtree3_get(tree, "counter", 7, &value, &value_len, &error);
    assert_int_equal(WTREE3_OK, rc);
    assert_int_equal(18, *(int*)value);
    free(value);

    wtree3_tree_close(tree);
}

/* ============================================================
 * Transaction-based Upsert Tests
 * ============================================================ */

static void test_upsert_txn_basic(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "upsert_txn", 0, 0, &error);
    assert_non_null(tree);

    wtree3_txn_t *txn = wtree3_txn_begin(test_db, true, &error);
    assert_non_null(txn);

    /* Upsert in transaction */
    int rc = wtree3_upsert_txn(txn, tree, "key1", 4, "value1", 6, &error);
    assert_int_equal(WTREE3_OK, rc);

    rc = wtree3_txn_commit(txn, &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Verify */
    void *value;
    size_t value_len;
    rc = wtree3_get(tree, "key1", 4, &value, &value_len, &error);
    assert_int_equal(WTREE3_OK, rc);
    assert_memory_equal("value1", value, 6);
    free(value);

    wtree3_tree_close(tree);
}

static void test_upsert_txn_multiple_in_one_transaction(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "upsert_multi_txn", 0, 0, &error);
    assert_non_null(tree);

    wtree3_tree_set_merge_fn(tree, merge_concat, NULL);

    wtree3_txn_t *txn = wtree3_txn_begin(test_db, true, &error);
    assert_non_null(txn);

    /* Multiple upserts in same transaction */
    int rc = wtree3_upsert_txn(txn, tree, "key1", 4, "a", 1, &error);
    assert_int_equal(WTREE3_OK, rc);

    rc = wtree3_upsert_txn(txn, tree, "key1", 4, "b", 1, &error);
    assert_int_equal(WTREE3_OK, rc);

    rc = wtree3_upsert_txn(txn, tree, "key1", 4, "c", 1, &error);
    assert_int_equal(WTREE3_OK, rc);

    rc = wtree3_txn_commit(txn, &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Verify concatenated result */
    void *value;
    size_t value_len;
    rc = wtree3_get(tree, "key1", 4, &value, &value_len, &error);
    assert_int_equal(WTREE3_OK, rc);
    assert_int_equal(3, value_len);
    assert_memory_equal("abc", value, 3);
    free(value);

    wtree3_tree_close(tree);
}

/* ============================================================
 * Index Maintenance During Upsert
 * ============================================================ */

/* Simple key extractor for testing */
static bool simple_key_extractor(const void *value, size_t value_len,
                                  void *user_data,
                                  void **out_key, size_t *out_len) {
    (void)user_data;

    /* Extract first 3 chars as index key */
    const char *val = (const char *)value;
    size_t key_len = (value_len < 3) ? value_len : 3;

    char *key = malloc(key_len);
    if (!key) return false;

    memcpy(key, val, key_len);
    *out_key = key;
    *out_len = key_len;
    return true;
}

static void test_upsert_maintains_indexes(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "upsert_idx", 0, 0, &error);
    assert_non_null(tree);

    /* Add index */
    wtree3_index_config_t idx_config = {
        .name = "prefix_idx",
        .key_fn = simple_key_extractor,
        .user_data = NULL,
        .user_data_cleanup = NULL,
        .unique = false,
        .sparse = false,
        .compare = NULL
    };

    int rc = wtree3_tree_add_index(tree, &idx_config, &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Insert with prefix "abc" */
    rc = wtree3_insert_one(tree, "key1", 4, "abc123", 6, &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Upsert to change prefix to "xyz" */
    rc = wtree3_upsert(tree, "key1", 4, "xyz789", 6, &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Search old index - should not find */
    wtree3_iterator_t *iter = wtree3_index_seek(tree, "prefix_idx", "abc", 3, &error);
    assert_non_null(iter);
    assert_false(wtree3_iterator_valid(iter));
    wtree3_iterator_close(iter);

    /* Search new index - should find */
    iter = wtree3_index_seek(tree, "prefix_idx", "xyz", 3, &error);
    assert_non_null(iter);
    assert_true(wtree3_iterator_valid(iter));

    const void *main_key;
    size_t main_key_len;
    wtree3_index_iterator_main_key(iter, &main_key, &main_key_len);
    assert_int_equal(4, main_key_len);
    assert_memory_equal("key1", main_key, 4);

    wtree3_iterator_close(iter);
    wtree3_tree_close(tree);
}

static void test_upsert_unique_index_violation(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "upsert_uniq", 0, 0, &error);
    assert_non_null(tree);

    /* Add UNIQUE index */
    wtree3_index_config_t idx_config = {
        .name = "unique_prefix",
        .key_fn = simple_key_extractor,
        .user_data = NULL,
        .user_data_cleanup = NULL,
        .unique = true,
        .sparse = false,
        .compare = NULL
    };

    int rc = wtree3_tree_add_index(tree, &idx_config, &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Insert two keys with different prefixes */
    rc = wtree3_insert_one(tree, "key1", 4, "abc123", 6, &error);
    assert_int_equal(WTREE3_OK, rc);

    rc = wtree3_insert_one(tree, "key2", 4, "xyz789", 6, &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Try to upsert key1 to have same prefix as key2 - should fail */
    rc = wtree3_upsert(tree, "key1", 4, "xyz000", 6, &error);
    assert_int_equal(WTREE3_INDEX_ERROR, rc);

    /* Verify key1 still has old value */
    void *value;
    size_t value_len;
    rc = wtree3_get(tree, "key1", 4, &value, &value_len, &error);
    assert_int_equal(WTREE3_OK, rc);
    assert_memory_equal("abc123", value, 6);
    free(value);

    wtree3_tree_close(tree);
}

/* ============================================================
 * Error Cases
 * ============================================================ */

static void test_upsert_null_params(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "upsert_null", 0, 0, &error);
    assert_non_null(tree);

    /* Null tree */
    int rc = wtree3_upsert(NULL, "key", 3, "val", 3, &error);
    assert_int_equal(WTREE3_EINVAL, rc);

    /* Null key */
    wtree3_txn_t *txn = wtree3_txn_begin(test_db, true, &error);
    rc = wtree3_upsert_txn(txn, tree, NULL, 0, "val", 3, &error);
    assert_int_equal(WTREE3_EINVAL, rc);
    wtree3_txn_abort(txn);

    /* Null value */
    txn = wtree3_txn_begin(test_db, true, &error);
    rc = wtree3_upsert_txn(txn, tree, "key", 3, NULL, 0, &error);
    assert_int_equal(WTREE3_EINVAL, rc);
    wtree3_txn_abort(txn);

    wtree3_tree_close(tree);
}

static void test_upsert_readonly_txn(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "upsert_ro", 0, 0, &error);
    assert_non_null(tree);

    /* Try upsert in read-only transaction */
    wtree3_txn_t *txn = wtree3_txn_begin(test_db, false, &error);
    assert_non_null(txn);

    int rc = wtree3_upsert_txn(txn, tree, "key", 3, "val", 3, &error);
    assert_int_equal(WTREE3_EINVAL, rc);

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
}

static void test_upsert_merge_callback_returns_null(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "upsert_fail_merge", 0, 0, &error);
    assert_non_null(tree);

    /* Set failing merge callback */
    wtree3_tree_set_merge_fn(tree, merge_failing, NULL);

    /* Insert initial value */
    int rc = wtree3_insert_one(tree, "key1", 4, "old", 3, &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Upsert with failing merge - should fail */
    rc = wtree3_upsert(tree, "key1", 4, "new", 3, &error);
    assert_int_equal(WTREE3_ERROR, rc);

    /* Verify old value is unchanged */
    void *value;
    size_t value_len;
    rc = wtree3_get(tree, "key1", 4, &value, &value_len, &error);
    assert_int_equal(WTREE3_OK, rc);
    assert_memory_equal("old", value, 3);
    free(value);

    wtree3_tree_close(tree);
}

/* ============================================================
 * Edge Cases
 * ============================================================ */

static void test_upsert_empty_value(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "upsert_empty", 0, 0, &error);
    assert_non_null(tree);

    /* Upsert with empty value */
    int rc = wtree3_upsert(tree, "key1", 4, "", 0, &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Verify empty value */
    void *value;
    size_t value_len;
    rc = wtree3_get(tree, "key1", 4, &value, &value_len, &error);
    assert_int_equal(WTREE3_OK, rc);
    assert_int_equal(0, value_len);

    wtree3_tree_close(tree);
}

static void test_upsert_large_merge(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "upsert_large", 0, 0, &error);
    assert_non_null(tree);

    wtree3_tree_set_merge_fn(tree, merge_concat, NULL);

    /* Create large value */
    char large_val[1000];
    memset(large_val, 'A', 1000);

    /* Insert */
    int rc = wtree3_insert_one(tree, "key1", 4, large_val, 1000, &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Upsert with another large value */
    char large_val2[1000];
    memset(large_val2, 'B', 1000);

    rc = wtree3_upsert(tree, "key1", 4, large_val2, 1000, &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Verify merged size is 2000 */
    void *value;
    size_t value_len;
    rc = wtree3_get(tree, "key1", 4, &value, &value_len, &error);
    assert_int_equal(WTREE3_OK, rc);
    assert_int_equal(2000, value_len);

    /* Verify first half is 'A', second half is 'B' */
    char *merged = (char*)value;
    for (int i = 0; i < 1000; i++) {
        assert_int_equal('A', merged[i]);
    }
    for (int i = 1000; i < 2000; i++) {
        assert_int_equal('B', merged[i]);
    }

    free(value);
    wtree3_tree_close(tree);
}

/* ============================================================
 * Batch Upsert (upsert_many) Tests
 * ============================================================ */

static void test_upsert_many_basic(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "upsert_many_basic", 0, 0, &error);
    assert_non_null(tree);

    /* Batch upsert - all new keys */
    wtree3_kv_t kvs[] = {
        {.key = "key1", .key_len = 4, .value = "value1", .value_len = 6},
        {.key = "key2", .key_len = 4, .value = "value2", .value_len = 6},
        {.key = "key3", .key_len = 4, .value = "value3", .value_len = 6},
    };

    wtree3_txn_t *txn = wtree3_txn_begin(test_db, true, &error);
    assert_non_null(txn);

    int rc = wtree3_upsert_many_txn(txn, tree, kvs, 3, &error);
    assert_int_equal(WTREE3_OK, rc);

    rc = wtree3_txn_commit(txn, &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Verify all entries were inserted */
    void *value;
    size_t value_len;
    rc = wtree3_get(tree, "key1", 4, &value, &value_len, &error);
    assert_int_equal(WTREE3_OK, rc);
    assert_memory_equal("value1", value, 6);
    free(value);

    assert_int_equal(3, wtree3_tree_count(tree));

    wtree3_tree_close(tree);
}

static void test_upsert_many_mixed_insert_update(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "upsert_many_mixed", 0, 0, &error);
    assert_non_null(tree);

    /* Insert initial keys */
    int rc = wtree3_insert_one(tree, "key1", 4, "old1", 4, &error);
    assert_int_equal(WTREE3_OK, rc);
    rc = wtree3_insert_one(tree, "key3", 4, "old3", 4, &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Batch upsert - mix of updates and inserts */
    wtree3_kv_t kvs[] = {
        {.key = "key1", .key_len = 4, .value = "new1", .value_len = 4},  // Update
        {.key = "key2", .key_len = 4, .value = "new2", .value_len = 4},  // Insert
        {.key = "key3", .key_len = 4, .value = "new3", .value_len = 4},  // Update
        {.key = "key4", .key_len = 4, .value = "new4", .value_len = 4},  // Insert
    };

    wtree3_txn_t *txn = wtree3_txn_begin(test_db, true, &error);
    assert_non_null(txn);

    rc = wtree3_upsert_many_txn(txn, tree, kvs, 4, &error);
    assert_int_equal(WTREE3_OK, rc);

    rc = wtree3_txn_commit(txn, &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Verify updates */
    void *value;
    size_t value_len;
    rc = wtree3_get(tree, "key1", 4, &value, &value_len, &error);
    assert_int_equal(WTREE3_OK, rc);
    assert_memory_equal("new1", value, 4);
    free(value);

    rc = wtree3_get(tree, "key3", 4, &value, &value_len, &error);
    assert_int_equal(WTREE3_OK, rc);
    assert_memory_equal("new3", value, 4);
    free(value);

    /* Verify inserts */
    rc = wtree3_get(tree, "key2", 4, &value, &value_len, &error);
    assert_int_equal(WTREE3_OK, rc);
    assert_memory_equal("new2", value, 4);
    free(value);

    /* Count should be 4 (2 initial + 2 new) */
    assert_int_equal(4, wtree3_tree_count(tree));

    wtree3_tree_close(tree);
}

static void test_upsert_many_with_merge(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "upsert_many_merge", 0, 0, &error);
    assert_non_null(tree);

    /* Set concat merge callback */
    wtree3_tree_set_merge_fn(tree, merge_concat, NULL);

    /* Insert initial values */
    int rc = wtree3_insert_one(tree, "key1", 4, "a", 1, &error);
    assert_int_equal(WTREE3_OK, rc);
    rc = wtree3_insert_one(tree, "key2", 4, "b", 1, &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Batch upsert with merge */
    wtree3_kv_t kvs[] = {
        {.key = "key1", .key_len = 4, .value = "x", .value_len = 1},  // Merge: a+x
        {.key = "key2", .key_len = 4, .value = "y", .value_len = 1},  // Merge: b+y
        {.key = "key3", .key_len = 4, .value = "z", .value_len = 1},  // Insert: z
    };

    wtree3_txn_t *txn = wtree3_txn_begin(test_db, true, &error);
    assert_non_null(txn);

    rc = wtree3_upsert_many_txn(txn, tree, kvs, 3, &error);
    assert_int_equal(WTREE3_OK, rc);

    rc = wtree3_txn_commit(txn, &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Verify merged values */
    void *value;
    size_t value_len;
    rc = wtree3_get(tree, "key1", 4, &value, &value_len, &error);
    assert_int_equal(WTREE3_OK, rc);
    assert_int_equal(2, value_len);
    assert_memory_equal("ax", value, 2);
    free(value);

    rc = wtree3_get(tree, "key2", 4, &value, &value_len, &error);
    assert_int_equal(WTREE3_OK, rc);
    assert_int_equal(2, value_len);
    assert_memory_equal("by", value, 2);
    free(value);

    rc = wtree3_get(tree, "key3", 4, &value, &value_len, &error);
    assert_int_equal(WTREE3_OK, rc);
    assert_int_equal(1, value_len);
    assert_memory_equal("z", value, 1);
    free(value);

    wtree3_tree_close(tree);
}

static void test_upsert_many_index_maintenance(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "upsert_many_idx", 0, 0, &error);
    assert_non_null(tree);

    /* Add index */
    wtree3_index_config_t idx_config = {
        .name = "prefix_idx",
        .key_fn = simple_key_extractor,
        .user_data = NULL,
        .user_data_cleanup = NULL,
        .unique = false,
        .sparse = false,
        .compare = NULL
    };

    int rc = wtree3_tree_add_index(tree, &idx_config, &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Insert initial data */
    rc = wtree3_insert_one(tree, "k1", 2, "abc123", 6, &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Batch upsert - changes index key */
    wtree3_kv_t kvs[] = {
        {.key = "k1", .key_len = 2, .value = "xyz789", .value_len = 6},  // Change prefix
        {.key = "k2", .key_len = 2, .value = "abc456", .value_len = 6},  // New with abc
    };

    wtree3_txn_t *txn = wtree3_txn_begin(test_db, true, &error);
    assert_non_null(txn);

    rc = wtree3_upsert_many_txn(txn, tree, kvs, 2, &error);
    assert_int_equal(WTREE3_OK, rc);

    rc = wtree3_txn_commit(txn, &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Search for "xyz" - should find k1 */
    wtree3_iterator_t *iter = wtree3_index_seek(tree, "prefix_idx", "xyz", 3, &error);
    assert_non_null(iter);
    assert_true(wtree3_iterator_valid(iter));

    const void *main_key;
    size_t main_key_len;
    wtree3_index_iterator_main_key(iter, &main_key, &main_key_len);
    assert_int_equal(2, main_key_len);
    assert_memory_equal("k1", main_key, 2);
    wtree3_iterator_close(iter);

    /* Search for "abc" - should find k2 (k1 no longer has abc prefix) */
    iter = wtree3_index_seek(tree, "prefix_idx", "abc", 3, &error);
    assert_non_null(iter);
    assert_true(wtree3_iterator_valid(iter));
    wtree3_index_iterator_main_key(iter, &main_key, &main_key_len);
    assert_int_equal(2, main_key_len);
    assert_memory_equal("k2", main_key, 2);
    wtree3_iterator_close(iter);

    wtree3_tree_close(tree);
}

static void test_upsert_many_error_cases(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "upsert_many_err", 0, 0, &error);
    assert_non_null(tree);

    wtree3_kv_t kvs[] = {
        {.key = "key1", .key_len = 4, .value = "value1", .value_len = 6},
    };

    /* Null transaction */
    int rc = wtree3_upsert_many_txn(NULL, tree, kvs, 1, &error);
    assert_int_equal(WTREE3_EINVAL, rc);

    /* Null kvs */
    wtree3_txn_t *txn = wtree3_txn_begin(test_db, true, &error);
    rc = wtree3_upsert_many_txn(txn, tree, NULL, 1, &error);
    assert_int_equal(WTREE3_EINVAL, rc);
    wtree3_txn_abort(txn);

    /* Zero count */
    txn = wtree3_txn_begin(test_db, true, &error);
    rc = wtree3_upsert_many_txn(txn, tree, kvs, 0, &error);
    assert_int_equal(WTREE3_EINVAL, rc);
    wtree3_txn_abort(txn);

    /* Read-only transaction */
    txn = wtree3_txn_begin(test_db, false, &error);
    rc = wtree3_upsert_many_txn(txn, tree, kvs, 1, &error);
    assert_int_equal(WTREE3_EINVAL, rc);
    wtree3_txn_abort(txn);

    wtree3_tree_close(tree);
}

static void test_upsert_many_unique_violation(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "upsert_many_uniq_vio", 0, 0, &error);
    assert_non_null(tree);

    /* Add UNIQUE index */
    wtree3_index_config_t idx_config = {
        .name = "unique_prefix",
        .key_fn = simple_key_extractor,
        .user_data = NULL,
        .user_data_cleanup = NULL,
        .unique = true,
        .sparse = false,
        .compare = NULL
    };

    int rc = wtree3_tree_add_index(tree, &idx_config, &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Insert initial data */
    rc = wtree3_insert_one(tree, "k1", 2, "abc123", 6, &error);
    assert_int_equal(WTREE3_OK, rc);
    rc = wtree3_insert_one(tree, "k2", 2, "xyz789", 6, &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Try to upsert k1 to have same prefix as k2 - should fail */
    wtree3_kv_t kvs[] = {
        {.key = "k1", .key_len = 2, .value = "xyz000", .value_len = 6},
    };

    wtree3_txn_t *txn = wtree3_txn_begin(test_db, true, &error);
    assert_non_null(txn);

    rc = wtree3_upsert_many_txn(txn, tree, kvs, 1, &error);
    assert_int_equal(WTREE3_INDEX_ERROR, rc);

    wtree3_txn_abort(txn);

    /* Verify k1 still has old value */
    void *value;
    size_t value_len;
    rc = wtree3_get(tree, "k1", 2, &value, &value_len, &error);
    assert_int_equal(WTREE3_OK, rc);
    assert_memory_equal("abc123", value, 6);
    free(value);

    wtree3_tree_close(tree);
}

static void test_upsert_many_counter_batch(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "upsert_many_counter", 0, 0, &error);
    assert_non_null(tree);

    /* Set integer addition merge callback */
    wtree3_tree_set_merge_fn(tree, merge_add_int, NULL);

    /* Initialize counters */
    int initial1 = 10;
    int initial2 = 20;
    int rc = wtree3_insert_one(tree, "counter1", 8, &initial1, sizeof(int), &error);
    assert_int_equal(WTREE3_OK, rc);
    rc = wtree3_insert_one(tree, "counter2", 8, &initial2, sizeof(int), &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Batch increment */
    int inc1 = 5;
    int inc2 = 3;
    int inc3 = 100;  // New counter
    wtree3_kv_t kvs[] = {
        {.key = "counter1", .key_len = 8, .value = &inc1, .value_len = sizeof(int)},
        {.key = "counter2", .key_len = 8, .value = &inc2, .value_len = sizeof(int)},
        {.key = "counter3", .key_len = 8, .value = &inc3, .value_len = sizeof(int)},
    };

    wtree3_txn_t *txn = wtree3_txn_begin(test_db, true, &error);
    assert_non_null(txn);

    rc = wtree3_upsert_many_txn(txn, tree, kvs, 3, &error);
    assert_int_equal(WTREE3_OK, rc);

    rc = wtree3_txn_commit(txn, &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Verify results */
    void *value;
    size_t value_len;

    rc = wtree3_get(tree, "counter1", 8, &value, &value_len, &error);
    assert_int_equal(WTREE3_OK, rc);
    assert_int_equal(15, *(int*)value);  // 10 + 5
    free(value);

    rc = wtree3_get(tree, "counter2", 8, &value, &value_len, &error);
    assert_int_equal(WTREE3_OK, rc);
    assert_int_equal(23, *(int*)value);  // 20 + 3
    free(value);

    rc = wtree3_get(tree, "counter3", 8, &value, &value_len, &error);
    assert_int_equal(WTREE3_OK, rc);
    assert_int_equal(100, *(int*)value);  // New counter
    free(value);

    wtree3_tree_close(tree);
}

/* ============================================================
 * Main
 * ============================================================ */

int main(void) {
    const struct CMUnitTest tests[] = {
        /* Basic upsert tests */
        cmocka_unit_test(test_upsert_insert_new_key),
        cmocka_unit_test(test_upsert_overwrite_existing_no_merge),
        cmocka_unit_test(test_upsert_with_concat_merge),
        cmocka_unit_test(test_upsert_with_int_add_merge),

        /* Transaction-based tests */
        cmocka_unit_test(test_upsert_txn_basic),
        cmocka_unit_test(test_upsert_txn_multiple_in_one_transaction),

        /* Index maintenance tests */
        cmocka_unit_test(test_upsert_maintains_indexes),
        cmocka_unit_test(test_upsert_unique_index_violation),

        /* Error cases */
        cmocka_unit_test(test_upsert_null_params),
        cmocka_unit_test(test_upsert_readonly_txn),
        cmocka_unit_test(test_upsert_merge_callback_returns_null),

        /* Edge cases */
        cmocka_unit_test(test_upsert_empty_value),
        cmocka_unit_test(test_upsert_large_merge),

        /* Batch upsert tests */
        cmocka_unit_test(test_upsert_many_basic),
        cmocka_unit_test(test_upsert_many_mixed_insert_update),
        cmocka_unit_test(test_upsert_many_with_merge),
        cmocka_unit_test(test_upsert_many_index_maintenance),
        cmocka_unit_test(test_upsert_many_error_cases),
        cmocka_unit_test(test_upsert_many_unique_violation),
        cmocka_unit_test(test_upsert_many_counter_batch),
    };

    return cmocka_run_group_tests(tests, setup_db, teardown_db);
}
