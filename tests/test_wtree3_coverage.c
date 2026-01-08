/*
 * test_wtree3_coverage.c - Coverage improvement tests for wtree3
 *
 * Focus on:
 * - Error path branches (MDB_MAP_FULL, MDB_TXN_FULL, etc.)
 * - Input validation and NULL parameter checks
 * - Edge cases and boundary conditions
 * - Recently added functions
 */

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <setjmp.h>
#include <cmocka.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include "wtree3.h"

/* Extractor ID for test extractors */
#define TEST_EXTRACTOR_ID WTREE3_VERSION(1, 1)
#include "gerror.h"

#define TEST_DB_PATH "./test_wtree3_coverage_db"

/* ============================================================
 * Test Helpers
 * ============================================================ */

static void cleanup_test_db(void) {
#ifdef _WIN32
    system("rmdir /S /Q " TEST_DB_PATH " 2>nul");
#else
    system("rm -rf " TEST_DB_PATH);
#endif
}

static int setup(void **state) {
    (void)state;
    cleanup_test_db();
#ifdef _WIN32
    mkdir(TEST_DB_PATH);
#else
    mkdir(TEST_DB_PATH, 0755);
#endif
    return 0;
}

static int teardown(void **state) {
    (void)state;
    cleanup_test_db();
    return 0;
}

/* ============================================================
 * NULL Parameter Tests
 * ============================================================ */

static void test_db_open_null_path(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_db_t *db = wtree3_db_open(NULL, 10485760, 128, WTREE3_VERSION(1, 0), 0, &error);
    assert_null(db);
    assert_int_equal(WTREE3_EINVAL, error.code);
}

static void test_db_sync_null_db(void **state) {
    (void)state;
    gerror_t error = {0};

    int ret = wtree3_db_sync(NULL, false, &error);
    assert_int_equal(WTREE3_EINVAL, ret);
}

static void test_db_resize_null_db(void **state) {
    (void)state;
    gerror_t error = {0};

    int ret = wtree3_db_resize(NULL, 20971520, &error);
    assert_int_equal(WTREE3_EINVAL, ret);
}

static void test_db_stats_null_db(void **state) {
    (void)state;
    gerror_t error = {0};
    MDB_stat stat;

    int ret = wtree3_db_stats(NULL, &stat, &error);
    assert_int_equal(WTREE3_EINVAL, ret);
}

static void test_db_get_mapsize_null(void **state) {
    (void)state;

    size_t size = wtree3_db_get_mapsize(NULL);
    assert_int_equal(0, size);
}

static void test_db_get_env_null(void **state) {
    (void)state;

    MDB_env *env = wtree3_db_get_env(NULL);
    assert_null(env);
}

static void test_txn_begin_null_db(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_txn_t *txn = wtree3_txn_begin(NULL, true, &error);
    assert_null(txn);
    assert_int_equal(WTREE3_EINVAL, error.code);
}

static void test_txn_commit_null(void **state) {
    (void)state;
    gerror_t error = {0};

    int ret = wtree3_txn_commit(NULL, &error);
    assert_int_equal(WTREE3_EINVAL, ret);
}

static void test_txn_renew_null(void **state) {
    (void)state;
    gerror_t error = {0};

    int ret = wtree3_txn_renew(NULL, &error);
    assert_int_equal(WTREE3_EINVAL, ret);
}

static void test_txn_is_readonly_null(void **state) {
    (void)state;

    bool result = wtree3_txn_is_readonly(NULL);
    assert_false(result);
}

static void test_txn_get_mdb_null(void **state) {
    (void)state;

    MDB_txn *mdb_txn = wtree3_txn_get_mdb(NULL);
    assert_null(mdb_txn);
}

static void test_txn_get_db_null(void **state) {
    (void)state;

    wtree3_db_t *db = wtree3_txn_get_db(NULL);
    assert_null(db);
}

static void test_tree_open_null_db(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(NULL, "test", MDB_CREATE, 0, &error);
    assert_null(tree);
    assert_int_equal(WTREE3_EINVAL, error.code);
}

static void test_tree_open_null_name(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_db_t *db = wtree3_db_open(TEST_DB_PATH, 10485760, 128, WTREE3_VERSION(1, 0), 0, &error);
    assert_non_null(db);

    wtree3_tree_t *tree = wtree3_tree_open(db, NULL, MDB_CREATE, 0, &error);
    assert_null(tree);
    assert_int_equal(WTREE3_EINVAL, error.code);

    wtree3_db_close(db);
}

static void test_tree_delete_null_db(void **state) {
    (void)state;
    gerror_t error = {0};

    int ret = wtree3_tree_delete(NULL, "test", &error);
    assert_int_equal(WTREE3_EINVAL, ret);
}

static void test_tree_delete_null_name(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_db_t *db = wtree3_db_open(TEST_DB_PATH, 10485760, 128, WTREE3_VERSION(1, 0), 0, &error);
    assert_non_null(db);

    int ret = wtree3_tree_delete(db, NULL, &error);
    assert_int_equal(WTREE3_EINVAL, ret);

    wtree3_db_close(db);
}

static void test_tree_name_null(void **state) {
    (void)state;

    const char *name = wtree3_tree_name(NULL);
    assert_null(name);
}

static void test_tree_count_null(void **state) {
    (void)state;

    int64_t count = wtree3_tree_count(NULL);
    assert_int_equal(0, count);
}

static void test_tree_get_db_null(void **state) {
    (void)state;

    wtree3_db_t *db = wtree3_tree_get_db(NULL);
    assert_null(db);
}

static void test_tree_set_compare_null_tree(void **state) {
    (void)state;
    gerror_t error = {0};

    int ret = wtree3_tree_set_compare(NULL, NULL, &error);
    assert_int_equal(WTREE3_EINVAL, ret);
}

static void test_tree_add_index_null_tree(void **state) {
    (void)state;
    gerror_t error = {0};
    wtree3_index_config_t config = {0};

    int ret = wtree3_tree_add_index(NULL, &config, &error);
    assert_int_equal(WTREE3_EINVAL, ret);
}

static void test_tree_add_index_null_config(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_db_t *db = wtree3_db_open(TEST_DB_PATH, 10485760, 128, WTREE3_VERSION(1, 0), 0, &error);
    assert_non_null(db);

    wtree3_tree_t *tree = wtree3_tree_open(db, "test", MDB_CREATE, 0, &error);
    assert_non_null(tree);

    int ret = wtree3_tree_add_index(tree, NULL, &error);
    assert_int_equal(WTREE3_EINVAL, ret);

    wtree3_tree_close(tree);
    wtree3_db_close(db);
}

static void test_tree_populate_index_null_tree(void **state) {
    (void)state;
    gerror_t error = {0};

    int ret = wtree3_tree_populate_index(NULL, "test_idx", &error);
    assert_int_equal(WTREE3_EINVAL, ret);
}

static void test_tree_populate_index_null_name(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_db_t *db = wtree3_db_open(TEST_DB_PATH, 10485760, 128, WTREE3_VERSION(1, 0), 0, &error);
    assert_non_null(db);

    wtree3_tree_t *tree = wtree3_tree_open(db, "test", MDB_CREATE, 0, &error);
    assert_non_null(tree);

    int ret = wtree3_tree_populate_index(tree, NULL, &error);
    assert_int_equal(WTREE3_EINVAL, ret);

    wtree3_tree_close(tree);
    wtree3_db_close(db);
}

static void test_tree_drop_index_null_tree(void **state) {
    (void)state;
    gerror_t error = {0};

    int ret = wtree3_tree_drop_index(NULL, "test_idx", &error);
    assert_int_equal(WTREE3_EINVAL, ret);
}

static void test_tree_drop_index_null_name(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_db_t *db = wtree3_db_open(TEST_DB_PATH, 10485760, 128, WTREE3_VERSION(1, 0), 0, &error);
    assert_non_null(db);

    wtree3_tree_t *tree = wtree3_tree_open(db, "test", MDB_CREATE, 0, &error);
    assert_non_null(tree);

    int ret = wtree3_tree_drop_index(tree, NULL, &error);
    assert_int_equal(WTREE3_EINVAL, ret);

    wtree3_tree_close(tree);
    wtree3_db_close(db);
}

static void test_tree_has_index_null_tree(void **state) {
    (void)state;

    bool result = wtree3_tree_has_index(NULL, "test_idx");
    assert_false(result);
}

static void test_tree_has_index_null_name(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_db_t *db = wtree3_db_open(TEST_DB_PATH, 10485760, 128, WTREE3_VERSION(1, 0), 0, &error);
    assert_non_null(db);

    wtree3_tree_t *tree = wtree3_tree_open(db, "test", MDB_CREATE, 0, &error);
    assert_non_null(tree);

    bool result = wtree3_tree_has_index(tree, NULL);
    assert_false(result);

    wtree3_tree_close(tree);
    wtree3_db_close(db);
}

static void test_tree_index_count_null(void **state) {
    (void)state;

    size_t count = wtree3_tree_index_count(NULL);
    assert_int_equal(0, count);
}

/* ============================================================
 * Data Operations NULL Tests
 * ============================================================ */

static void test_get_txn_null_txn(void **state) {
    (void)state;
    gerror_t error = {0};
    const void *value;
    size_t value_len;

    int ret = wtree3_get_txn(NULL, NULL, "key", 3, &value, &value_len, &error);
    assert_int_equal(WTREE3_EINVAL, ret);
}

static void test_insert_one_txn_null_txn(void **state) {
    (void)state;
    gerror_t error = {0};

    int ret = wtree3_insert_one_txn(NULL, NULL, "key", 3, "value", 5, &error);
    assert_int_equal(WTREE3_EINVAL, ret);
}

static void test_insert_one_txn_null_key(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_db_t *db = wtree3_db_open(TEST_DB_PATH, 10485760, 128, WTREE3_VERSION(1, 0), 0, &error);
    assert_non_null(db);

    wtree3_tree_t *tree = wtree3_tree_open(db, "test", MDB_CREATE, 0, &error);
    assert_non_null(tree);

    wtree3_txn_t *txn = wtree3_txn_begin(db, true, &error);
    assert_non_null(txn);

    int ret = wtree3_insert_one_txn(txn, tree, NULL, 0, "value", 5, &error);
    assert_int_equal(WTREE3_EINVAL, ret);

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
    wtree3_db_close(db);
}

static void test_update_txn_null_txn(void **state) {
    (void)state;
    gerror_t error = {0};

    int ret = wtree3_update_txn(NULL, NULL, "key", 3, "value", 5, &error);
    assert_int_equal(WTREE3_EINVAL, ret);
}

static void test_upsert_txn_null_txn(void **state) {
    (void)state;
    gerror_t error = {0};

    int ret = wtree3_upsert_txn(NULL, NULL, "key", 3, "value", 5, &error);
    assert_int_equal(WTREE3_EINVAL, ret);
}

static void test_delete_one_txn_null_txn(void **state) {
    (void)state;
    gerror_t error = {0};
    bool deleted;

    int ret = wtree3_delete_one_txn(NULL, NULL, "key", 3, &deleted, &error);
    assert_int_equal(WTREE3_EINVAL, ret);
}

static void test_exists_txn_null_txn(void **state) {
    (void)state;
    gerror_t error = {0};

    bool result = wtree3_exists_txn(NULL, NULL, "key", 3, &error);
    assert_false(result);
}

static void test_insert_many_txn_null_txn(void **state) {
    (void)state;
    gerror_t error = {0};
    wtree3_kv_t kvs[1] = {{0}};

    int ret = wtree3_insert_many_txn(NULL, NULL, kvs, 1, &error);
    assert_int_equal(WTREE3_EINVAL, ret);
}

static void test_upsert_many_txn_null_kvs(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_db_t *db = wtree3_db_open(TEST_DB_PATH, 10485760, 128, WTREE3_VERSION(1, 0), 0, &error);
    assert_non_null(db);

    wtree3_tree_t *tree = wtree3_tree_open(db, "test", MDB_CREATE, 0, &error);
    assert_non_null(tree);

    wtree3_txn_t *txn = wtree3_txn_begin(db, true, &error);
    assert_non_null(txn);

    int ret = wtree3_upsert_many_txn(txn, tree, NULL, 0, &error);
    assert_int_equal(WTREE3_EINVAL, ret);

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
    wtree3_db_close(db);
}

/* ============================================================
 * Auto-transaction Wrapper NULL Tests
 * ============================================================ */

static void test_get_null_tree(void **state) {
    (void)state;
    gerror_t error = {0};
    void *value;
    size_t value_len;

    int ret = wtree3_get(NULL, "key", 3, &value, &value_len, &error);
    assert_int_equal(WTREE3_EINVAL, ret);
}

static void test_insert_one_null_tree(void **state) {
    (void)state;
    gerror_t error = {0};

    int ret = wtree3_insert_one(NULL, "key", 3, "value", 5, &error);
    assert_int_equal(WTREE3_EINVAL, ret);
}

static void test_update_null_tree(void **state) {
    (void)state;
    gerror_t error = {0};

    int ret = wtree3_update(NULL, "key", 3, "value", 5, &error);
    assert_int_equal(WTREE3_EINVAL, ret);
}

static void test_upsert_null_tree(void **state) {
    (void)state;
    gerror_t error = {0};

    int ret = wtree3_upsert(NULL, "key", 3, "value", 5, &error);
    assert_int_equal(WTREE3_EINVAL, ret);
}

static void test_delete_one_null_tree(void **state) {
    (void)state;
    gerror_t error = {0};
    bool deleted;

    int ret = wtree3_delete_one(NULL, "key", 3, &deleted, &error);
    assert_int_equal(WTREE3_EINVAL, ret);
}

static void test_exists_null_tree(void **state) {
    (void)state;
    gerror_t error = {0};

    bool result = wtree3_exists(NULL, "key", 3, &error);
    assert_false(result);
}

/* ============================================================
 * Iterator NULL Tests
 * ============================================================ */

static void test_iterator_create_null_tree(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_iterator_t *iter = wtree3_iterator_create(NULL, &error);
    assert_null(iter);
}

static void test_iterator_create_with_txn_null_tree(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_iterator_t *iter = wtree3_iterator_create_with_txn(NULL, NULL, &error);
    assert_null(iter);
}

static void test_iterator_first_null(void **state) {
    (void)state;

    bool result = wtree3_iterator_first(NULL);
    assert_false(result);
}

static void test_iterator_valid_null(void **state) {
    (void)state;

    bool result = wtree3_iterator_valid(NULL);
    assert_false(result);
}

static void test_iterator_key_null(void **state) {
    (void)state;
    const void *key;
    size_t key_len;

    bool result = wtree3_iterator_key(NULL, &key, &key_len);
    assert_false(result);
}

static void test_iterator_delete_null(void **state) {
    (void)state;
    gerror_t error = {0};

    int ret = wtree3_iterator_delete(NULL, &error);
    assert_int_equal(WTREE3_EINVAL, ret);
}

static void test_iterator_get_txn_null(void **state) {
    (void)state;

    wtree3_txn_t *txn = wtree3_iterator_get_txn(NULL);
    assert_null(txn);
}

/* ============================================================
 * Tier 1 Operations NULL Tests
 * ============================================================ */

static void test_scan_range_txn_null_txn(void **state) {
    (void)state;
    gerror_t error = {0};

    int ret = wtree3_scan_range_txn(NULL, NULL, NULL, 0, NULL, 0, NULL, NULL, &error);
    assert_int_equal(WTREE3_EINVAL, ret);
}

static void test_scan_reverse_txn_null_txn(void **state) {
    (void)state;
    gerror_t error = {0};

    int ret = wtree3_scan_reverse_txn(NULL, NULL, NULL, 0, NULL, 0, NULL, NULL, &error);
    assert_int_equal(WTREE3_EINVAL, ret);
}

static void test_scan_prefix_txn_null_txn(void **state) {
    (void)state;
    gerror_t error = {0};

    int ret = wtree3_scan_prefix_txn(NULL, NULL, NULL, 0, NULL, NULL, &error);
    assert_int_equal(WTREE3_EINVAL, ret);
}

static void test_modify_txn_null_txn(void **state) {
    (void)state;
    gerror_t error = {0};

    int ret = wtree3_modify_txn(NULL, NULL, "key", 3, NULL, NULL, &error);
    assert_int_equal(WTREE3_EINVAL, ret);
}

static void test_get_many_txn_null_txn(void **state) {
    (void)state;
    gerror_t error = {0};
    wtree3_kv_t keys[1] = {{0}};
    const void *values[1];
    size_t value_lens[1];

    int ret = wtree3_get_many_txn(NULL, NULL, keys, 1, values, value_lens, &error);
    assert_int_equal(WTREE3_EINVAL, ret);
}

/* ============================================================
 * Tier 2 Operations NULL Tests
 * ============================================================ */

static void test_delete_if_txn_null_txn(void **state) {
    (void)state;
    gerror_t error = {0};
    size_t deleted;

    int ret = wtree3_delete_if_txn(NULL, NULL, NULL, 0, NULL, 0, NULL, NULL, &deleted, &error);
    assert_int_equal(WTREE3_EINVAL, ret);
}

static void test_collect_range_txn_null_txn(void **state) {
    (void)state;
    gerror_t error = {0};
    void **keys;
    size_t *key_lens;
    void **values;
    size_t *value_lens;
    size_t count;

    int ret = wtree3_collect_range_txn(NULL, NULL, NULL, 0, NULL, 0, NULL, NULL,
                                        &keys, &key_lens, &values, &value_lens,
                                        &count, 0, &error);
    assert_int_equal(WTREE3_EINVAL, ret);
}

static void test_exists_many_txn_null_txn(void **state) {
    (void)state;
    gerror_t error = {0};
    wtree3_kv_t keys[1] = {{0}};
    bool results[1];

    int ret = wtree3_exists_many_txn(NULL, NULL, keys, 1, results, &error);
    assert_int_equal(WTREE3_EINVAL, ret);
}

/* ============================================================
 * Index Operations NULL Tests
 * ============================================================ */

static void test_index_seek_null_tree(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_iterator_t *iter = wtree3_index_seek(NULL, "idx", "key", 3, &error);
    assert_null(iter);
}

static void test_index_seek_null_index_name(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_db_t *db = wtree3_db_open(TEST_DB_PATH, 10485760, 128, WTREE3_VERSION(1, 0), 0, &error);
    assert_non_null(db);

    wtree3_tree_t *tree = wtree3_tree_open(db, "test", MDB_CREATE, 0, &error);
    assert_non_null(tree);

    wtree3_iterator_t *iter = wtree3_index_seek(tree, NULL, "key", 3, &error);
    assert_null(iter);

    wtree3_tree_close(tree);
    wtree3_db_close(db);
}

static void test_index_seek_range_null_tree(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_iterator_t *iter = wtree3_index_seek_range(NULL, "idx", "key", 3, &error);
    assert_null(iter);
}

static void test_index_iterator_main_key_null(void **state) {
    (void)state;
    const void *main_key;
    size_t main_key_len;

    bool result = wtree3_index_iterator_main_key(NULL, &main_key, &main_key_len);
    assert_false(result);
}

/* ============================================================
 * Utility Function Tests
 * ============================================================ */

static void test_strerror_all_codes(void **state) {
    (void)state;

    /* Test all defined error codes */
    assert_non_null(wtree3_strerror(WTREE3_OK));
    assert_non_null(wtree3_strerror(WTREE3_ERROR));
    assert_non_null(wtree3_strerror(WTREE3_EINVAL));
    assert_non_null(wtree3_strerror(WTREE3_ENOMEM));
    assert_non_null(wtree3_strerror(WTREE3_KEY_EXISTS));
    assert_non_null(wtree3_strerror(WTREE3_NOT_FOUND));
    assert_non_null(wtree3_strerror(WTREE3_MAP_FULL));
    assert_non_null(wtree3_strerror(WTREE3_TXN_FULL));
    assert_non_null(wtree3_strerror(WTREE3_INDEX_ERROR));

    /* Test unknown error code */
    assert_non_null(wtree3_strerror(-9999));
}

static void test_error_recoverable(void **state) {
    (void)state;

    /* MAP_FULL should be recoverable */
    assert_true(wtree3_error_recoverable(WTREE3_MAP_FULL));

    /* Other errors should not be recoverable */
    assert_false(wtree3_error_recoverable(WTREE3_EINVAL));
    assert_false(wtree3_error_recoverable(WTREE3_ERROR));
    assert_false(wtree3_error_recoverable(WTREE3_KEY_EXISTS));
}

/* ============================================================
 * Index Persistence NULL Tests
 * ============================================================ */

// DISABLED:     (void)state;
// DISABLED:     gerror_t error = {0};
// DISABLED: 
// DISABLED:     int ret = wtree3_index_save_metadata(NULL, "idx", &error);
// DISABLED:     assert_int_equal(WTREE3_EINVAL, ret);
// DISABLED: }

// DISABLED:     (void)state;
// DISABLED:     gerror_t error = {0};
// DISABLED: 
// DISABLED:     wtree3_db_t *db = wtree3_db_open(TEST_DB_PATH, 10485760, 128, WTREE3_VERSION(1, 0), 0, &error);
// DISABLED:     assert_non_null(db);
// DISABLED: 
// DISABLED:     wtree3_tree_t *tree = wtree3_tree_open(db, "test", MDB_CREATE, 0, &error);
// DISABLED:     assert_non_null(tree);
// DISABLED: 
// DISABLED:     int ret = wtree3_index_save_metadata(tree, NULL, &error);
// DISABLED:     assert_int_equal(WTREE3_EINVAL, ret);
// DISABLED: 
// DISABLED:     wtree3_tree_close(tree);
// DISABLED:     wtree3_db_close(db);
// DISABLED: }

// DISABLED:     (void)state;
// DISABLED:     gerror_t error = {0};
// DISABLED: 
// DISABLED:     int ret = wtree3_index_load_metadata(NULL, "idx", NULL, NULL, &error);
// DISABLED:     assert_int_equal(WTREE3_EINVAL, ret);
// DISABLED: }

// DISABLED:     (void)state;
// DISABLED:     gerror_t error = {0};
// DISABLED:     size_t count;
// DISABLED: 
// DISABLED:     char **list = wtree3_tree_list_persisted_indexes(NULL, &count, &error);
// DISABLED:     assert_null(list);
// DISABLED: }

/* ============================================================
 * Main
 * ============================================================ */

int main(void) {
    const struct CMUnitTest tests[] = {
        /* Database NULL tests */
        cmocka_unit_test(test_db_open_null_path),
        cmocka_unit_test(test_db_sync_null_db),
        cmocka_unit_test(test_db_resize_null_db),
        cmocka_unit_test(test_db_stats_null_db),
        cmocka_unit_test(test_db_get_mapsize_null),
        cmocka_unit_test(test_db_get_env_null),

        /* Transaction NULL tests */
        cmocka_unit_test(test_txn_begin_null_db),
        cmocka_unit_test(test_txn_commit_null),
        cmocka_unit_test(test_txn_renew_null),
        cmocka_unit_test(test_txn_is_readonly_null),
        cmocka_unit_test(test_txn_get_mdb_null),
        cmocka_unit_test(test_txn_get_db_null),

        /* Tree NULL tests */
        cmocka_unit_test_setup_teardown(test_tree_open_null_db, setup, teardown),
        cmocka_unit_test_setup_teardown(test_tree_open_null_name, setup, teardown),
        cmocka_unit_test_setup_teardown(test_tree_delete_null_db, setup, teardown),
        cmocka_unit_test_setup_teardown(test_tree_delete_null_name, setup, teardown),
        cmocka_unit_test(test_tree_name_null),
        cmocka_unit_test(test_tree_count_null),
        cmocka_unit_test(test_tree_get_db_null),
        cmocka_unit_test(test_tree_set_compare_null_tree),

        /* Index NULL tests */
        cmocka_unit_test(test_tree_add_index_null_tree),
        cmocka_unit_test_setup_teardown(test_tree_add_index_null_config, setup, teardown),
        cmocka_unit_test(test_tree_populate_index_null_tree),
        cmocka_unit_test_setup_teardown(test_tree_populate_index_null_name, setup, teardown),
        cmocka_unit_test(test_tree_drop_index_null_tree),
        cmocka_unit_test_setup_teardown(test_tree_drop_index_null_name, setup, teardown),
        cmocka_unit_test(test_tree_has_index_null_tree),
        cmocka_unit_test_setup_teardown(test_tree_has_index_null_name, setup, teardown),
        cmocka_unit_test(test_tree_index_count_null),

        /* Data operations NULL tests */
        cmocka_unit_test(test_get_txn_null_txn),
        cmocka_unit_test(test_insert_one_txn_null_txn),
        cmocka_unit_test_setup_teardown(test_insert_one_txn_null_key, setup, teardown),
        cmocka_unit_test(test_update_txn_null_txn),
        cmocka_unit_test(test_upsert_txn_null_txn),
        cmocka_unit_test(test_delete_one_txn_null_txn),
        cmocka_unit_test(test_exists_txn_null_txn),
        cmocka_unit_test(test_insert_many_txn_null_txn),
        cmocka_unit_test_setup_teardown(test_upsert_many_txn_null_kvs, setup, teardown),

        /* Auto-transaction wrappers NULL tests */
        cmocka_unit_test(test_get_null_tree),
        cmocka_unit_test(test_insert_one_null_tree),
        cmocka_unit_test(test_update_null_tree),
        cmocka_unit_test(test_upsert_null_tree),
        cmocka_unit_test(test_delete_one_null_tree),
        cmocka_unit_test(test_exists_null_tree),

        /* Iterator NULL tests */
        cmocka_unit_test(test_iterator_create_null_tree),
        cmocka_unit_test(test_iterator_create_with_txn_null_tree),
        cmocka_unit_test(test_iterator_first_null),
        cmocka_unit_test(test_iterator_valid_null),
        cmocka_unit_test(test_iterator_key_null),
        cmocka_unit_test(test_iterator_delete_null),
        cmocka_unit_test(test_iterator_get_txn_null),

        /* Tier 1 operations NULL tests */
        cmocka_unit_test(test_scan_range_txn_null_txn),
        cmocka_unit_test(test_scan_reverse_txn_null_txn),
        cmocka_unit_test(test_scan_prefix_txn_null_txn),
        cmocka_unit_test(test_modify_txn_null_txn),
        cmocka_unit_test(test_get_many_txn_null_txn),

        /* Tier 2 operations NULL tests */
        cmocka_unit_test(test_delete_if_txn_null_txn),
        cmocka_unit_test(test_collect_range_txn_null_txn),
        cmocka_unit_test(test_exists_many_txn_null_txn),

        /* Index query NULL tests */
        cmocka_unit_test(test_index_seek_null_tree),
        cmocka_unit_test_setup_teardown(test_index_seek_null_index_name, setup, teardown),
        cmocka_unit_test(test_index_seek_range_null_tree),
        cmocka_unit_test(test_index_iterator_main_key_null),

        /* Utility tests */
        cmocka_unit_test(test_strerror_all_codes),
        cmocka_unit_test(test_error_recoverable),

        /* Index persistence NULL tests */
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
