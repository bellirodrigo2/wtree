/*
 * test_wtree3_errors.c - Error handling and edge case tests for wtree3
 *
 * This file complements test_wtree3.c by focusing on:
 * - NULL parameter validation
 * - Error conditions and recovery
 * - Edge cases and boundary conditions
 * - LMDB error translations
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
    #define mkdir(path, mode) _mkdir(path)
    #define getpid() _getpid()
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
    snprintf(test_db_path, sizeof(test_db_path), "%s\\test_wtree3_errors_%d",
             getenv("TEMP"), getpid());
#else
    snprintf(test_db_path, sizeof(test_db_path), "/tmp/test_wtree3_errors_%d", getpid());
#endif
    mkdir(test_db_path, 0755);

    gerror_t error = {0};
    test_db = wtree3_db_open(test_db_path, 64 * 1024 * 1024, 32, 0, &error);
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
 * Database Error Tests
 * ============================================================ */

static void test_db_open_null_path(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_db_t *db = wtree3_db_open(NULL, 1024 * 1024, 10, 0, &error);
    assert_null(db);
    assert_int_not_equal(0, error.code);
}

static void test_db_sync_null(void **state) {
    (void)state;
    gerror_t error = {0};

    int rc = wtree3_db_sync(NULL, false, &error);
    assert_int_not_equal(WTREE3_OK, rc);
}

static void test_db_resize_null(void **state) {
    (void)state;
    gerror_t error = {0};

    int rc = wtree3_db_resize(NULL, 1024 * 1024, &error);
    assert_int_not_equal(WTREE3_OK, rc);
}

static void test_db_get_mapsize_null(void **state) {
    (void)state;

    size_t size = wtree3_db_get_mapsize(NULL);
    assert_int_equal(0, size);
}

static void test_db_stats_null(void **state) {
    (void)state;
    gerror_t error = {0};
    MDB_stat stat;

    // NULL database
    int rc = wtree3_db_stats(NULL, &stat, &error);
    assert_int_not_equal(WTREE3_OK, rc);

    // NULL stat
    rc = wtree3_db_stats(test_db, NULL, &error);
    assert_int_not_equal(WTREE3_OK, rc);
}

static void test_db_get_env_null(void **state) {
    (void)state;

    MDB_env *env = wtree3_db_get_env(NULL);
    assert_null(env);
}

/* ============================================================
 * Transaction Error Tests
 * ============================================================ */

static void test_txn_begin_null_db(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_txn_t *txn = wtree3_txn_begin(NULL, true, &error);
    assert_null(txn);
}

static void test_txn_commit_null(void **state) {
    (void)state;
    gerror_t error = {0};

    int rc = wtree3_txn_commit(NULL, &error);
    assert_int_not_equal(WTREE3_OK, rc);
}

static void test_txn_abort_null(void **state) {
    (void)state;

    // Should not crash
    wtree3_txn_abort(NULL);
    assert_true(1);
}

static void test_txn_reset_null(void **state) {
    (void)state;

    // Should not crash
    wtree3_txn_reset(NULL);
    assert_true(1);
}

static void test_txn_renew_null(void **state) {
    (void)state;
    gerror_t error = {0};

    int rc = wtree3_txn_renew(NULL, &error);
    assert_int_not_equal(WTREE3_OK, rc);
}

static void test_txn_renew_write_txn(void **state) {
    (void)state;
    gerror_t error = {0};

    // Create write transaction
    wtree3_txn_t *txn = wtree3_txn_begin(test_db, true, &error);
    assert_non_null(txn);

    // Try to renew (should fail - only read txns can be renewed)
    int rc = wtree3_txn_renew(txn, &error);
    assert_int_not_equal(WTREE3_OK, rc);

    wtree3_txn_abort(txn);
}

static void test_txn_is_readonly_null(void **state) {
    (void)state;

    bool readonly = wtree3_txn_is_readonly(NULL);
    assert_false(readonly);  // NULL txn returns false
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

/* ============================================================
 * Tree Error Tests
 * ============================================================ */

static void test_tree_open_null_db(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(NULL, "test", 0, 0, &error);
    assert_null(tree);
}

static void test_tree_open_null_name(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, NULL, 0, 0, &error);
    assert_null(tree);
}

static void test_tree_close_null(void **state) {
    (void)state;

    // Should not crash
    wtree3_tree_close(NULL);
    assert_true(1);
}

static void test_tree_delete_null_db(void **state) {
    (void)state;
    gerror_t error = {0};

    int rc = wtree3_tree_delete(NULL, "test", &error);
    assert_int_not_equal(WTREE3_OK, rc);
}

static void test_tree_delete_null_name(void **state) {
    (void)state;
    gerror_t error = {0};

    int rc = wtree3_tree_delete(test_db, NULL, &error);
    assert_int_not_equal(WTREE3_OK, rc);
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

    int rc = wtree3_tree_set_compare(NULL, NULL, &error);
    assert_int_not_equal(WTREE3_OK, rc);
}

/* ============================================================
 * Index Error Tests
 * ============================================================ */

static void test_add_index_null_tree(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_index_config_t config = {
        .name = "test_idx",
        .key_fn = NULL,
        .user_data = NULL,
        .unique = false,
        .sparse = false,
        .compare = NULL
    };

    int rc = wtree3_tree_add_index(NULL, &config, &error);
    assert_int_not_equal(WTREE3_OK, rc);
}

static void test_add_index_null_config(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "idx_test", 0, 0, &error);
    assert_non_null(tree);

    int rc = wtree3_tree_add_index(tree, NULL, &error);
    assert_int_not_equal(WTREE3_OK, rc);

    wtree3_tree_close(tree);
}

static void test_tree_has_index_null(void **state) {
    (void)state;

    bool has = wtree3_tree_has_index(NULL, "test");
    assert_false(has);
}

static void test_tree_index_count_null(void **state) {
    (void)state;

    size_t count = wtree3_tree_index_count(NULL);
    assert_int_equal(0, count);
}

static void test_drop_index_null_tree(void **state) {
    (void)state;
    gerror_t error = {0};

    int rc = wtree3_tree_drop_index(NULL, "test", &error);
    assert_int_not_equal(WTREE3_OK, rc);
}

static void test_populate_index_null_tree(void **state) {
    (void)state;
    gerror_t error = {0};

    int rc = wtree3_tree_populate_index(NULL, "test", &error);
    assert_int_not_equal(WTREE3_OK, rc);
}

/* ============================================================
 * Data Operation Error Tests
 * ============================================================ */

static void test_get_null_tree(void **state) {
    (void)state;
    gerror_t error = {0};
    void *value = NULL;
    size_t value_len = 0;

    int rc = wtree3_get(NULL, "key", 3, &value, &value_len, &error);
    assert_int_not_equal(WTREE3_OK, rc);
}

static void test_get_not_found(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "get_test", 0, 0, &error);
    assert_non_null(tree);

    void *value = NULL;
    size_t value_len = 0;
    int rc = wtree3_get(tree, "nonexistent", 11, &value, &value_len, &error);
    assert_int_equal(WTREE3_NOT_FOUND, rc);

    wtree3_tree_close(tree);
}

static void test_insert_null_tree(void **state) {
    (void)state;
    gerror_t error = {0};

    int rc = wtree3_insert_one(NULL, "key", 3, "val", 3, &error);
    assert_int_not_equal(WTREE3_OK, rc);
}

static void test_update_null_tree(void **state) {
    (void)state;
    gerror_t error = {0};

    int rc = wtree3_update(NULL, "key", 3, "val", 3, &error);
    assert_int_not_equal(WTREE3_OK, rc);
}

static void test_delete_null_tree(void **state) {
    (void)state;
    gerror_t error = {0};
    bool deleted = false;

    int rc = wtree3_delete_one(NULL, "key", 3, &deleted, &error);
    assert_int_not_equal(WTREE3_OK, rc);
}

static void test_exists_null_tree(void **state) {
    (void)state;
    gerror_t error = {0};

    bool exists = wtree3_exists(NULL, "key", 3, &error);
    assert_false(exists);
}

/* ============================================================
 * Transaction-based Operation Error Tests
 * ============================================================ */

static void test_get_txn_null_params(void **state) {
    (void)state;
    gerror_t error = {0};
    const void *value = NULL;
    size_t value_len = 0;

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "txn_test", 0, 0, &error);
    wtree3_txn_t *txn = wtree3_txn_begin(test_db, false, &error);

    // NULL transaction
    int rc = wtree3_get_txn(NULL, tree, "key", 3, &value, &value_len, &error);
    assert_int_not_equal(WTREE3_OK, rc);

    // NULL tree
    rc = wtree3_get_txn(txn, NULL, "key", 3, &value, &value_len, &error);
    assert_int_not_equal(WTREE3_OK, rc);

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
}

static void test_insert_readonly_txn(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "readonly_test", 0, 0, &error);
    wtree3_txn_t *txn = wtree3_txn_begin(test_db, false, &error);  // read-only

    int rc = wtree3_insert_one_txn(txn, tree, "key", 3, "val", 3, &error);
    assert_int_not_equal(WTREE3_OK, rc);

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
}

static void test_update_readonly_txn(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "readonly_test2", 0, 0, &error);
    wtree3_txn_t *txn = wtree3_txn_begin(test_db, false, &error);  // read-only

    int rc = wtree3_update_txn(txn, tree, "key", 3, "val", 3, &error);
    assert_int_not_equal(WTREE3_OK, rc);

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
}

static void test_delete_readonly_txn(void **state) {
    (void)state;
    gerror_t error = {0};
    bool deleted = false;

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "readonly_test3", 0, 0, &error);
    wtree3_txn_t *txn = wtree3_txn_begin(test_db, false, &error);  // read-only

    int rc = wtree3_delete_one_txn(txn, tree, "key", 3, &deleted, &error);
    assert_int_not_equal(WTREE3_OK, rc);

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
}

static void test_exists_txn_null_params(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "exists_test", 0, 0, &error);
    wtree3_txn_t *txn = wtree3_txn_begin(test_db, false, &error);

    // NULL transaction
    bool exists = wtree3_exists_txn(NULL, tree, "key", 3, &error);
    assert_false(exists);

    // NULL tree
    exists = wtree3_exists_txn(txn, NULL, "key", 3, &error);
    assert_false(exists);

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
}

/* ============================================================
 * Iterator Error Tests
 * ============================================================ */

static void test_iterator_create_null_tree(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_iterator_t *iter = wtree3_iterator_create(NULL, &error);
    assert_null(iter);
}

static void test_iterator_create_txn_null_params(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "iter_test", 0, 0, &error);
    wtree3_txn_t *txn = wtree3_txn_begin(test_db, false, &error);

    // NULL tree
    wtree3_iterator_t *iter = wtree3_iterator_create_with_txn(NULL, txn, &error);
    assert_null(iter);

    // NULL transaction
    iter = wtree3_iterator_create_with_txn(tree, NULL, &error);
    assert_null(iter);

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
}

static void test_iterator_close_null(void **state) {
    (void)state;

    // Should not crash
    wtree3_iterator_close(NULL);
    assert_true(1);
}

static void test_iterator_ops_null(void **state) {
    (void)state;

    // All should return false/invalid without crashing
    assert_false(wtree3_iterator_first(NULL));
    assert_false(wtree3_iterator_last(NULL));
    assert_false(wtree3_iterator_next(NULL));
    assert_false(wtree3_iterator_prev(NULL));
    assert_false(wtree3_iterator_seek(NULL, "key", 3));
    assert_false(wtree3_iterator_seek_range(NULL, "key", 3));
    assert_false(wtree3_iterator_valid(NULL));
}

static void test_iterator_key_value_null(void **state) {
    (void)state;
    const void *ptr = NULL;
    size_t len = 0;

    assert_false(wtree3_iterator_key(NULL, &ptr, &len));
    assert_false(wtree3_iterator_value(NULL, &ptr, &len));
}

static void test_iterator_copy_null(void **state) {
    (void)state;
    void *ptr = NULL;
    size_t len = 0;

    assert_false(wtree3_iterator_key_copy(NULL, &ptr, &len));
    assert_false(wtree3_iterator_value_copy(NULL, &ptr, &len));
}

static void test_iterator_delete_null(void **state) {
    (void)state;
    gerror_t error = {0};

    int rc = wtree3_iterator_delete(NULL, &error);
    assert_int_not_equal(WTREE3_OK, rc);
}

static void test_iterator_get_txn_null(void **state) {
    (void)state;

    wtree3_txn_t *txn = wtree3_iterator_get_txn(NULL);
    assert_null(txn);
}

/* ============================================================
 * Index Query Error Tests
 * ============================================================ */

static void test_index_seek_null_tree(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_iterator_t *iter = wtree3_index_seek(NULL, "idx", "key", 3, &error);
    assert_null(iter);
}

static void test_index_seek_null_name(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "idx_seek_test", 0, 0, &error);

    wtree3_iterator_t *iter = wtree3_index_seek(tree, NULL, "key", 3, &error);
    assert_null(iter);

    wtree3_tree_close(tree);
}

static void test_index_iterator_main_key_null(void **state) {
    (void)state;
    const void *key = NULL;
    size_t key_len = 0;

    bool result = wtree3_index_iterator_main_key(NULL, &key, &key_len);
    assert_false(result);
}

/* ============================================================
 * Error Code Tests
 * ============================================================ */

static void test_error_codes(void **state) {
    (void)state;

    // Test all error code strings
    assert_string_equal("Success", wtree3_strerror(WTREE3_OK));
    assert_string_equal("Generic error", wtree3_strerror(WTREE3_ERROR));
    assert_string_equal("Invalid argument", wtree3_strerror(WTREE3_EINVAL));
    assert_string_equal("Out of memory", wtree3_strerror(WTREE3_ENOMEM));
    assert_string_equal("Key already exists", wtree3_strerror(WTREE3_KEY_EXISTS));
    assert_string_equal("Key not found", wtree3_strerror(WTREE3_NOT_FOUND));
    assert_string_equal("Database map is full, resize needed", wtree3_strerror(WTREE3_MAP_FULL));
    assert_string_equal("Transaction has too many dirty pages", wtree3_strerror(WTREE3_TXN_FULL));
    assert_string_equal("Index error (duplicate key violation)", wtree3_strerror(WTREE3_INDEX_ERROR));

    // Unknown error code - falls back to mdb_strerror
    const char *unknown_err = wtree3_strerror(-9999);
    assert_non_null(unknown_err);
}

static void test_error_recoverability(void **state) {
    (void)state;

    // Recoverable errors
    assert_true(wtree3_error_recoverable(WTREE3_MAP_FULL));
    assert_true(wtree3_error_recoverable(WTREE3_TXN_FULL));

    // Non-recoverable errors
    assert_false(wtree3_error_recoverable(WTREE3_OK));
    assert_false(wtree3_error_recoverable(WTREE3_ERROR));
    assert_false(wtree3_error_recoverable(WTREE3_EINVAL));
    assert_false(wtree3_error_recoverable(WTREE3_ENOMEM));
}

/* ============================================================
 * Main
 * ============================================================ */

int main(void) {
    const struct CMUnitTest tests[] = {
        /* Database errors */
        cmocka_unit_test(test_db_open_null_path),
        cmocka_unit_test(test_db_sync_null),
        cmocka_unit_test(test_db_resize_null),
        cmocka_unit_test(test_db_get_mapsize_null),
        cmocka_unit_test(test_db_stats_null),
        cmocka_unit_test(test_db_get_env_null),

        /* Transaction errors */
        cmocka_unit_test(test_txn_begin_null_db),
        cmocka_unit_test(test_txn_commit_null),
        cmocka_unit_test(test_txn_abort_null),
        cmocka_unit_test(test_txn_reset_null),
        cmocka_unit_test(test_txn_renew_null),
        cmocka_unit_test(test_txn_renew_write_txn),
        cmocka_unit_test(test_txn_is_readonly_null),
        cmocka_unit_test(test_txn_get_mdb_null),
        cmocka_unit_test(test_txn_get_db_null),

        /* Tree errors */
        cmocka_unit_test(test_tree_open_null_db),
        cmocka_unit_test(test_tree_open_null_name),
        cmocka_unit_test(test_tree_close_null),
        cmocka_unit_test(test_tree_delete_null_db),
        cmocka_unit_test(test_tree_delete_null_name),
        cmocka_unit_test(test_tree_name_null),
        cmocka_unit_test(test_tree_count_null),
        cmocka_unit_test(test_tree_get_db_null),
        cmocka_unit_test(test_tree_set_compare_null_tree),

        /* Index errors */
        cmocka_unit_test(test_add_index_null_tree),
        cmocka_unit_test(test_add_index_null_config),
        cmocka_unit_test(test_tree_has_index_null),
        cmocka_unit_test(test_tree_index_count_null),
        cmocka_unit_test(test_drop_index_null_tree),
        cmocka_unit_test(test_populate_index_null_tree),

        /* Data operation errors */
        cmocka_unit_test(test_get_null_tree),
        cmocka_unit_test(test_get_not_found),
        cmocka_unit_test(test_insert_null_tree),
        cmocka_unit_test(test_update_null_tree),
        cmocka_unit_test(test_delete_null_tree),
        cmocka_unit_test(test_exists_null_tree),

        /* Transaction-based operation errors */
        cmocka_unit_test(test_get_txn_null_params),
        cmocka_unit_test(test_insert_readonly_txn),
        cmocka_unit_test(test_update_readonly_txn),
        cmocka_unit_test(test_delete_readonly_txn),
        cmocka_unit_test(test_exists_txn_null_params),

        /* Iterator errors */
        cmocka_unit_test(test_iterator_create_null_tree),
        cmocka_unit_test(test_iterator_create_txn_null_params),
        cmocka_unit_test(test_iterator_close_null),
        cmocka_unit_test(test_iterator_ops_null),
        cmocka_unit_test(test_iterator_key_value_null),
        cmocka_unit_test(test_iterator_copy_null),
        cmocka_unit_test(test_iterator_delete_null),
        cmocka_unit_test(test_iterator_get_txn_null),

        /* Index query errors */
        cmocka_unit_test(test_index_seek_null_tree),
        cmocka_unit_test(test_index_seek_null_name),
        cmocka_unit_test(test_index_iterator_main_key_null),

        /* Error codes */
        cmocka_unit_test(test_error_codes),
        cmocka_unit_test(test_error_recoverability),
    };

    return cmocka_run_group_tests(tests, setup_db, teardown_db);
}
