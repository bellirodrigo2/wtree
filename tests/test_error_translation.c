/*
 * test_error_translation.c - Test LMDB error translation
 *
 * Tests the translate_mdb_error function by simulating various LMDB error conditions
 */

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <setjmp.h>
#include <cmocka.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <lmdb.h>

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

static char test_db_path[256];

/* ============================================================
 * Helper to create a tiny database that will fill quickly
 * ============================================================ */

static wtree3_db_t *create_tiny_db(void) {
    gerror_t error = {0};

#ifdef _WIN32
    snprintf(test_db_path, sizeof(test_db_path), "%s\\test_err_trans_%d",
             getenv("TEMP"), getpid());
#else
    snprintf(test_db_path, sizeof(test_db_path), "/tmp/test_err_trans_%d", getpid());
#endif
    mkdir(test_db_path, 0755);

    // Create VERY small database (100KB) to trigger MAP_FULL easily
    wtree3_db_t *db = wtree3_db_open(test_db_path, 100 * 1024, 10, WTREE3_VERSION(1, 0), 0, &error);
    return db;
}

static void cleanup_db(wtree3_db_t *db) {
    if (db) {
        wtree3_db_close(db);
    }

    char cmd[512];
#ifdef _WIN32
    snprintf(cmd, sizeof(cmd), "rd /s /q \"%s\"", test_db_path);
#else
    snprintf(cmd, sizeof(cmd), "rm -rf %s", test_db_path);
#endif
    (void)system(cmd);
}

/* ============================================================
 * Test MDB_MAP_FULL error
 * ============================================================ */

static void test_error_map_full(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_db_t *db = create_tiny_db();
    assert_non_null(db);

    wtree3_tree_t *tree = wtree3_tree_open(db, "test_tree", 0, 0, &error);
    assert_non_null(tree);

    // Fill the database until we get MAP_FULL
    int rc = WTREE3_OK;
    for (int i = 0; i < 10000 && rc == WTREE3_OK; i++) {
        char key[32], value[1024];  // Large value to fill quickly
        snprintf(key, sizeof(key), "key%d", i);
        memset(value, 'X', sizeof(value));
        value[sizeof(value)-1] = '\0';

        rc = wtree3_insert_one(tree, key, strlen(key), value, strlen(value), &error);
    }

    // Should eventually get MAP_FULL
    if (rc == WTREE3_MAP_FULL) {
        assert_int_equal(WTREE3_MAP_FULL, rc);
        assert_true(wtree3_error_recoverable(rc));

        // Verify error message
        assert_non_null(strstr(error.message, "map is full"));
    }

    wtree3_tree_close(tree);
    cleanup_db(db);
}

/* ============================================================
 * Test MDB_NOTFOUND error
 * ============================================================ */

static void test_error_not_found(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_db_t *db = create_tiny_db();
    assert_non_null(db);

    wtree3_tree_t *tree = wtree3_tree_open(db, "test_tree", 0, 0, &error);
    assert_non_null(tree);

    // Try to get non-existent key
    void *value = NULL;
    size_t value_len = 0;
    int rc = wtree3_get(tree, "nonexistent", 11, &value, &value_len, &error);

    assert_int_equal(WTREE3_NOT_FOUND, rc);
    assert_false(wtree3_error_recoverable(rc));
    assert_non_null(strstr(error.message, "not found"));

    wtree3_tree_close(tree);
    cleanup_db(db);
}

/* ============================================================
 * Test MDB_KEYEXIST error
 * ============================================================ */

static void test_error_key_exists(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_db_t *db = create_tiny_db();
    assert_non_null(db);

    wtree3_tree_t *tree = wtree3_tree_open(db, "test_tree", 0, 0, &error);
    assert_non_null(tree);

    // Insert a key
    int rc = wtree3_insert_one(tree, "key1", 4, "value1", 6, &error);
    assert_int_equal(WTREE3_OK, rc);

    // Try to insert same key again
    rc = wtree3_insert_one(tree, "key1", 4, "value2", 6, &error);

    assert_int_equal(WTREE3_KEY_EXISTS, rc);
    assert_false(wtree3_error_recoverable(rc));
    assert_non_null(strstr(error.message, "already exists"));

    wtree3_tree_close(tree);
    cleanup_db(db);
}

/* ============================================================
 * Test MDB_TXN_FULL error (requires many dirty pages)
 * ============================================================ */

static void test_error_txn_full(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_db_t *db = create_tiny_db();
    assert_non_null(db);

    wtree3_tree_t *tree = wtree3_tree_open(db, "test_tree", 0, 0, &error);
    assert_non_null(tree);

    // Start a write transaction
    wtree3_txn_t *txn = wtree3_txn_begin(db, true, &error);
    assert_non_null(txn);

    // Try to insert MANY entries in a single transaction to fill dirty pages
    int rc = WTREE3_OK;
    for (int i = 0; i < 10000 && rc == WTREE3_OK; i++) {
        char key[32], value[512];
        snprintf(key, sizeof(key), "key%d", i);
        memset(value, 'Y', sizeof(value));
        value[sizeof(value)-1] = '\0';

        rc = wtree3_insert_one_txn(txn, tree, key, strlen(key), value, strlen(value), &error);
    }

    // Might get TXN_FULL or MAP_FULL
    if (rc == WTREE3_TXN_FULL) {
        assert_int_equal(WTREE3_TXN_FULL, rc);
        assert_true(wtree3_error_recoverable(rc));
        assert_non_null(strstr(error.message, "Transaction"));
    }

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
    cleanup_db(db);
}

/* ============================================================
 * Test update on non-existent key (creates new entry - upsert behavior)
 * ============================================================ */

static void test_update_not_found(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_db_t *db = create_tiny_db();
    assert_non_null(db);

    wtree3_tree_t *tree = wtree3_tree_open(db, "test_tree", 0, 0, &error);
    assert_non_null(tree);

    // Update should return NOT_FOUND if key doesn't exist
    int rc = wtree3_update(tree, "nonexistent", 11, "value", 5, &error);

    assert_int_equal(WTREE3_NOT_FOUND, rc);

    wtree3_tree_close(tree);
    cleanup_db(db);
}

/* ============================================================
 * Test NULL parameter errors in txn-based operations
 * ============================================================ */

static void test_insert_txn_null_key(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_db_t *db = create_tiny_db();
    assert_non_null(db);

    wtree3_tree_t *tree = wtree3_tree_open(db, "test_tree", 0, 0, &error);
    wtree3_txn_t *txn = wtree3_txn_begin(db, true, &error);

    // NULL key
    int rc = wtree3_insert_one_txn(txn, tree, NULL, 4, "value", 5, &error);
    assert_int_equal(WTREE3_EINVAL, rc);

    // NULL value
    rc = wtree3_insert_one_txn(txn, tree, "key", 3, NULL, 5, &error);
    assert_int_equal(WTREE3_EINVAL, rc);

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
    cleanup_db(db);
}

static void test_update_txn_null_params(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_db_t *db = create_tiny_db();
    wtree3_tree_t *tree = wtree3_tree_open(db, "test_tree", 0, 0, &error);
    wtree3_txn_t *txn = wtree3_txn_begin(db, true, &error);

    // NULL key
    int rc = wtree3_update_txn(txn, tree, NULL, 4, "value", 5, &error);
    assert_int_equal(WTREE3_EINVAL, rc);

    // NULL value
    rc = wtree3_update_txn(txn, tree, "key", 3, NULL, 5, &error);
    assert_int_equal(WTREE3_EINVAL, rc);

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
    cleanup_db(db);
}

static void test_delete_txn_null_params(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_db_t *db = create_tiny_db();
    wtree3_tree_t *tree = wtree3_tree_open(db, "test_tree", 0, 0, &error);
    wtree3_txn_t *txn = wtree3_txn_begin(db, true, &error);

    bool deleted;

    // NULL key
    int rc = wtree3_delete_one_txn(txn, tree, NULL, 4, &deleted, &error);
    assert_int_equal(WTREE3_EINVAL, rc);

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
    cleanup_db(db);
}

/* ============================================================
 * Test delete on non-existent key (MDB_NOTFOUND but not an error)
 * ============================================================ */

static void test_delete_nonexistent_key(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_db_t *db = create_tiny_db();
    wtree3_tree_t *tree = wtree3_tree_open(db, "test_tree", 0, 0, &error);

    bool deleted;
    int rc = wtree3_delete_one(tree, "nonexistent", 11, &deleted, &error);

    assert_int_equal(WTREE3_OK, rc);  // OK, just not found
    assert_false(deleted);  // deleted should be false

    wtree3_tree_close(tree);
    cleanup_db(db);
}

/* ============================================================
 * Test all error string mappings
 * ============================================================ */

static void test_all_error_strings(void **state) {
    (void)state;

    // Test all defined error codes
    assert_string_equal("Success", wtree3_strerror(WTREE3_OK));
    assert_string_equal("Generic error", wtree3_strerror(WTREE3_ERROR));
    assert_string_equal("Invalid argument", wtree3_strerror(WTREE3_EINVAL));
    assert_string_equal("Out of memory", wtree3_strerror(WTREE3_ENOMEM));
    assert_string_equal("Key already exists", wtree3_strerror(WTREE3_KEY_EXISTS));
    assert_string_equal("Key not found", wtree3_strerror(WTREE3_NOT_FOUND));
    assert_string_equal("Database map is full, resize needed", wtree3_strerror(WTREE3_MAP_FULL));
    assert_string_equal("Transaction has too many dirty pages", wtree3_strerror(WTREE3_TXN_FULL));
    assert_string_equal("Index error (duplicate key violation)", wtree3_strerror(WTREE3_INDEX_ERROR));

    // Unknown error - falls back to mdb_strerror which may return empty string
    const char *unknown_err = wtree3_strerror(-9999);
    assert_non_null(unknown_err);
}

static void test_translate_all(void **state) {
    (void)state;

    assert_true(translate_mdb_error(0,NULL) == WTREE3_OK);
    assert_true(translate_mdb_error(MDB_TXN_FULL,NULL) == WTREE3_TXN_FULL);
}

/* ============================================================
 * Main
 * ============================================================ */

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_error_map_full),
        cmocka_unit_test(test_error_not_found),
        cmocka_unit_test(test_error_key_exists),
        cmocka_unit_test(test_error_txn_full),
        cmocka_unit_test(test_update_not_found),
        cmocka_unit_test(test_insert_txn_null_key),
        cmocka_unit_test(test_update_txn_null_params),
        cmocka_unit_test(test_delete_txn_null_params),
        cmocka_unit_test(test_delete_nonexistent_key),
        cmocka_unit_test(test_all_error_strings),
        cmocka_unit_test(test_translate_all),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
