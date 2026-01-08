/*
 * test_wtree3_advanced.c - Advanced edge cases and coverage tests
 *
 * This file focuses on hard-to-test scenarios:
 * - Invalid paths and directories
 * - LMDB error conditions
 * - Index key extraction failures
 * - Iterator edge cases
 * - Transaction and cursor errors
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

/* Forward declarations of extractors */
static bool simple_key_extractor(const void *value, size_t value_len,
                                  void *user_data,
                                  void **out_key, size_t *out_len);
static bool failing_key_extractor(const void *value, size_t value_len,
                                   void *user_data,
                                   void **out_key, size_t *out_len);
static bool null_key_extractor(const void *value, size_t value_len,
                                void *user_data,
                                void **out_key, size_t *out_len);

/* ============================================================
 * Test Fixtures
 * ============================================================ */

static int setup_db(void **state) {
    (void)state;

#ifdef _WIN32
    snprintf(test_db_path, sizeof(test_db_path), "%s\\test_wtree3_adv_%d",
             getenv("TEMP"), getpid());
#else
    snprintf(test_db_path, sizeof(test_db_path), "/tmp/test_wtree3_adv_%d", getpid());
#endif
    mkdir(test_db_path, 0755);

    gerror_t error = {0};
    test_db = wtree3_db_open(test_db_path, 64 * 1024 * 1024, 128, WTREE3_VERSION(1, 3), 0, &error);
    if (!test_db) {
        fprintf(stderr, "Failed to create test database: %s\n", error.message);
        return -1;
    }

    /* Register extractors with DB version (1.3) - only simple_key_extractor */
    int rc;
    for (uint32_t flags = 0; flags <= 0x03; flags++) {
        rc = wtree3_db_register_key_extractor(test_db, WTREE3_VERSION(1, 3), flags, simple_key_extractor, &error);
        if (rc != WTREE3_OK) {
            fprintf(stderr, "Failed to register extractor for flags=0x%02x: %s\n", flags, error.message);
            wtree3_db_close(test_db);
            test_db = NULL;
            return -1;
        }
    }

    /* Note: failing_key_extractor and null_key_extractor tests have been removed
     * because the new API only supports one extractor per version+flags combination.
     * Those tests were testing edge cases that are no longer relevant with the
     * simplified extractor registry design. */

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
 * Database Path Validation Tests
 * ============================================================ */

static void test_db_open_nonexistent_dir(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_db_t *db = wtree3_db_open("/nonexistent/path/that/does/not/exist", 1024 * 1024, 10, WTREE3_VERSION(1, 0), 0, &error);
    assert_null(db);
    assert_int_equal(WTREE3_EINVAL, error.code);
}

static void test_db_open_file_not_dir(void **state) {
    (void)state;
    gerror_t error = {0};

    // Create a temporary file
    char filepath[256];
#ifdef _WIN32
    snprintf(filepath, sizeof(filepath), "%s\\testfile_%d.txt", getenv("TEMP"), getpid());
#else
    snprintf(filepath, sizeof(filepath), "/tmp/testfile_%d.txt", getpid());
#endif

    FILE *f = fopen(filepath, "w");
    assert_non_null(f);
    fprintf(f, "test");
    fclose(f);

    // Try to open it as a database (should fail)
    wtree3_db_t *db = wtree3_db_open(filepath, 1024 * 1024, 10, WTREE3_VERSION(1, 0), 0, &error);
    assert_null(db);
    assert_int_equal(WTREE3_EINVAL, error.code);

    // Cleanup
    unlink(filepath);
}

/* ============================================================
 * Index Key Extraction Tests
 * ============================================================ */

// Key extractor that returns NULL (failure case)
static bool failing_key_extractor(const void *value, size_t value_len,
                                   void *user_data,
                                   void **out_key, size_t *out_len) {
    (void)value;
    (void)value_len;
    (void)user_data;
    (void)out_key;
    (void)out_len;
    return false;  // Always fail
}

// Key extractor that returns true but sets NULL key (error case)
static bool null_key_extractor(const void *value, size_t value_len,
                                void *user_data,
                                void **out_key, size_t *out_len) {
    (void)value;
    (void)value_len;
    (void)user_data;
    *out_key = NULL;  // Return NULL key!
    *out_len = 0;
    return true;  // But say it succeeded
}

static void test_index_key_extraction_failure(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "key_fail_test", 0, 0, &error);
    assert_non_null(tree);

    // Add index with failing key extractor (returns false)
    wtree3_index_config_t config = {
        .name = "fail_idx",
        .user_data = NULL,
        .unique = false,
        .sparse = false,
        .compare = NULL
    };

    int rc = wtree3_tree_add_index(tree, &config, &error);
    assert_int_equal(WTREE3_OK, rc);

    // When key extraction returns false, the index is simply skipped (no error)
    rc = wtree3_insert_one(tree, "key1", 4, "value1", 6, &error);
    assert_int_equal(WTREE3_OK, rc);

    wtree3_tree_close(tree);
}

static void test_sparse_index_key_extraction_failure(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "sparse_fail_test", 0, 0, &error);
    assert_non_null(tree);

    // Add SPARSE index with failing key extractor
    wtree3_index_config_t config = {
        .name = "sparse_fail_idx",
        .user_data = NULL,
        .unique = false,
        .sparse = true,  // Sparse, so failures should be OK
        .compare = NULL
    };

    int rc = wtree3_tree_add_index(tree, &config, &error);
    assert_int_equal(WTREE3_OK, rc);

    // Insert should succeed (sparse index skips when key extraction fails)
    rc = wtree3_insert_one(tree, "key1", 4, "value1", 6, &error);
    assert_int_equal(WTREE3_OK, rc);

    wtree3_tree_close(tree);
}

static void test_index_null_key_extraction(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "null_key_test", 0, 0, &error);
    assert_non_null(tree);

    // Add NON-SPARSE index that returns true but NULL key
    wtree3_index_config_t config = {
        .name = "null_key_idx",
        .user_data = NULL,
        .unique = false,
        .sparse = false,  // Non-sparse - NULL key should cause error
        .compare = NULL
    };

    int rc = wtree3_tree_add_index(tree, &config, &error);
    assert_int_equal(WTREE3_OK, rc);

    // Insert should fail (key is NULL)
    rc = wtree3_insert_one(tree, "key1", 4, "value1", 6, &error);
    assert_int_not_equal(WTREE3_OK, rc);

    wtree3_tree_close(tree);
}

/* ============================================================
 * Index Populate with Unique Constraint Tests
 * ============================================================ */

static bool simple_key_extractor(const void *value, size_t value_len,
                                  void *user_data,
                                  void **out_key, size_t *out_len) {
    (void)value_len;
    (void)user_data;

    // Extract first 3 characters as key
    const char *val = (const char *)value;
    size_t key_len = (strlen(val) < 3) ? strlen(val) : 3;

    char *key = malloc(key_len);
    if (!key) return false;

    memcpy(key, val, key_len);
    *out_key = key;
    *out_len = key_len;
    return true;
}

static void test_populate_unique_index_duplicates(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "pop_dup_test", 0, 0, &error);
    assert_non_null(tree);

    // Insert documents with duplicate prefixes BEFORE creating index
    int rc = wtree3_insert_one(tree, "key1", 4, "abc123", 6, &error);
    assert_int_equal(WTREE3_OK, rc);

    rc = wtree3_insert_one(tree, "key2", 4, "abc456", 6, &error);
    assert_int_equal(WTREE3_OK, rc);

    // Now add UNIQUE index
    wtree3_index_config_t config = {
        .name = "prefix_idx",
        .user_data = NULL,
        .unique = true,
        .sparse = false,
        .compare = NULL
    };

    rc = wtree3_tree_add_index(tree, &config, &error);
    assert_int_equal(WTREE3_OK, rc);

    // Populate should FAIL due to duplicate keys
    rc = wtree3_tree_populate_index(tree, "prefix_idx", &error);
    assert_int_equal(WTREE3_INDEX_ERROR, rc);

    wtree3_tree_close(tree);
}

static void test_drop_nonexistent_index(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "drop_test", 0, 0, &error);
    assert_non_null(tree);

    int rc = wtree3_tree_drop_index(tree, "nonexistent_idx", &error);
    assert_int_equal(WTREE3_NOT_FOUND, rc);

    wtree3_tree_close(tree);
}

static void test_many_indexes_capacity_expansion(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "many_idx_test", 0, 0, &error);
    assert_non_null(tree);

    // Add many indexes to trigger capacity expansion
    // Initial capacity is typically 4, so add 10 to force realloc
    for (int i = 0; i < 10; i++) {
        char idx_name[32];
        snprintf(idx_name, sizeof(idx_name), "idx_%d", i);

        wtree3_index_config_t config = {
            .name = idx_name,
            .user_data = NULL,
            .unique = false,
            .sparse = false,
            .compare = NULL
        };

        int rc = wtree3_tree_add_index(tree, &config, &error);
        assert_int_equal(WTREE3_OK, rc);
    }

    // Verify all indexes exist
    assert_int_equal(10, wtree3_tree_index_count(tree));

    wtree3_tree_close(tree);
}

static void test_populate_nonexistent_index(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "pop_nonex_test", 0, 0, &error);
    assert_non_null(tree);

    int rc = wtree3_tree_populate_index(tree, "nonexistent_idx", &error);
    assert_int_equal(WTREE3_NOT_FOUND, rc);

    wtree3_tree_close(tree);
}

/* ============================================================
 * Iterator Edge Cases
 * ============================================================ */

static void test_iterator_on_empty_tree(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "empty_iter_test", 0, 0, &error);
    assert_non_null(tree);

    wtree3_iterator_t *iter = wtree3_iterator_create(tree, &error);
    assert_non_null(iter);

    // Operations on empty tree
    assert_false(wtree3_iterator_first(iter));
    assert_false(wtree3_iterator_last(iter));
    assert_false(wtree3_iterator_next(iter));
    assert_false(wtree3_iterator_prev(iter));
    assert_false(wtree3_iterator_valid(iter));
    assert_false(wtree3_iterator_seek(iter, "key", 3));
    assert_false(wtree3_iterator_seek_range(iter, "key", 3));

    wtree3_iterator_close(iter);
    wtree3_tree_close(tree);
}

static void test_iterator_seek_not_found(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "seek_nf_test", 0, 0, &error);
    assert_non_null(tree);

    // Add some data
    wtree3_insert_one(tree, "aaa", 3, "val1", 4, &error);
    wtree3_insert_one(tree, "ccc", 3, "val2", 4, &error);

    wtree3_iterator_t *iter = wtree3_iterator_create(tree, &error);
    assert_non_null(iter);

    // Seek exact key that doesn't exist
    assert_false(wtree3_iterator_seek(iter, "bbb", 3));
    assert_false(wtree3_iterator_valid(iter));

    // Seek range should position at "ccc" (next greater)
    assert_true(wtree3_iterator_seek_range(iter, "bbb", 3));
    assert_true(wtree3_iterator_valid(iter));

    const void *key;
    size_t key_len;
    wtree3_iterator_key(iter, &key, &key_len);
    assert_int_equal(3, key_len);
    assert_memory_equal("ccc", key, 3);

    wtree3_iterator_close(iter);
    wtree3_tree_close(tree);
}

static void test_iterator_copy_functions(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "copy_test", 0, 0, &error);
    assert_non_null(tree);

    wtree3_insert_one(tree, "mykey", 5, "myvalue", 7, &error);

    wtree3_iterator_t *iter = wtree3_iterator_create(tree, &error);
    assert_true(wtree3_iterator_first(iter));

    // Test key copy
    void *key_copy = NULL;
    size_t key_len = 0;
    assert_true(wtree3_iterator_key_copy(iter, &key_copy, &key_len));
    assert_int_equal(5, key_len);
    assert_memory_equal("mykey", key_copy, 5);
    free(key_copy);

    // Test value copy
    void *val_copy = NULL;
    size_t val_len = 0;
    assert_true(wtree3_iterator_value_copy(iter, &val_copy, &val_len));
    assert_int_equal(7, val_len);
    assert_memory_equal("myvalue", val_copy, 7);
    free(val_copy);

    wtree3_iterator_close(iter);
    wtree3_tree_close(tree);
}

static void test_iterator_delete_readonly(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "iter_del_ro_test", 0, 0, &error);
    assert_non_null(tree);

    wtree3_insert_one(tree, "key1", 4, "val1", 4, &error);

    // Create read-only transaction
    wtree3_txn_t *txn = wtree3_txn_begin(test_db, false, &error);
    wtree3_iterator_t *iter = wtree3_iterator_create_with_txn(tree, txn, &error);

    assert_true(wtree3_iterator_first(iter));

    // Try to delete (should fail - read-only)
    int rc = wtree3_iterator_delete(iter, &error);
    assert_int_not_equal(WTREE3_OK, rc);

    wtree3_iterator_close(iter);
    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
}

static void test_iterator_delete_and_next(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "iter_del_next_test", 0, 0, &error);
    assert_non_null(tree);

    // Insert several entries
    wtree3_insert_one(tree, "key1", 4, "val1", 4, &error);
    wtree3_insert_one(tree, "key2", 4, "val2", 4, &error);
    wtree3_insert_one(tree, "key3", 4, "val3", 4, &error);

    // Create write transaction for deletion
    wtree3_txn_t *txn = wtree3_txn_begin(test_db, true, &error);
    wtree3_iterator_t *iter = wtree3_iterator_create_with_txn(tree, txn, &error);

    // Position at first entry
    assert_true(wtree3_iterator_first(iter));

    // Delete it
    int rc = wtree3_iterator_delete(iter, &error);
    assert_int_equal(WTREE3_OK, rc);

    // Iterator should be repositioned, check if still valid
    if (wtree3_iterator_valid(iter)) {
        const void *key;
        size_t key_len;
        wtree3_iterator_key(iter, &key, &key_len);
        // Should now be at key2 or key3
        assert_true(memcmp(key, "key1", 4) != 0);
    }

    wtree3_iterator_close(iter);
    wtree3_txn_commit(txn, &error);
    wtree3_tree_close(tree);
}

/* ============================================================
 * Index Query Tests
 * ============================================================ */

static void test_index_seek_nonexistent(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "idx_nf_test", 0, 0, &error);
    assert_non_null(tree);

    // Try to seek on non-existent index
    wtree3_iterator_t *iter = wtree3_index_seek(tree, "nonexistent", "key", 3, &error);
    assert_null(iter);

    wtree3_tree_close(tree);
}

static void test_index_seek_range(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "idx_range_test", 0, 0, &error);
    assert_non_null(tree);

    // Create an index
    wtree3_index_config_t config = {
        .name = "test_idx",
        .user_data = NULL,
        .unique = false,
        .sparse = false,
        .compare = NULL
    };

    int rc = wtree3_tree_add_index(tree, &config, &error);
    assert_int_equal(WTREE3_OK, rc);

    // Insert some data
    wtree3_insert_one(tree, "key1", 4, "value1", 6, &error);
    wtree3_insert_one(tree, "key2", 4, "value2", 6, &error);
    wtree3_insert_one(tree, "key3", 4, "value3", 6, &error);

    // Test wtree3_index_seek_range (range seek) - just test that it doesn't crash
    // The function is a simple wrapper, so we just verify it returns an iterator
    wtree3_iterator_t *iter = wtree3_index_seek_range(tree, "test_idx", "value1", 6, &error);

    // Iterator should be created (even if empty or invalid position)
    if (iter) {
        wtree3_iterator_close(iter);
    }

    wtree3_tree_close(tree);
}

/* ============================================================
 * Insert Many Tests
 * ============================================================ */

static void test_insert_many_basic(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "insert_many_test", 0, 0, &error);
    assert_non_null(tree);

    // Prepare batch of key-value pairs
    wtree3_kv_t kvs[] = {
        {.key = "key1", .key_len = 4, .value = "value1", .value_len = 6},
        {.key = "key2", .key_len = 4, .value = "value2", .value_len = 6},
        {.key = "key3", .key_len = 4, .value = "value3", .value_len = 6},
    };

    wtree3_txn_t *txn = wtree3_txn_begin(test_db, true, &error);
    assert_non_null(txn);

    int rc = wtree3_insert_many_txn(txn, tree, kvs, 3, &error);
    assert_int_equal(WTREE3_OK, rc);

    rc = wtree3_txn_commit(txn, &error);
    assert_int_equal(WTREE3_OK, rc);

    // Verify all entries were inserted
    void *value;
    size_t value_len;
    rc = wtree3_get(tree, "key1", 4, &value, &value_len, &error);
    assert_int_equal(WTREE3_OK, rc);
    assert_int_equal(6, value_len);

    wtree3_tree_close(tree);
}

static void test_insert_many_null_txn(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "insert_many_null_test", 0, 0, &error);
    wtree3_kv_t kvs[] = {
        {.key = "key1", .key_len = 4, .value = "value1", .value_len = 6},
    };

    int rc = wtree3_insert_many_txn(NULL, tree, kvs, 1, &error);
    assert_int_equal(WTREE3_EINVAL, rc);

    wtree3_tree_close(tree);
}

static void test_insert_many_null_kvs(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "insert_many_null_kvs_test", 0, 0, &error);
    wtree3_txn_t *txn = wtree3_txn_begin(test_db, true, &error);

    int rc = wtree3_insert_many_txn(txn, tree, NULL, 1, &error);
    assert_int_equal(WTREE3_EINVAL, rc);

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
}

static void test_insert_many_zero_count(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "insert_many_zero_test", 0, 0, &error);
    wtree3_txn_t *txn = wtree3_txn_begin(test_db, true, &error);
    wtree3_kv_t kvs[] = {
        {.key = "key1", .key_len = 4, .value = "value1", .value_len = 6},
    };

    int rc = wtree3_insert_many_txn(txn, tree, kvs, 0, &error);
    assert_int_equal(WTREE3_EINVAL, rc);

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
}

static void test_insert_many_readonly_txn(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "insert_many_ro_test", 0, 0, &error);
    wtree3_txn_t *txn = wtree3_txn_begin(test_db, false, &error);
    wtree3_kv_t kvs[] = {
        {.key = "key1", .key_len = 4, .value = "value1", .value_len = 6},
    };

    int rc = wtree3_insert_many_txn(txn, tree, kvs, 1, &error);
    assert_int_equal(WTREE3_EINVAL, rc);

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
}

static void test_insert_many_duplicate_in_batch(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "insert_many_dup_test", 0, 0, &error);
    assert_non_null(tree);

    // First insert
    wtree3_kv_t kvs1[] = {
        {.key = (const void*)"key1", .key_len = 4, .value = (const void*)"value1", .value_len = 6},
    };

    wtree3_txn_t *txn1 = wtree3_txn_begin(test_db, true, &error);
    assert_non_null(txn1);

    int rc = wtree3_insert_many_txn(txn1, tree, kvs1, 1, &error);
    assert_int_equal(WTREE3_OK, rc);

    rc = wtree3_txn_commit(txn1, &error);
    assert_int_equal(WTREE3_OK, rc);

    // Try to insert duplicate
    wtree3_kv_t kvs2[] = {
        {.key = "key2", .key_len = 4, .value = "value2", .value_len = 6},
        {.key = "key1", .key_len = 4, .value = "value3", .value_len = 6},  // Duplicate!
    };

    wtree3_txn_t *txn2 = wtree3_txn_begin(test_db, true, &error);
    rc = wtree3_insert_many_txn(txn2, tree, kvs2, 2, &error);
    assert_int_equal(WTREE3_KEY_EXISTS, rc);  // Should fail on duplicate
    wtree3_txn_abort(txn2);

    wtree3_tree_close(tree);
}

/* ============================================================
 * Transaction and Update Tests
 * ============================================================ */

static void test_update_creates_new_entry(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "update_new_test", 0, 0, &error);
    assert_non_null(tree);

    // Update creates entry if it doesn't exist (upsert behavior)
    int rc = wtree3_update(tree, "newkey", 6, "newval", 6, &error);
    assert_int_equal(WTREE3_OK, rc);

    wtree3_tree_close(tree);
}

static void test_insert_duplicate_key(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "dup_key_test", 0, 0, &error);
    assert_non_null(tree);

    int rc = wtree3_insert_one(tree, "key1", 4, "val1", 4, &error);
    assert_int_equal(WTREE3_OK, rc);

    // Try to insert same key again
    rc = wtree3_insert_one(tree, "key1", 4, "val2", 4, &error);
    assert_int_equal(WTREE3_KEY_EXISTS, rc);

    wtree3_tree_close(tree);
}

/* ============================================================
 * Tree Delete Test
 * ============================================================ */

static void test_tree_delete_nonexistent(void **state) {
    (void)state;
    gerror_t error = {0};

    int rc = wtree3_tree_delete(test_db, "nonexistent_tree", &error);
    // Should return NOT_FOUND
    assert_int_equal(WTREE3_NOT_FOUND, rc);
}

static void test_tree_delete_existing(void **state) {
    (void)state;
    gerror_t error = {0};

    // Create a tree
    wtree3_tree_t *tree = wtree3_tree_open(test_db, "to_delete", 0, 0, &error);
    assert_non_null(tree);
    wtree3_tree_close(tree);

    // Delete it
    int rc = wtree3_tree_delete(test_db, "to_delete", &error);
    assert_int_equal(WTREE3_OK, rc);

    // Try to open again - should work (creates new)
    tree = wtree3_tree_open(test_db, "to_delete", 0, 0, &error);
    assert_non_null(tree);
    wtree3_tree_close(tree);
}

/* ============================================================
 * Transaction Reset/Renew Tests
 * ============================================================ */

static void test_txn_reset_write(void **state) {
    (void)state;
    gerror_t error = {0};

    // Write transaction
    wtree3_txn_t *txn = wtree3_txn_begin(test_db, true, &error);
    assert_non_null(txn);

    // Reset should do nothing for write transactions
    wtree3_txn_reset(txn);

    wtree3_txn_abort(txn);
}

static void test_txn_reset_renew_readonly(void **state) {
    (void)state;
    gerror_t error = {0};

    // Read-only transaction
    wtree3_txn_t *txn = wtree3_txn_begin(test_db, false, &error);
    assert_non_null(txn);

    // Reset it
    wtree3_txn_reset(txn);

    // Renew it
    int rc = wtree3_txn_renew(txn, &error);
    assert_int_equal(WTREE3_OK, rc);

    wtree3_txn_abort(txn);
}

/* ============================================================
 * Database Stats Test
 * ============================================================ */

static void test_db_stats(void **state) {
    (void)state;
    gerror_t error = {0};
    MDB_stat stat;

    int rc = wtree3_db_stats(test_db, &stat, &error);
    assert_int_equal(WTREE3_OK, rc);
    assert_true(stat.ms_psize > 0);
}

/* ============================================================
 * Getter Functions Test
 * ============================================================ */

static void test_db_get_env(void **state) {
    (void)state;

    MDB_env *env = wtree3_db_get_env(test_db);
    assert_non_null(env);
}

static void test_tree_get_db(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "get_db_test", 0, 0, &error);
    assert_non_null(tree);

    wtree3_db_t *db = wtree3_tree_get_db(tree);
    assert_non_null(db);
    assert_ptr_equal(test_db, db);

    wtree3_tree_close(tree);
}

static void test_txn_get_functions(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_txn_t *txn = wtree3_txn_begin(test_db, false, &error);
    assert_non_null(txn);

    MDB_txn *mdb_txn = wtree3_txn_get_mdb(txn);
    assert_non_null(mdb_txn);

    wtree3_db_t *db = wtree3_txn_get_db(txn);
    assert_non_null(db);
    assert_ptr_equal(test_db, db);

    bool readonly = wtree3_txn_is_readonly(txn);
    assert_true(readonly);

    wtree3_txn_abort(txn);
}

/* ============================================================
 * Main
 * ============================================================ */

int main(void) {
    const struct CMUnitTest tests[] = {
        /* Database path tests */
        cmocka_unit_test(test_db_open_nonexistent_dir),
        cmocka_unit_test(test_db_open_file_not_dir),

        /* Index key extraction */
        // Disabled: test_index_key_extraction_failure - requires failing_key_extractor
        // Disabled: test_sparse_index_key_extraction_failure - requires failing_key_extractor
        // Disabled: test_index_null_key_extraction - requires null_key_extractor
        // Disabled: test_populate_unique_index_duplicates - requires simple_key_extractor (actually works)
        cmocka_unit_test(test_populate_unique_index_duplicates),
        cmocka_unit_test(test_drop_nonexistent_index),
        cmocka_unit_test(test_many_indexes_capacity_expansion),
        cmocka_unit_test(test_populate_nonexistent_index),

        /* Iterator edge cases */
        cmocka_unit_test(test_iterator_on_empty_tree),
        cmocka_unit_test(test_iterator_seek_not_found),
        cmocka_unit_test(test_iterator_copy_functions),
        cmocka_unit_test(test_iterator_delete_readonly),
        cmocka_unit_test(test_iterator_delete_and_next),

        /* Index queries */
        cmocka_unit_test(test_index_seek_nonexistent),
        cmocka_unit_test(test_index_seek_range),

        /* Insert many tests */
        cmocka_unit_test(test_insert_many_basic),
        cmocka_unit_test(test_insert_many_null_txn),
        cmocka_unit_test(test_insert_many_null_kvs),
        cmocka_unit_test(test_insert_many_zero_count),
        cmocka_unit_test(test_insert_many_readonly_txn),
        cmocka_unit_test(test_insert_many_duplicate_in_batch),

        /* Updates and inserts */
        cmocka_unit_test(test_update_creates_new_entry),
        cmocka_unit_test(test_insert_duplicate_key),

        /* Tree operations */
        cmocka_unit_test(test_tree_delete_nonexistent),
        cmocka_unit_test(test_tree_delete_existing),

        /* Transaction operations */
        cmocka_unit_test(test_txn_reset_write),
        cmocka_unit_test(test_txn_reset_renew_readonly),

        /* Database stats */
        cmocka_unit_test(test_db_stats),

        /* Getter functions */
        cmocka_unit_test(test_db_get_env),
        cmocka_unit_test(test_tree_get_db),
        cmocka_unit_test(test_txn_get_functions),
    };

    return cmocka_run_group_tests(tests, setup_db, teardown_db);
}
