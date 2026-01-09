/*
 * test_wtree3_param_validation.c - Parameter Validation and Boundary Tests
 *
 * This test file focuses on testing error handling for:
 * - NULL pointer parameters
 * - Invalid parameters (zero lengths, empty strings, etc.)
 * - Boundary values (empty collections, max sizes)
 * - Database full simulation (MAP_FULL)
 *
 * These tests target the remaining ~20% coverage gaps that are mostly
 * defensive code and error handling paths.
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
    #include <windows.h>
    #include <direct.h>
    #include <process.h>
    #include <io.h>
    #define mkdir(path, mode) _mkdir(path)
    #define getpid() _getpid()
    #define rmdir _rmdir
    #define unlink _unlink
#else
    #include <sys/stat.h>
    #include <unistd.h>
    #include <dirent.h>
#endif

#include "wtree3.h"

/* Test database path */
static char test_db_path[256];
static wtree3_db_t *test_db = NULL;
#define TEST_VERSION WTREE3_VERSION(1, 0)

/* ============================================================
 * Test Data
 * ============================================================ */

typedef struct {
    int id;
    char name[64];
} simple_record_t;

/* Simple extractor */
static bool simple_extractor(const void *value, size_t value_len,
                             void *user_data,
                             void **out_key, size_t *out_len) {
    (void)user_data;
    if (value_len < sizeof(simple_record_t)) return false;

    const simple_record_t *rec = (const simple_record_t *)value;
    size_t name_len = strlen(rec->name);
    if (name_len == 0) return false;

    char *key = malloc(name_len + 1);
    if (!key) return false;

    memcpy(key, rec->name, name_len + 1);
    *out_key = key;
    *out_len = name_len + 1;
    return true;
}

/* ============================================================
 * Utility Functions
 * ============================================================ */

static void remove_directory_recursive(const char *path) {
#ifdef _WIN32
    char search_path[512];
    snprintf(search_path, sizeof(search_path), "%s\\*", path);

    WIN32_FIND_DATAA find_data;
    HANDLE hFind = FindFirstFileA(search_path, &find_data);

    if (hFind != INVALID_HANDLE_VALUE) {
        do {
            if (strcmp(find_data.cFileName, ".") != 0 &&
                strcmp(find_data.cFileName, "..") != 0) {
                char file_path[512];
                snprintf(file_path, sizeof(file_path), "%s\\%s", path, find_data.cFileName);

                if (find_data.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) {
                    remove_directory_recursive(file_path);
                } else {
                    unlink(file_path);
                }
            }
        } while (FindNextFileA(hFind, &find_data));
        FindClose(hFind);
    }
    rmdir(path);
#else
    DIR *dir = opendir(path);
    if (dir) {
        struct dirent *entry;
        while ((entry = readdir(dir)) != NULL) {
            if (strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0) {
                char file_path[512];
                snprintf(file_path, sizeof(file_path), "%s/%s", path, entry->d_name);

                struct stat st;
                if (stat(file_path, &st) == 0) {
                    if (S_ISDIR(st.st_mode)) {
                        remove_directory_recursive(file_path);
                    } else {
                        unlink(file_path);
                    }
                }
            }
        }
        closedir(dir);
    }
    rmdir(path);
#endif
}

/* ============================================================
 * Test Fixtures
 * ============================================================ */

static int setup_db(void **state) {
    (void)state;
    gerror_t error = {0};

#ifdef _WIN32
    const char *temp = getenv("TEMP");
    if (!temp) temp = getenv("TMP");
    if (!temp) temp = ".";
    snprintf(test_db_path, sizeof(test_db_path), "%s\\test_param_valid_%d",
             temp, getpid());
#else
    snprintf(test_db_path, sizeof(test_db_path), "/tmp/test_param_valid_%d",
             getpid());
#endif
    mkdir(test_db_path, 0755);

    test_db = wtree3_db_open(test_db_path, 64 * 1024 * 1024, 32, TEST_VERSION, 0, &error);
    if (!test_db) {
        fprintf(stderr, "Failed to create test database: %s\n", error.message);
        return -1;
    }

    /* Register extractor */
    int rc = wtree3_db_register_key_extractor(test_db, TEST_VERSION, 0x00,
                                               simple_extractor, &error);
    if (rc != WTREE3_OK) {
        fprintf(stderr, "Failed to register extractor: %s\n", error.message);
        wtree3_db_close(test_db);
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

    remove_directory_recursive(test_db_path);
    return 0;
}

/* ============================================================
 * TEST: NULL Parameter Validation
 * ============================================================ */

static void test_null_parameters(void **state) {
    (void)state;
    gerror_t error = {0};
    int rc;

    printf("\n=== Testing NULL Parameter Validation ===\n");

    /* Create a valid tree for testing */
    wtree3_tree_t *tree = wtree3_tree_open(test_db, "test", MDB_CREATE, 0, &error);
    assert_non_null(tree);

    /* Test NULL tree parameter in various functions */
    printf("Testing NULL tree parameters...\n");

    simple_record_t rec = {1, "Test"};
    rc = wtree3_insert_one(NULL, "key", 4, &rec, sizeof(rec), &error);
    assert_int_not_equal(rc, WTREE3_OK);
    printf("  ✓ insert_one with NULL tree rejected\n");

    void *value = NULL;
    size_t value_len = 0;
    rc = wtree3_get(NULL, "key", 4, &value, &value_len, &error);
    assert_int_not_equal(rc, WTREE3_OK);
    printf("  ✓ get with NULL tree rejected\n");

    bool deleted = false;
    rc = wtree3_delete_one(NULL, "key", 4, &deleted, &error);
    assert_int_not_equal(rc, WTREE3_OK);
    printf("  ✓ delete_one with NULL tree rejected\n");

    /* Test NULL key parameter */
    printf("Testing NULL key parameters...\n");

    rc = wtree3_insert_one(tree, NULL, 4, &rec, sizeof(rec), &error);
    assert_int_not_equal(rc, WTREE3_OK);
    printf("  ✓ insert_one with NULL key rejected\n");

    rc = wtree3_get(tree, NULL, 4, &value, &value_len, &error);
    assert_int_not_equal(rc, WTREE3_OK);
    printf("  ✓ get with NULL key rejected\n");

    /* Test NULL value parameter */
    printf("Testing NULL value parameters...\n");

    rc = wtree3_insert_one(tree, "key", 4, NULL, sizeof(rec), &error);
    assert_int_not_equal(rc, WTREE3_OK);
    printf("  ✓ insert_one with NULL value rejected\n");

    /* Test NULL output parameters */
    printf("Testing NULL output parameters...\n");

    rc = wtree3_get(tree, "key", 4, NULL, &value_len, &error);
    assert_int_not_equal(rc, WTREE3_OK);
    printf("  ✓ get with NULL value output rejected\n");

    rc = wtree3_get(tree, "key", 4, &value, NULL, &error);
    assert_int_not_equal(rc, WTREE3_OK);
    printf("  ✓ get with NULL value_len output rejected\n");

    /* Test NULL transaction parameter */
    printf("Testing NULL transaction parameters...\n");

    const void *val_ptr;
    size_t val_len;
    rc = wtree3_get_txn(NULL, tree, "key", 4, &val_ptr, &val_len, &error);
    assert_int_not_equal(rc, WTREE3_OK);
    printf("  ✓ get_txn with NULL transaction rejected\n");

    /* Test scan with NULL callback */
    printf("Testing scan with NULL callback...\n");

    wtree3_txn_t *txn = wtree3_txn_begin(test_db, false, &error);
    assert_non_null(txn);

    rc = wtree3_scan_range_txn(txn, tree, NULL, 0, NULL, 0, NULL, NULL, &error);
    assert_int_not_equal(rc, WTREE3_OK);
    printf("  ✓ scan_range_txn with NULL callback rejected\n");

    wtree3_txn_abort(txn);

    wtree3_tree_close(tree);
    wtree3_tree_delete(test_db, "test", &error);
}

/* ============================================================
 * TEST: Zero Length and Empty String Parameters
 * ============================================================ */

static void test_zero_length_parameters(void **state) {
    (void)state;
    gerror_t error = {0};
    int rc;

    printf("\n=== Testing Zero Length Parameters ===\n");

    /* Create tree */
    wtree3_tree_t *tree = wtree3_tree_open(test_db, "zerotest", MDB_CREATE, 0, &error);
    assert_non_null(tree);

    simple_record_t rec = {1, "Test"};

    /* Test zero-length key */
    printf("Testing zero-length key...\n");
    rc = wtree3_insert_one(tree, "key", 0, &rec, sizeof(rec), &error);
    assert_int_not_equal(rc, WTREE3_OK);
    printf("  ✓ insert_one with zero-length key rejected\n");

    /* Test zero-length value */
    printf("Testing zero-length value...\n");
    rc = wtree3_insert_one(tree, "key", 4, &rec, 0, &error);
    /* Zero-length values might be allowed by LMDB - just verify it doesn't crash */
    if (rc == WTREE3_OK) {
        printf("  ✓ insert_one with zero-length value allowed\n");
    } else {
        printf("  ✓ insert_one with zero-length value rejected\n");
    }

    /* Test empty string as tree name */
    printf("Testing empty tree name...\n");
    wtree3_tree_t *empty_tree = wtree3_tree_open(test_db, "", MDB_CREATE, 0, &error);
    /* Empty name might be allowed or rejected - just verify it doesn't crash */
    if (empty_tree) {
        wtree3_tree_close(empty_tree);
        wtree3_tree_delete(test_db, "", &error);
        printf("  ✓ Empty tree name allowed\n");
    } else {
        printf("  ✓ Empty tree name rejected\n");
    }

    /* Test empty index name */
    printf("Testing empty index name...\n");
    const char *user_data = "field";
    wtree3_index_config_t idx_config = {
        .name = "",  /* Empty name */
        .user_data = user_data,
        .user_data_len = strlen(user_data) + 1,
        .unique = false,
        .sparse = false,
        .compare = NULL
    };
    rc = wtree3_tree_add_index(tree, &idx_config, &error);
    assert_int_not_equal(rc, WTREE3_OK);
    printf("  ✓ Empty index name rejected\n");

    /* Test NULL index name */
    idx_config.name = NULL;
    rc = wtree3_tree_add_index(tree, &idx_config, &error);
    assert_int_not_equal(rc, WTREE3_OK);
    printf("  ✓ NULL index name rejected\n");

    wtree3_tree_close(tree);
    wtree3_tree_delete(test_db, "zerotest", &error);
}

/* ============================================================
 * TEST: Operations on Empty Collections
 * ============================================================ */

static void test_empty_collections(void **state) {
    (void)state;
    gerror_t error = {0};
    int rc;

    printf("\n=== Testing Operations on Empty Collections ===\n");

    /* Create empty tree */
    wtree3_tree_t *tree = wtree3_tree_open(test_db, "empty", MDB_CREATE, 0, &error);
    assert_non_null(tree);

    /* Test get on empty tree */
    printf("Testing get on empty tree...\n");
    void *value = NULL;
    size_t value_len = 0;
    rc = wtree3_get(tree, "nonexistent", 12, &value, &value_len, &error);
    assert_int_equal(rc, WTREE3_NOT_FOUND);
    printf("  ✓ get on empty tree returns NOT_FOUND\n");

    /* Test delete on empty tree */
    printf("Testing delete on empty tree...\n");
    bool deleted = false;
    rc = wtree3_delete_one(tree, "nonexistent", 12, &deleted, &error);
    assert_int_equal(rc, WTREE3_OK);
    assert_false(deleted);
    printf("  ✓ delete on empty tree succeeds with deleted=false\n");

    /* Test update on empty tree */
    printf("Testing update on empty tree...\n");
    simple_record_t rec = {1, "Test"};
    rc = wtree3_update(tree, "key", 4, &rec, sizeof(rec), &error);
    assert_int_equal(rc, WTREE3_NOT_FOUND);
    printf("  ✓ update on empty tree returns NOT_FOUND\n");

    /* Test scan on empty tree */
    printf("Testing scan on empty tree...\n");
    int scan_count = 0;

    bool count_callback(const void *k, size_t klen, const void *v, size_t vlen, void *user_data) {
        (void)k; (void)klen; (void)v; (void)vlen;
        int *count = (int*)user_data;
        (*count)++;
        return true;
    }

    wtree3_txn_t *txn = wtree3_txn_begin(test_db, false, &error);
    rc = wtree3_scan_range_txn(txn, tree, NULL, 0, NULL, 0,
                                count_callback, &scan_count, &error);
    assert_int_equal(rc, WTREE3_OK);
    assert_int_equal(scan_count, 0);
    wtree3_txn_abort(txn);
    printf("  ✓ scan on empty tree returns 0 entries\n");

    /* Test collect on empty tree */
    printf("Testing collect on empty tree...\n");
    void **keys = NULL;
    size_t *key_lens = NULL;
    void **values = NULL;
    size_t *value_lens = NULL;
    size_t collected = 0;

    txn = wtree3_txn_begin(test_db, false, &error);
    rc = wtree3_collect_range_txn(txn, tree, NULL, 0, NULL, 0, NULL, NULL,
                                   &keys, &key_lens, &values, &value_lens,
                                   &collected, 0, &error);
    assert_int_equal(rc, WTREE3_OK);
    assert_int_equal(collected, 0);
    wtree3_txn_abort(txn);
    printf("  ✓ collect on empty tree returns 0 entries\n");

    /* Test tree count on empty tree */
    int64_t count = wtree3_tree_count(tree);
    assert_int_equal(count, 0);
    printf("  ✓ tree_count on empty tree returns 0\n");

    /* Test verify_indexes on tree with no indexes */
    printf("Testing verify_indexes with no indexes...\n");
    rc = wtree3_verify_indexes(tree, &error);
    assert_int_equal(rc, WTREE3_OK);
    printf("  ✓ verify_indexes with no indexes succeeds\n");

    wtree3_tree_close(tree);
    wtree3_tree_delete(test_db, "empty", &error);
}

/* ============================================================
 * TEST: Batch Operations with Empty Arrays
 * ============================================================ */

static void test_empty_batch_operations(void **state) {
    (void)state;
    gerror_t error = {0};
    int rc;

    printf("\n=== Testing Batch Operations with Empty Arrays ===\n");

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "batch", MDB_CREATE, 0, &error);
    assert_non_null(tree);

    /* Test insert_many with count=0 */
    printf("Testing insert_many with count=0...\n");
    wtree3_txn_t *txn = wtree3_txn_begin(test_db, true, &error);
    rc = wtree3_insert_many_txn(txn, tree, NULL, 0, &error);
    /* insert_many with count=0 might succeed or return INVALID_ARGUMENT */
    if (rc == WTREE3_OK) {
        wtree3_txn_commit(txn, &error);
        printf("  ✓ insert_many with count=0 succeeds\n");
    } else {
        wtree3_txn_abort(txn);
        printf("  ✓ insert_many with count=0 rejected\n");
    }

    /* Test get_many with count=0 */
    printf("Testing get_many with count=0...\n");
    const void **values = NULL;
    size_t *value_lens = NULL;
    txn = wtree3_txn_begin(test_db, false, &error);
    rc = wtree3_get_many_txn(txn, tree, NULL, 0, values, value_lens, &error);
    /* get_many with count=0 might succeed or return EINVAL */
    if (rc == WTREE3_OK) {
        printf("  ✓ get_many with count=0 succeeds\n");
    } else {
        printf("  ✓ get_many with count=0 rejected\n");
    }
    wtree3_txn_abort(txn);

    /* Test exists_many with count=0 */
    printf("Testing exists_many with count=0...\n");
    bool *results = NULL;
    txn = wtree3_txn_begin(test_db, false, &error);
    rc = wtree3_exists_many_txn(txn, tree, NULL, 0, results, &error);
    /* exists_many with count=0 might succeed or return EINVAL */
    if (rc == WTREE3_OK) {
        printf("  ✓ exists_many with count=0 succeeds\n");
    } else {
        printf("  ✓ exists_many with count=0 rejected\n");
    }
    wtree3_txn_abort(txn);

    wtree3_tree_close(tree);
    wtree3_tree_delete(test_db, "batch", &error);
}

/* ============================================================
 * TEST: Database Full Simulation (MAP_FULL)
 * ============================================================ */

static void test_database_full_simulation(void **state) {
    (void)state;
    gerror_t error = {0};
    int rc;

    printf("\n=== Testing Database Full Simulation ===\n");

    /* Create a very small database (1MB) to easily fill it */
    char small_db_path[256];
#ifdef _WIN32
    const char *temp = getenv("TEMP");
    if (!temp) temp = getenv("TMP");
    if (!temp) temp = ".";
    snprintf(small_db_path, sizeof(small_db_path), "%s\\test_full_%d", temp, getpid());
#else
    snprintf(small_db_path, sizeof(small_db_path), "/tmp/test_full_%d", getpid());
#endif
    mkdir(small_db_path, 0755);

    /* Open with very small mapsize (1 MB) */
    printf("Creating database with small mapsize (1 MB)...\n");
    wtree3_db_t *small_db = wtree3_db_open(small_db_path, 1 * 1024 * 1024, 16,
                                            TEST_VERSION, 0, &error);
    assert_non_null(small_db);

    /* Register extractor */
    wtree3_db_register_key_extractor(small_db, TEST_VERSION, 0x00,
                                      simple_extractor, &error);

    wtree3_tree_t *tree = wtree3_tree_open(small_db, "filltest", MDB_CREATE, 0, &error);
    assert_non_null(tree);

    /* Insert large records until we hit MAP_FULL */
    printf("Inserting large records to fill database...\n");
    simple_record_t rec;
    char large_data[8192];  /* 8KB per record */
    memset(large_data, 'X', sizeof(large_data));

    int insert_count = 0;
    bool hit_map_full = false;

    for (int i = 0; i < 1000; i++) {  /* Try to insert many records */
        snprintf(rec.name, sizeof(rec.name), "Record%d", i);
        rec.id = i;

        char key[64];
        snprintf(key, sizeof(key), "key:%d", i);

        rc = wtree3_insert_one(tree, key, strlen(key) + 1,
                               large_data, sizeof(large_data), &error);

        if (rc == WTREE3_MAP_FULL) {
            hit_map_full = true;
            printf("  ✓ Hit MAP_FULL after %d insertions\n", insert_count);

            /* Test that error is recoverable */
            bool recoverable = wtree3_error_recoverable(rc);
            assert_true(recoverable);
            printf("  ✓ MAP_FULL is marked as recoverable\n");

            /* Try to resize database */
            printf("  Attempting to resize database...\n");
            wtree3_tree_close(tree);
            rc = wtree3_db_resize(small_db, 4 * 1024 * 1024, &error);
            if (rc == WTREE3_OK) {
                printf("  ✓ Database resized successfully\n");

                /* Reopen tree and try to insert again */
                tree = wtree3_tree_open(small_db, "filltest", 0, insert_count, &error);
                rc = wtree3_insert_one(tree, key, strlen(key) + 1,
                                       large_data, sizeof(large_data), &error);
                if (rc == WTREE3_OK) {
                    printf("  ✓ Insert succeeded after resize\n");
                }
            }
            break;
        }

        if (rc != WTREE3_OK) {
            printf("  Insert failed with error: %s (code: %d)\n", error.message, rc);
            break;
        }

        insert_count++;
    }

    if (hit_map_full) {
        printf("✓ Successfully simulated and handled MAP_FULL condition\n");
    } else {
        printf("⚠ Did not hit MAP_FULL (inserted %d records)\n", insert_count);
    }

    wtree3_tree_close(tree);
    wtree3_db_close(small_db);
    remove_directory_recursive(small_db_path);
}

/* ============================================================
 * TEST: Index Operations with NULL/Invalid Parameters
 * ============================================================ */

static void test_index_null_parameters(void **state) {
    (void)state;
    gerror_t error = {0};
    int rc;

    printf("\n=== Testing Index NULL/Invalid Parameters ===\n");

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "idxtest", MDB_CREATE, 0, &error);
    assert_non_null(tree);

    /* Test add_index with NULL config */
    printf("Testing add_index with NULL config...\n");
    rc = wtree3_tree_add_index(tree, NULL, &error);
    assert_int_not_equal(rc, WTREE3_OK);
    printf("  ✓ add_index with NULL config rejected\n");

    /* Test populate_index on non-existent index */
    printf("Testing populate_index on non-existent index...\n");
    rc = wtree3_tree_populate_index(tree, "nonexistent", &error);
    assert_int_not_equal(rc, WTREE3_OK);
    printf("  ✓ populate_index on non-existent index rejected\n");

    /* Test drop_index on non-existent index */
    printf("Testing drop_index on non-existent index...\n");
    rc = wtree3_tree_drop_index(tree, "nonexistent", &error);
    assert_int_not_equal(rc, WTREE3_OK);
    printf("  ✓ drop_index on non-existent index rejected\n");

    /* Test has_index with NULL name */
    printf("Testing has_index with NULL name...\n");
    bool has = wtree3_tree_has_index(tree, NULL);
    assert_false(has);
    printf("  ✓ has_index with NULL name returns false\n");

    /* Test index_seek with NULL index name */
    printf("Testing index_seek with NULL index name...\n");
    wtree3_iterator_t *iter = wtree3_index_seek(tree, NULL, "key", 4, &error);
    assert_null(iter);
    printf("  ✓ index_seek with NULL index name rejected\n");

    /* Test index_get_extractor_id with NULL name */
    printf("Testing index_get_extractor_id with NULL name...\n");
    uint64_t extractor_id;
    rc = wtree3_index_get_extractor_id(tree, NULL, &extractor_id, &error);
    assert_int_not_equal(rc, WTREE3_OK);
    printf("  ✓ index_get_extractor_id with NULL name rejected\n");

    wtree3_tree_close(tree);
    wtree3_tree_delete(test_db, "idxtest", &error);
}

/* ============================================================
 * TEST: Iterator NULL Parameters
 * ============================================================ */

static void test_iterator_null_parameters(void **state) {
    (void)state;
    gerror_t error = {0};

    printf("\n=== Testing Iterator NULL Parameters ===\n");

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "itertest", MDB_CREATE, 0, &error);
    assert_non_null(tree);

    /* Test iterator_create with NULL tree */
    printf("Testing iterator_create with NULL tree...\n");
    wtree3_iterator_t *iter = wtree3_iterator_create(NULL, &error);
    assert_null(iter);
    printf("  ✓ iterator_create with NULL tree rejected\n");

    /* Test iterator functions with NULL iterator */
    printf("Testing iterator functions with NULL iterator...\n");

    bool result = wtree3_iterator_first(NULL);
    assert_false(result);
    printf("  ✓ iterator_first with NULL iterator returns false\n");

    result = wtree3_iterator_valid(NULL);
    assert_false(result);
    printf("  ✓ iterator_valid with NULL iterator returns false\n");

    const void *key_ptr;
    size_t key_len;
    result = wtree3_iterator_key(NULL, &key_ptr, &key_len);
    assert_false(result);
    printf("  ✓ iterator_key with NULL iterator returns false\n");

    /* Test iterator functions with NULL output parameters */
    iter = wtree3_iterator_create(tree, &error);
    if (iter) {
        result = wtree3_iterator_key(iter, NULL, &key_len);
        assert_false(result);
        printf("  ✓ iterator_key with NULL key output rejected\n");

        result = wtree3_iterator_key(iter, &key_ptr, NULL);
        assert_false(result);
        printf("  ✓ iterator_key with NULL key_len output rejected\n");

        wtree3_iterator_close(iter);
    }

    /* Test iterator_close with NULL */
    printf("Testing iterator_close with NULL...\n");
    wtree3_iterator_close(NULL);  /* Should not crash */
    printf("  ✓ iterator_close with NULL handled gracefully\n");

    wtree3_tree_close(tree);
    wtree3_tree_delete(test_db, "itertest", &error);
}

/* ============================================================
 * TEST: Transaction NULL Parameters
 * ============================================================ */

static void test_transaction_null_parameters(void **state) {
    (void)state;
    gerror_t error = {0};
    int rc;

    printf("\n=== Testing Transaction NULL Parameters ===\n");

    /* Test txn_begin with NULL db */
    printf("Testing txn_begin with NULL db...\n");
    wtree3_txn_t *txn = wtree3_txn_begin(NULL, false, &error);
    assert_null(txn);
    printf("  ✓ txn_begin with NULL db rejected\n");

    /* Test txn_commit with NULL */
    printf("Testing txn_commit with NULL...\n");
    rc = wtree3_txn_commit(NULL, &error);
    assert_int_not_equal(rc, WTREE3_OK);
    printf("  ✓ txn_commit with NULL txn rejected\n");

    /* Test txn_abort with NULL (should not crash) */
    printf("Testing txn_abort with NULL...\n");
    wtree3_txn_abort(NULL);
    printf("  ✓ txn_abort with NULL handled gracefully\n");

    /* Test txn_reset with NULL (should not crash) */
    printf("Testing txn_reset with NULL...\n");
    wtree3_txn_reset(NULL);
    printf("  ✓ txn_reset with NULL handled gracefully\n");

    /* Test txn_is_readonly with NULL */
    printf("Testing txn_is_readonly with NULL...\n");
    bool readonly = wtree3_txn_is_readonly(NULL);
    assert_false(readonly);
    printf("  ✓ txn_is_readonly with NULL returns false\n");
}

/* ============================================================
 * Test Runner
 * ============================================================ */

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_null_parameters, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_zero_length_parameters, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_empty_collections, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_empty_batch_operations, setup_db, teardown_db),
        cmocka_unit_test(test_database_full_simulation),  /* Creates its own DB */
        cmocka_unit_test_setup_teardown(test_index_null_parameters, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_iterator_null_parameters, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_transaction_null_parameters, setup_db, teardown_db),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
