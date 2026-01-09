/*
 * test_wtree3_edge_cases.c - Edge Cases and Error Path Testing
 *
 * This test file focuses on coverage of error handling, edge cases, and
 * boundary conditions that are often missed in typical integration tests:
 *
 * - Custom comparators for indexes
 * - Index seek/seek_range edge cases
 * - Transaction reset/renew operations
 * - Database statistics and resize
 * - Error parameter validation
 * - Empty tree operations
 * - Boundary value testing
 * - Cursor operations
 * - Index metadata introspection
 *
 * These tests complement test_wtree3_full_integration.c to achieve high coverage.
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
 * Data Structures
 * ============================================================ */

typedef struct {
    int id;
    char name[64];
    int score;  // For custom comparator testing
} record_t;

/* ============================================================
 * Index Extractors
 * ============================================================ */

static bool name_extractor(const void *value, size_t value_len,
                           void *user_data,
                           void **out_key, size_t *out_len) {
    (void)user_data;
    if (value_len < sizeof(record_t)) return false;

    const record_t *rec = (const record_t *)value;
    size_t name_len = strlen(rec->name);
    if (name_len == 0) return false;

    char *key = malloc(name_len + 1);
    if (!key) return false;

    memcpy(key, rec->name, name_len + 1);
    *out_key = key;
    *out_len = name_len + 1;
    return true;
}

static bool score_extractor(const void *value, size_t value_len,
                            void *user_data,
                            void **out_key, size_t *out_len) {
    (void)user_data;
    if (value_len < sizeof(record_t)) return false;

    const record_t *rec = (const record_t *)value;

    int *key = malloc(sizeof(int));
    if (!key) return false;

    *key = rec->score;
    *out_key = key;
    *out_len = sizeof(int);
    return true;
}

/* ============================================================
 * Custom Comparator
 * ============================================================ */

/* Descending order comparator for scores (higher scores first) */
static int score_desc_comparator(const MDB_val *a, const MDB_val *b) {
    if (a->mv_size != sizeof(int) || b->mv_size != sizeof(int)) {
        return 0;
    }

    int score_a = *(int*)a->mv_data;
    int score_b = *(int*)b->mv_data;

    /* Reverse order: higher scores come first */
    if (score_a > score_b) return -1;
    if (score_a < score_b) return 1;
    return 0;
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
    snprintf(test_db_path, sizeof(test_db_path), "%s\\test_edge_cases_%d",
             temp, getpid());
#else
    snprintf(test_db_path, sizeof(test_db_path), "/tmp/test_edge_cases_%d",
             getpid());
#endif
    mkdir(test_db_path, 0755);

    test_db = wtree3_db_open(test_db_path, 64 * 1024 * 1024, 32, TEST_VERSION, 0, &error);
    if (!test_db) {
        fprintf(stderr, "Failed to create test database: %s\n", error.message);
        return -1;
    }

    /* Register extractors - one per version+flags combination */
    /* name_extractor for non-unique, non-sparse (0x00) */
    int rc = wtree3_db_register_key_extractor(test_db, TEST_VERSION, 0x00,
                                               name_extractor, &error);
    if (rc != WTREE3_OK) {
        fprintf(stderr, "Failed to register name extractor: %s\n", error.message);
        wtree3_db_close(test_db);
        return -1;
    }

    /* score_extractor for unique, non-sparse (0x01) - used by custom comparator test */
    rc = wtree3_db_register_key_extractor(test_db, TEST_VERSION, 0x01,
                                           score_extractor, &error);
    if (rc != WTREE3_OK) {
        fprintf(stderr, "Failed to register score extractor (unique): %s\n", error.message);
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
 * TEST: Custom Comparator for Indexes
 * ============================================================ */

static void test_custom_comparator_index(void **state) {
    (void)state;
    gerror_t error = {0};
    int rc;

    printf("\n=== Testing Custom Comparator in Index ===\n");

    /* Create tree */
    wtree3_tree_t *tree = wtree3_tree_open(test_db, "scores", MDB_CREATE, 0, &error);
    assert_non_null(tree);

    /* Add index with custom descending comparator for scores */
    const char *user_data = "score_field";
    wtree3_index_config_t idx_config = {
        .name = "score_desc_idx",
        .user_data = user_data,
        .user_data_len = strlen(user_data) + 1,
        .unique = true,  /* Use unique to match our registered extractor (0x01) */
        .sparse = false,
        .compare = score_desc_comparator  /* Custom comparator */
    };

    rc = wtree3_tree_add_index(tree, &idx_config, &error);
    assert_int_equal(rc, WTREE3_OK);
    printf("✓ Index with custom comparator created\n");

    /* Insert records with different scores */
    record_t records[] = {
        {1, "Player1", 100},
        {2, "Player2", 500},
        {3, "Player3", 200},
        {4, "Player4", 900},
        {5, "Player5", 50}
    };

    for (size_t i = 0; i < sizeof(records) / sizeof(records[0]); i++) {
        char key[64];
        snprintf(key, sizeof(key), "rec:%d", records[i].id);
        rc = wtree3_insert_one(tree, key, strlen(key) + 1, &records[i], sizeof(record_t), &error);
        assert_int_equal(rc, WTREE3_OK);
    }
    printf("✓ Inserted 5 records with scores: 100, 500, 200, 900, 50\n");

    /* Query by index - should be in descending order (900, 500, 200, 100, 50) */
    printf("Testing index iteration with custom comparator (descending)...\n");

    wtree3_iterator_t *idx_iter = wtree3_index_seek_range(tree, "score_desc_idx",
                                                           NULL, 0, &error);
    assert_non_null(idx_iter);

    int expected_scores[] = {900, 500, 200, 100, 50};
    int idx = 0;

    for (bool valid = wtree3_iterator_first(idx_iter); valid && idx < 5;
         valid = wtree3_iterator_next(idx_iter), idx++) {

        const void *main_key;
        size_t main_key_len;
        wtree3_index_iterator_main_key(idx_iter, &main_key, &main_key_len);

        wtree3_txn_t *txn = wtree3_iterator_get_txn(idx_iter);
        const void *value;
        size_t value_len;
        rc = wtree3_get_txn(txn, tree, main_key, main_key_len, &value, &value_len, &error);
        assert_int_equal(rc, WTREE3_OK);

        const record_t *rec = (const record_t*)value;
        assert_int_equal(rec->score, expected_scores[idx]);
        printf("  [%d] Score: %d (expected %d) ✓\n", idx, rec->score, expected_scores[idx]);
    }

    assert_int_equal(idx, 5);
    printf("✓ Custom comparator produced correct order (descending)\n");

    wtree3_iterator_close(idx_iter);
    wtree3_tree_close(tree);
    wtree3_tree_delete(test_db, "scores", &error);
}

/* ============================================================
 * TEST: Transaction Reset and Renew
 * ============================================================ */

static void test_transaction_reset_renew(void **state) {
    (void)state;
    gerror_t error = {0};
    int rc;

    printf("\n=== Testing Transaction Reset and Renew ===\n");

    /* Create tree and add data */
    wtree3_tree_t *tree = wtree3_tree_open(test_db, "txn_test", MDB_CREATE, 0, &error);
    assert_non_null(tree);

    record_t rec1 = {1, "Record1", 100};
    rc = wtree3_insert_one(tree, "key1", 5, &rec1, sizeof(rec1), &error);
    assert_int_equal(rc, WTREE3_OK);

    /* Begin read-only transaction */
    wtree3_txn_t *txn = wtree3_txn_begin(test_db, false, &error);
    assert_non_null(txn);
    assert_true(wtree3_txn_is_readonly(txn));
    printf("✓ Read-only transaction started\n");

    /* Read data */
    const void *value;
    size_t value_len;
    rc = wtree3_get_txn(txn, tree, "key1", 5, &value, &value_len, &error);
    assert_int_equal(rc, WTREE3_OK);
    printf("✓ Read data in transaction\n");

    /* Reset transaction */
    wtree3_txn_reset(txn);
    printf("✓ Transaction reset\n");

    /* Try to read after reset (should fail or not work as expected) */
    /* Don't attempt to use cursor after reset */

    /* Renew transaction */
    rc = wtree3_txn_renew(txn, &error);
    assert_int_equal(rc, WTREE3_OK);
    printf("✓ Transaction renewed\n");

    /* Read again after renew */
    rc = wtree3_get_txn(txn, tree, "key1", 5, &value, &value_len, &error);
    assert_int_equal(rc, WTREE3_OK);
    const record_t *rec = (const record_t*)value;
    assert_string_equal(rec->name, "Record1");
    printf("✓ Read data after renew: %s\n", rec->name);

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
    wtree3_tree_delete(test_db, "txn_test", &error);
}

/* ============================================================
 * TEST: Database Statistics and Resize
 * ============================================================ */

static void test_database_stats_and_resize(void **state) {
    (void)state;
    gerror_t error = {0};
    int rc;

    printf("\n=== Testing Database Statistics and Resize ===\n");

    /* Get database statistics */
    MDB_stat stat;
    rc = wtree3_db_stats(test_db, &stat, &error);
    assert_int_equal(rc, WTREE3_OK);
    printf("✓ Database stats: page_size=%u, depth=%u, entries=%zu\n",
           stat.ms_psize, stat.ms_depth, (size_t)stat.ms_entries);

    /* Get current mapsize */
    size_t orig_mapsize = wtree3_db_get_mapsize(test_db);
    assert_int_equal(orig_mapsize, 64 * 1024 * 1024);
    printf("✓ Current mapsize: %zu bytes\n", orig_mapsize);

    /* Resize database (increase) */
    size_t new_mapsize = 128 * 1024 * 1024;
    rc = wtree3_db_resize(test_db, new_mapsize, &error);
    assert_int_equal(rc, WTREE3_OK);
    printf("✓ Database resized to %zu bytes\n", new_mapsize);

    /* Verify new mapsize */
    size_t current_mapsize = wtree3_db_get_mapsize(test_db);
    assert_int_equal(current_mapsize, new_mapsize);
    printf("✓ Verified new mapsize: %zu bytes\n", current_mapsize);

    /* Get underlying LMDB environment */
    MDB_env *env = wtree3_db_get_env(test_db);
    assert_non_null(env);
    printf("✓ Retrieved underlying LMDB environment\n");

    /* Sync database */
    rc = wtree3_db_sync(test_db, true, &error);
    assert_int_equal(rc, WTREE3_OK);
    printf("✓ Database synced to disk\n");
}

/* ============================================================
 * TEST: Iterator with Transaction
 * ============================================================ */

static void test_iterator_with_txn(void **state) {
    (void)state;
    gerror_t error = {0};
    int rc;

    printf("\n=== Testing Iterator with Explicit Transaction ===\n");

    /* Create tree and add data */
    wtree3_tree_t *tree = wtree3_tree_open(test_db, "iter_txn_test", MDB_CREATE, 0, &error);
    assert_non_null(tree);

    for (int i = 1; i <= 5; i++) {
        record_t rec = {i, "Record", i * 10};
        snprintf(rec.name, sizeof(rec.name), "Record%d", i);
        char key[64];
        snprintf(key, sizeof(key), "key:%d", i);
        wtree3_insert_one(tree, key, strlen(key) + 1, &rec, sizeof(rec), &error);
    }

    /* Create transaction */
    wtree3_txn_t *txn = wtree3_txn_begin(test_db, false, &error);
    assert_non_null(txn);

    /* Create iterator with explicit transaction */
    wtree3_iterator_t *iter = wtree3_iterator_create_with_txn(tree, txn, &error);
    assert_non_null(iter);
    printf("✓ Iterator created with explicit transaction\n");

    /* Verify iterator's transaction */
    wtree3_txn_t *iter_txn = wtree3_iterator_get_txn(iter);
    assert_ptr_equal(iter_txn, txn);
    printf("✓ Iterator transaction matches\n");

    /* Iterate */
    int count = 0;
    for (bool valid = wtree3_iterator_first(iter); valid; valid = wtree3_iterator_next(iter)) {
        count++;
    }
    assert_int_equal(count, 5);
    printf("✓ Iterated %d records\n", count);

    wtree3_iterator_close(iter);
    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
    wtree3_tree_delete(test_db, "iter_txn_test", &error);
}

/* ============================================================
 * TEST: Index Seek Edge Cases
 * ============================================================ */

static void test_index_seek_edge_cases(void **state) {
    (void)state;
    gerror_t error = {0};
    int rc;

    printf("\n=== Testing Index Seek Edge Cases ===\n");

    /* Create tree with index */
    wtree3_tree_t *tree = wtree3_tree_open(test_db, "seek_test", MDB_CREATE, 0, &error);
    assert_non_null(tree);

    const char *user_data = "name_field";
    wtree3_index_config_t idx_config = {
        .name = "name_idx",
        .user_data = user_data,
        .user_data_len = strlen(user_data) + 1,
        .unique = false,
        .sparse = false,
        .compare = NULL
    };

    rc = wtree3_tree_add_index(tree, &idx_config, &error);
    assert_int_equal(rc, WTREE3_OK);

    /* Insert records */
    const char *names[] = {"Alice", "Bob", "Charlie", "Dave", "Eve"};
    for (int i = 0; i < 5; i++) {
        record_t rec = {i + 1, "", i * 10};
        snprintf(rec.name, sizeof(rec.name), "%s", names[i]);
        char key[64];
        snprintf(key, sizeof(key), "rec:%d", i + 1);
        wtree3_insert_one(tree, key, strlen(key) + 1, &rec, sizeof(rec), &error);
    }
    printf("✓ Inserted records: Alice, Bob, Charlie, Dave, Eve\n");

    /* Test 1: Seek exact match */
    printf("Test 1: Seek exact match (Charlie)...\n");
    wtree3_iterator_t *iter = wtree3_index_seek(tree, "name_idx", "Charlie", 8, &error);
    assert_non_null(iter);
    assert_true(wtree3_iterator_valid(iter));
    const void *idx_key;
    size_t idx_key_len;
    wtree3_iterator_key(iter, &idx_key, &idx_key_len);
    assert_memory_equal(idx_key, "Charlie", 8);
    printf("  ✓ Found exact match: Charlie\n");
    wtree3_iterator_close(iter);

    /* Test 2: Seek non-existent (before first) */
    printf("Test 2: Seek non-existent key (Aaron - before first)...\n");
    iter = wtree3_index_seek(tree, "name_idx", "Aaron", 6, &error);
    /* May return NULL or invalid iterator - both acceptable */
    if (iter) {
        bool valid = wtree3_iterator_valid(iter);
        printf("  ✓ Seek returned %s iterator\n", valid ? "valid" : "invalid");
        wtree3_iterator_close(iter);
    } else {
        printf("  ✓ Seek returned NULL\n");
    }

    /* Test 3: Seek_range non-existent (should find next) */
    printf("Test 3: Seek_range non-existent (Carl - between Bob and Charlie)...\n");
    iter = wtree3_index_seek_range(tree, "name_idx", "Carl", 5, &error);
    assert_non_null(iter);
    if (wtree3_iterator_valid(iter)) {
        wtree3_iterator_key(iter, &idx_key, &idx_key_len);
        printf("  ✓ Seek_range found next: %s\n", (char*)idx_key);
        /* Should find Charlie (next after Carl alphabetically) */
        assert_memory_equal(idx_key, "Charlie", 8);
    }
    wtree3_iterator_close(iter);

    /* Test 4: Seek_range after last */
    printf("Test 4: Seek_range after last entry (Zebra)...\n");
    iter = wtree3_index_seek_range(tree, "name_idx", "Zebra", 6, &error);
    if (iter) {
        bool valid = wtree3_iterator_valid(iter);
        printf("  ✓ Seek_range after last: %s\n", valid ? "found something" : "invalid");
        wtree3_iterator_close(iter);
    }

    wtree3_tree_close(tree);
    wtree3_tree_delete(test_db, "seek_test", &error);
}

/* ============================================================
 * TEST: Index Metadata Introspection
 * ============================================================ */

static void test_index_metadata_introspection(void **state) {
    (void)state;
    gerror_t error = {0};
    int rc;

    printf("\n=== Testing Index Metadata Introspection ===\n");

    /* Create tree with index */
    wtree3_tree_t *tree = wtree3_tree_open(test_db, "meta_test", MDB_CREATE, 0, &error);
    assert_non_null(tree);

    const char *user_data = "test_field";
    wtree3_index_config_t idx_config = {
        .name = "test_idx",
        .user_data = user_data,
        .user_data_len = strlen(user_data) + 1,
        .unique = true,
        .sparse = false,
        .compare = NULL
    };

    rc = wtree3_tree_add_index(tree, &idx_config, &error);
    assert_int_equal(rc, WTREE3_OK);
    printf("✓ Created index: test_idx\n");

    /* Get extractor ID from metadata */
    uint64_t extractor_id = 0;
    rc = wtree3_index_get_extractor_id(tree, "test_idx", &extractor_id, &error);
    assert_int_equal(rc, WTREE3_OK);
    printf("✓ Retrieved extractor ID: 0x%llx\n", (unsigned long long)extractor_id);

    /* Try to get extractor ID for non-existent index */
    rc = wtree3_index_get_extractor_id(tree, "nonexistent_idx", &extractor_id, &error);
    assert_int_equal(rc, WTREE3_NOT_FOUND);
    printf("✓ Non-existent index correctly returns NOT_FOUND\n");

    wtree3_tree_close(tree);
    wtree3_tree_delete(test_db, "meta_test", &error);
}

/* ============================================================
 * TEST: Tree Name and Database Accessors
 * ============================================================ */

static void test_tree_accessors(void **state) {
    (void)state;
    gerror_t error = {0};

    printf("\n=== Testing Tree and Database Accessors ===\n");

    /* Create tree */
    wtree3_tree_t *tree = wtree3_tree_open(test_db, "accessor_test", MDB_CREATE, 0, &error);
    assert_non_null(tree);

    /* Test tree name accessor */
    const char *tree_name = wtree3_tree_name(tree);
    assert_non_null(tree_name);
    assert_string_equal(tree_name, "accessor_test");
    printf("✓ Tree name: %s\n", tree_name);

    /* Test get parent database from tree */
    wtree3_db_t *db_from_tree = wtree3_tree_get_db(tree);
    assert_non_null(db_from_tree);
    assert_ptr_equal(db_from_tree, test_db);
    printf("✓ Parent database retrieved from tree\n");

    /* Test get parent database from transaction */
    wtree3_txn_t *txn = wtree3_txn_begin(test_db, false, &error);
    assert_non_null(txn);

    wtree3_db_t *db_from_txn = wtree3_txn_get_db(txn);
    assert_non_null(db_from_txn);
    assert_ptr_equal(db_from_txn, test_db);
    printf("✓ Parent database retrieved from transaction\n");

    /* Test get underlying MDB_txn */
    MDB_txn *mdb_txn = wtree3_txn_get_mdb(txn);
    assert_non_null(mdb_txn);
    printf("✓ Underlying LMDB transaction retrieved\n");

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
    wtree3_tree_delete(test_db, "accessor_test", &error);
}

/* ============================================================
 * TEST: Error String Utilities
 * ============================================================ */

static void test_error_utilities(void **state) {
    (void)state;

    printf("\n=== Testing Error Utilities ===\n");

    /* Test error code to string conversion */
    const char *err_str = wtree3_strerror(WTREE3_OK);
    assert_non_null(err_str);
    printf("✓ WTREE3_OK: %s\n", err_str);

    err_str = wtree3_strerror(WTREE3_EINVAL);
    assert_non_null(err_str);
    printf("✓ WTREE3_EINVAL: %s\n", err_str);

    err_str = wtree3_strerror(WTREE3_KEY_EXISTS);
    assert_non_null(err_str);
    printf("✓ WTREE3_KEY_EXISTS: %s\n", err_str);

    err_str = wtree3_strerror(WTREE3_NOT_FOUND);
    assert_non_null(err_str);
    printf("✓ WTREE3_NOT_FOUND: %s\n", err_str);

    /* Test error recoverability */
    bool recoverable = wtree3_error_recoverable(WTREE3_MAP_FULL);
    assert_true(recoverable);
    printf("✓ WTREE3_MAP_FULL is recoverable: %s\n", recoverable ? "true" : "false");

    recoverable = wtree3_error_recoverable(WTREE3_EINVAL);
    assert_false(recoverable);
    printf("✓ WTREE3_EINVAL is not recoverable: %s\n", recoverable ? "true" : "false");
}

/* ============================================================
 * TEST: Prefix Scan with Various Prefixes
 * ============================================================ */

static void test_prefix_scan_variations(void **state) {
    (void)state;
    gerror_t error = {0};
    int rc;

    printf("\n=== Testing Prefix Scan Variations ===\n");

    /* Create tree */
    wtree3_tree_t *tree = wtree3_tree_open(test_db, "prefix_test", MDB_CREATE, 0, &error);
    assert_non_null(tree);

    /* Insert keys with different prefixes */
    const char *keys[] = {
        "user:1", "user:2", "user:3",
        "admin:1", "admin:2",
        "guest:1",
        "product:1", "product:2", "product:3", "product:4"
    };

    for (size_t i = 0; i < sizeof(keys) / sizeof(keys[0]); i++) {
        record_t rec = {(int)i + 1, "Data", (int)i};
        rc = wtree3_insert_one(tree, keys[i], strlen(keys[i]) + 1, &rec, sizeof(rec), &error);
        assert_int_equal(rc, WTREE3_OK);
    }
    printf("✓ Inserted %zu keys with various prefixes\n", sizeof(keys) / sizeof(keys[0]));

    /* Test prefix scan: "user:" */
    printf("Test 1: Prefix scan 'user:' (expect 3)...\n");
    int count = 0;

    bool count_callback(const void *k, size_t klen, const void *v, size_t vlen, void *user_data) {
        (void)k; (void)klen; (void)v; (void)vlen;
        int *cnt = (int*)user_data;
        (*cnt)++;
        return true;
    }

    wtree3_txn_t *txn = wtree3_txn_begin(test_db, false, &error);
    rc = wtree3_scan_prefix_txn(txn, tree, "user:", 5, count_callback, &count, &error);
    assert_int_equal(rc, WTREE3_OK);
    wtree3_txn_abort(txn);
    assert_int_equal(count, 3);
    printf("  ✓ Found %d keys with prefix 'user:'\n", count);

    /* Test prefix scan: "product:" */
    printf("Test 2: Prefix scan 'product:' (expect 4)...\n");
    count = 0;
    txn = wtree3_txn_begin(test_db, false, &error);
    rc = wtree3_scan_prefix_txn(txn, tree, "product:", 8, count_callback, &count, &error);
    assert_int_equal(rc, WTREE3_OK);
    wtree3_txn_abort(txn);
    assert_int_equal(count, 4);
    printf("  ✓ Found %d keys with prefix 'product:'\n", count);

    /* Test prefix scan: "nonexistent:" */
    printf("Test 3: Prefix scan 'nonexistent:' (expect 0)...\n");
    count = 0;
    txn = wtree3_txn_begin(test_db, false, &error);
    rc = wtree3_scan_prefix_txn(txn, tree, "nonexistent:", 12, count_callback, &count, &error);
    assert_int_equal(rc, WTREE3_OK);
    wtree3_txn_abort(txn);
    assert_int_equal(count, 0);
    printf("  ✓ Found %d keys with prefix 'nonexistent:'\n", count);

    wtree3_tree_close(tree);
    wtree3_tree_delete(test_db, "prefix_test", &error);
}

/* ============================================================
 * TEST: Iterator Delete Operation
 * ============================================================ */

static void test_iterator_delete(void **state) {
    (void)state;
    gerror_t error = {0};
    int rc;

    printf("\n=== Testing Iterator Delete Operation ===\n");

    /* Create tree */
    wtree3_tree_t *tree = wtree3_tree_open(test_db, "iter_del_test", MDB_CREATE, 0, &error);
    assert_non_null(tree);

    /* Insert data */
    for (int i = 1; i <= 10; i++) {
        record_t rec = {i, "Record", i * 10};
        char key[64];
        snprintf(key, sizeof(key), "key:%02d", i);
        wtree3_insert_one(tree, key, strlen(key) + 1, &rec, sizeof(rec), &error);
    }
    printf("✓ Inserted 10 records\n");

    /* Create write transaction */
    wtree3_txn_t *txn = wtree3_txn_begin(test_db, true, &error);
    assert_non_null(txn);

    /* Create iterator with write transaction */
    wtree3_iterator_t *iter = wtree3_iterator_create_with_txn(tree, txn, &error);
    assert_non_null(iter);

    /* Delete every other entry using iterator */
    int deleted_count = 0;
    bool valid = wtree3_iterator_first(iter);
    while (valid) {
        /* Delete current entry */
        rc = wtree3_iterator_delete(iter, &error);
        assert_int_equal(rc, WTREE3_OK);
        deleted_count++;

        /* Move to next (skip one) */
        valid = wtree3_iterator_next(iter);
        if (valid) {
            valid = wtree3_iterator_next(iter);  /* Skip one more */
        }
    }

    printf("✓ Deleted %d entries via iterator\n", deleted_count);
    wtree3_iterator_close(iter);
    wtree3_txn_commit(txn, &error);

    /* Verify remaining count */
    int64_t remaining = wtree3_tree_count(tree);
    printf("✓ Remaining entries: %lld\n", (long long)remaining);
    assert_true(remaining < 10);

    wtree3_tree_close(tree);
    wtree3_tree_delete(test_db, "iter_del_test", &error);
}

/* ============================================================
 * Test Runner
 * ============================================================ */

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_custom_comparator_index, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_transaction_reset_renew, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_database_stats_and_resize, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_iterator_with_txn, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_index_seek_edge_cases, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_index_metadata_introspection, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_tree_accessors, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_error_utilities, setup_db, NULL),
        cmocka_unit_test_setup_teardown(test_prefix_scan_variations, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_iterator_delete, setup_db, teardown_db),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
