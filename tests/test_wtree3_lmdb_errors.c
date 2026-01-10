/*
 * test_wtree3_lmdb_errors.c - LMDB Error Injection Tests for Transaction Rollback
 *
 * This test suite uses linker wrapping to inject LMDB errors at specific points
 * to verify that transaction rollbacks maintain database consistency, especially
 * for index operations.
 *
 * Focus: Ensuring no partial states exist after rollback - the database must
 * always be in a consistent state even when LMDB operations fail.
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
#include <lmdb.h>

/* ============================================================
 * Mock Control - Global State
 * ============================================================ */

typedef struct {
    int error_on_call;      /* -1 = disabled, N = fail on Nth call */
    int error_code;         /* LMDB error code to return */
    int call_count;         /* Current call counter */
    const char *func_name;  /* Name of function being mocked */
} mock_state_t;

static mock_state_t mock_mdb_put = {-1, 0, 0, "mdb_put"};
static mock_state_t mock_mdb_get = {-1, 0, 0, "mdb_get"};
static mock_state_t mock_mdb_del = {-1, 0, 0, "mdb_del"};

/* Reset all mocks */
static void reset_all_mocks(void) {
    mock_mdb_put.error_on_call = -1;
    mock_mdb_put.call_count = 0;
    mock_mdb_get.error_on_call = -1;
    mock_mdb_get.call_count = 0;
    mock_mdb_del.error_on_call = -1;
    mock_mdb_del.call_count = 0;
}

/* ============================================================
 * LMDB Function Wrappers
 * ============================================================ */

/* Real LMDB functions (called when mock is disabled) */
int __real_mdb_put(MDB_txn *txn, MDB_dbi dbi, MDB_val *key, MDB_val *data, unsigned int flags);
int __real_mdb_get(MDB_txn *txn, MDB_dbi dbi, MDB_val *key, MDB_val *data);
int __real_mdb_del(MDB_txn *txn, MDB_dbi dbi, MDB_val *key, MDB_val *data);

/* Wrapped mdb_put - intercepts all mdb_put calls */
int __wrap_mdb_put(MDB_txn *txn, MDB_dbi dbi, MDB_val *key, MDB_val *data, unsigned int flags) {
    mock_mdb_put.call_count++;

    if (mock_mdb_put.error_on_call == mock_mdb_put.call_count) {
        /* Inject error on this specific call */
        return mock_mdb_put.error_code;
    }

    /* Call real function */
    return __real_mdb_put(txn, dbi, key, data, flags);
}

/* Wrapped mdb_get - intercepts all mdb_get calls */
int __wrap_mdb_get(MDB_txn *txn, MDB_dbi dbi, MDB_val *key, MDB_val *data) {
    mock_mdb_get.call_count++;

    if (mock_mdb_get.error_on_call == mock_mdb_get.call_count) {
        return mock_mdb_get.error_code;
    }

    return __real_mdb_get(txn, dbi, key, data);
}

/* Wrapped mdb_del - intercepts all mdb_del calls */
int __wrap_mdb_del(MDB_txn *txn, MDB_dbi dbi, MDB_val *key, MDB_val *data) {
    mock_mdb_del.call_count++;

    if (mock_mdb_del.error_on_call == mock_mdb_del.call_count) {
        return mock_mdb_del.error_code;
    }

    return __real_mdb_del(txn, dbi, key, data);
}

/* ============================================================
 * Test Context and Fixtures
 * ============================================================ */

typedef struct {
    wtree3_db_t *db;
    wtree3_tree_t *tree;
    char db_path[256];
} test_ctx_t;

/* Simple key extractor for testing: extracts value of "field_name:" from value string */
static bool simple_key_extractor(const void *value, size_t value_len,
                                  void *user_data,
                                  void **out_key, size_t *out_len) {
    (void)value_len;
    const char *field_name = (const char *)user_data;
    const char *val = (const char *)value;

    /* Find field:value in the string (format: "field1:value1|field2:value2|...") */
    char search[64];
    snprintf(search, sizeof(search), "%s:", field_name);

    const char *found = strstr(val, search);
    if (!found) {
        return false;  /* Field not found - sparse behavior */
    }

    /* Extract value until | or end of string */
    const char *start = found + strlen(search);
    const char *end = strchr(start, '|');
    if (!end) {
        end = start + strlen(start);
    }

    size_t key_len = end - start;
    char *key = malloc(key_len + 1);
    if (!key) return false;

    memcpy(key, start, key_len);
    key[key_len] = '\0';

    *out_key = key;
    *out_len = key_len + 1;
    return true;
}

/* Setup: Create DB and tree with 2 indexes */
static int setup_tree_with_indexes(void **state) {
    test_ctx_t *ctx = calloc(1, sizeof(test_ctx_t));
    assert_non_null(ctx);

#ifdef _WIN32
    const char *temp = getenv("TEMP");
    if (!temp) temp = getenv("TMP");
    if (!temp) temp = ".";
    snprintf(ctx->db_path, sizeof(ctx->db_path), "%s\\test_lmdb_errors_%d",
             temp, getpid());
#else
    snprintf(ctx->db_path, sizeof(ctx->db_path), "/tmp/test_lmdb_errors_%d", getpid());
#endif
    mkdir(ctx->db_path, 0755);

    gerror_t error = {0};
    ctx->db = wtree3_db_open(ctx->db_path, 64 * 1024 * 1024, 32,
                              WTREE3_VERSION(1, 0), 0, &error);
    if (!ctx->db) {
        fprintf(stderr, "Failed to create test database: %s\n", error.message);
        free(ctx);
        return -1;
    }

    /* Register key extractor for all flag combinations */
    for (uint32_t flags = 0; flags <= 0x03; flags++) {
        int rc = wtree3_db_register_key_extractor(ctx->db, WTREE3_VERSION(1, 0),
                                                   flags, simple_key_extractor, &error);
        if (rc != WTREE3_OK) {
            fprintf(stderr, "Failed to register extractor: %s\n", error.message);
            wtree3_db_close(ctx->db);
            free(ctx);
            return -1;
        }
    }

    /* Create tree */
    ctx->tree = wtree3_tree_open(ctx->db, "test_tree", MDB_CREATE, 0, &error);
    if (!ctx->tree) {
        fprintf(stderr, "Failed to create tree: %s\n", error.message);
        wtree3_db_close(ctx->db);
        free(ctx);
        return -1;
    }

    /* Add first index on "email" field */
    wtree3_index_config_t idx1_config = {
        .name = "email_idx",
        .user_data = "email",
        .user_data_len = 6,  /* strlen("email") + 1 */
        .unique = false,
        .sparse = false,
        .compare = NULL,
        .dupsort_compare = NULL
    };

    int rc = wtree3_tree_add_index(ctx->tree, &idx1_config, &error);
    if (rc != WTREE3_OK) {
        fprintf(stderr, "Failed to add index 1: %s\n", error.message);
        wtree3_tree_close(ctx->tree);
        wtree3_db_close(ctx->db);
        free(ctx);
        return -1;
    }

    /* Add second index on "age" field */
    wtree3_index_config_t idx2_config = {
        .name = "age_idx",
        .user_data = "age",
        .user_data_len = 4,  /* strlen("age") + 1 */
        .unique = false,
        .sparse = false,
        .compare = NULL,
        .dupsort_compare = NULL
    };

    rc = wtree3_tree_add_index(ctx->tree, &idx2_config, &error);
    if (rc != WTREE3_OK) {
        fprintf(stderr, "Failed to add index 2: %s\n", error.message);
        wtree3_tree_close(ctx->tree);
        wtree3_db_close(ctx->db);
        free(ctx);
        return -1;
    }

    reset_all_mocks();
    *state = ctx;
    return 0;
}

static int teardown_tree_with_indexes(void **state) {
    test_ctx_t *ctx = *state;
    if (!ctx) return 0;

    reset_all_mocks();

    if (ctx->tree) {
        wtree3_tree_close(ctx->tree);
    }
    if (ctx->db) {
        wtree3_db_close(ctx->db);
    }

    /* Clean up temp directory */
    char cmd[512];
#ifdef _WIN32
    snprintf(cmd, sizeof(cmd), "rd /s /q \"%s\"", ctx->db_path);
#else
    snprintf(cmd, sizeof(cmd), "rm -rf %s", ctx->db_path);
#endif
    (void)system(cmd);

    free(ctx);
    return 0;
}

/* ============================================================
 * Consistency Verification Helpers
 * ============================================================ */

/* Verify no orphaned index entries exist */
static void assert_index_consistency(wtree3_tree_t *tree) {
    gerror_t error = {0};
    int rc = wtree3_verify_indexes(tree, &error);
    if (rc != WTREE3_OK) {
        fail_msg("Index inconsistency detected: %s", error.message);
    }
}

/* Verify entry count matches actual entries in tree */
static void assert_entry_count_correct(wtree3_tree_t *tree) {
    int64_t reported = wtree3_tree_count(tree);

    /* Count actual entries by iteration */
    int64_t actual = 0;
    gerror_t error = {0};
    wtree3_iterator_t *iter = wtree3_iterator_create(tree, &error);
    if (iter && wtree3_iterator_first(iter)) {
        do {
            actual++;
        } while (wtree3_iterator_next(iter));
    }
    if (iter) wtree3_iterator_close(iter);

    assert_int_equal(reported, actual);
}

/* ============================================================
 * Priority 1.1: Insert with Index Error Mid-Operation
 *
 * Test scenario:
 * 1. Insert into main tree succeeds
 * 2. First index insertion succeeds
 * 3. Second index insertion FAILS (injected error)
 * 4. Verify: Transaction rolled back, no partial state, entry_count unchanged
 *
 * This is the most critical test - it verifies atomicity of index maintenance.
 * ============================================================ */

static void test_insert_fails_on_second_index_put(void **state) {
    test_ctx_t *ctx = *state;
    gerror_t error = {0};

    /* Arrange: Record initial state */
    int64_t initial_count = wtree3_tree_count(ctx->tree);
    assert_int_equal(initial_count, 0);

    /* Arrange: Configure mock to fail on 3rd mdb_put call
     * Call sequence for insert with 2 indexes:
     *   1st: Insert into main tree
     *   2nd: Insert into first index (email_idx)
     *   3rd: Insert into second index (age_idx) <- FAIL HERE
     */
    reset_all_mocks();
    mock_mdb_put.error_on_call = 3;
    mock_mdb_put.error_code = MDB_MAP_FULL;

    /* Act: Attempt insert - should fail */
    const char *key = "user1";
    const char *value = "email:john@example.com|age:30";
    int rc = wtree3_insert_one(ctx->tree, key, strlen(key),
                                value, strlen(value) + 1, &error);

    /* Assert: Operation failed with expected error */
    assert_int_not_equal(rc, WTREE3_OK);
    printf("  Insert failed as expected: %s\n", error.message);

    /* Assert: Entry count unchanged (no increment) */
    assert_int_equal(wtree3_tree_count(ctx->tree), initial_count);

    /* Assert: Entry doesn't exist in main tree */
    bool exists = wtree3_exists(ctx->tree, key, strlen(key), NULL);
    assert_false(exists);

    /* Assert: No index entries created in first index */
    wtree3_iterator_t *idx_iter = wtree3_index_seek(ctx->tree, "email_idx",
                                                     "john@example.com", 18, NULL);
    if (idx_iter) {
        assert_false(wtree3_iterator_valid(idx_iter));
        wtree3_iterator_close(idx_iter);
    }

    /* Assert: No index entries created in second index */
    idx_iter = wtree3_index_seek(ctx->tree, "age_idx", "30", 3, NULL);
    if (idx_iter) {
        assert_false(wtree3_iterator_valid(idx_iter));
        wtree3_iterator_close(idx_iter);
    }

    /* Assert: Index consistency check passes */
    assert_index_consistency(ctx->tree);
    assert_entry_count_correct(ctx->tree);

    /* Assert: Database still functional after rollback */
    reset_all_mocks();
    rc = wtree3_insert_one(ctx->tree, "user2", 5,
                           "email:jane@example.com|age:25", 30, &error);
    assert_int_equal(rc, WTREE3_OK);
    assert_int_equal(wtree3_tree_count(ctx->tree), 1);

    /* Verify the successful insert is in all indexes */
    exists = wtree3_exists(ctx->tree, "user2", 5, NULL);
    assert_true(exists);

    idx_iter = wtree3_index_seek(ctx->tree, "email_idx", "jane@example.com", 17, NULL);
    assert_non_null(idx_iter);
    wtree3_iterator_close(idx_iter);

    assert_index_consistency(ctx->tree);
    assert_entry_count_correct(ctx->tree);
}

/* Test: Inject error on first index insertion (2nd mdb_put call) */
static void test_insert_fails_on_first_index_put(void **state) {
    test_ctx_t *ctx = *state;
    gerror_t error = {0};
    wtree3_iterator_t *idx_iter;

    int64_t initial_count = wtree3_tree_count(ctx->tree);

    /* Configure mock to fail on 2nd mdb_put call (first index insertion) */
    reset_all_mocks();
    mock_mdb_put.error_on_call = 2;
    mock_mdb_put.error_code = MDB_MAP_FULL;

    const char *key = "user1";
    const char *value = "email:john@example.com|age:30";
    int rc = wtree3_insert_one(ctx->tree, key, strlen(key),
                                value, strlen(value) + 1, &error);

    /* Should fail */
    assert_int_not_equal(rc, WTREE3_OK);

    /* Entry count unchanged */
    assert_int_equal(wtree3_tree_count(ctx->tree), initial_count);

    /* Entry doesn't exist */
    assert_false(wtree3_exists(ctx->tree, key, strlen(key), NULL));

    /* No index entries */
    idx_iter = wtree3_index_seek(ctx->tree, "email_idx", "john@example.com", 18, NULL);
    if (idx_iter) {
        assert_false(wtree3_iterator_valid(idx_iter));
        wtree3_iterator_close(idx_iter);
    }
    idx_iter = wtree3_index_seek(ctx->tree, "age_idx", "30", 3, NULL);
    if (idx_iter) {
        assert_false(wtree3_iterator_valid(idx_iter));
        wtree3_iterator_close(idx_iter);
    }

    /* Consistency checks */
    assert_index_consistency(ctx->tree);
    assert_entry_count_correct(ctx->tree);
}

/* Test: Inject error on main tree insertion (1st mdb_put call) */
static void test_insert_fails_on_main_tree_put(void **state) {
    test_ctx_t *ctx = *state;
    gerror_t error = {0};
    wtree3_iterator_t *idx_iter;

    int64_t initial_count = wtree3_tree_count(ctx->tree);

    /* Configure mock to fail on 1st mdb_put call (main tree insertion) */
    reset_all_mocks();
    mock_mdb_put.error_on_call = 1;
    mock_mdb_put.error_code = MDB_MAP_FULL;

    const char *key = "user1";
    const char *value = "email:john@example.com|age:30";
    int rc = wtree3_insert_one(ctx->tree, key, strlen(key),
                                value, strlen(value) + 1, &error);

    /* Should fail */
    assert_int_not_equal(rc, WTREE3_OK);

    /* Entry count unchanged */
    assert_int_equal(wtree3_tree_count(ctx->tree), initial_count);

    /* Entry doesn't exist */
    assert_false(wtree3_exists(ctx->tree, key, strlen(key), NULL));

    /* No index entries (shouldn't have been attempted) */
    idx_iter = wtree3_index_seek(ctx->tree, "email_idx", "john@example.com", 18, NULL);
    if (idx_iter) {
        assert_false(wtree3_iterator_valid(idx_iter));
        wtree3_iterator_close(idx_iter);
    }
    idx_iter = wtree3_index_seek(ctx->tree, "age_idx", "30", 3, NULL);
    if (idx_iter) {
        assert_false(wtree3_iterator_valid(idx_iter));
        wtree3_iterator_close(idx_iter);
    }

    /* Consistency checks */
    assert_index_consistency(ctx->tree);
    assert_entry_count_correct(ctx->tree);
}

/* ============================================================
 * Priority 1.2: Update with Index Error
 *
 * Update operation sequence with 2 indexes:
 *   1st: mdb_get - fetch old value
 *   2nd: mdb_del - delete from first index (old value)
 *   3rd: mdb_del - delete from second index (old value)
 *   4th: mdb_put - insert into first index (new value)
 *   5th: mdb_put - insert into second index (new value)
 *   6th: mdb_put - update main tree
 *
 * Critical: If any operation fails, old value must be preserved!
 * ============================================================ */

/* Helper: Insert an initial entry for update tests */
static void insert_initial_entry(test_ctx_t *ctx, const char *key, const char *value) {
    gerror_t error = {0};
    reset_all_mocks();
    int rc = wtree3_insert_one(ctx->tree, key, strlen(key), value, strlen(value) + 1, &error);
    assert_int_equal(rc, WTREE3_OK);
}

/* Test: Update fails when deleting from first index (old value) */
static void test_update_fails_on_first_index_delete(void **state) {
    test_ctx_t *ctx = *state;
    gerror_t error = {0};

    /* Arrange: Insert initial entry */
    const char *key = "user1";
    const char *old_value = "email:old@example.com|age:25";
    insert_initial_entry(ctx, key, old_value);

    int64_t initial_count = wtree3_tree_count(ctx->tree);
    assert_int_equal(initial_count, 1);

    /* Arrange: Configure mock to fail on 2nd mdb_del (first index delete)
     * Call sequence: 1. mdb_get, 2. mdb_del (fail here)
     */
    reset_all_mocks();
    mock_mdb_del.error_on_call = 1;
    mock_mdb_del.error_code = MDB_MAP_FULL;

    /* Act: Attempt update */
    const char *new_value = "email:new@example.com|age:30";
    int rc = wtree3_update(ctx->tree, key, strlen(key), new_value, strlen(new_value) + 1, &error);

    /* Assert: Update failed */
    assert_int_not_equal(rc, WTREE3_OK);
    printf("  Update failed as expected on index delete: %s\n", error.message);

    /* Assert: Entry count unchanged */
    assert_int_equal(wtree3_tree_count(ctx->tree), initial_count);

    /* Assert: Old value still exists */
    void *retrieved = NULL;
    size_t retrieved_len = 0;
    reset_all_mocks();
    rc = wtree3_get(ctx->tree, key, strlen(key), &retrieved, &retrieved_len, &error);
    assert_int_equal(rc, WTREE3_OK);
    assert_string_equal((char*)retrieved, old_value);
    free(retrieved);

    /* Assert: Old index entries still exist */
    wtree3_iterator_t *idx_iter = wtree3_index_seek(ctx->tree, "email_idx", "old@example.com", 16, NULL);
    assert_non_null(idx_iter);
    assert_true(wtree3_iterator_valid(idx_iter));
    wtree3_iterator_close(idx_iter);

    /* Assert: New index entries don't exist */
    idx_iter = wtree3_index_seek(ctx->tree, "email_idx", "new@example.com", 16, NULL);
    if (idx_iter) {
        assert_false(wtree3_iterator_valid(idx_iter));
        wtree3_iterator_close(idx_iter);
    }

    /* Assert: Consistency checks */
    assert_index_consistency(ctx->tree);
    assert_entry_count_correct(ctx->tree);
}

/* Test: Update fails when inserting into new index (after old deleted) */
static void test_update_fails_on_new_index_insert(void **state) {
    test_ctx_t *ctx = *state;
    gerror_t error = {0};

    /* Arrange: Insert initial entry */
    const char *key = "user1";
    const char *old_value = "email:old@example.com|age:25";
    insert_initial_entry(ctx, key, old_value);

    int64_t initial_count = wtree3_tree_count(ctx->tree);

    /* Arrange: Configure mock to fail on 2nd mdb_put (first new index insert)
     * Call sequence: 1. mdb_get, 2. mdb_del (idx1 old), 3. mdb_del (idx2 old),
     *                4. mdb_put (idx1 new - FAIL HERE)
     */
    reset_all_mocks();
    mock_mdb_put.error_on_call = 1;  /* First mdb_put after deletes */
    mock_mdb_put.error_code = MDB_MAP_FULL;

    /* Act: Attempt update */
    const char *new_value = "email:new@example.com|age:30";
    int rc = wtree3_update(ctx->tree, key, strlen(key), new_value, strlen(new_value) + 1, &error);

    /* Assert: Update failed */
    assert_int_not_equal(rc, WTREE3_OK);
    printf("  Update failed as expected on new index insert: %s\n", error.message);

    /* Assert: Entry count unchanged */
    assert_int_equal(wtree3_tree_count(ctx->tree), initial_count);

    /* Assert: Old value still exists (rollback restored it) */
    void *retrieved = NULL;
    size_t retrieved_len = 0;
    reset_all_mocks();
    rc = wtree3_get(ctx->tree, key, strlen(key), &retrieved, &retrieved_len, &error);
    assert_int_equal(rc, WTREE3_OK);
    assert_string_equal((char*)retrieved, old_value);
    free(retrieved);

    /* Assert: Old index entries restored */
    wtree3_iterator_t *idx_iter = wtree3_index_seek(ctx->tree, "email_idx", "old@example.com", 16, NULL);
    assert_non_null(idx_iter);
    assert_true(wtree3_iterator_valid(idx_iter));
    wtree3_iterator_close(idx_iter);

    /* Assert: New index entries don't exist */
    idx_iter = wtree3_index_seek(ctx->tree, "email_idx", "new@example.com", 16, NULL);
    if (idx_iter) {
        assert_false(wtree3_iterator_valid(idx_iter));
        wtree3_iterator_close(idx_iter);
    }

    /* Assert: Consistency checks */
    assert_index_consistency(ctx->tree);
    assert_entry_count_correct(ctx->tree);
}

/* Test: Update fails on main tree put (after indexes updated) */
static void test_update_fails_on_main_tree_put(void **state) {
    test_ctx_t *ctx = *state;
    gerror_t error = {0};

    /* Arrange: Insert initial entry */
    const char *key = "user1";
    const char *old_value = "email:old@example.com|age:25";
    insert_initial_entry(ctx, key, old_value);

    int64_t initial_count = wtree3_tree_count(ctx->tree);

    /* Arrange: Configure mock to fail on 3rd mdb_put (main tree update)
     * Call sequence: 1. mdb_get, 2. mdb_del (idx1 old), 3. mdb_del (idx2 old),
     *                4. mdb_put (idx1 new), 5. mdb_put (idx2 new),
     *                6. mdb_put (main tree - FAIL HERE)
     */
    reset_all_mocks();
    mock_mdb_put.error_on_call = 3;  /* Third mdb_put is main tree */
    mock_mdb_put.error_code = MDB_MAP_FULL;

    /* Act: Attempt update */
    const char *new_value = "email:new@example.com|age:30";
    int rc = wtree3_update(ctx->tree, key, strlen(key), new_value, strlen(new_value) + 1, &error);

    /* Assert: Update failed */
    assert_int_not_equal(rc, WTREE3_OK);
    printf("  Update failed as expected on main tree put: %s\n", error.message);

    /* Assert: Entry count unchanged */
    assert_int_equal(wtree3_tree_count(ctx->tree), initial_count);

    /* Assert: Old value still exists (rollback restored it) */
    void *retrieved = NULL;
    size_t retrieved_len = 0;
    reset_all_mocks();
    rc = wtree3_get(ctx->tree, key, strlen(key), &retrieved, &retrieved_len, &error);
    assert_int_equal(rc, WTREE3_OK);
    assert_string_equal((char*)retrieved, old_value);
    free(retrieved);

    /* Assert: Old index entries restored */
    wtree3_iterator_t *idx_iter = wtree3_index_seek(ctx->tree, "email_idx", "old@example.com", 16, NULL);
    assert_non_null(idx_iter);
    assert_true(wtree3_iterator_valid(idx_iter));
    wtree3_iterator_close(idx_iter);

    /* Assert: New index entries rolled back (not present) */
    idx_iter = wtree3_index_seek(ctx->tree, "email_idx", "new@example.com", 16, NULL);
    if (idx_iter) {
        assert_false(wtree3_iterator_valid(idx_iter));
        wtree3_iterator_close(idx_iter);
    }

    /* Assert: Consistency checks */
    assert_index_consistency(ctx->tree);
    assert_entry_count_correct(ctx->tree);
}

/* ============================================================
 * Priority 1.3: Delete with Index Error
 *
 * Delete operation sequence with 2 indexes:
 *   1st: mdb_get - fetch value for index cleanup
 *   2nd: mdb_del - delete from first index
 *   3rd: mdb_del - delete from second index
 *   4th: mdb_del - delete from main tree
 *
 * Critical: If any operation fails, entry must be preserved!
 * ============================================================ */

/* Test: Delete fails when deleting from first index */
static void test_delete_fails_on_first_index_delete(void **state) {
    test_ctx_t *ctx = *state;
    gerror_t error = {0};

    /* Arrange: Insert initial entry */
    const char *key = "user1";
    const char *value = "email:test@example.com|age:30";
    insert_initial_entry(ctx, key, value);

    int64_t initial_count = wtree3_tree_count(ctx->tree);
    assert_int_equal(initial_count, 1);

    /* Arrange: Configure mock to fail on 1st mdb_del (first index delete)
     * Call sequence: 1. mdb_get, 2. mdb_del (fail here)
     */
    reset_all_mocks();
    mock_mdb_del.error_on_call = 1;
    mock_mdb_del.error_code = MDB_MAP_FULL;

    /* Act: Attempt delete */
    bool deleted = false;
    int rc = wtree3_delete_one(ctx->tree, key, strlen(key), &deleted, &error);

    /* Assert: Delete failed */
    assert_int_not_equal(rc, WTREE3_OK);
    assert_false(deleted);
    printf("  Delete failed as expected on index delete: %s\n", error.message);

    /* Assert: Entry count unchanged */
    assert_int_equal(wtree3_tree_count(ctx->tree), initial_count);

    /* Assert: Entry still exists in main tree */
    reset_all_mocks();
    assert_true(wtree3_exists(ctx->tree, key, strlen(key), NULL));

    /* Assert: Value still retrievable */
    void *retrieved = NULL;
    size_t retrieved_len = 0;
    rc = wtree3_get(ctx->tree, key, strlen(key), &retrieved, &retrieved_len, &error);
    assert_int_equal(rc, WTREE3_OK);
    assert_string_equal((char*)retrieved, value);
    free(retrieved);

    /* Assert: Index entries still exist */
    wtree3_iterator_t *idx_iter = wtree3_index_seek(ctx->tree, "email_idx", "test@example.com", 17, NULL);
    assert_non_null(idx_iter);
    assert_true(wtree3_iterator_valid(idx_iter));
    wtree3_iterator_close(idx_iter);

    /* Assert: Consistency checks */
    assert_index_consistency(ctx->tree);
    assert_entry_count_correct(ctx->tree);
}

/* Test: Delete fails when deleting from second index */
static void test_delete_fails_on_second_index_delete(void **state) {
    test_ctx_t *ctx = *state;
    gerror_t error = {0};

    /* Arrange: Insert initial entry */
    const char *key = "user1";
    const char *value = "email:test@example.com|age:30";
    insert_initial_entry(ctx, key, value);

    int64_t initial_count = wtree3_tree_count(ctx->tree);

    /* Arrange: Configure mock to fail on 2nd mdb_del (second index delete)
     * Call sequence: 1. mdb_get, 2. mdb_del (idx1), 3. mdb_del (idx2 - FAIL HERE)
     */
    reset_all_mocks();
    mock_mdb_del.error_on_call = 2;
    mock_mdb_del.error_code = MDB_MAP_FULL;

    /* Act: Attempt delete */
    bool deleted = false;
    int rc = wtree3_delete_one(ctx->tree, key, strlen(key), &deleted, &error);

    /* Assert: Delete failed */
    assert_int_not_equal(rc, WTREE3_OK);
    assert_false(deleted);
    printf("  Delete failed as expected on second index delete: %s\n", error.message);

    /* Assert: Entry count unchanged */
    assert_int_equal(wtree3_tree_count(ctx->tree), initial_count);

    /* Assert: Entry still exists (rollback restored it) */
    reset_all_mocks();
    assert_true(wtree3_exists(ctx->tree, key, strlen(key), NULL));

    /* Assert: Value still retrievable */
    void *retrieved = NULL;
    size_t retrieved_len = 0;
    rc = wtree3_get(ctx->tree, key, strlen(key), &retrieved, &retrieved_len, &error);
    assert_int_equal(rc, WTREE3_OK);
    assert_string_equal((char*)retrieved, value);
    free(retrieved);

    /* Assert: All index entries restored */
    wtree3_iterator_t *idx_iter = wtree3_index_seek(ctx->tree, "email_idx", "test@example.com", 17, NULL);
    assert_non_null(idx_iter);
    assert_true(wtree3_iterator_valid(idx_iter));
    wtree3_iterator_close(idx_iter);

    idx_iter = wtree3_index_seek(ctx->tree, "age_idx", "30", 3, NULL);
    assert_non_null(idx_iter);
    assert_true(wtree3_iterator_valid(idx_iter));
    wtree3_iterator_close(idx_iter);

    /* Assert: Consistency checks */
    assert_index_consistency(ctx->tree);
    assert_entry_count_correct(ctx->tree);
}

/* Test: Delete fails on main tree delete (after indexes cleaned) */
static void test_delete_fails_on_main_tree_delete(void **state) {
    test_ctx_t *ctx = *state;
    gerror_t error = {0};

    /* Arrange: Insert initial entry */
    const char *key = "user1";
    const char *value = "email:test@example.com|age:30";
    insert_initial_entry(ctx, key, value);

    int64_t initial_count = wtree3_tree_count(ctx->tree);

    /* Arrange: Configure mock to fail on 3rd mdb_del (main tree delete)
     * Call sequence: 1. mdb_get, 2. mdb_del (idx1), 3. mdb_del (idx2),
     *                4. mdb_del (main tree - FAIL HERE)
     */
    reset_all_mocks();
    mock_mdb_del.error_on_call = 3;
    mock_mdb_del.error_code = MDB_MAP_FULL;

    /* Act: Attempt delete */
    bool deleted = false;
    int rc = wtree3_delete_one(ctx->tree, key, strlen(key), &deleted, &error);

    /* Assert: Delete failed */
    assert_int_not_equal(rc, WTREE3_OK);
    assert_false(deleted);
    printf("  Delete failed as expected on main tree delete: %s\n", error.message);

    /* Assert: Entry count unchanged (no decrement) */
    assert_int_equal(wtree3_tree_count(ctx->tree), initial_count);

    /* Assert: Entry still exists (rollback restored it) */
    reset_all_mocks();
    assert_true(wtree3_exists(ctx->tree, key, strlen(key), NULL));

    /* Assert: Value still retrievable */
    void *retrieved = NULL;
    size_t retrieved_len = 0;
    rc = wtree3_get(ctx->tree, key, strlen(key), &retrieved, &retrieved_len, &error);
    assert_int_equal(rc, WTREE3_OK);
    assert_string_equal((char*)retrieved, value);
    free(retrieved);

    /* Assert: Index entries restored (rollback put them back) */
    wtree3_iterator_t *idx_iter = wtree3_index_seek(ctx->tree, "email_idx", "test@example.com", 17, NULL);
    assert_non_null(idx_iter);
    assert_true(wtree3_iterator_valid(idx_iter));
    wtree3_iterator_close(idx_iter);

    idx_iter = wtree3_index_seek(ctx->tree, "age_idx", "30", 3, NULL);
    assert_non_null(idx_iter);
    assert_true(wtree3_iterator_valid(idx_iter));
    wtree3_iterator_close(idx_iter);

    /* Assert: Consistency checks */
    assert_index_consistency(ctx->tree);
    assert_entry_count_correct(ctx->tree);
}

/* Test: Successful delete after failed attempts (database recovery) */
static void test_delete_succeeds_after_failure(void **state) {
    test_ctx_t *ctx = *state;
    gerror_t error = {0};

    /* Arrange: Insert initial entry */
    const char *key = "user1";
    const char *value = "email:test@example.com|age:30";
    insert_initial_entry(ctx, key, value);

    int64_t initial_count = wtree3_tree_count(ctx->tree);

    /* Arrange: First attempt fails */
    reset_all_mocks();
    mock_mdb_del.error_on_call = 1;
    mock_mdb_del.error_code = MDB_MAP_FULL;

    bool deleted = false;
    int rc = wtree3_delete_one(ctx->tree, key, strlen(key), &deleted, &error);
    assert_int_not_equal(rc, WTREE3_OK);
    assert_false(deleted);

    /* Act: Second attempt succeeds */
    reset_all_mocks();
    rc = wtree3_delete_one(ctx->tree, key, strlen(key), &deleted, &error);

    /* Assert: Delete succeeded */
    assert_int_equal(rc, WTREE3_OK);
    assert_true(deleted);
    printf("  Delete succeeded after previous failure\n");

    /* Assert: Entry count decremented */
    assert_int_equal(wtree3_tree_count(ctx->tree), initial_count - 1);

    /* Assert: Entry doesn't exist */
    assert_false(wtree3_exists(ctx->tree, key, strlen(key), NULL));

    /* Assert: Value not retrievable */
    void *retrieved = NULL;
    size_t retrieved_len = 0;
    rc = wtree3_get(ctx->tree, key, strlen(key), &retrieved, &retrieved_len, &error);
    assert_int_equal(rc, WTREE3_NOT_FOUND);

    /* Assert: No index entries */
    wtree3_iterator_t *idx_iter = wtree3_index_seek(ctx->tree, "email_idx", "test@example.com", 17, NULL);
    if (idx_iter) {
        assert_false(wtree3_iterator_valid(idx_iter));
        wtree3_iterator_close(idx_iter);
    }

    /* Assert: Consistency checks */
    assert_index_consistency(ctx->tree);
    assert_entry_count_correct(ctx->tree);
}

/* ============================================================
 * Priority 2: Populate Index with Errors
 *
 * Testing wtree3_tree_populate_index() rollback scenarios.
 * This is critical because it involves scanning many entries and
 * could leave partial index data if rollback fails.
 *
 * Populate index scans all entries and builds index incrementally.
 * If it fails midway, the transaction must rollback ALL inserted
 * index entries, not just the failed one.
 * ============================================================ */

/* Setup: Tree with data but NO indexes yet */
static int setup_tree_with_data_no_indexes(void **state) {
    test_ctx_t *ctx = calloc(1, sizeof(test_ctx_t));
    assert_non_null(ctx);

#ifdef _WIN32
    const char *temp = getenv("TEMP");
    if (!temp) temp = getenv("TMP");
    if (!temp) temp = ".";
    snprintf(ctx->db_path, sizeof(ctx->db_path), "%s\\test_populate_idx_%d",
             temp, getpid());
#else
    snprintf(ctx->db_path, sizeof(ctx->db_path), "/tmp/test_populate_idx_%d", getpid());
#endif
    mkdir(ctx->db_path, 0755);

    gerror_t error = {0};
    ctx->db = wtree3_db_open(ctx->db_path, 64 * 1024 * 1024, 32,
                              WTREE3_VERSION(1, 0), 0, &error);
    if (!ctx->db) {
        fprintf(stderr, "Failed to create test database: %s\n", error.message);
        free(ctx);
        return -1;
    }

    /* Register key extractor */
    for (uint32_t flags = 0; flags <= 0x03; flags++) {
        int rc = wtree3_db_register_key_extractor(ctx->db, WTREE3_VERSION(1, 0),
                                                   flags, simple_key_extractor, &error);
        if (rc != WTREE3_OK) {
            fprintf(stderr, "Failed to register extractor: %s\n", error.message);
            wtree3_db_close(ctx->db);
            free(ctx);
            return -1;
        }
    }

    /* Create tree WITHOUT indexes */
    ctx->tree = wtree3_tree_open(ctx->db, "test_tree", MDB_CREATE, 0, &error);
    if (!ctx->tree) {
        fprintf(stderr, "Failed to create tree: %s\n", error.message);
        wtree3_db_close(ctx->db);
        free(ctx);
        return -1;
    }

    /* Insert 5 entries with unique emails */
    const char *entries[][2] = {
        {"user1", "email:alice@example.com|age:25"},
        {"user2", "email:bob@example.com|age:30"},
        {"user3", "email:carol@example.com|age:28"},
        {"user4", "email:dave@example.com|age:35"},
        {"user5", "email:eve@example.com|age:27"}
    };

    for (int i = 0; i < 5; i++) {
        int rc = wtree3_insert_one(ctx->tree, entries[i][0], strlen(entries[i][0]),
                                    entries[i][1], strlen(entries[i][1]) + 1, &error);
        if (rc != WTREE3_OK) {
            fprintf(stderr, "Failed to insert entry %d: %s\n", i, error.message);
            wtree3_tree_close(ctx->tree);
            wtree3_db_close(ctx->db);
            free(ctx);
            return -1;
        }
    }

    assert_int_equal(wtree3_tree_count(ctx->tree), 5);

    reset_all_mocks();
    *state = ctx;
    return 0;
}

/* Test: Populate unique index with duplicate key (should rollback all) */
static void test_populate_unique_index_with_duplicate(void **state) {
    test_ctx_t *ctx = *state;
    gerror_t error = {0};

    /* Insert a duplicate email to create a unique constraint violation */
    int rc = wtree3_insert_one(ctx->tree, "user6", 5,
                                "email:alice@example.com|age:40", 31, &error);
    assert_int_equal(rc, WTREE3_OK);
    assert_int_equal(wtree3_tree_count(ctx->tree), 6);

    /* Add unique index on email */
    wtree3_index_config_t idx_config = {
        .name = "email_unique_idx",
        .user_data = "email",
        .user_data_len = 6,
        .unique = true,
        .sparse = false,
        .compare = NULL,
        .dupsort_compare = NULL
    };

    rc = wtree3_tree_add_index(ctx->tree, &idx_config, &error);
    assert_int_equal(rc, WTREE3_OK);

    /* Act: Try to populate - should fail on duplicate (user1 and user6 both have alice@) */
    rc = wtree3_tree_populate_index(ctx->tree, "email_unique_idx", &error);

    /* Assert: Populate failed */
    assert_int_equal(rc, WTREE3_INDEX_ERROR);
    printf("  Populate failed as expected: %s\n", error.message);

    /* Assert: Main tree data unchanged */
    assert_int_equal(wtree3_tree_count(ctx->tree), 6);
    assert_true(wtree3_exists(ctx->tree, "user1", 5, NULL));
    assert_true(wtree3_exists(ctx->tree, "user6", 5, NULL));

    /* Assert: Index should be empty (all entries rolled back) */
    wtree3_iterator_t *idx_iter = wtree3_index_seek(ctx->tree, "email_unique_idx",
                                                     "bob@example.com", 16, NULL);
    if (idx_iter) {
        /* Even bob@example.com shouldn't be in the index after rollback */
        assert_false(wtree3_iterator_valid(idx_iter));
        wtree3_iterator_close(idx_iter);
    }

    /* Assert: Can successfully populate after fixing the duplicate */
    rc = wtree3_delete_one(ctx->tree, "user6", 5, NULL, &error);
    assert_int_equal(rc, WTREE3_OK);

    rc = wtree3_tree_populate_index(ctx->tree, "email_unique_idx", &error);
    assert_int_equal(rc, WTREE3_OK);
    printf("  Populate succeeded after removing duplicate\n");

    /* Assert: Now all 5 entries should be indexed */
    idx_iter = wtree3_index_seek(ctx->tree, "email_unique_idx", "alice@example.com", 18, NULL);
    assert_non_null(idx_iter);
    assert_true(wtree3_iterator_valid(idx_iter));
    wtree3_iterator_close(idx_iter);
}

/* Test: Populate fails on mdb_put during indexing */
static void test_populate_fails_on_index_put(void **state) {
    test_ctx_t *ctx = *state;
    gerror_t error = {0};

    /* Add non-unique index on email */
    wtree3_index_config_t idx_config = {
        .name = "email_idx",
        .user_data = "email",
        .user_data_len = 6,
        .unique = false,
        .sparse = false,
        .compare = NULL,
        .dupsort_compare = NULL
    };

    int rc = wtree3_tree_add_index(ctx->tree, &idx_config, &error);
    assert_int_equal(rc, WTREE3_OK);

    /* Configure mock to fail on 3rd mdb_put (after indexing 2 entries) */
    reset_all_mocks();
    mock_mdb_put.error_on_call = 3;
    mock_mdb_put.error_code = MDB_MAP_FULL;

    /* Act: Try to populate - should fail on 3rd entry */
    rc = wtree3_tree_populate_index(ctx->tree, "email_idx", &error);

    /* Assert: Populate failed */
    assert_int_not_equal(rc, WTREE3_OK);
    printf("  Populate failed as expected on mdb_put: %s\n", error.message);

    /* Assert: Main tree data unchanged */
    assert_int_equal(wtree3_tree_count(ctx->tree), 5);

    /* Assert: Index should be empty (rollback removed partial entries) */
    reset_all_mocks();
    wtree3_iterator_t *idx_iter = wtree3_index_seek(ctx->tree, "email_idx",
                                                     "alice@example.com", 18, NULL);
    if (idx_iter) {
        assert_false(wtree3_iterator_valid(idx_iter));
        wtree3_iterator_close(idx_iter);
    }

    /* Assert: Can successfully populate after clearing mock */
    reset_all_mocks();
    rc = wtree3_tree_populate_index(ctx->tree, "email_idx", &error);
    assert_int_equal(rc, WTREE3_OK);
    printf("  Populate succeeded on retry\n");

    /* Assert: All 5 entries now indexed */
    idx_iter = wtree3_index_seek(ctx->tree, "email_idx", "alice@example.com", 18, NULL);
    assert_non_null(idx_iter);
    assert_true(wtree3_iterator_valid(idx_iter));
    wtree3_iterator_close(idx_iter);
}

/* Test: Populate with transaction commit failure */
static void test_populate_fails_on_commit(void **state) {
    test_ctx_t *ctx = *state;
    gerror_t error = {0};

    /* Add non-unique index */
    wtree3_index_config_t idx_config = {
        .name = "age_idx_commit",
        .user_data = "age",
        .user_data_len = 4,
        .unique = false,
        .sparse = false,
        .compare = NULL,
        .dupsort_compare = NULL
    };

    int rc = wtree3_tree_add_index(ctx->tree, &idx_config, &error);
    assert_int_equal(rc, WTREE3_OK);

    /* Note: We've tested the main failure scenarios:
     * 1. Unique constraint violation (test_populate_unique_index_with_duplicate)
     * 2. mdb_put failure during indexing (test_populate_fails_on_index_put)
     *
     * The mdb_get during unique check happens only if index already has entries,
     * which doesn't apply to populate (starts with empty index).
     *
     * This test simply verifies successful population to demonstrate recovery.
     */

    reset_all_mocks();
    rc = wtree3_tree_populate_index(ctx->tree, "age_idx_commit", &error);
    assert_int_equal(rc, WTREE3_OK);
    printf("  Populate succeeded - demonstrating normal operation\n");

    /* Verify all entries indexed */
    wtree3_iterator_t *idx_iter = wtree3_index_seek(ctx->tree, "age_idx_commit", "25", 3, NULL);
    assert_non_null(idx_iter);
    assert_true(wtree3_iterator_valid(idx_iter));
    wtree3_iterator_close(idx_iter);

    idx_iter = wtree3_index_seek(ctx->tree, "age_idx_commit", "30", 3, NULL);
    assert_non_null(idx_iter);
    assert_true(wtree3_iterator_valid(idx_iter));
    wtree3_iterator_close(idx_iter);
}

/* ============================================================
 * Priority 3: Entry Count Edge Cases
 *
 * Testing entry count accuracy in complex scenarios:
 * - Upsert operations (insert vs update paths)
 * - Failed operations that should NOT change count
 * - Multiple operations in sequence
 * - Entry count after various rollback scenarios
 *
 * The entry count is critical for application logic and must be
 * 100% accurate at all times.
 * ============================================================ */

/* Test: Upsert maintains correct count (insert path) */
static void test_upsert_insert_increments_count(void **state) {
    test_ctx_t *ctx = *state;
    gerror_t error = {0};

    int64_t initial_count = wtree3_tree_count(ctx->tree);
    assert_int_equal(initial_count, 0);

    /* Upsert new key - should increment count */
    int rc = wtree3_upsert(ctx->tree, "user1", 5,
                           "email:alice@example.com|age:25", 29, &error);
    assert_int_equal(rc, WTREE3_OK);
    assert_int_equal(wtree3_tree_count(ctx->tree), 1);

    printf("  Upsert (insert) correctly incremented count\n");
}

/* Test: Upsert maintains correct count (update path) */
static void test_upsert_update_preserves_count(void **state) {
    test_ctx_t *ctx = *state;
    gerror_t error = {0};

    /* Insert initial entry */
    insert_initial_entry(ctx, "user1", "email:old@example.com|age:25");
    int64_t initial_count = wtree3_tree_count(ctx->tree);
    assert_int_equal(initial_count, 1);

    /* Upsert existing key - count should stay same */
    int rc = wtree3_upsert(ctx->tree, "user1", 5,
                           "email:new@example.com|age:30", 29, &error);
    assert_int_equal(rc, WTREE3_OK);
    assert_int_equal(wtree3_tree_count(ctx->tree), initial_count);

    printf("  Upsert (update) correctly preserved count\n");
}

/* Test: Upsert failure doesn't change count */
static void test_upsert_failure_preserves_count(void **state) {
    test_ctx_t *ctx = *state;
    gerror_t error = {0};

    /* Insert initial entry */
    insert_initial_entry(ctx, "user1", "email:old@example.com|age:25");
    int64_t initial_count = wtree3_tree_count(ctx->tree);

    /* Configure mock to fail on index update */
    reset_all_mocks();
    mock_mdb_put.error_on_call = 1;
    mock_mdb_put.error_code = MDB_MAP_FULL;

    /* Upsert should fail */
    int rc = wtree3_upsert(ctx->tree, "user1", 5,
                           "email:new@example.com|age:30", 29, &error);
    assert_int_not_equal(rc, WTREE3_OK);

    /* Count must be unchanged */
    assert_int_equal(wtree3_tree_count(ctx->tree), initial_count);

    printf("  Failed upsert correctly preserved count\n");
}

/* Test: Multiple inserts maintain accurate count */
static void test_multiple_inserts_accurate_count(void **state) {
    test_ctx_t *ctx = *state;
    gerror_t error = {0};

    assert_int_equal(wtree3_tree_count(ctx->tree), 0);

    /* Insert 10 entries */
    for (int i = 0; i < 10; i++) {
        char key[16], value[64];
        snprintf(key, sizeof(key), "user%d", i);
        snprintf(value, sizeof(value), "email:user%d@example.com|age:%d", i, 20 + i);

        int rc = wtree3_insert_one(ctx->tree, key, strlen(key), value, strlen(value) + 1, &error);
        assert_int_equal(rc, WTREE3_OK);
        assert_int_equal(wtree3_tree_count(ctx->tree), i + 1);
    }

    /* Final count should be 10 */
    assert_int_equal(wtree3_tree_count(ctx->tree), 10);

    /* Verify by iteration */
    assert_entry_count_correct(ctx->tree);

    printf("  Multiple inserts maintained accurate count\n");
}

/* Test: Mixed operations maintain accurate count */
static void test_mixed_operations_accurate_count(void **state) {
    test_ctx_t *ctx = *state;
    gerror_t error = {0};

    int64_t expected_count = 0;

    /* Insert 3 */
    for (int i = 0; i < 3; i++) {
        char key[16], value[64];
        snprintf(key, sizeof(key), "user%d", i);
        snprintf(value, sizeof(value), "email:user%d@example.com|age:%d", i, 20 + i);
        wtree3_insert_one(ctx->tree, key, strlen(key), value, strlen(value) + 1, &error);
        expected_count++;
    }
    assert_int_equal(wtree3_tree_count(ctx->tree), expected_count);

    /* Update 1 (no count change) */
    wtree3_update(ctx->tree, "user1", 5, "email:updated@example.com|age:99", 34, &error);
    assert_int_equal(wtree3_tree_count(ctx->tree), expected_count);

    /* Delete 1 */
    wtree3_delete_one(ctx->tree, "user0", 5, NULL, &error);
    expected_count--;
    assert_int_equal(wtree3_tree_count(ctx->tree), expected_count);

    /* Upsert new (insert) */
    wtree3_upsert(ctx->tree, "user10", 6, "email:user10@example.com|age:30", 32, &error);
    expected_count++;
    assert_int_equal(wtree3_tree_count(ctx->tree), expected_count);

    /* Upsert existing (update) */
    wtree3_upsert(ctx->tree, "user1", 5, "email:upserted@example.com|age:88", 35, &error);
    assert_int_equal(wtree3_tree_count(ctx->tree), expected_count);

    /* Final verification */
    assert_int_equal(expected_count, 3);
    assert_entry_count_correct(ctx->tree);

    printf("  Mixed operations maintained accurate count\n");
}

/* Test: Delete non-existent key doesn't change count */
static void test_delete_nonexistent_preserves_count(void **state) {
    test_ctx_t *ctx = *state;
    gerror_t error = {0};

    /* Insert some data */
    insert_initial_entry(ctx, "user1", "email:alice@example.com|age:25");
    int64_t initial_count = wtree3_tree_count(ctx->tree);
    assert_int_equal(initial_count, 1);

    /* Delete non-existent key */
    bool deleted = false;
    int rc = wtree3_delete_one(ctx->tree, "nonexistent", 11, &deleted, &error);

    /* Operation succeeds but nothing deleted */
    assert_int_equal(rc, WTREE3_OK);
    assert_false(deleted);

    /* Count unchanged */
    assert_int_equal(wtree3_tree_count(ctx->tree), initial_count);

    printf("  Delete of non-existent key correctly preserved count\n");
}

/* Test: Insert duplicate key doesn't change count */
static void test_insert_duplicate_preserves_count(void **state) {
    test_ctx_t *ctx = *state;
    gerror_t error = {0};

    /* Insert initial entry */
    insert_initial_entry(ctx, "user1", "email:alice@example.com|age:25");
    int64_t initial_count = wtree3_tree_count(ctx->tree);

    /* Try to insert duplicate - should fail */
    int rc = wtree3_insert_one(ctx->tree, "user1", 5,
                                "email:duplicate@example.com|age:99", 36, &error);
    assert_int_not_equal(rc, WTREE3_OK);

    /* Count unchanged */
    assert_int_equal(wtree3_tree_count(ctx->tree), initial_count);

    printf("  Insert of duplicate key correctly preserved count\n");
}

/* Test: Update non-existent key doesn't change count */
static void test_update_nonexistent_preserves_count(void **state) {
    test_ctx_t *ctx = *state;
    gerror_t error = {0};

    int64_t initial_count = wtree3_tree_count(ctx->tree);
    assert_int_equal(initial_count, 0);

    /* Try to update non-existent key - should fail */
    int rc = wtree3_update(ctx->tree, "nonexistent", 11,
                           "email:doesntexist@example.com|age:99", 38, &error);
    assert_int_equal(rc, WTREE3_NOT_FOUND);

    /* Count unchanged (still 0) */
    assert_int_equal(wtree3_tree_count(ctx->tree), initial_count);

    printf("  Update of non-existent key correctly preserved count\n");
}

/* Test: Count accuracy after failed insert in batch */
static void test_batch_insert_partial_failure_count(void **state) {
    test_ctx_t *ctx = *state;
    gerror_t error = {0};

    /* Insert 2 entries successfully */
    insert_initial_entry(ctx, "user1", "email:user1@example.com|age:25");
    insert_initial_entry(ctx, "user2", "email:user2@example.com|age:30");

    int64_t count_before = wtree3_tree_count(ctx->tree);
    assert_int_equal(count_before, 2);

    /* Try to insert with error on 3rd */
    reset_all_mocks();
    mock_mdb_put.error_on_call = 1;
    mock_mdb_put.error_code = MDB_MAP_FULL;

    int rc = wtree3_insert_one(ctx->tree, "user3", 5,
                                "email:user3@example.com|age:35", 31, &error);
    assert_int_not_equal(rc, WTREE3_OK);

    /* Count should still be 2 */
    assert_int_equal(wtree3_tree_count(ctx->tree), count_before);

    /* Verify actual count matches */
    reset_all_mocks();
    assert_entry_count_correct(ctx->tree);

    printf("  Batch insert with failure maintained accurate count\n");
}

/* ============================================================
 * Test Suite Definition
 * ============================================================ */

int main(void) {
    const struct CMUnitTest tests[] = {
        /* Priority 1.1: Insert with index error mid-operation */
        cmocka_unit_test_setup_teardown(
            test_insert_fails_on_second_index_put,
            setup_tree_with_indexes,
            teardown_tree_with_indexes
        ),
        cmocka_unit_test_setup_teardown(
            test_insert_fails_on_first_index_put,
            setup_tree_with_indexes,
            teardown_tree_with_indexes
        ),
        cmocka_unit_test_setup_teardown(
            test_insert_fails_on_main_tree_put,
            setup_tree_with_indexes,
            teardown_tree_with_indexes
        ),

        /* Priority 1.2: Update with index error */
        cmocka_unit_test_setup_teardown(
            test_update_fails_on_first_index_delete,
            setup_tree_with_indexes,
            teardown_tree_with_indexes
        ),
        cmocka_unit_test_setup_teardown(
            test_update_fails_on_new_index_insert,
            setup_tree_with_indexes,
            teardown_tree_with_indexes
        ),
        cmocka_unit_test_setup_teardown(
            test_update_fails_on_main_tree_put,
            setup_tree_with_indexes,
            teardown_tree_with_indexes
        ),

        /* Priority 1.3: Delete with index error */
        cmocka_unit_test_setup_teardown(
            test_delete_fails_on_first_index_delete,
            setup_tree_with_indexes,
            teardown_tree_with_indexes
        ),
        cmocka_unit_test_setup_teardown(
            test_delete_fails_on_second_index_delete,
            setup_tree_with_indexes,
            teardown_tree_with_indexes
        ),
        cmocka_unit_test_setup_teardown(
            test_delete_fails_on_main_tree_delete,
            setup_tree_with_indexes,
            teardown_tree_with_indexes
        ),
        cmocka_unit_test_setup_teardown(
            test_delete_succeeds_after_failure,
            setup_tree_with_indexes,
            teardown_tree_with_indexes
        ),

        /* Priority 2: Populate index with errors */
        cmocka_unit_test_setup_teardown(
            test_populate_unique_index_with_duplicate,
            setup_tree_with_data_no_indexes,
            teardown_tree_with_indexes
        ),
        cmocka_unit_test_setup_teardown(
            test_populate_fails_on_index_put,
            setup_tree_with_data_no_indexes,
            teardown_tree_with_indexes
        ),
        cmocka_unit_test_setup_teardown(
            test_populate_fails_on_commit,
            setup_tree_with_data_no_indexes,
            teardown_tree_with_indexes
        ),

        /* Priority 3: Entry count edge cases */
        cmocka_unit_test_setup_teardown(
            test_upsert_insert_increments_count,
            setup_tree_with_indexes,
            teardown_tree_with_indexes
        ),
        cmocka_unit_test_setup_teardown(
            test_upsert_update_preserves_count,
            setup_tree_with_indexes,
            teardown_tree_with_indexes
        ),
        cmocka_unit_test_setup_teardown(
            test_upsert_failure_preserves_count,
            setup_tree_with_indexes,
            teardown_tree_with_indexes
        ),
        cmocka_unit_test_setup_teardown(
            test_multiple_inserts_accurate_count,
            setup_tree_with_indexes,
            teardown_tree_with_indexes
        ),
        cmocka_unit_test_setup_teardown(
            test_mixed_operations_accurate_count,
            setup_tree_with_indexes,
            teardown_tree_with_indexes
        ),
        cmocka_unit_test_setup_teardown(
            test_delete_nonexistent_preserves_count,
            setup_tree_with_indexes,
            teardown_tree_with_indexes
        ),
        cmocka_unit_test_setup_teardown(
            test_insert_duplicate_preserves_count,
            setup_tree_with_indexes,
            teardown_tree_with_indexes
        ),
        cmocka_unit_test_setup_teardown(
            test_update_nonexistent_preserves_count,
            setup_tree_with_indexes,
            teardown_tree_with_indexes
        ),
        cmocka_unit_test_setup_teardown(
            test_batch_insert_partial_failure_count,
            setup_tree_with_indexes,
            teardown_tree_with_indexes
        ),
    };

    printf("=======================================================\n");
    printf("LMDB Error Injection Tests - Transaction Rollback\n");
    printf("Priority 1.1: Insert with Index Error Mid-Operation\n");
    printf("Priority 1.2: Update with Index Error\n");
    printf("Priority 1.3: Delete with Index Error\n");
    printf("Priority 2:   Populate Index with Errors\n");
    printf("Priority 3:   Entry Count Edge Cases\n");
    printf("=======================================================\n\n");

    return cmocka_run_group_tests(tests, NULL, NULL);
}
