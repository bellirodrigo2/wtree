/*
 * test_wtree3_tier1.c - Comprehensive tests for Tier 1 operations
 *
 * Tests generic low-level primitives including:
 * - Range scanning (forward and reverse)
 * - Prefix scanning
 * - Atomic read-modify-write operations
 * - Batch read operations
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
    snprintf(test_db_path, sizeof(test_db_path), "%s\\test_wtree3_tier1_%d",
             getenv("TEMP"), getpid());
#else
    snprintf(test_db_path, sizeof(test_db_path), "/tmp/test_wtree3_tier1_%d", getpid());
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

/* Populate tree with test data (key1..key9) */
static void populate_tree(wtree3_tree_t *tree) {
    gerror_t error = {0};

    const char *keys[] = {"key1", "key2", "key3", "key5", "key7", "key8", "key9"};
    const char *values[] = {"val1", "val2", "val3", "val5", "val7", "val8", "val9"};

    for (size_t i = 0; i < 7; i++) {
        int rc = wtree3_insert_one(tree, keys[i], strlen(keys[i]) + 1,
                                   values[i], strlen(values[i]) + 1, &error);
        assert_int_equal(rc, 0);
    }
}

/* Scan callback that collects keys into a string buffer */
typedef struct {
    char buffer[256];
    size_t offset;
    size_t count;
} scan_collector_t;

static bool collect_keys(const void *key, size_t key_len,
                         const void *value, size_t value_len,
                         void *user_data) {
    (void)value;
    (void)value_len;

    scan_collector_t *collector = (scan_collector_t*)user_data;

    if (collector->count > 0) {
        collector->offset += snprintf(collector->buffer + collector->offset,
                                      sizeof(collector->buffer) - collector->offset,
                                      ",");
    }

    collector->offset += snprintf(collector->buffer + collector->offset,
                                  sizeof(collector->buffer) - collector->offset,
                                  "%s", (const char*)key);
    collector->count++;
    return true;
}

/* Scan callback that stops after N entries */
typedef struct {
    size_t max_count;
    size_t count;
} scan_limiter_t;

static bool limit_scan(const void *key, size_t key_len,
                       const void *value, size_t value_len,
                       void *user_data) {
    (void)key;
    (void)key_len;
    (void)value;
    (void)value_len;

    scan_limiter_t *limiter = (scan_limiter_t*)user_data;
    limiter->count++;
    return limiter->count < limiter->max_count;
}

/* ============================================================
 * Scan Range Tests (Forward)
 * ============================================================ */

static void test_scan_range_full(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "scan_full", 0, 0, &error);
    assert_non_null(tree);

    populate_tree(tree);

    /* Scan entire tree (NULL start/end) */
    wtree3_txn_t *txn = wtree3_txn_begin(test_db, false, &error);
    assert_non_null(txn);

    scan_collector_t collector = {0};
    int rc = wtree3_scan_range_txn(txn, tree, NULL, 0, NULL, 0,
                                   collect_keys, &collector, &error);
    assert_int_equal(rc, 0);
    assert_string_equal(collector.buffer, "key1,key2,key3,key5,key7,key8,key9");
    assert_int_equal(collector.count, 7);

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
}

static void test_scan_range_partial(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "scan_partial", 0, 0, &error);
    assert_non_null(tree);

    populate_tree(tree);

    wtree3_txn_t *txn = wtree3_txn_begin(test_db, false, &error);
    assert_non_null(txn);

    /* Scan from key3 to key7 */
    scan_collector_t collector = {0};
    int rc = wtree3_scan_range_txn(txn, tree,
                                   "key3", strlen("key3") + 1,
                                   "key7", strlen("key7") + 1,
                                   collect_keys, &collector, &error);
    assert_int_equal(rc, 0);
    assert_string_equal(collector.buffer, "key3,key5,key7");

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
}

static void test_scan_range_start_only(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "scan_start", 0, 0, &error);
    assert_non_null(tree);

    populate_tree(tree);

    wtree3_txn_t *txn = wtree3_txn_begin(test_db, false, &error);
    assert_non_null(txn);

    /* Scan from key5 to end */
    scan_collector_t collector = {0};
    int rc = wtree3_scan_range_txn(txn, tree,
                                   "key5", strlen("key5") + 1,
                                   NULL, 0,
                                   collect_keys, &collector, &error);
    assert_int_equal(rc, 0);
    assert_string_equal(collector.buffer, "key5,key7,key8,key9");

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
}

static void test_scan_range_end_only(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "scan_end", 0, 0, &error);
    assert_non_null(tree);

    populate_tree(tree);

    wtree3_txn_t *txn = wtree3_txn_begin(test_db, false, &error);
    assert_non_null(txn);

    /* Scan from start to key5 */
    scan_collector_t collector = {0};
    int rc = wtree3_scan_range_txn(txn, tree,
                                   NULL, 0,
                                   "key5", strlen("key5") + 1,
                                   collect_keys, &collector, &error);
    assert_int_equal(rc, 0);
    assert_string_equal(collector.buffer, "key1,key2,key3,key5");

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
}

static void test_scan_range_early_stop(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "scan_stop", 0, 0, &error);
    assert_non_null(tree);

    populate_tree(tree);

    wtree3_txn_t *txn = wtree3_txn_begin(test_db, false, &error);
    assert_non_null(txn);

    /* Scan but stop after 3 entries */
    scan_limiter_t limiter = {.max_count = 3, .count = 0};
    int rc = wtree3_scan_range_txn(txn, tree, NULL, 0, NULL, 0,
                                   limit_scan, &limiter, &error);
    assert_int_equal(rc, 0);
    assert_int_equal(limiter.count, 3);

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
}

static void test_scan_range_empty(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "scan_empty", 0, 0, &error);
    assert_non_null(tree);

    wtree3_txn_t *txn = wtree3_txn_begin(test_db, false, &error);
    assert_non_null(txn);

    /* Scan empty tree */
    scan_collector_t collector = {0};
    int rc = wtree3_scan_range_txn(txn, tree, NULL, 0, NULL, 0,
                                   collect_keys, &collector, &error);
    assert_int_equal(rc, 0);
    assert_int_equal(collector.count, 0);

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
}

/* ============================================================
 * Scan Range Tests (Reverse)
 * ============================================================ */

static void test_scan_reverse_full(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "scan_rev_full", 0, 0, &error);
    assert_non_null(tree);

    populate_tree(tree);

    wtree3_txn_t *txn = wtree3_txn_begin(test_db, false, &error);
    assert_non_null(txn);

    /* Scan entire tree in reverse */
    scan_collector_t collector = {0};
    int rc = wtree3_scan_reverse_txn(txn, tree, NULL, 0, NULL, 0,
                                     collect_keys, &collector, &error);
    assert_int_equal(rc, 0);
    assert_string_equal(collector.buffer, "key9,key8,key7,key5,key3,key2,key1");

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
}

static void test_scan_reverse_partial(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "scan_rev_partial", 0, 0, &error);
    assert_non_null(tree);

    populate_tree(tree);

    wtree3_txn_t *txn = wtree3_txn_begin(test_db, false, &error);
    assert_non_null(txn);

    /* Scan from key7 down to key3 */
    scan_collector_t collector = {0};
    int rc = wtree3_scan_reverse_txn(txn, tree,
                                     "key7", strlen("key7") + 1,
                                     "key3", strlen("key3") + 1,
                                     collect_keys, &collector, &error);
    assert_int_equal(rc, 0);
    assert_string_equal(collector.buffer, "key7,key5,key3");

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
}

/* ============================================================
 * Scan Prefix Tests
 * ============================================================ */

static void test_scan_prefix_basic(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "scan_prefix", 0, 0, &error);
    assert_non_null(tree);

    /* Insert keys with different prefixes */
    wtree3_insert_one(tree, "user:1", 7, "alice", 6, &error);
    wtree3_insert_one(tree, "user:2", 7, "bob", 4, &error);
    wtree3_insert_one(tree, "user:3", 7, "carol", 6, &error);
    wtree3_insert_one(tree, "post:1", 7, "hello", 6, &error);
    wtree3_insert_one(tree, "post:2", 7, "world", 6, &error);

    wtree3_txn_t *txn = wtree3_txn_begin(test_db, false, &error);
    assert_non_null(txn);

    /* Scan all keys with "user:" prefix */
    scan_collector_t collector = {0};
    int rc = wtree3_scan_prefix_txn(txn, tree, "user:", 5,
                                    collect_keys, &collector, &error);
    assert_int_equal(rc, 0);
    assert_string_equal(collector.buffer, "user:1,user:2,user:3");
    assert_int_equal(collector.count, 3);

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
}

static void test_scan_prefix_no_match(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "scan_prefix_none", 0, 0, &error);
    assert_non_null(tree);

    populate_tree(tree);

    wtree3_txn_t *txn = wtree3_txn_begin(test_db, false, &error);
    assert_non_null(txn);

    /* Scan for non-existent prefix */
    scan_collector_t collector = {0};
    int rc = wtree3_scan_prefix_txn(txn, tree, "xyz:", 4,
                                    collect_keys, &collector, &error);
    assert_int_equal(rc, 0);
    assert_int_equal(collector.count, 0);

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
}

/* ============================================================
 * Modify Operation Tests
 * ============================================================ */

/* Modify callback that appends "-modified" to value */
static void* modify_append(const void *existing_value, size_t existing_len,
                          void *user_data, size_t *out_len) {
    (void)user_data;

    const char *suffix = "-modified";
    size_t suffix_len = strlen(suffix);

    if (!existing_value) {
        /* Key doesn't exist, create new value */
        char *result = malloc(suffix_len + 1);
        strcpy(result, suffix);
        *out_len = suffix_len + 1;
        return result;
    }

    /* Append to existing value */
    size_t new_len = existing_len + suffix_len;
    char *result = malloc(new_len);
    memcpy(result, existing_value, existing_len - 1);  /* -1 to overwrite null terminator */
    memcpy(result + existing_len - 1, suffix, suffix_len + 1);

    *out_len = new_len;
    return result;
}

/* Modify callback that increments an integer */
static void* modify_increment(const void *existing_value, size_t existing_len,
                             void *user_data, size_t *out_len) {
    (void)existing_len;
    (void)user_data;

    int *result = malloc(sizeof(int));

    if (!existing_value) {
        /* Initialize to 1 */
        *result = 1;
    } else {
        /* Increment existing */
        *result = (*(int*)existing_value) + 1;
    }

    *out_len = sizeof(int);
    return result;
}

/* Modify callback that returns NULL to delete */
static void* modify_delete(const void *existing_value, size_t existing_len,
                          void *user_data, size_t *out_len) {
    (void)existing_value;
    (void)existing_len;
    (void)user_data;
    (void)out_len;
    return NULL;
}

static void test_modify_update_existing(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "modify_update", 0, 0, &error);
    assert_non_null(tree);

    /* Insert initial value */
    wtree3_insert_one(tree, "counter", 8, "value", 6, &error);

    wtree3_txn_t *txn = wtree3_txn_begin(test_db, true, &error);
    assert_non_null(txn);

    /* Modify to append "-modified" */
    int rc = wtree3_modify_txn(txn, tree, "counter", 8,
                               modify_append, NULL, &error);
    assert_int_equal(rc, 0);

    rc = wtree3_txn_commit(txn, &error);
    assert_int_equal(rc, 0);

    /* Verify modified value */
    const void *value;
    size_t value_len;
    txn = wtree3_txn_begin(test_db, false, &error);
    rc = wtree3_get_txn(txn, tree, "counter", 8, &value, &value_len, &error);
    assert_int_equal(rc, 0);
    assert_string_equal((const char*)value, "value-modified");

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
}

static void test_modify_insert_new(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "modify_insert", 0, 0, &error);
    assert_non_null(tree);

    wtree3_txn_t *txn = wtree3_txn_begin(test_db, true, &error);
    assert_non_null(txn);

    /* Modify non-existent key (should insert) */
    int rc = wtree3_modify_txn(txn, tree, "newkey", 7,
                               modify_append, NULL, &error);
    assert_int_equal(rc, 0);

    rc = wtree3_txn_commit(txn, &error);
    assert_int_equal(rc, 0);

    /* Verify inserted value */
    const void *value;
    size_t value_len;
    txn = wtree3_txn_begin(test_db, false, &error);
    rc = wtree3_get_txn(txn, tree, "newkey", 7, &value, &value_len, &error);
    assert_int_equal(rc, 0);
    assert_string_equal((const char*)value, "-modified");

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
}

static void test_modify_delete_key(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "modify_delete", 0, 0, &error);
    assert_non_null(tree);

    /* Insert value */
    wtree3_insert_one(tree, "todelete", 9, "value", 6, &error);

    wtree3_txn_t *txn = wtree3_txn_begin(test_db, true, &error);
    assert_non_null(txn);

    /* Modify with callback that returns NULL */
    int rc = wtree3_modify_txn(txn, tree, "todelete", 9,
                               modify_delete, NULL, &error);
    assert_int_equal(rc, 0);

    rc = wtree3_txn_commit(txn, &error);
    assert_int_equal(rc, 0);

    /* Verify key was deleted */
    assert_int_equal(wtree3_tree_count(tree), 0);

    wtree3_tree_close(tree);
}

static void test_modify_counter(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "modify_counter", 0, 0, &error);
    assert_non_null(tree);

    /* Increment counter multiple times */
    for (int i = 0; i < 5; i++) {
        wtree3_txn_t *txn = wtree3_txn_begin(test_db, true, &error);
        int rc = wtree3_modify_txn(txn, tree, "count", 6,
                                   modify_increment, NULL, &error);
        assert_int_equal(rc, 0);
        wtree3_txn_commit(txn, &error);
    }

    /* Verify final count is 5 */
    const void *value;
    size_t value_len;
    wtree3_txn_t *txn = wtree3_txn_begin(test_db, false, &error);
    int rc = wtree3_get_txn(txn, tree, "count", 6, &value, &value_len, &error);
    assert_int_equal(rc, 0);
    assert_int_equal(*(int*)value, 5);

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
}

/* ============================================================
 * Get Many Tests
 * ============================================================ */

static void test_get_many_basic(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "get_many", 0, 0, &error);
    assert_non_null(tree);

    populate_tree(tree);

    wtree3_txn_t *txn = wtree3_txn_begin(test_db, false, &error);
    assert_non_null(txn);

    /* Batch read 3 keys */
    wtree3_kv_t keys[] = {
        {.key = "key1", .key_len = 5},
        {.key = "key3", .key_len = 5},
        {.key = "key7", .key_len = 5}
    };

    const void *values[3];
    size_t value_lens[3];

    int rc = wtree3_get_many_txn(txn, tree, keys, 3, values, value_lens, &error);
    assert_int_equal(rc, 0);

    assert_string_equal((const char*)values[0], "val1");
    assert_string_equal((const char*)values[1], "val3");
    assert_string_equal((const char*)values[2], "val7");

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
}

static void test_get_many_missing_keys(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "get_many_miss", 0, 0, &error);
    assert_non_null(tree);

    populate_tree(tree);

    wtree3_txn_t *txn = wtree3_txn_begin(test_db, false, &error);
    assert_non_null(txn);

    /* Batch read with some missing keys */
    wtree3_kv_t keys[] = {
        {.key = "key1", .key_len = 5},
        {.key = "key4", .key_len = 5},  /* doesn't exist */
        {.key = "key7", .key_len = 5}
    };

    const void *values[3];
    size_t value_lens[3];

    int rc = wtree3_get_many_txn(txn, tree, keys, 3, values, value_lens, &error);
    assert_int_equal(rc, 0);

    assert_string_equal((const char*)values[0], "val1");
    assert_null(values[1]);  /* Missing key */
    assert_int_equal(value_lens[1], 0);
    assert_string_equal((const char*)values[2], "val7");

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
}

/* ============================================================
 * Error Cases
 * ============================================================ */

static void test_scan_null_params(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "scan_null", 0, 0, &error);
    assert_non_null(tree);

    wtree3_txn_t *txn = wtree3_txn_begin(test_db, false, &error);

    /* NULL callback should fail */
    int rc = wtree3_scan_range_txn(txn, tree, NULL, 0, NULL, 0,
                                   NULL, NULL, &error);
    assert_int_equal(rc, WTREE3_EINVAL);

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
}

static void test_modify_readonly_txn(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "modify_ro", 0, 0, &error);
    assert_non_null(tree);

    wtree3_txn_t *txn = wtree3_txn_begin(test_db, false, &error);

    /* Modify on read-only transaction should fail */
    int rc = wtree3_modify_txn(txn, tree, "key", 4,
                               modify_append, NULL, &error);
    assert_int_equal(rc, WTREE3_EINVAL);

    wtree3_txn_abort(txn);
    wtree3_tree_close(tree);
}

/* ============================================================
 * Main Test Suite
 * ============================================================ */

int main(void) {
    const struct CMUnitTest tests[] = {
        /* Scan range forward */
        cmocka_unit_test_setup_teardown(test_scan_range_full, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_scan_range_partial, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_scan_range_start_only, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_scan_range_end_only, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_scan_range_early_stop, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_scan_range_empty, setup_db, teardown_db),

        /* Scan range reverse */
        cmocka_unit_test_setup_teardown(test_scan_reverse_full, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_scan_reverse_partial, setup_db, teardown_db),

        /* Scan prefix */
        cmocka_unit_test_setup_teardown(test_scan_prefix_basic, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_scan_prefix_no_match, setup_db, teardown_db),

        /* Modify operations */
        cmocka_unit_test_setup_teardown(test_modify_update_existing, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_modify_insert_new, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_modify_delete_key, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_modify_counter, setup_db, teardown_db),

        /* Get many operations */
        cmocka_unit_test_setup_teardown(test_get_many_basic, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_get_many_missing_keys, setup_db, teardown_db),

        /* Error cases */
        cmocka_unit_test_setup_teardown(test_scan_null_params, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_modify_readonly_txn, setup_db, teardown_db),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
