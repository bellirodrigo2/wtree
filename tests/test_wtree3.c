/*
 * test_wtree3.c - Comprehensive tests for wtree3 unified storage layer
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

/* Extractor ID for simple_key_extractor */
#define TEST_EXTRACTOR_ID WTREE3_VERSION(1, 1)

/* Forward declaration */
static bool simple_key_extractor(const void *value, size_t value_len,
                                  void *user_data,
                                  void **out_key, size_t *out_len);

/* ============================================================
 * Test Fixtures
 * ============================================================ */

static int setup_db(void **state) {
    (void)state;

    /* Create temp directory */
#ifdef _WIN32
    snprintf(test_db_path, sizeof(test_db_path), "%s\\test_wtree3_%d", getenv("TEMP"), getpid());
#else
    snprintf(test_db_path, sizeof(test_db_path), "/tmp/test_wtree3_%d", getpid());
#endif
    mkdir(test_db_path, 0755);

    gerror_t error = {0};
    test_db = wtree3_db_open(test_db_path, 64 * 1024 * 1024, 32, WTREE3_VERSION(1, 0), 0, &error);
    if (!test_db) {
        fprintf(stderr, "Failed to create test database: %s\n", error.message);
        return -1;
    }

    /* Register the simple key extractor for all flag combinations */
    int rc;
    for (uint32_t flags = 0; flags <= 0x03; flags++) {
        rc = wtree3_db_register_key_extractor(test_db, WTREE3_VERSION(1, 0), flags, simple_key_extractor, &error);
        if (rc != WTREE3_OK) {
            fprintf(stderr, "Failed to register extractor for flags=0x%02x: %s\n", flags, error.message);
            wtree3_db_close(test_db);
            test_db = NULL;
            return -1;
        }
    }

    return 0;
}

static int teardown_db(void **state) {
    (void)state;

    if (test_db) {
        wtree3_db_close(test_db);
        test_db = NULL;
    }

    /* Clean up temp directory */
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
 * Simple Index Key Extractor
 *
 * For testing, we use a simple format:
 * Value = "field1:value1|field2:value2|..."
 * We extract the value of the field named by user_data.
 * ============================================================ */

static bool simple_key_extractor(const void *value, size_t value_len,
                                  void *user_data,
                                  void **out_key, size_t *out_len) {
    (void)value_len;
    const char *field_name = (const char *)user_data;
    const char *val = (const char *)value;

    /* Find field:value in the string */
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
    char *key = malloc(key_len);
    if (!key) return false;

    memcpy(key, start, key_len);

    *out_key = key;
    *out_len = key_len;
    return true;
}

/* ============================================================
 * Database Tests
 * ============================================================ */

static void test_db_open_close(void **state) {
    (void)state;
    gerror_t error = {0};

    /* test_db is already open from setup */
    assert_non_null(test_db);
    assert_true(wtree3_db_get_mapsize(test_db) > 0);

    /* Test sync */
    int rc = wtree3_db_sync(test_db, false, &error);
    assert_int_equal(rc, WTREE3_OK);
}

static void test_db_resize(void **state) {
    (void)state;
    gerror_t error = {0};

    size_t old_size = wtree3_db_get_mapsize(test_db);
    size_t new_size = old_size * 2;

    int rc = wtree3_db_resize(test_db, new_size, &error);
    assert_int_equal(rc, WTREE3_OK);
    assert_int_equal(wtree3_db_get_mapsize(test_db), new_size);
}

/* ============================================================
 * Basic Tree Tests
 * ============================================================ */

static void test_tree_open_close(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "test_tree", 0, 0, &error);
    assert_non_null(tree);
    assert_string_equal(wtree3_tree_name(tree), "test_tree");
    assert_int_equal(wtree3_tree_count(tree), 0);

    wtree3_tree_close(tree);
}

static void test_basic_crud(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "crud_tree", 0, 0, &error);
    assert_non_null(tree);

    /* Insert */
    const char *key1 = "key1";
    const char *val1 = "value1";
    int rc = wtree3_insert_one(tree, key1, strlen(key1), val1, strlen(val1) + 1, &error);
    assert_int_equal(rc, WTREE3_OK);
    assert_int_equal(wtree3_tree_count(tree), 1);

    /* Get */
    void *retrieved = NULL;
    size_t retrieved_size = 0;
    rc = wtree3_get(tree, key1, strlen(key1), &retrieved, &retrieved_size, &error);
    assert_int_equal(rc, WTREE3_OK);
    assert_string_equal((char *)retrieved, "value1");
    free(retrieved);

    /* Update */
    const char *val1_new = "value1_updated";
    rc = wtree3_update(tree, key1, strlen(key1), val1_new, strlen(val1_new) + 1, &error);
    assert_int_equal(rc, WTREE3_OK);
    assert_int_equal(wtree3_tree_count(tree), 1);  /* Count unchanged */

    rc = wtree3_get(tree, key1, strlen(key1), &retrieved, &retrieved_size, &error);
    assert_int_equal(rc, WTREE3_OK);
    assert_string_equal((char *)retrieved, "value1_updated");
    free(retrieved);

    /* Exists */
    assert_true(wtree3_exists(tree, key1, strlen(key1), &error));
    assert_false(wtree3_exists(tree, "nonexistent", 11, &error));

    /* Delete */
    bool deleted = false;
    rc = wtree3_delete_one(tree, key1, strlen(key1), &deleted, &error);
    assert_int_equal(rc, WTREE3_OK);
    assert_true(deleted);
    assert_int_equal(wtree3_tree_count(tree), 0);

    /* Delete non-existent */
    deleted = false;
    rc = wtree3_delete_one(tree, key1, strlen(key1), &deleted, &error);
    assert_int_equal(rc, WTREE3_OK);
    assert_false(deleted);

    wtree3_tree_close(tree);
}

static void test_transaction_basic(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "txn_tree", 0, 0, &error);
    assert_non_null(tree);

    /* Start transaction */
    wtree3_txn_t *txn = wtree3_txn_begin(test_db, true, &error);
    assert_non_null(txn);
    assert_false(wtree3_txn_is_readonly(txn));

    /* Insert within transaction */
    const char *key1 = "key1";
    const char *val1 = "value1";
    int rc = wtree3_insert_one_txn(txn, tree, key1, strlen(key1), val1, strlen(val1) + 1, &error);
    assert_int_equal(rc, WTREE3_OK);

    /* Commit */
    rc = wtree3_txn_commit(txn, &error);
    assert_int_equal(rc, WTREE3_OK);

    /* Verify data persisted */
    assert_true(wtree3_exists(tree, key1, strlen(key1), &error));

    wtree3_tree_close(tree);
}

static void test_transaction_abort(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "txn_abort_tree", 0, 0, &error);
    assert_non_null(tree);

    /* Start transaction */
    wtree3_txn_t *txn = wtree3_txn_begin(test_db, true, &error);
    assert_non_null(txn);

    /* Insert within transaction */
    const char *key1 = "key1";
    const char *val1 = "value1";
    wtree3_insert_one_txn(txn, tree, key1, strlen(key1), val1, strlen(val1) + 1, &error);

    /* Abort instead of commit */
    wtree3_txn_abort(txn);

    /* Verify data was NOT persisted */
    assert_false(wtree3_exists(tree, key1, strlen(key1), &error));

    wtree3_tree_close(tree);
}

static void test_iterator(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "iter_tree", 0, 0, &error);
    assert_non_null(tree);

    /* Insert some entries */
    char key[16], val[16];
    for (int i = 1; i <= 5; i++) {
        snprintf(key, sizeof(key), "key%d", i);
        snprintf(val, sizeof(val), "val%d", i);
        wtree3_insert_one(tree, key, strlen(key), val, strlen(val) + 1, &error);
    }
    assert_int_equal(wtree3_tree_count(tree), 5);

    /* Forward iteration */
    wtree3_iterator_t *iter = wtree3_iterator_create(tree, &error);
    assert_non_null(iter);

    int count = 0;
    if (wtree3_iterator_first(iter)) {
        do {
            const void *k, *v;
            size_t klen, vlen;
            assert_true(wtree3_iterator_key(iter, &k, &klen));
            assert_true(wtree3_iterator_value(iter, &v, &vlen));
            count++;
        } while (wtree3_iterator_next(iter));
    }
    assert_int_equal(count, 5);

    wtree3_iterator_close(iter);

    /* Backward iteration */
    iter = wtree3_iterator_create(tree, &error);
    count = 0;
    if (wtree3_iterator_last(iter)) {
        do {
            count++;
        } while (wtree3_iterator_prev(iter));
    }
    assert_int_equal(count, 5);

    wtree3_iterator_close(iter);
    wtree3_tree_close(tree);
}

static void test_iterator_seek(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "seek_tree", 0, 0, &error);
    assert_non_null(tree);

    /* Insert entries */
    for (int i = 1; i <= 5; i++) {
        char key[16], val[16];
        snprintf(key, sizeof(key), "key%d", i);
        snprintf(val, sizeof(val), "val%d", i);
        wtree3_insert_one(tree, key, strlen(key), val, strlen(val) + 1, &error);
    }

    /* Seek to exact key */
    wtree3_iterator_t *iter = wtree3_iterator_create(tree, &error);
    assert_true(wtree3_iterator_seek(iter, "key3", 4));
    assert_true(wtree3_iterator_valid(iter));

    const void *k;
    size_t klen;
    wtree3_iterator_key(iter, &k, &klen);
    assert_int_equal(klen, 4);
    assert_memory_equal(k, "key3", 4);

    wtree3_iterator_close(iter);

    /* Seek range (key >= search) */
    iter = wtree3_iterator_create(tree, &error);
    assert_true(wtree3_iterator_seek_range(iter, "key25", 5));  /* Between key2 and key3 */
    wtree3_iterator_key(iter, &k, &klen);
    assert_memory_equal(k, "key3", 4);  /* Should land on key3 */

    wtree3_iterator_close(iter);
    wtree3_tree_close(tree);
}

/* ============================================================
 * Index Tests
 * ============================================================ */

static void test_add_index(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "idx_tree1", 0, 0, &error);
    assert_non_null(tree);

    wtree3_index_config_t config = {
        .name = "email_idx",
        .user_data = (void *)"email",
        .user_data_len = 6,
        .unique = true,
        .sparse = false,
        .compare = NULL
    };

    int rc = wtree3_tree_add_index(tree, &config, &error);
    assert_int_equal(rc, WTREE3_OK);
    assert_true(wtree3_tree_has_index(tree, "email_idx"));
    assert_int_equal(wtree3_tree_index_count(tree), 1);

    /* Adding duplicate should fail */
    rc = wtree3_tree_add_index(tree, &config, &error);
    assert_int_equal(rc, WTREE3_KEY_EXISTS);

    wtree3_tree_close(tree);
}

static void test_index_maintenance_insert(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "idx_tree2", 0, 0, &error);
    assert_non_null(tree);

    /* Add email index */
    wtree3_index_config_t config = {
        .name = "email",
        .user_data = (void *)"email",
        .user_data_len = 6,
        .unique = false,
        .sparse = false,
        .compare = NULL
    };
    int rc = wtree3_tree_add_index(tree, &config, &error);
    assert_int_equal(rc, WTREE3_OK);

    /* Insert document with email */
    const char *key1 = "doc1";
    const char *val1 = "name:Alice|email:alice@test.com";
    rc = wtree3_insert_one(tree, key1, strlen(key1), val1, strlen(val1) + 1, &error);
    assert_int_equal(rc, WTREE3_OK);

    /* Verify we can find via index */
    wtree3_iterator_t *iter = wtree3_index_seek(tree, "email",
                                                  "alice@test.com", 14, &error);
    assert_non_null(iter);
    assert_true(wtree3_iterator_valid(iter));

    const void *main_key;
    size_t main_key_size;
    assert_true(wtree3_index_iterator_main_key(iter, &main_key, &main_key_size));
    assert_memory_equal(main_key, "doc1", 4);

    wtree3_iterator_close(iter);
    wtree3_tree_close(tree);
}

static void test_unique_index_violation(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "idx_tree3", 0, 0, &error);
    assert_non_null(tree);

    /* Add unique email index */
    wtree3_index_config_t config = {
        .name = "email",
        .user_data = (void *)"email",
        .user_data_len = 6,
        .unique = true,
        .sparse = false,
        .compare = NULL
    };
    wtree3_tree_add_index(tree, &config, &error);

    /* Insert first document */
    const char *key1 = "doc1";
    const char *val1 = "name:Alice|email:alice@test.com";
    int rc = wtree3_insert_one(tree, key1, strlen(key1), val1, strlen(val1) + 1, &error);
    assert_int_equal(rc, WTREE3_OK);

    /* Insert second document with same email - should fail */
    const char *key2 = "doc2";
    const char *val2 = "name:Bob|email:alice@test.com";
    rc = wtree3_insert_one(tree, key2, strlen(key2), val2, strlen(val2) + 1, &error);
    assert_int_equal(rc, WTREE3_INDEX_ERROR);
    assert_int_equal(wtree3_tree_count(tree), 1);  /* Only 1 doc inserted */

    /* Insert with different email - should succeed */
    const char *key3 = "doc3";
    const char *val3 = "name:Charlie|email:charlie@test.com";
    rc = wtree3_insert_one(tree, key3, strlen(key3), val3, strlen(val3) + 1, &error);
    assert_int_equal(rc, WTREE3_OK);
    assert_int_equal(wtree3_tree_count(tree), 2);

    wtree3_tree_close(tree);
}

static void test_sparse_index(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "idx_tree4", 0, 0, &error);
    assert_non_null(tree);

    /* Add sparse email index */
    wtree3_index_config_t config = {
        .name = "email",
        .user_data = (void *)"email",
        .user_data_len = 6,
        .unique = true,
        .sparse = true,
        .compare = NULL
    };
    wtree3_tree_add_index(tree, &config, &error);

    /* Insert document WITH email */
    const char *key1 = "doc1";
    const char *val1 = "name:Alice|email:alice@test.com";
    int rc = wtree3_insert_one(tree, key1, strlen(key1), val1, strlen(val1) + 1, &error);
    assert_int_equal(rc, WTREE3_OK);

    /* Insert document WITHOUT email - should succeed (sparse index) */
    const char *key2 = "doc2";
    const char *val2 = "name:Bob|phone:12345";
    rc = wtree3_insert_one(tree, key2, strlen(key2), val2, strlen(val2) + 1, &error);
    assert_int_equal(rc, WTREE3_OK);
    assert_int_equal(wtree3_tree_count(tree), 2);

    /* Verify second doc is NOT in the index */
    wtree3_iterator_t *iter = wtree3_index_seek(tree, "email", "bob", 3, &error);
    assert_non_null(iter);
    assert_false(wtree3_iterator_valid(iter));
    wtree3_iterator_close(iter);

    wtree3_tree_close(tree);
}

static void test_index_maintenance_update(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "idx_tree5", 0, 0, &error);
    assert_non_null(tree);

    /* Add email index */
    wtree3_index_config_t config = {
        .name = "email",
        .user_data = (void *)"email",
        .user_data_len = 6,
        .unique = true,
        .sparse = false,
        .compare = NULL
    };
    wtree3_tree_add_index(tree, &config, &error);

    /* Insert document */
    const char *key1 = "doc1";
    const char *val1 = "name:Alice|email:alice@test.com";
    wtree3_insert_one(tree, key1, strlen(key1), val1, strlen(val1) + 1, &error);

    /* Update email */
    const char *val1_updated = "name:Alice|email:alice.new@test.com";
    int rc = wtree3_update(tree, key1, strlen(key1), val1_updated, strlen(val1_updated) + 1, &error);
    assert_int_equal(rc, WTREE3_OK);

    /* Old email should NOT be findable */
    wtree3_iterator_t *iter = wtree3_index_seek(tree, "email", "alice@test.com", 14, &error);
    assert_non_null(iter);
    assert_false(wtree3_iterator_valid(iter));
    wtree3_iterator_close(iter);

    /* New email SHOULD be findable */
    iter = wtree3_index_seek(tree, "email", "alice.new@test.com", 18, &error);
    assert_non_null(iter);
    assert_true(wtree3_iterator_valid(iter));
    wtree3_iterator_close(iter);

    wtree3_tree_close(tree);
}

static void test_index_maintenance_delete(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "idx_tree6", 0, 0, &error);
    assert_non_null(tree);

    /* Add email index */
    wtree3_index_config_t config = {
        .name = "email",
        .user_data = (void *)"email",
        .user_data_len = 6,
        .unique = false,
        .sparse = false,
        .compare = NULL
    };
    wtree3_tree_add_index(tree, &config, &error);

    /* Insert document */
    const char *key1 = "doc1";
    const char *val1 = "name:Alice|email:alice@test.com";
    wtree3_insert_one(tree, key1, strlen(key1), val1, strlen(val1) + 1, &error);

    /* Delete document */
    bool deleted = false;
    int rc = wtree3_delete_one(tree, key1, strlen(key1), &deleted, &error);
    assert_int_equal(rc, WTREE3_OK);
    assert_true(deleted);

    /* Email should NOT be findable in index */
    wtree3_iterator_t *iter = wtree3_index_seek(tree, "email", "alice@test.com", 14, &error);
    assert_non_null(iter);
    assert_false(wtree3_iterator_valid(iter));
    wtree3_iterator_close(iter);

    wtree3_tree_close(tree);
}

static void test_populate_index(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "idx_tree7", 0, 0, &error);
    assert_non_null(tree);

    /* Insert documents BEFORE adding index */
    const char *key1 = "doc1";
    const char *val1 = "name:Alice|email:alice@test.com";
    wtree3_insert_one(tree, key1, strlen(key1), val1, strlen(val1) + 1, &error);

    const char *key2 = "doc2";
    const char *val2 = "name:Bob|email:bob@test.com";
    wtree3_insert_one(tree, key2, strlen(key2), val2, strlen(val2) + 1, &error);

    /* Add index AFTER data exists */
    wtree3_index_config_t config = {
        .name = "email",
        .user_data = (void *)"email",
        .user_data_len = 6,
        .unique = false,
        .sparse = false,
        .compare = NULL
    };
    wtree3_tree_add_index(tree, &config, &error);

    /* Populate index */
    int rc = wtree3_tree_populate_index(tree, "email", &error);
    assert_int_equal(rc, WTREE3_OK);

    /* Both emails should be findable */
    wtree3_iterator_t *iter = wtree3_index_seek(tree, "email", "alice@test.com", 14, &error);
    assert_non_null(iter);
    assert_true(wtree3_iterator_valid(iter));
    wtree3_iterator_close(iter);

    iter = wtree3_index_seek(tree, "email", "bob@test.com", 12, &error);
    assert_non_null(iter);
    assert_true(wtree3_iterator_valid(iter));
    wtree3_iterator_close(iter);

    wtree3_tree_close(tree);
}

static void test_drop_index(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "idx_tree8", 0, 0, &error);
    assert_non_null(tree);

    /* Add index */
    wtree3_index_config_t config = {
        .name = "email",
        .user_data = (void *)"email",
        .user_data_len = 6,
        .unique = false,
        .sparse = false,
        .compare = NULL
    };
    wtree3_tree_add_index(tree, &config, &error);
    assert_true(wtree3_tree_has_index(tree, "email"));

    /* Insert a document */
    const char *key1 = "doc1";
    const char *val1 = "name:Alice|email:alice@test.com";
    wtree3_insert_one(tree, key1, strlen(key1), val1, strlen(val1) + 1, &error);

    /* Drop index */
    int rc = wtree3_tree_drop_index(tree, "email", &error);
    assert_int_equal(rc, WTREE3_OK);
    assert_false(wtree3_tree_has_index(tree, "email"));
    assert_int_equal(wtree3_tree_index_count(tree), 0);

    /* Document should still exist in main tree */
    void *retrieved = NULL;
    size_t retrieved_size = 0;
    rc = wtree3_get(tree, key1, strlen(key1), &retrieved, &retrieved_size, &error);
    assert_int_equal(rc, WTREE3_OK);
    free(retrieved);

    wtree3_tree_close(tree);
}

static void test_multiple_indexes(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "idx_tree9", 0, 0, &error);
    assert_non_null(tree);

    /* Add email index */
    wtree3_index_config_t email_config = {
        .name = "email",
        .user_data = (void *)"email",
        .user_data_len = 6,
        .unique = true,
        .sparse = false,
        .compare = NULL
    };
    wtree3_tree_add_index(tree, &email_config, &error);

    /* Add name index */
    wtree3_index_config_t name_config = {
        .name = "name",
        .user_data = (void *)"name",
        .user_data_len = 5,
        .unique = false,
        .sparse = false,
        .compare = NULL
    };
    wtree3_tree_add_index(tree, &name_config, &error);

    assert_int_equal(wtree3_tree_index_count(tree), 2);

    /* Insert document */
    const char *key1 = "doc1";
    const char *val1 = "name:Alice|email:alice@test.com";
    int rc = wtree3_insert_one(tree, key1, strlen(key1), val1, strlen(val1) + 1, &error);
    assert_int_equal(rc, WTREE3_OK);

    /* Both indexes should work */
    wtree3_iterator_t *iter = wtree3_index_seek(tree, "email", "alice@test.com", 14, &error);
    assert_true(wtree3_iterator_valid(iter));
    wtree3_iterator_close(iter);

    iter = wtree3_index_seek(tree, "name", "Alice", 5, &error);
    assert_true(wtree3_iterator_valid(iter));
    wtree3_iterator_close(iter);

    wtree3_tree_close(tree);
}

static void test_error_strings(void **state) {
    (void)state;

    assert_string_equal(wtree3_strerror(WTREE3_OK), "Success");
    assert_string_equal(wtree3_strerror(WTREE3_EINVAL), "Invalid argument");
    assert_string_equal(wtree3_strerror(WTREE3_ENOMEM), "Out of memory");
    assert_string_equal(wtree3_strerror(WTREE3_KEY_EXISTS), "Key already exists");
    assert_string_equal(wtree3_strerror(WTREE3_NOT_FOUND), "Key not found");
    assert_string_equal(wtree3_strerror(WTREE3_MAP_FULL), "Database map is full, resize needed");
    assert_string_equal(wtree3_strerror(WTREE3_INDEX_ERROR), "Index error (duplicate key violation)");

    /* Error recoverability */
    assert_true(wtree3_error_recoverable(WTREE3_MAP_FULL));
    assert_true(wtree3_error_recoverable(WTREE3_TXN_FULL));
    assert_false(wtree3_error_recoverable(WTREE3_EINVAL));
}

/* ============================================================
 * Main
 * ============================================================ */

int main(void) {
    const struct CMUnitTest tests[] = {
        /* Database tests */
        cmocka_unit_test(test_db_open_close),
        cmocka_unit_test(test_db_resize),

        /* Basic tree tests */
        cmocka_unit_test(test_tree_open_close),
        cmocka_unit_test(test_basic_crud),
        cmocka_unit_test(test_transaction_basic),
        cmocka_unit_test(test_transaction_abort),
        cmocka_unit_test(test_iterator),
        cmocka_unit_test(test_iterator_seek),

        /* Index tests */
        cmocka_unit_test(test_add_index),
        cmocka_unit_test(test_index_maintenance_insert),
        cmocka_unit_test(test_unique_index_violation),
        cmocka_unit_test(test_sparse_index),
        cmocka_unit_test(test_index_maintenance_update),
        cmocka_unit_test(test_index_maintenance_delete),
        cmocka_unit_test(test_populate_index),
        cmocka_unit_test(test_drop_index),
        cmocka_unit_test(test_multiple_indexes),

        /* Utility tests */
        cmocka_unit_test(test_error_strings),
    };

    return cmocka_run_group_tests(tests, setup_db, teardown_db);
}
