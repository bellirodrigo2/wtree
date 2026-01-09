/*
 * test_wtree3_dupsort.c - Test custom duplicate value comparison for non-unique indexes
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
 * Test Key Extractor
 * ============================================================ */

/*
 * Simple key extractor for testing
 * Value format: "category:value|id:number"
 * Extracts the category field
 */
static bool category_extractor(const void *value, size_t value_len,
                                void *user_data,
                                void **out_key, size_t *out_len) {
    (void)value_len;
    (void)user_data;
    const char *val = (const char *)value;

    const char *prefix = "category:";
    const char *found = strstr(val, prefix);
    if (!found) {
        return false;
    }

    const char *start = found + strlen(prefix);
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
 * Custom Duplicate Value Comparators
 * ============================================================ */

/*
 * Reverse lexicographic comparison for duplicate values
 * This will sort the main tree keys (which are IDs) in reverse order
 */
static int reverse_cmp(const MDB_val *a, const MDB_val *b) {
    int diff;
    size_t len_diff;
    size_t len = a->mv_size < b->mv_size ? a->mv_size : b->mv_size;

    diff = memcmp(a->mv_data, b->mv_data, len);
    if (diff) {
        /* Reverse the comparison */
        return -diff;
    }

    len_diff = a->mv_size - b->mv_size;
    /* Reverse length comparison too */
    return len_diff < 0 ? 1 : (len_diff > 0 ? -1 : 0);
}

/*
 * Numeric comparison for duplicate values
 * Assumes the main tree keys are string representations of numbers
 */
static int numeric_cmp(const MDB_val *a, const MDB_val *b) {
    char buf_a[32], buf_b[32];

    size_t len_a = a->mv_size < sizeof(buf_a) - 1 ? a->mv_size : sizeof(buf_a) - 1;
    size_t len_b = b->mv_size < sizeof(buf_b) - 1 ? b->mv_size : sizeof(buf_b) - 1;

    memcpy(buf_a, a->mv_data, len_a);
    buf_a[len_a] = '\0';

    memcpy(buf_b, b->mv_data, len_b);
    buf_b[len_b] = '\0';

    int num_a = atoi(buf_a);
    int num_b = atoi(buf_b);

    return num_a - num_b;
}

/* ============================================================
 * Test Fixtures
 * ============================================================ */

static int setup_db(void **state) {
    (void)state;

    /* Create temp directory */
#ifdef _WIN32
    snprintf(test_db_path, sizeof(test_db_path), "%s\\test_wtree3_dupsort_%d", getenv("TEMP"), getpid());
#else
    snprintf(test_db_path, sizeof(test_db_path), "/tmp/test_wtree3_dupsort_%d", getpid());
#endif
    mkdir(test_db_path, 0755);

    gerror_t error = {0};
    test_db = wtree3_db_open(test_db_path, 64 * 1024 * 1024, 32, WTREE3_VERSION(1, 0), 0, &error);
    if (!test_db) {
        fprintf(stderr, "Failed to create test database: %s\n", error.message);
        return -1;
    }

    /* Register the category extractor (non-unique, non-sparse) */
    int rc = wtree3_db_register_key_extractor(test_db, WTREE3_VERSION(1, 0), 0x00, category_extractor, &error);
    if (rc != WTREE3_OK) {
        fprintf(stderr, "Failed to register extractor: %s\n", error.message);
        wtree3_db_close(test_db);
        test_db = NULL;
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
 * Tests
 * ============================================================ */

/*
 * Test that duplicate values are sorted in reverse order
 * when using a custom dupsort comparator
 */
static void test_dupsort_reverse_order(void **state) {
    (void)state;
    gerror_t error = {0};

    /* Create tree */
    wtree3_tree_t *tree = wtree3_tree_open(test_db, "dupsort_tree", MDB_CREATE, 0, &error);
    assert_non_null(tree);

    /* Add non-unique index with reverse dupsort comparison */
    wtree3_index_config_t idx_config = {
        .name = "category_idx",
        .user_data = NULL,
        .user_data_len = 0,
        .unique = false,
        .sparse = false,
        .compare = NULL,
        .dupsort_compare = reverse_cmp
    };

    int rc = wtree3_tree_add_index(tree, &idx_config, &error);
    assert_int_equal(rc, WTREE3_OK);

    /* Insert multiple entries with same category but different IDs */
    const char *entries[][2] = {
        {"id1", "category:books|id:1"},
        {"id2", "category:books|id:2"},
        {"id3", "category:books|id:3"},
        {"id4", "category:books|id:4"},
    };

    for (int i = 0; i < 4; i++) {
        rc = wtree3_insert_one(tree, entries[i][0], strlen(entries[i][0]),
                               entries[i][1], strlen(entries[i][1]) + 1, &error);
        assert_int_equal(rc, WTREE3_OK);
    }

    /* Query index and verify order is reversed (id4, id3, id2, id1) */
    const char *category_key = "books";
    wtree3_iterator_t *iter = wtree3_index_seek(tree, "category_idx",
                                                  category_key, strlen(category_key), &error);
    assert_non_null(iter);

    /* Expected order: id4, id3, id2, id1 (reversed) */
    const char *expected_order[] = {"id4", "id3", "id2", "id1"};
    int idx = 0;

    while (wtree3_iterator_valid(iter)) {
        const void *main_key;
        size_t main_key_len;
        bool ok = wtree3_index_iterator_main_key(iter, &main_key, &main_key_len);
        assert_true(ok);

        assert_int_equal(main_key_len, strlen(expected_order[idx]));
        assert_memory_equal(main_key, expected_order[idx], main_key_len);

        idx++;
        wtree3_iterator_next(iter);
    }

    assert_int_equal(idx, 4);

    wtree3_iterator_close(iter);
    wtree3_tree_close(tree);
}

/*
 * Test that duplicate values are sorted numerically
 * when using a custom numeric dupsort comparator
 */
static void test_dupsort_numeric_order(void **state) {
    (void)state;
    gerror_t error = {0};

    /* Create tree */
    wtree3_tree_t *tree = wtree3_tree_open(test_db, "numeric_tree", MDB_CREATE, 0, &error);
    assert_non_null(tree);

    /* Add non-unique index with numeric dupsort comparison */
    wtree3_index_config_t idx_config = {
        .name = "category_idx",
        .user_data = NULL,
        .user_data_len = 0,
        .unique = false,
        .sparse = false,
        .compare = NULL,
        .dupsort_compare = numeric_cmp
    };

    int rc = wtree3_tree_add_index(tree, &idx_config, &error);
    assert_int_equal(rc, WTREE3_OK);

    /* Insert entries with numeric IDs in random order */
    const char *entries[][2] = {
        {"100", "category:tools|id:100"},
        {"5", "category:tools|id:5"},
        {"50", "category:tools|id:50"},
        {"10", "category:tools|id:10"},
    };

    for (int i = 0; i < 4; i++) {
        rc = wtree3_insert_one(tree, entries[i][0], strlen(entries[i][0]),
                               entries[i][1], strlen(entries[i][1]) + 1, &error);
        assert_int_equal(rc, WTREE3_OK);
    }

    /* Query index and verify numeric order (5, 10, 50, 100) */
    const char *category_key = "tools";
    wtree3_iterator_t *iter = wtree3_index_seek(tree, "category_idx",
                                                  category_key, strlen(category_key), &error);
    assert_non_null(iter);

    /* Expected numeric order: 5, 10, 50, 100 */
    const char *expected_order[] = {"5", "10", "50", "100"};
    int idx = 0;

    while (wtree3_iterator_valid(iter)) {
        const void *main_key;
        size_t main_key_len;
        bool ok = wtree3_index_iterator_main_key(iter, &main_key, &main_key_len);
        assert_true(ok);

        char buf[32];
        memcpy(buf, main_key, main_key_len);
        buf[main_key_len] = '\0';

        assert_string_equal(buf, expected_order[idx]);

        idx++;
        wtree3_iterator_next(iter);
    }

    assert_int_equal(idx, 4);

    wtree3_iterator_close(iter);
    wtree3_tree_close(tree);
}

/*
 * Test that without custom dupsort comparator,
 * default lexicographic order is used
 */
static void test_dupsort_default_order(void **state) {
    (void)state;
    gerror_t error = {0};

    /* Create tree */
    wtree3_tree_t *tree = wtree3_tree_open(test_db, "default_tree", MDB_CREATE, 0, &error);
    assert_non_null(tree);

    /* Add non-unique index WITHOUT custom dupsort comparison */
    wtree3_index_config_t idx_config = {
        .name = "category_idx",
        .user_data = NULL,
        .user_data_len = 0,
        .unique = false,
        .sparse = false,
        .compare = NULL,
        .dupsort_compare = NULL  /* Use default */
    };

    int rc = wtree3_tree_add_index(tree, &idx_config, &error);
    assert_int_equal(rc, WTREE3_OK);

    /* Insert numeric IDs in random order */
    const char *entries[][2] = {
        {"100", "category:items|id:100"},
        {"5", "category:items|id:5"},
        {"50", "category:items|id:50"},
        {"10", "category:items|id:10"},
    };

    for (int i = 0; i < 4; i++) {
        rc = wtree3_insert_one(tree, entries[i][0], strlen(entries[i][0]),
                               entries[i][1], strlen(entries[i][1]) + 1, &error);
        assert_int_equal(rc, WTREE3_OK);
    }

    /* Query index and verify lexicographic order (10, 100, 5, 50) */
    const char *category_key = "items";
    wtree3_iterator_t *iter = wtree3_index_seek(tree, "category_idx",
                                                  category_key, strlen(category_key), &error);
    assert_non_null(iter);

    /* Expected lexicographic order: 10, 100, 5, 50 */
    const char *expected_order[] = {"10", "100", "5", "50"};
    int idx = 0;

    while (wtree3_iterator_valid(iter)) {
        const void *main_key;
        size_t main_key_len;
        bool ok = wtree3_index_iterator_main_key(iter, &main_key, &main_key_len);
        assert_true(ok);

        char buf[32];
        memcpy(buf, main_key, main_key_len);
        buf[main_key_len] = '\0';

        assert_string_equal(buf, expected_order[idx]);

        idx++;
        wtree3_iterator_next(iter);
    }

    assert_int_equal(idx, 4);

    wtree3_iterator_close(iter);
    wtree3_tree_close(tree);
}

/* ============================================================
 * Test Runner
 * ============================================================ */

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_dupsort_reverse_order, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_dupsort_numeric_order, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_dupsort_default_order, setup_db, teardown_db),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
