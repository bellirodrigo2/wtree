/*
 * test_wtree3_persistence_new.c - Index persistence tests for new API
 *
 * Tests for the simplified persistence API that stores user_data as raw bytes.
 * This replaces the old callback-based serialization system.
 *
 * Coverage targets:
 * - save_index_metadata (wtree3_index_persist.c)
 * - load_index_metadata (wtree3_index_persist.c)
 * - wtree3_tree_list_persisted_indexes (wtree3_index_persist.c)
 * - wtree3_index_get_extractor_id (wtree3_index_persist.c)
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
#include "wtree3_internal.h"  // For internal functions

/* Test database path */
static char test_db_path[256];
static wtree3_db_t *test_db = NULL;

#define TEST_VERSION WTREE3_VERSION(1, 0)

/* ============================================================
 * Test Extractor Functions
 * ============================================================ */

typedef struct {
    int id;
    char name[64];
    char email[64];
} user_t;

static bool email_extractor(const void *value, size_t value_len,
                             void *user_data,
                             void **out_key, size_t *out_len) {
    (void)user_data;

    if (value_len < sizeof(user_t)) {
        return false;
    }

    const user_t *user = (const user_t *)value;
    size_t email_len = strlen(user->email) + 1;

    char *key = malloc(email_len);
    if (!key) return false;

    memcpy(key, user->email, email_len);
    *out_key = key;
    *out_len = email_len;
    return true;
}

/* ============================================================
 * Test Fixtures
 * ============================================================ */

static int setup_db(void **state) {
    (void)state;

#ifdef _WIN32
    const char *temp = getenv("TEMP");
    if (!temp) temp = getenv("TMP");
    if (!temp) temp = ".";
    snprintf(test_db_path, sizeof(test_db_path), "%s\\test_persist_new_%d",
             temp, getpid());
#else
    snprintf(test_db_path, sizeof(test_db_path), "/tmp/test_persist_new_%d",
             getpid());
#endif
    mkdir(test_db_path, 0755);

    gerror_t error = {0};
    test_db = wtree3_db_open(test_db_path, 64 * 1024 * 1024, 32, TEST_VERSION, 0, &error);
    if (!test_db) {
        fprintf(stderr, "Failed to create test database: %s\n", error.message);
        return -1;
    }

    /* Register extractor for all flag combinations */
    for (uint32_t flags = 0; flags <= 0x03; flags++) {
        int rc = wtree3_db_register_key_extractor(test_db, TEST_VERSION, flags,
                                                   email_extractor, &error);
        if (rc != WTREE3_OK) {
            fprintf(stderr, "Failed to register extractor: %s\n", error.message);
            wtree3_db_close(test_db);
            return -1;
        }
    }

    return 0;
}

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
 * Basic Persistence Tests
 * ============================================================ */

static void test_basic_persistence_workflow(void **state) {
    (void)state;
    gerror_t error = {0};

    /* Create tree and add index with user_data */
    wtree3_tree_t *tree = wtree3_tree_open(test_db, "users", 0, 0, &error);
    assert_non_null(tree);

    const char *user_data = "email_field";
    wtree3_index_config_t config = {
        .name = "email_idx",
        .user_data = user_data,
        .user_data_len = strlen(user_data) + 1,
        .unique = true,
        .sparse = false,
        .compare = NULL
    };

    int rc = wtree3_tree_add_index(tree, &config, &error);
    assert_int_equal(rc, WTREE3_OK);

    /* Insert data */
    user_t user = {1, "Alice", "alice@example.com"};
    rc = wtree3_insert_one(tree, &user.id, sizeof(user.id), &user, sizeof(user), &error);
    assert_int_equal(rc, WTREE3_OK);

    wtree3_tree_close(tree);

    /* Reopen tree - index should be auto-loaded */
    tree = wtree3_tree_open(test_db, "users", 0, 0, &error);
    assert_non_null(tree);

    /* Verify index exists and works by seeking */
    const char *email = "alice@example.com";
    wtree3_iterator_t *iter = wtree3_index_seek(tree, "email_idx", email,
                                                  strlen(email) + 1, &error);
    assert_non_null(iter);

    /* Just verify we can find a result - that proves the index was persisted */
    const void *pk;
    size_t pk_len;
    bool found = wtree3_iterator_value(iter, &pk, &pk_len);
    assert_true(found);
    assert_int_equal(pk_len, sizeof(int));

    int id = *(const int *)pk;
    assert_int_equal(id, 1);

    wtree3_iterator_close(iter);
    wtree3_tree_close(tree);
}

static void test_multiple_indexes_persist(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "multi_idx", 0, 0, &error);
    assert_non_null(tree);

    /* Add multiple indexes */
    const char *indexes_to_create[] = {"idx1", "idx2", "idx3"};
    for (int i = 0; i < 3; i++) {
        wtree3_index_config_t config = {
            .name = indexes_to_create[i],
            .user_data = NULL,
            .user_data_len = 0,
            .unique = false,
            .sparse = false,
            .compare = NULL
        };

        int rc = wtree3_tree_add_index(tree, &config, &error);
        assert_int_equal(rc, WTREE3_OK);
    }

    /* Insert some test data */
    user_t user = {1, "Test", "test@example.com"};
    int rc = wtree3_insert_one(tree, &user.id, sizeof(user.id), &user, sizeof(user), &error);
    assert_int_equal(rc, WTREE3_OK);

    wtree3_tree_close(tree);

    /* Reopen - all indexes should be automatically restored */
    tree = wtree3_tree_open(test_db, "multi_idx", 0, 0, &error);
    assert_non_null(tree);

    /* Verify all indexes work */
    for (int i = 0; i < 3; i++) {
        const char *email = "test@example.com";
        wtree3_iterator_t *iter = wtree3_index_seek(tree, indexes_to_create[i], email,
                                                      strlen(email) + 1, &error);
        assert_non_null(iter);

        const void *value;
        size_t value_len;
        bool found = wtree3_iterator_value(iter, &value, &value_len);
        assert_true(found);

        wtree3_iterator_close(iter);
    }

    wtree3_tree_close(tree);
}

static void test_get_extractor_id(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "test_extractor", 0, 0, &error);
    assert_non_null(tree);

    /* Add index with unique flag */
    wtree3_index_config_t config = {
        .name = "unique_idx",
        .user_data = NULL,
        .user_data_len = 0,
        .unique = true,
        .sparse = false,
        .compare = NULL
    };

    int rc = wtree3_tree_add_index(tree, &config, &error);
    assert_int_equal(rc, WTREE3_OK);

    /* Get extractor ID */
    uint64_t extractor_id = 0;
    rc = wtree3_index_get_extractor_id(tree, "unique_idx", &extractor_id, &error);
    assert_int_equal(rc, WTREE3_OK);

    /* Verify it's the expected value: version (1.0) << 32 | flags (0x01 for unique) */
    uint64_t expected_id = ((uint64_t)TEST_VERSION << 32) | 0x01;
    assert_int_equal(extractor_id, expected_id);

    wtree3_tree_close(tree);
}

static void test_persistence_with_user_data(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "userdata_test", 0, 0, &error);
    assert_non_null(tree);

    /* Create index with complex user_data (simulating BSON field spec) */
    typedef struct {
        uint32_t field_type;
        char field_name[32];
    } field_spec_t;

    field_spec_t spec = {.field_type = 5, .field_name = "email"};

    wtree3_index_config_t config = {
        .name = "complex_idx",
        .user_data = &spec,
        .user_data_len = sizeof(spec),
        .unique = false,
        .sparse = true,
        .compare = NULL
    };

    int rc = wtree3_tree_add_index(tree, &config, &error);
    assert_int_equal(rc, WTREE3_OK);

    wtree3_tree_close(tree);

    /* Reopen and verify user_data is preserved */
    tree = wtree3_tree_open(test_db, "userdata_test", 0, 0, &error);
    assert_non_null(tree);

    /* Find the index */
    wtree3_index_t *idx = find_index(tree, "complex_idx");
    assert_non_null(idx);

    /* Verify user_data */
    assert_int_equal(idx->user_data_len, sizeof(spec));
    assert_non_null(idx->user_data);

    field_spec_t *loaded_spec = (field_spec_t *)idx->user_data;
    assert_int_equal(loaded_spec->field_type, 5);
    assert_string_equal(loaded_spec->field_name, "email");

    /* Verify flags */
    assert_false(idx->unique);
    assert_true(idx->sparse);

    wtree3_tree_close(tree);
}

static void test_drop_index_removes_metadata(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "drop_test", 0, 0, &error);
    assert_non_null(tree);

    /* Add index and insert data */
    wtree3_index_config_t config = {
        .name = "temp_idx",
        .user_data = NULL,
        .user_data_len = 0,
        .unique = false,
        .sparse = false,
        .compare = NULL
    };

    int rc = wtree3_tree_add_index(tree, &config, &error);
    assert_int_equal(rc, WTREE3_OK);

    user_t user = {1, "Test", "test@example.com"};
    rc = wtree3_insert_one(tree, &user.id, sizeof(user.id), &user, sizeof(user), &error);
    assert_int_equal(rc, WTREE3_OK);

    /* Verify index works */
    const char *email = "test@example.com";
    wtree3_iterator_t *iter = wtree3_index_seek(tree, "temp_idx", email,
                                                  strlen(email) + 1, &error);
    assert_non_null(iter);
    wtree3_iterator_close(iter);

    /* Drop index */
    rc = wtree3_tree_drop_index(tree, "temp_idx", &error);
    assert_int_equal(rc, WTREE3_OK);

    wtree3_tree_close(tree);

    /* Reopen and verify index is gone (seek should fail) */
    tree = wtree3_tree_open(test_db, "drop_test", 0, 0, &error);
    assert_non_null(tree);

    /* Try to use the dropped index - should fail */
    iter = wtree3_index_seek(tree, "temp_idx", email, strlen(email) + 1, &error);
    assert_null(iter);

    wtree3_tree_close(tree);
}

static void test_persistence_different_flag_combinations(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "flags_test", 0, 0, &error);
    assert_non_null(tree);

    /* Create indexes with different flag combinations */
    struct {
        const char *name;
        bool unique;
        bool sparse;
        uint32_t expected_flags;
    } test_cases[] = {
        {"normal_idx", false, false, 0x00},
        {"unique_idx", true, false, 0x01},
        {"sparse_idx", false, true, 0x02},
        {"unique_sparse_idx", true, true, 0x03}
    };

    for (int i = 0; i < 4; i++) {
        wtree3_index_config_t config = {
            .name = test_cases[i].name,
            .user_data = NULL,
            .user_data_len = 0,
            .unique = test_cases[i].unique,
            .sparse = test_cases[i].sparse,
            .compare = NULL
        };

        int rc = wtree3_tree_add_index(tree, &config, &error);
        assert_int_equal(rc, WTREE3_OK);
    }

    wtree3_tree_close(tree);

    /* Reopen and verify all flags are preserved */
    tree = wtree3_tree_open(test_db, "flags_test", 0, 0, &error);
    assert_non_null(tree);

    for (int i = 0; i < 4; i++) {
        wtree3_index_t *idx = find_index(tree, test_cases[i].name);
        assert_non_null(idx);
        assert_int_equal(idx->unique, test_cases[i].unique);
        assert_int_equal(idx->sparse, test_cases[i].sparse);

        /* Verify extractor_id matches flags */
        uint64_t expected_extractor_id = ((uint64_t)TEST_VERSION << 32) | test_cases[i].expected_flags;
        assert_int_equal(idx->extractor_id, expected_extractor_id);
    }

    wtree3_tree_close(tree);
}

/* ============================================================
 * Main Test Suite
 * ============================================================ */

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_basic_persistence_workflow, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_multiple_indexes_persist, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_get_extractor_id, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_persistence_with_user_data, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_drop_index_removes_metadata, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_persistence_different_flag_combinations, setup_db, teardown_db),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
