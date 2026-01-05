/*
 * test_wtree3_persistence.c - Index persistence and metadata tests
 *
 * Tests for:
 * - Index persistence (save/load metadata)
 * - get_metadata_dbi and build_metadata_key (tested indirectly)
 * - wtree3_tree_set_compare (success path)
 * - wtree3_tree_add_index with persistence
 * - wtree3_index_save_metadata (all paths)
 * - wtree3_index_load_metadata (all paths)
 * - wtree3_tree_list_persisted_indexes (all paths)
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
 * Serialization Helpers for Testing
 * ============================================================ */

/* Simple serialization: store string with length prefix */
static int test_serialize(void *user_data, void **out, size_t *out_len) {
    if (!user_data || !out || !out_len) {
        return -1;
    }

    const char *str = (const char *)user_data;
    size_t str_len = strlen(str) + 1; /* Include null terminator */

    /* Allocate: 4 bytes for length + string data */
    uint8_t *buffer = malloc(4 + str_len);
    if (!buffer) {
        return -1;
    }

    /* Write length */
    uint32_t len = (uint32_t)str_len;
    memcpy(buffer, &len, 4);

    /* Write string */
    memcpy(buffer + 4, str, str_len);

    *out = buffer;
    *out_len = 4 + str_len;
    return 0;
}

static void* test_deserialize(const void *data, size_t len) {
    if (!data || len < 4) {
        return NULL;
    }

    const uint8_t *buffer = (const uint8_t *)data;

    /* Read length */
    uint32_t str_len;
    memcpy(&str_len, buffer, 4);

    /* Validate */
    if (len < 4 + str_len) {
        return NULL;
    }

    /* Allocate and copy string */
    char *str = malloc(str_len);
    if (!str) {
        return NULL;
    }

    memcpy(str, buffer + 4, str_len);
    return str;
}

static void test_cleanup(void *user_data) {
    free(user_data);
}

/* ============================================================
 * Key Extractor for Tests
 * ============================================================ */

/* Extract field from "field1:value1|field2:value2" format */
static bool test_key_extractor(const void *value, size_t value_len,
                                 void *user_data, void **key, size_t *key_len) {
    (void)value_len;

    if (!value || !user_data || !key || !key_len) {
        return false;
    }

    const char *field_name = (const char *)user_data;
    const char *data = (const char *)value;

    /* Find field_name: */
    char search[64];
    snprintf(search, sizeof(search), "%s:", field_name);

    const char *start = strstr(data, search);
    if (!start) {
        return false; /* Sparse - field not present */
    }

    start += strlen(search);

    /* Find end (| or null terminator) */
    const char *end = strchr(start, '|');
    if (!end) {
        end = start + strlen(start);
    }

    size_t len = end - start;
    if (len == 0) {
        return false;
    }

    char *extracted = malloc(len + 1);
    if (!extracted) {
        return false;
    }

    memcpy(extracted, start, len);
    extracted[len] = '\0';

    *key = extracted;
    *key_len = len;
    return true;
}

/* Comparator for testing */
static int test_comparator(const MDB_val *a, const MDB_val *b) {
    size_t min_len = a->mv_size < b->mv_size ? a->mv_size : b->mv_size;
    int cmp = memcmp(a->mv_data, b->mv_data, min_len);
    if (cmp != 0) return cmp;

    if (a->mv_size < b->mv_size) return -1;
    if (a->mv_size > b->mv_size) return 1;
    return 0;
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
    snprintf(test_db_path, sizeof(test_db_path), "%s\\test_wtree3_persist_%d",
             temp, getpid());
#else
    snprintf(test_db_path, sizeof(test_db_path), "/tmp/test_wtree3_persist_%d", getpid());
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
 * Tests: wtree3_tree_set_compare
 * ============================================================ */

static void test_tree_set_compare_success(void **state) {
    (void)state;
    gerror_t error = {0};

    /* Create tree */
    wtree3_tree_t *tree = wtree3_tree_open(test_db, "cmp_tree", 0, 0, &error);
    assert_non_null(tree);

    /* Set custom comparator */
    int rc = wtree3_tree_set_compare(tree, test_comparator, &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Insert some data to verify comparator is active */
    rc = wtree3_insert_one(tree, "key1", 4, "value1", 6, &error);
    assert_int_equal(WTREE3_OK, rc);

    rc = wtree3_insert_one(tree, "key2", 4, "value2", 6, &error);
    assert_int_equal(WTREE3_OK, rc);

    wtree3_tree_close(tree);
}

/* ============================================================
 * Tests: Index with Persistence
 * ============================================================ */

static void test_add_index_with_persistence(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "persist_tree1", 0, 0, &error);
    assert_non_null(tree);

    /* Prepare persistence config */
    wtree3_user_data_persistence_t persistence = {
        .serialize = test_serialize,
        .deserialize = test_deserialize
    };

    /* User data that will be persisted */
    char *user_data = strdup("email_field");
    assert_non_null(user_data);

    /* Add index WITH persistence */
    wtree3_index_config_t config = {
        .name = "email_idx",
        .key_fn = test_key_extractor,
        .user_data = user_data,
        .user_data_cleanup = test_cleanup,
        .unique = true,
        .sparse = false,
        .compare = NULL,
        .persistence = &persistence
    };

    int rc = wtree3_tree_add_index(tree, &config, &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Verify index was added */
    assert_true(wtree3_tree_has_index(tree, "email_idx"));

    wtree3_tree_close(tree);
}

static void test_add_index_persistence_metadata_save(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "persist_tree2", 0, 0, &error);
    assert_non_null(tree);

    wtree3_user_data_persistence_t persistence = {
        .serialize = test_serialize,
        .deserialize = test_deserialize
    };

    char *user_data = strdup("test_field");
    assert_non_null(user_data);

    wtree3_index_config_t config = {
        .name = "test_idx",
        .key_fn = test_key_extractor,
        .user_data = user_data,
        .user_data_cleanup = test_cleanup,
        .unique = false,
        .sparse = true,
        .compare = NULL,
        .persistence = &persistence
    };

    int rc = wtree3_tree_add_index(tree, &config, &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Close and reopen tree */
    wtree3_tree_close(tree);

    /* Now load the persisted index */
    tree = wtree3_tree_open(test_db, "persist_tree2", 0, 0, &error);
    assert_non_null(tree);

    /* List persisted indexes */
    size_t count = 0;
    char **indexes = wtree3_tree_list_persisted_indexes(tree, &count, &error);
    assert_non_null(indexes);
    assert_int_equal(1, count);
    assert_string_equal("test_idx", indexes[0]);

    wtree3_index_list_free(indexes, count);
    wtree3_tree_close(tree);
}

/* ============================================================
 * Tests: wtree3_index_save_metadata
 * ============================================================ */

static void test_index_save_metadata_success(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "save_meta_tree", 0, 0, &error);
    assert_non_null(tree);

    wtree3_user_data_persistence_t persistence = {
        .serialize = test_serialize,
        .deserialize = test_deserialize
    };

    char *user_data = strdup("my_field");
    assert_non_null(user_data);

    wtree3_index_config_t config = {
        .name = "meta_idx",
        .key_fn = test_key_extractor,
        .user_data = user_data,
        .user_data_cleanup = test_cleanup,
        .unique = true,
        .sparse = false,
        .compare = NULL,
        .persistence = &persistence
    };

    int rc = wtree3_tree_add_index(tree, &config, &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Metadata should have been saved automatically during add_index */
    /* Verify by trying to save again (should succeed - it's an update) */
    rc = wtree3_index_save_metadata(tree, "meta_idx", &error);
    assert_int_equal(WTREE3_OK, rc);

    wtree3_tree_close(tree);
}

static void test_index_save_metadata_no_persistence(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "no_persist_tree", 0, 0, &error);
    assert_non_null(tree);

    /* Add index WITHOUT persistence */
    wtree3_index_config_t config = {
        .name = "no_persist_idx",
        .key_fn = test_key_extractor,
        .user_data = (void *)"field",
        .user_data_cleanup = NULL,
        .unique = false,
        .sparse = false,
        .compare = NULL,
        .persistence = NULL  /* No persistence */
    };

    int rc = wtree3_tree_add_index(tree, &config, &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Try to save metadata - should fail */
    rc = wtree3_index_save_metadata(tree, "no_persist_idx", &error);
    assert_int_equal(WTREE3_EINVAL, rc);

    wtree3_tree_close(tree);
}

static void test_index_save_metadata_not_found(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "meta_notfound_tree", 0, 0, &error);
    assert_non_null(tree);

    /* Try to save metadata for non-existent index */
    int rc = wtree3_index_save_metadata(tree, "nonexistent", &error);
    assert_int_equal(WTREE3_NOT_FOUND, rc);

    wtree3_tree_close(tree);
}

/* ============================================================
 * Tests: wtree3_index_load_metadata
 * ============================================================ */

static void test_index_load_metadata_success(void **state) {
    (void)state;
    gerror_t error = {0};

    /* First, create and save an index with metadata */
    wtree3_tree_t *tree = wtree3_tree_open(test_db, "load_meta_tree", 0, 0, &error);
    assert_non_null(tree);

    wtree3_user_data_persistence_t persistence = {
        .serialize = test_serialize,
        .deserialize = test_deserialize
    };

    char *user_data = strdup("loaded_field");
    assert_non_null(user_data);

    wtree3_index_config_t config = {
        .name = "load_idx",
        .key_fn = test_key_extractor,
        .user_data = user_data,
        .user_data_cleanup = test_cleanup,
        .unique = true,
        .sparse = true,
        .compare = NULL,
        .persistence = &persistence
    };

    int rc = wtree3_tree_add_index(tree, &config, &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Close tree (index is dropped from memory but metadata persists) */
    wtree3_tree_close(tree);

    /* Reopen tree */
    tree = wtree3_tree_open(test_db, "load_meta_tree", 0, 0, &error);
    assert_non_null(tree);

    /* Load the index from metadata */
    rc = wtree3_index_load_metadata(tree, "load_idx", test_key_extractor, &persistence, &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Verify index is loaded */
    assert_true(wtree3_tree_has_index(tree, "load_idx"));

    wtree3_tree_close(tree);
}

static void test_index_load_metadata_already_loaded(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "already_loaded_tree", 0, 0, &error);
    assert_non_null(tree);

    wtree3_user_data_persistence_t persistence = {
        .serialize = test_serialize,
        .deserialize = test_deserialize
    };

    char *user_data = strdup("dup_field");
    assert_non_null(user_data);

    wtree3_index_config_t config = {
        .name = "dup_idx",
        .key_fn = test_key_extractor,
        .user_data = user_data,
        .user_data_cleanup = test_cleanup,
        .unique = false,
        .sparse = false,
        .compare = NULL,
        .persistence = &persistence
    };

    int rc = wtree3_tree_add_index(tree, &config, &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Try to load again - should fail with KEY_EXISTS */
    rc = wtree3_index_load_metadata(tree, "dup_idx", test_key_extractor, &persistence, &error);
    assert_int_equal(WTREE3_KEY_EXISTS, rc);

    wtree3_tree_close(tree);
}

static void test_index_load_metadata_not_found(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "load_notfound_tree", 0, 0, &error);
    assert_non_null(tree);

    wtree3_user_data_persistence_t persistence = {
        .serialize = test_serialize,
        .deserialize = test_deserialize
    };

    /* Try to load non-existent metadata */
    int rc = wtree3_index_load_metadata(tree, "nonexistent", test_key_extractor, &persistence, &error);
    assert_int_equal(WTREE3_NOT_FOUND, rc);

    wtree3_tree_close(tree);
}

/* ============================================================
 * Tests: wtree3_tree_list_persisted_indexes
 * ============================================================ */

static void test_list_persisted_indexes_success(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "list_tree", 0, 0, &error);
    assert_non_null(tree);

    wtree3_user_data_persistence_t persistence = {
        .serialize = test_serialize,
        .deserialize = test_deserialize
    };

    /* Add multiple indexes with persistence */
    for (int i = 0; i < 3; i++) {
        char idx_name[32];
        snprintf(idx_name, sizeof(idx_name), "idx_%d", i);

        char *user_data = strdup("field");
        assert_non_null(user_data);

        wtree3_index_config_t config = {
            .name = idx_name,
            .key_fn = test_key_extractor,
            .user_data = user_data,
            .user_data_cleanup = test_cleanup,
            .unique = false,
            .sparse = false,
            .compare = NULL,
            .persistence = &persistence
        };

        int rc = wtree3_tree_add_index(tree, &config, &error);
        assert_int_equal(WTREE3_OK, rc);
    }

    /* List persisted indexes */
    size_t count = 0;
    char **indexes = wtree3_tree_list_persisted_indexes(tree, &count, &error);
    assert_non_null(indexes);
    assert_int_equal(3, count);

    /* Verify names (order may vary) */
    bool found[3] = {false, false, false};
    for (size_t i = 0; i < count; i++) {
        if (strcmp(indexes[i], "idx_0") == 0) found[0] = true;
        if (strcmp(indexes[i], "idx_1") == 0) found[1] = true;
        if (strcmp(indexes[i], "idx_2") == 0) found[2] = true;
    }
    assert_true(found[0] && found[1] && found[2]);

    wtree3_index_list_free(indexes, count);
    wtree3_tree_close(tree);
}

static void test_list_persisted_indexes_empty(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "empty_list_tree", 0, 0, &error);
    assert_non_null(tree);

    /* No indexes added */
    size_t count = 0;
    char **indexes = wtree3_tree_list_persisted_indexes(tree, &count, &error);

    /* Should return empty list, not NULL */
    assert_int_equal(0, count);
    if (indexes) {
        wtree3_index_list_free(indexes, count);
    }

    wtree3_tree_close(tree);
}

static void test_list_persisted_indexes_prefix_filtering(void **state) {
    (void)state;
    gerror_t error = {0};

    /* Create two different trees */
    wtree3_tree_t *tree1 = wtree3_tree_open(test_db, "prefix_tree1", 0, 0, &error);
    assert_non_null(tree1);

    wtree3_tree_t *tree2 = wtree3_tree_open(test_db, "prefix_tree2", 0, 0, &error);
    assert_non_null(tree2);

    wtree3_user_data_persistence_t persistence = {
        .serialize = test_serialize,
        .deserialize = test_deserialize
    };

    /* Add index to tree1 */
    char *user_data1 = strdup("field1");
    wtree3_index_config_t config1 = {
        .name = "idx_tree1",
        .key_fn = test_key_extractor,
        .user_data = user_data1,
        .user_data_cleanup = test_cleanup,
        .unique = false,
        .sparse = false,
        .compare = NULL,
        .persistence = &persistence
    };
    int rc = wtree3_tree_add_index(tree1, &config1, &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Add index to tree2 */
    char *user_data2 = strdup("field2");
    wtree3_index_config_t config2 = {
        .name = "idx_tree2",
        .key_fn = test_key_extractor,
        .user_data = user_data2,
        .user_data_cleanup = test_cleanup,
        .unique = false,
        .sparse = false,
        .compare = NULL,
        .persistence = &persistence
    };
    rc = wtree3_tree_add_index(tree2, &config2, &error);
    assert_int_equal(WTREE3_OK, rc);

    /* List for tree1 - should only see tree1's index */
    size_t count1 = 0;
    char **indexes1 = wtree3_tree_list_persisted_indexes(tree1, &count1, &error);
    assert_non_null(indexes1);
    assert_int_equal(1, count1);
    assert_string_equal("idx_tree1", indexes1[0]);

    /* List for tree2 - should only see tree2's index */
    size_t count2 = 0;
    char **indexes2 = wtree3_tree_list_persisted_indexes(tree2, &count2, &error);
    assert_non_null(indexes2);
    assert_int_equal(1, count2);
    assert_string_equal("idx_tree2", indexes2[0]);

    wtree3_index_list_free(indexes1, count1);
    wtree3_index_list_free(indexes2, count2);
    wtree3_tree_close(tree1);
    wtree3_tree_close(tree2);
}

/* ============================================================
 * Tests: Drop index with persistence
 * ============================================================ */

static void test_drop_index_with_persistence(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "drop_persist_tree", 0, 0, &error);
    assert_non_null(tree);

    wtree3_user_data_persistence_t persistence = {
        .serialize = test_serialize,
        .deserialize = test_deserialize
    };

    char *user_data = strdup("drop_field");
    assert_non_null(user_data);

    wtree3_index_config_t config = {
        .name = "drop_idx",
        .key_fn = test_key_extractor,
        .user_data = user_data,
        .user_data_cleanup = test_cleanup,
        .unique = false,
        .sparse = false,
        .compare = NULL,
        .persistence = &persistence
    };

    int rc = wtree3_tree_add_index(tree, &config, &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Verify metadata exists */
    size_t count = 0;
    char **indexes = wtree3_tree_list_persisted_indexes(tree, &count, &error);
    assert_int_equal(1, count);
    wtree3_index_list_free(indexes, count);

    /* Drop index */
    rc = wtree3_tree_drop_index(tree, "drop_idx", &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Verify metadata is also deleted */
    count = 0;
    indexes = wtree3_tree_list_persisted_indexes(tree, &count, &error);
    assert_int_equal(0, count);
    if (indexes) {
        wtree3_index_list_free(indexes, count);
    }

    wtree3_tree_close(tree);
}

/* ============================================================
 * Tests: Integration - Full persistence workflow
 * ============================================================ */

static void test_full_persistence_workflow(void **state) {
    (void)state;
    gerror_t error = {0};

    /* Phase 1: Create tree and add index with data */
    wtree3_tree_t *tree = wtree3_tree_open(test_db, "workflow_tree", 0, 0, &error);
    assert_non_null(tree);

    wtree3_user_data_persistence_t persistence = {
        .serialize = test_serialize,
        .deserialize = test_deserialize
    };

    char *user_data = strdup("email");
    assert_non_null(user_data);

    wtree3_index_config_t config = {
        .name = "email_idx",
        .key_fn = test_key_extractor,
        .user_data = user_data,
        .user_data_cleanup = test_cleanup,
        .unique = true,
        .sparse = false,
        .compare = NULL,
        .persistence = &persistence
    };

    int rc = wtree3_tree_add_index(tree, &config, &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Insert some data */
    const char *key = "user1";
    const char *val = "name:Alice|email:alice@test.com";
    rc = wtree3_insert_one(tree, key, strlen(key), val, strlen(val) + 1, &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Verify index works */
    wtree3_iterator_t *iter = wtree3_index_seek(tree, "email_idx", "alice@test.com", 14, &error);
    assert_non_null(iter);
    assert_true(wtree3_iterator_valid(iter));
    wtree3_iterator_close(iter);

    wtree3_tree_close(tree);

    /* Phase 2: Reopen and reload index from metadata */
    tree = wtree3_tree_open(test_db, "workflow_tree", 0, 0, &error);
    assert_non_null(tree);

    /* List persisted indexes */
    size_t count = 0;
    char **indexes = wtree3_tree_list_persisted_indexes(tree, &count, &error);
    assert_non_null(indexes);
    assert_int_equal(1, count);
    assert_string_equal("email_idx", indexes[0]);

    /* Load the index */
    rc = wtree3_index_load_metadata(tree, "email_idx", test_key_extractor, &persistence, &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Verify index still works after reload */
    iter = wtree3_index_seek(tree, "email_idx", "alice@test.com", 14, &error);
    assert_non_null(iter);
    assert_true(wtree3_iterator_valid(iter));
    wtree3_iterator_close(iter);

    wtree3_index_list_free(indexes, count);
    wtree3_tree_close(tree);
}

/* ============================================================
 * Main
 * ============================================================ */

int main(void) {
    const struct CMUnitTest tests[] = {
        /* wtree3_tree_set_compare */
        cmocka_unit_test(test_tree_set_compare_success),

        /* Add index with persistence */
        cmocka_unit_test(test_add_index_with_persistence),
        cmocka_unit_test(test_add_index_persistence_metadata_save),

        /* Save metadata */
        cmocka_unit_test(test_index_save_metadata_success),
        cmocka_unit_test(test_index_save_metadata_no_persistence),
        cmocka_unit_test(test_index_save_metadata_not_found),

        /* Load metadata */
        cmocka_unit_test(test_index_load_metadata_success),
        cmocka_unit_test(test_index_load_metadata_already_loaded),
        cmocka_unit_test(test_index_load_metadata_not_found),

        /* List persisted indexes */
        cmocka_unit_test(test_list_persisted_indexes_success),
        cmocka_unit_test(test_list_persisted_indexes_empty),
        cmocka_unit_test(test_list_persisted_indexes_prefix_filtering),

        /* Drop with persistence */
        cmocka_unit_test(test_drop_index_with_persistence),

        /* Integration */
        cmocka_unit_test(test_full_persistence_workflow),
    };

    return cmocka_run_group_tests(tests, setup_db, teardown_db);
}
