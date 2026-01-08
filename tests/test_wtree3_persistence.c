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
    test_db = wtree3_db_open(test_db_path, 64 * 1024 * 1024, 64, WTREE3_VERSION(1, 0), 0, &error);
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
        .unique = true,
        .sparse = false,
        .compare = NULL,
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
        .unique = false,
        .sparse = true,
        .compare = NULL,
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
        .unique = true,
        .sparse = false,
        .compare = NULL,
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
        .unique = false,
        .sparse = false,
        .compare = NULL,
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
        .unique = true,
        .sparse = true,
        .compare = NULL,
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
        .unique = false,
        .sparse = false,
        .compare = NULL,
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
            .unique = false,
            .sparse = false,
            .compare = NULL,
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
        .unique = false,
        .sparse = false,
        .compare = NULL,
    };
    int rc = wtree3_tree_add_index(tree1, &config1, &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Add index to tree2 */
    char *user_data2 = strdup("field2");
    wtree3_index_config_t config2 = {
        .name = "idx_tree2",
        .key_fn = test_key_extractor,
        .user_data = user_data2,
        .unique = false,
        .sparse = false,
        .compare = NULL,
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
        .unique = false,
        .sparse = false,
        .compare = NULL,
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
 * Tests: Error cases and edge cases
 * ============================================================ */

/* Serialization function that fails */
static int test_serialize_fail(void *user_data, void **out, size_t *out_len) {
    (void)user_data;
    (void)out;
    (void)out_len;
    return -1; /* Simulate failure */
}

/* Deserialize function that returns NULL (simulates corruption) */
static void* test_deserialize_fail(const void *data, size_t len) {
    (void)data;
    (void)len;
    return NULL; /* Simulate deserialization failure */
}

static void test_index_save_metadata_serialize_failure(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "serialize_fail_tree", 0, 0, &error);
    assert_non_null(tree);

    /* Persistence with failing serialize function */
    wtree3_user_data_persistence_t persistence = {
        .serialize = test_serialize_fail,  /* This will fail */
        .deserialize = test_deserialize
    };

    char *user_data = strdup("test_data");
    assert_non_null(user_data);

    wtree3_index_config_t config = {
        .name = "fail_idx",
        .key_fn = test_key_extractor,
        .user_data = user_data,
        .unique = false,
        .sparse = false,
        .compare = NULL,
    };

    /* This should fail because serialize will return error */
    int rc = wtree3_tree_add_index(tree, &config, &error);
    assert_int_not_equal(WTREE3_OK, rc);

    /* Index should not have been added */
    assert_false(wtree3_tree_has_index(tree, "fail_idx"));

    wtree3_tree_close(tree);
}

static void test_index_load_metadata_deserialize_failure(void **state) {
    (void)state;
    gerror_t error = {0};

    /* First create and save valid metadata */
    wtree3_tree_t *tree = wtree3_tree_open(test_db, "deserialize_fail_tree", 0, 0, &error);
    assert_non_null(tree);

    wtree3_user_data_persistence_t good_persistence = {
        .serialize = test_serialize,
        .deserialize = test_deserialize
    };

    char *user_data = strdup("test_field");
    assert_non_null(user_data);

    wtree3_index_config_t config = {
        .name = "deser_idx",
        .key_fn = test_key_extractor,
        .user_data = user_data,
        .unique = false,
        .sparse = false,
        .compare = NULL,
    };

    int rc = wtree3_tree_add_index(tree, &config, &error);
    assert_int_equal(WTREE3_OK, rc);

    wtree3_tree_close(tree);

    /* Now try to load with failing deserializer */
    tree = wtree3_tree_open(test_db, "deserialize_fail_tree", 0, 0, &error);
    assert_non_null(tree);

    wtree3_user_data_persistence_t bad_persistence = {
        .serialize = test_serialize,
        .deserialize = test_deserialize_fail  /* This will fail */
    };

    rc = wtree3_index_load_metadata(tree, "deser_idx", test_key_extractor, &bad_persistence, &error);
    assert_int_equal(WTREE3_ERROR, rc);

    /* Index should not be loaded */
    assert_false(wtree3_tree_has_index(tree, "deser_idx"));

    wtree3_tree_close(tree);
}

static void test_index_load_metadata_invalid_params(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "invalid_params_tree", 0, 0, &error);
    assert_non_null(tree);

    wtree3_user_data_persistence_t persistence = {
        .serialize = test_serialize,
        .deserialize = test_deserialize
    };

    /* NULL tree */
    int rc = wtree3_index_load_metadata(NULL, "idx", test_key_extractor, &persistence, &error);
    assert_int_equal(WTREE3_EINVAL, rc);

    /* NULL index_name */
    rc = wtree3_index_load_metadata(tree, NULL, test_key_extractor, &persistence, &error);
    assert_int_equal(WTREE3_EINVAL, rc);

    /* NULL key_fn */
    rc = wtree3_index_load_metadata(tree, "idx", NULL, &persistence, &error);
    assert_int_equal(WTREE3_EINVAL, rc);

    /* NULL persistence */
    rc = wtree3_index_load_metadata(tree, "idx", test_key_extractor, NULL, &error);
    assert_int_equal(WTREE3_EINVAL, rc);

    /* NULL deserialize in persistence */
    wtree3_user_data_persistence_t bad_persistence = {
        .serialize = test_serialize,
        .deserialize = NULL  /* NULL deserialize */
    };
    rc = wtree3_index_load_metadata(tree, "idx", test_key_extractor, &bad_persistence, &error);
    assert_int_equal(WTREE3_EINVAL, rc);

    wtree3_tree_close(tree);
}

static void test_list_persisted_indexes_null_count(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "null_count_tree", 0, 0, &error);
    assert_non_null(tree);

    /* NULL count parameter */
    char **indexes = wtree3_tree_list_persisted_indexes(tree, NULL, &error);
    assert_null(indexes);

    wtree3_tree_close(tree);
}

static void test_index_with_custom_comparator(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "cmp_idx_tree_v2", 0, 0, &error);
    assert_non_null(tree);

    wtree3_user_data_persistence_t persistence = {
        .serialize = test_serialize,
        .deserialize = test_deserialize
    };

    char *user_data = strdup("name");
    assert_non_null(user_data);

    /* Add index with custom comparator */
    wtree3_index_config_t config = {
        .name = "name_idx",
        .key_fn = test_key_extractor,
        .user_data = user_data,
        .unique = false,
        .sparse = false,
        .compare = test_comparator,  /* Custom comparator */
    };

    int rc = wtree3_tree_add_index(tree, &config, &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Insert some data */
    const char *k1 = "user1";
    const char *v1 = "name:Bob|email:bob@test.com";
    rc = wtree3_insert_one(tree, k1, strlen(k1), v1, strlen(v1) + 1, &error);
    assert_int_equal(WTREE3_OK, rc);

    const char *k2 = "user2";
    const char *v2 = "name:Alice|email:alice@test.com";
    rc = wtree3_insert_one(tree, k2, strlen(k2), v2, strlen(v2) + 1, &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Verify index works */
    wtree3_iterator_t *iter = wtree3_index_seek(tree, "name_idx", "Alice", 5, &error);
    assert_non_null(iter);
    assert_true(wtree3_iterator_valid(iter));
    wtree3_iterator_close(iter);

    wtree3_tree_close(tree);
}

static void test_populate_index_with_persistence(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "populate_persist_tree_v2", 0, 0, &error);
    assert_non_null(tree);

    /* Insert data first */
    const char *k1 = "doc1";
    const char *v1 = "title:First Document";
    int rc = wtree3_insert_one(tree, k1, strlen(k1), v1, strlen(v1) + 1, &error);
    assert_int_equal(WTREE3_OK, rc);

    const char *k2 = "doc2";
    const char *v2 = "title:Second Document";
    rc = wtree3_insert_one(tree, k2, strlen(k2), v2, strlen(v2) + 1, &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Add index with persistence */
    wtree3_user_data_persistence_t persistence = {
        .serialize = test_serialize,
        .deserialize = test_deserialize
    };

    char *user_data = strdup("title");
    assert_non_null(user_data);

    wtree3_index_config_t config = {
        .name = "title_idx",
        .key_fn = test_key_extractor,
        .user_data = user_data,
        .unique = false,
        .sparse = false,
        .compare = NULL,
    };

    rc = wtree3_tree_add_index(tree, &config, &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Populate the index */
    rc = wtree3_tree_populate_index(tree, "title_idx", &error);
    assert_int_equal(WTREE3_OK, rc);

    /* Verify both entries are in index */
    wtree3_iterator_t *iter = wtree3_index_seek(tree, "title_idx", "First Document", 14, &error);
    assert_non_null(iter);
    assert_true(wtree3_iterator_valid(iter));
    wtree3_iterator_close(iter);

    iter = wtree3_index_seek(tree, "title_idx", "Second Document", 15, &error);
    assert_non_null(iter);
    assert_true(wtree3_iterator_valid(iter));
    wtree3_iterator_close(iter);

    wtree3_tree_close(tree);
}

static void test_multiple_indexes_with_persistence(void **state) {
    (void)state;
    gerror_t error = {0};

    wtree3_tree_t *tree = wtree3_tree_open(test_db, "multi_persist_tree_v2", 0, 0, &error);
    assert_non_null(tree);

    wtree3_user_data_persistence_t persistence = {
        .serialize = test_serialize,
        .deserialize = test_deserialize
    };

    /* Add multiple indexes with persistence */
    for (int i = 0; i < 5; i++) {
        char idx_name[32];
        snprintf(idx_name, sizeof(idx_name), "multi_idx_%d", i);

        char *user_data = malloc(32);
        snprintf(user_data, 32, "field_%d", i);

        wtree3_index_config_t config = {
            .name = idx_name,
            .key_fn = test_key_extractor,
            .user_data = user_data,
            .unique = (i % 2 == 0),  /* Alternate unique/non-unique */
            .sparse = (i % 3 == 0),  /* Some sparse */
            .compare = NULL,
        };

        int rc = wtree3_tree_add_index(tree, &config, &error);
        assert_int_equal(WTREE3_OK, rc);
    }

    /* Verify all are persisted */
    size_t count = 0;
    char **indexes = wtree3_tree_list_persisted_indexes(tree, &count, &error);
    assert_non_null(indexes);
    assert_int_equal(5, count);

    wtree3_index_list_free(indexes, count);
    wtree3_tree_close(tree);
}

/* ============================================================
 * Tests: Integration - Full persistence workflow
 * ============================================================ */

static void test_full_persistence_workflow(void **state) {
    (void)state;
    gerror_t error = {0};

    /* Phase 1: Create tree and add index with data */
    wtree3_tree_t *tree = wtree3_tree_open(test_db, "workflow_tree_v2", 0, 0, &error);
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
        .unique = true,
        .sparse = false,
        .compare = NULL,
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
    tree = wtree3_tree_open(test_db, "workflow_tree_v2", 0, 0, &error);
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

/*
 * Test: Auto-loading indexes with loader callback
 */
static void test_auto_load_indexes(void **state) {
    (void)state;
    gerror_t error = {0};

    // Create tree and add indexes with persistence
    wtree3_tree_t *tree = wtree3_tree_open(test_db, "auto_load_tree", 0, 0, &error);
    assert_non_null(tree);

    // Define persistence callbacks
    wtree3_user_data_persistence_t persistence = {
        .serialize = test_serialize,
        .deserialize = test_deserialize
    };

    // Add first index with persistence
    char *user_data1 = strdup("test_data_1");
    assert_non_null(user_data1);

    wtree3_index_config_t idx_config1 = {
        .name = "auto_idx1",
        .key_fn = test_key_extractor,
        .user_data = user_data1,
        .unique = true,
        .sparse = false,
        .compare = NULL,
    };

    int rc = wtree3_tree_add_index(tree, &idx_config1, &error);
    assert_int_equal(rc, 0);

    // Save metadata
    rc = wtree3_index_save_metadata(tree, "auto_idx1", &error);
    assert_int_equal(rc, 0);

    // Add second index with different flags
    char *user_data2 = strdup("test_data_2");
    assert_non_null(user_data2);

    wtree3_index_config_t idx_config2 = {
        .name = "auto_idx2",
        .key_fn = test_key_extractor,
        .user_data = user_data2,
        .unique = false,
        .sparse = true,
        .compare = NULL,
    };

    rc = wtree3_tree_add_index(tree, &idx_config2, &error);
    assert_int_equal(rc, 0);

    rc = wtree3_index_save_metadata(tree, "auto_idx2", &error);
    assert_int_equal(rc, 0);

    // Close the tree
    wtree3_tree_close(tree);

    // Define loader callback that will be called for each index
    typedef struct {
        int idx1_called;
        int idx2_called;
        bool idx1_unique;
        bool idx1_sparse;
        bool idx2_unique;
        bool idx2_sparse;
    } loader_context_t;

    loader_context_t context = {0};

    // Loader callback function
    int loader_fn(const char *index_name, bool unique, bool sparse,
                  wtree3_index_key_fn *out_key_fn,
                  wtree3_user_data_persistence_t **out_persistence,
                  void *ctx) {
        loader_context_t *lctx = (loader_context_t*)ctx;

        if (strcmp(index_name, "auto_idx1") == 0) {
            lctx->idx1_called = 1;
            lctx->idx1_unique = unique;
            lctx->idx1_sparse = sparse;
            *out_key_fn = test_key_extractor;
            static wtree3_user_data_persistence_t pers = {test_serialize, test_deserialize};
            *out_persistence = &pers;
            return 0;  // Load this index
        }
        else if (strcmp(index_name, "auto_idx2") == 0) {
            lctx->idx2_called = 1;
            lctx->idx2_unique = unique;
            lctx->idx2_sparse = sparse;
            *out_key_fn = test_key_extractor;
            static wtree3_user_data_persistence_t pers2 = {test_serialize, test_deserialize};
            *out_persistence = &pers2;
            return 0;  // Load this index
        }

        return -1;  // Skip unknown indexes
    }

    // Re-open tree with auto-loading using setter
    tree = wtree3_tree_open(test_db, "auto_load_tree", 0, 0, &error);
    assert_non_null(tree);

    rc = wtree3_tree_set_index_loader(tree, loader_fn, &context, &error);
    assert_int_equal(rc, 0);

    // Verify callback was called for both indexes
    assert_int_equal(context.idx1_called, 1);
    assert_int_equal(context.idx2_called, 1);

    // Verify flags were passed correctly
    assert_true(context.idx1_unique);   // Should be true
    assert_false(context.idx1_sparse);  // Should be false
    assert_false(context.idx2_unique);  // Should be false
    assert_true(context.idx2_sparse);   // Should be true

    // Verify indexes were actually loaded by trying to use them
    // We can verify this by checking that index queries work

    // Insert some test data
    typedef struct {
        int id;
        char name[32];
    } test_record_t;

    test_record_t rec = {1, "test"};
    rc = wtree3_insert_one(tree, &rec.id, sizeof(rec.id), &rec, sizeof(rec), &error);
    assert_int_equal(rc, 0);

    // Try to query via first index - if it works, index is loaded
    const char *key1 = "test";
    wtree3_iterator_t *iter1 = wtree3_index_seek(tree, "auto_idx1", key1, strlen(key1), &error);
    assert_non_null(iter1);  // Index was loaded successfully
    wtree3_iterator_close(iter1);

    // Try to query via second index
    wtree3_iterator_t *iter2 = wtree3_index_seek(tree, "auto_idx2", key1, strlen(key1), &error);
    assert_non_null(iter2);  // Index was loaded successfully
    wtree3_iterator_close(iter2);

    printf("Auto-load test: Successfully loaded 2 indexes automatically\n");

    wtree3_tree_close(tree);
}

/*
 * Test: Selective loading - skip some indexes
 */
static void test_auto_load_selective(void **state) {
    (void)state;
    gerror_t error = {0};
    int rc;

    // Create tree and add 2 indexes
    wtree3_tree_t *tree = wtree3_tree_open(test_db, "selective_load_tree", 0, 0, &error);
    assert_non_null(tree);

    wtree3_user_data_persistence_t persistence = {
        .serialize = test_serialize,
        .deserialize = test_deserialize
    };

    // Add first index
    char *user_data1 = strdup("data_1");
    wtree3_index_config_t idx_config1 = {
        .name = "load_me",
        .key_fn = test_key_extractor,
        .user_data = user_data1,
        .unique = false,
        .sparse = false,
        .compare = NULL,
    };
    wtree3_tree_add_index(tree, &idx_config1, &error);
    wtree3_index_save_metadata(tree, "load_me", &error);

    // Add second index
    char *user_data2 = strdup("data_2");
    wtree3_index_config_t idx_config2 = {
        .name = "skip_me",
        .key_fn = test_key_extractor,
        .user_data = user_data2,
        .unique = false,
        .sparse = false,
        .compare = NULL,
    };
    wtree3_tree_add_index(tree, &idx_config2, &error);
    wtree3_index_save_metadata(tree, "skip_me", &error);

    wtree3_tree_close(tree);

    // Loader that only loads "load_me"
    int selective_loader(const char *index_name, bool unique, bool sparse,
                        wtree3_index_key_fn *out_key_fn,
                        wtree3_user_data_persistence_t **out_persistence,
                        void *ctx) {
        (void)unique; (void)sparse; (void)ctx;

        if (strcmp(index_name, "load_me") == 0) {
            *out_key_fn = test_key_extractor;
            static wtree3_user_data_persistence_t pers = {test_serialize, test_deserialize};
            *out_persistence = &pers;
            return 0;  // Load this one
        }

        return -1;  // Skip all others
    }

    // Re-open with selective loading using setter
    tree = wtree3_tree_open(test_db, "selective_load_tree", 0, 0, &error);
    assert_non_null(tree);

    rc = wtree3_tree_set_index_loader(tree, selective_loader, NULL, &error);
    assert_int_equal(rc, 0);

    // Verify correct index was loaded - should work
    const char *dummy_key = "x";
    wtree3_iterator_t *iter_loaded = wtree3_index_seek(tree, "load_me", dummy_key, 1, &error);
    assert_non_null(iter_loaded);  // This should succeed - index exists
    wtree3_iterator_close(iter_loaded);

    // Verify skipped index was NOT loaded - should fail
    gerror_t skip_error = {0};
    wtree3_iterator_t *iter_skipped = wtree3_index_seek(tree, "skip_me", dummy_key, 1, &skip_error);
    assert_null(iter_skipped);  // This should fail - index not loaded

    printf("Selective load test: Loaded 1 of 2 indexes as expected\n");

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

        /* Auto-loading indexes */
        cmocka_unit_test(test_auto_load_indexes),
        cmocka_unit_test(test_auto_load_selective),

        /* Error cases and edge cases */
        cmocka_unit_test(test_index_save_metadata_serialize_failure),
        cmocka_unit_test(test_index_load_metadata_deserialize_failure),
        cmocka_unit_test(test_index_load_metadata_invalid_params),
        cmocka_unit_test(test_list_persisted_indexes_null_count),
    };

    return cmocka_run_group_tests(tests, setup_db, teardown_db);
}
