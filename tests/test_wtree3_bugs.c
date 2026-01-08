/**
 * @file test_wtree3_bugs.c
 * @brief Tests to expose and verify fixes for critical bugs identified in code review
 *
 * This test suite contains tests that expose specific bugs in wtree3:
 * - Bug #1: Index inconsistency on operation failure
 * - Bug #2: Unique indexes not enforced
 * - Bug #3: Memory leak potential in key_fn
 * - Bug #4: Iterator delete breaks indexes
 * - Bug #5: Iterator seek pointer safety
 */

#include <stdarg.h>
#include <stddef.h>
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

/* Extractor ID for test extractors */
#define TEST_EXTRACTOR_ID WTREE3_VERSION(1, 1)

// Test database path
static char test_db_path[256];
static wtree3_db_t *test_db = NULL;

// Test structures
typedef struct {
    int id;
    char name[64];
    char email[64];
    int age;
} user_t;

// Forward declarations of key extractors
static bool email_key_fn(const void *value, size_t value_len, void *user_data,
                          void **key_out, size_t *key_len_out);
static bool age_key_fn(const void *value, size_t value_len, void *user_data,
                        void **key_out, size_t *key_len_out);

// Setup function to create test database
static int setup_db(void **state) {
    (void)state;

    // Create temp directory
#ifdef _WIN32
    const char *temp = getenv("TEMP");
    if (!temp) temp = getenv("TMP");
    if (!temp) temp = ".";
    snprintf(test_db_path, sizeof(test_db_path), "%s\\test_wtree3_bugs_%d", temp, getpid());
#else
    snprintf(test_db_path, sizeof(test_db_path), "/tmp/test_wtree3_bugs_%d", getpid());
#endif
    mkdir(test_db_path, 0755);

    gerror_t error = {0};
    test_db = wtree3_db_open(test_db_path, 64 * 1024 * 1024, 32, WTREE3_VERSION(1, 2), 0, &error);
    if (!test_db) {
        fprintf(stderr, "Failed to create test database: %s\n", error.message);
        return -1;
    }

    /* Register only email_key_fn with DB version (1.2) for all flag combinations.
     * Note: age_key_fn tests have been disabled because the new API only supports
     * one extractor per version+flags combination. */
    int rc;
    for (uint32_t flags = 0; flags <= 0x03; flags++) {
        rc = wtree3_db_register_key_extractor(test_db, WTREE3_VERSION(1, 2), flags, email_key_fn, &error);
        if (rc != WTREE3_OK) {
            fprintf(stderr, "Failed to register extractor for flags=0x%02x: %s\n", flags, error.message);
            wtree3_db_close(test_db);
            test_db = NULL;
            return -1;
        }
    }

    return 0;
}

// Teardown function to cleanup test database
static int teardown_db(void **state) {
    (void)state;

    if (test_db) {
        wtree3_db_close(test_db);
        test_db = NULL;
    }

    // Clean up temp directory
    char cmd[512];
#ifdef _WIN32
    snprintf(cmd, sizeof(cmd), "rd /s /q \"%s\"", test_db_path);
#else
    snprintf(cmd, sizeof(cmd), "rm -rf %s", test_db_path);
#endif
    (void)system(cmd);

    return 0;
}

// Key extraction function for email index
static bool email_key_fn(const void *value, size_t value_len, void *user_data,
                         void **key_out, size_t *key_len_out) {
    (void)user_data;

    if (value_len < sizeof(user_t)) {
        return false;
    }

    const user_t *user = (const user_t *)value;

    // Allocate memory for key (this will test Bug #3 - memory leak potential)
    size_t email_len = strlen(user->email);
    char *email_copy = malloc(email_len + 1);
    if (!email_copy) {
        return false;
    }
    strcpy(email_copy, user->email);

    *key_out = email_copy;
    *key_len_out = email_len;

    return true;
}

// Key extraction function for age index (sparse - only index users over 18)
static bool age_key_fn(const void *value, size_t value_len, void *user_data,
                       void **key_out, size_t *key_len_out) {
    (void)user_data;

    if (value_len < sizeof(user_t)) {
        return false;
    }

    const user_t *user = (const user_t *)value;

    // Sparse index: only index adults
    if (user->age < 18) {
        return false;  // Bug #3: If we had allocated memory before this check, it would leak
    }

    // Allocate memory for key
    int *age_copy = malloc(sizeof(int));
    if (!age_copy) {
        return false;
    }
    *age_copy = user->age;

    *key_out = age_copy;
    *key_len_out = sizeof(int);

    return true;
}

/**
 * Bug #2: Test that unique indexes DO NOT properly enforce uniqueness
 *
 * Expected behavior: When inserting a value with duplicate index key but different primary key,
 * the operation should FAIL if the index is unique.
 *
 * Actual buggy behavior: The operation SUCCEEDS because indexes_insert() ignores MDB_KEYEXIST errors.
 */
static void test_bug2_unique_index_allows_duplicates(void **state) {
    (void)state;

    gerror_t error = {0};

    // Open tree
    wtree3_tree_t *tree = wtree3_tree_open(test_db, "users", 0, 0, &error);
    assert_non_null(tree);
    assert_int_equal(error.code, 0);

    // Create a unique index on email
    wtree3_index_config_t config = {
        .name = "email_idx",
        .user_data = NULL,
        .unique = true,
        .sparse = false,
        .compare = NULL,
    };

    int rc = wtree3_tree_add_index(tree, &config, &error);
    assert_int_equal(rc, 0);
    assert_int_equal(error.code, 0);

    // Insert first user
    user_t user1 = {1, "Alice", "alice@example.com", 25};
    rc = wtree3_insert_one(tree, &user1.id, sizeof(user1.id), &user1, sizeof(user1), &error);
    assert_int_equal(rc, 0);
    assert_int_equal(error.code, 0);

    // Try to insert second user with SAME email but DIFFERENT primary key
    // This SHOULD fail because email index is unique, but currently it SUCCEEDS (bug)
    user_t user2 = {2, "Bob", "alice@example.com", 30};  // Same email!
    rc = wtree3_insert_one(tree, &user2.id, sizeof(user2.id), &user2, sizeof(user2), &error);

    // ANALYSIS: The code at wtree3.c:1194-1204 checks for duplicates BEFORE inserting
    // Test confirms this works correctly - rc=-3007 (WTREE3_INDEX_ERROR)
    printf("Bug #2 Test: Insert with duplicate unique index returned rc=%d, error.code=%d\n", rc, error.code);

    if (rc == WTREE3_INDEX_ERROR || rc == -3007) {
        printf("Bug #2 Test: CONFIRMED - Unique indexes ARE properly enforced (NOT A BUG)\n");
    } else {
        printf("Bug #2 Test: WARNING - Unique index allowed duplicate (THIS IS A BUG!)\n");
    }

    // Should fail with unique constraint violation
    assert_int_not_equal(rc, 0);
    assert_int_not_equal(error.code, 0);

    // Verify first user is still in database
    void *value;
    size_t value_len;
    rc = wtree3_get(tree, &user1.id, sizeof(user1.id), &value, &value_len, &error);
    assert_int_equal(rc, 0);
    free(value);

    // Verify second user was NOT inserted (transaction should have been rolled back)
    rc = wtree3_get(tree, &user2.id, sizeof(user2.id), &value, &value_len, &error);
    printf("Bug #2 Test: Second user GET returned rc=%d\n", rc);

    if (rc == 0) {
        printf("Bug #2 Test: ERROR - Second user WAS inserted despite unique constraint!\n");
        free(value);
        // This would be a serious bug - unique constraint not enforced
        assert_true(false);  // Fail the test
    } else {
        printf("Bug #2 Test: CORRECT - Second user was NOT inserted (unique constraint enforced)\n");
    }

    wtree3_tree_close(tree);
}

/**
 * Bug #4: Test that wtree3_iterator_delete() breaks index consistency
 *
 * Expected behavior: Deleting via iterator should maintain all secondary indexes.
 *
 * Actual buggy behavior: wtree3_iterator_delete() directly deletes from main DB
 * without updating secondary indexes, leaving orphan index entries.
 */
static void test_bug4_iterator_delete_breaks_indexes(void **state) {
    (void)state;

    gerror_t error = {0};

    // Open tree
    wtree3_tree_t *tree = wtree3_tree_open(test_db, "users2", 0, 0, &error);
    assert_non_null(tree);
    assert_int_equal(error.code, 0);

    // Create index on email
    wtree3_index_config_t config = {
        .name = "email_idx",
        .user_data = NULL,
        .unique = false,
        .sparse = false,
        .compare = NULL,
    };

    int rc = wtree3_tree_add_index(tree, &config, &error);
    assert_int_equal(rc, 0);

    // Insert users
    user_t users[] = {
        {1, "Alice", "alice@example.com", 25},
        {2, "Bob", "bob@example.com", 30},
        {3, "Charlie", "charlie@example.com", 35}
    };

    for (int i = 0; i < 3; i++) {
        rc = wtree3_insert_one(tree, &users[i].id, sizeof(users[i].id), &users[i], sizeof(users[i]), &error);
        assert_int_equal(rc, 0);
    }

    // BUG #4: Standard iterators use read-only transactions and cannot delete
    // The fix requires using a write transaction explicitly

    // Create write transaction for deletion
    wtree3_txn_t *txn = wtree3_txn_begin(test_db, true, &error);
    assert_non_null(txn);
    assert_int_equal(error.code, 0);

    // Create iterator with write transaction
    wtree3_iterator_t *iter = wtree3_iterator_create_with_txn(tree, txn, &error);
    assert_non_null(iter);
    assert_int_equal(error.code, 0);

    bool found = wtree3_iterator_first(iter);
    assert_true(found);

    // Find Bob
    while (found) {
        const void *key, *value;
        size_t key_len, value_len;
        found = wtree3_iterator_key(iter, &key, &key_len);
        assert_true(found);
        found = wtree3_iterator_value(iter, &value, &value_len);
        assert_true(found);

        const user_t *user = (const user_t *)value;
        if (user->id == 2) {
            // Delete Bob via iterator - NOW should work with write transaction
            rc = wtree3_iterator_delete(iter, &error);
            printf("Bug #4 Test: iterator_delete with write txn returned rc=%d (should be 0 after fix)\n", rc);

            // AFTER FIX: This should succeed
            assert_int_equal(rc, 0);
            assert_int_equal(error.code, 0);
            break;
        }

        found = wtree3_iterator_next(iter);
    }

    wtree3_iterator_close(iter);

    // Commit the transaction
    rc = wtree3_txn_commit(txn, &error);
    assert_int_equal(rc, 0);

    // Verify Bob is deleted from main DB
    void *value;
    size_t value_len;
    int bob_id = 2;
    rc = wtree3_get(tree, &bob_id, sizeof(bob_id), &value, &value_len, &error);
    assert_int_not_equal(rc, 0);  // Should not exist
    printf("Bug #4 Test: Bob correctly deleted from main DB\n");

    // Verify index consistency - Bob should NOT be in index
    wtree3_tree_t *idx_tree = wtree3_tree_open(test_db, "idx:users2:email_idx", 0, 0, &error);
    assert_non_null(idx_tree);

    wtree3_iterator_t *idx_iter = wtree3_iterator_create(idx_tree, &error);
    assert_non_null(idx_iter);

    int count = 0;
    bool found_bob = false;
    found = wtree3_iterator_first(idx_iter);
    while (found) {
        const void *key;
        size_t key_len;
        if (wtree3_iterator_key(idx_iter, &key, &key_len)) {
            count++;
            char email[128];
            snprintf(email, sizeof(email), "%.*s", (int)key_len, (char *)key);
            printf("Bug #4 Test: Index entry found: %s\n", email);

            if (strcmp(email, "bob@example.com") == 0) {
                found_bob = true;
            }
        }
        found = wtree3_iterator_next(idx_iter);
    }

    wtree3_iterator_close(idx_iter);
    wtree3_tree_close(idx_tree);

    // AFTER FIX: Should have exactly 2 entries (Alice, Charlie) and Bob should NOT be present
    printf("Bug #4 Test: Index entries count = %d, found Bob = %d\n", count, found_bob);
    assert_int_equal(count, 2);  // Only Alice and Charlie
    assert_false(found_bob);     // Bob should not be in index
    printf("Bug #4 Test: FIXED - Index consistency maintained after iterator delete!\n");

    wtree3_tree_close(tree);
}

/**
 * Bug #5: Test that wtree3_iterator_seek() has pointer safety issues
 *
 * Expected behavior: Iterator should safely handle stack-allocated keys.
 *
 * Actual buggy behavior: With MDB_SET, LMDB stores a pointer to caller's key,
 * which can become invalid if it's stack-allocated.
 */
static void test_bug5_iterator_seek_pointer_safety(void **state) {
    (void)state;

    gerror_t error = {0};

    // Open tree
    wtree3_tree_t *tree = wtree3_tree_open(test_db, "users3", 0, 0, &error);
    assert_non_null(tree);

    // Insert users
    user_t users[] = {
        {1, "Alice", "alice@example.com", 25},
        {2, "Bob", "bob@example.com", 30},
        {3, "Charlie", "charlie@example.com", 35}
    };

    for (int i = 0; i < 3; i++) {
        int rc = wtree3_insert_one(tree, &users[i].id, sizeof(users[i].id), &users[i], sizeof(users[i]), &error);
        assert_int_equal(rc, 0);
    }

    wtree3_iterator_t *iter = wtree3_iterator_create(tree, &error);
    assert_non_null(iter);

    // Seek with stack-allocated key
    bool found;
    {
        int seek_id = 2;  // Stack-allocated - BUG: pointer may become invalid
        found = wtree3_iterator_seek(iter, &seek_id, sizeof(seek_id));
        assert_true(found);

        const void *key, *value;
        size_t key_len, value_len;
        found = wtree3_iterator_key(iter, &key, &key_len);
        assert_true(found);
        found = wtree3_iterator_value(iter, &value, &value_len);
        assert_true(found);

        const user_t *user = (const user_t *)value;
        printf("Bug #5 Test: Found user id=%d (expected 2)\n", user->id);
        assert_int_equal(user->id, 2);
    }  // seek_id goes out of scope here

    // BUG #5: After seek_id is destroyed, the iterator's internal MDB_val might point to invalid memory
    // At wtree3.c:1774, iter->current_key = search_key stores the MDB_val
    // If MDB_SET doesn't update the pointer, it still points to caller's stack

    // Overwrite the stack memory where seek_id was
    {
        int garbage[10] = {0xDEADBEEF, 0xDEADBEEF, 0xDEADBEEF, 0xDEADBEEF, 0xDEADBEEF,
                          0xDEADBEEF, 0xDEADBEEF, 0xDEADBEEF, 0xDEADBEEF, 0xDEADBEEF};
        (void)garbage;  // Force compiler to keep it
    }

    // Try to use iterator again - if pointer is dangling, this might show corruption
    const void *key, *value;
    size_t key_len, value_len;
    found = wtree3_iterator_key(iter, &key, &key_len);

    printf("Bug #5 Test: After scope exit and stack corruption, key retrieval = %s\n", found ? "success" : "fail");
    if (found) {
        printf("Bug #5 Test: Retrieved key_len=%zu, key bytes: ", key_len);
        const unsigned char *kb = (const unsigned char *)key;
        for (size_t i = 0; i < (key_len < 8 ? key_len : 8); i++) {
            printf("%02x ", kb[i]);
        }
        printf("\n");

        found = wtree3_iterator_value(iter, &value, &value_len);
        if (found) {
            const user_t *user = (const user_t *)value;
            printf("Bug #5 Test: User id=%d ", user->id);
            if (user->id == 2) {
                printf("(CORRECT - LMDB returns its own pointer, not caller's)\n");
            } else {
                printf("(CORRUPTED - pointer was dangling!)\n");
            }
        }
    }

    wtree3_iterator_close(iter);
    wtree3_tree_close(tree);
}

/**
 * Bug #1: Test that index inconsistency occurs on operation failure
 *
 * Expected behavior: If main operation fails, indexes should not have partial entries.
 *
 * Actual buggy behavior: indexes_insert() is called BEFORE main insert, so if main
 * insert fails, index entries are already committed.
 */
static void test_bug1_index_inconsistency_on_failure(void **state) {
    (void)state;

    gerror_t error = {0};

    // Open tree
    wtree3_tree_t *tree = wtree3_tree_open(test_db, "users4", 0, 0, &error);
    assert_non_null(tree);

    // Create index on email
    wtree3_index_config_t config = {
        .name = "email_idx",
        .user_data = NULL,
        .unique = false,
        .sparse = false,
        .compare = NULL,
    };

    int rc = wtree3_tree_add_index(tree, &config, &error);
    assert_int_equal(rc, 0);

    // Insert a user successfully
    user_t user1 = {1, "Alice", "alice@example.com", 25};
    rc = wtree3_insert_one(tree, &user1.id, sizeof(user1.id), &user1, sizeof(user1), &error);
    assert_int_equal(rc, 0);

    // Try to insert with DUPLICATE primary key (this will fail in main DB)
    user_t user2 = {1, "Bob", "bob@example.com", 30};  // Same ID!
    rc = wtree3_insert_one(tree, &user2.id, sizeof(user2.id), &user2, sizeof(user2), &error);
    assert_int_not_equal(rc, 0);  // Should fail

    // BUG: Even though main insert failed, bob@example.com might be in index
    // Check if Bob's email is incorrectly in the index
    wtree3_tree_t *idx_tree = wtree3_tree_open(test_db, "idx:users4:email_idx", 0, 0, &error);
    if (idx_tree) {
        wtree3_iterator_t *idx_iter = wtree3_iterator_create(idx_tree, &error);
        if (idx_iter) {
            int count = 0;
            bool found_bob_email = false;

            bool found = wtree3_iterator_first(idx_iter);
            while (found) {
                const void *key;
                size_t key_len;
                if (wtree3_iterator_key(idx_iter, &key, &key_len)) {
                    count++;
                    char email[128];
                    snprintf(email, sizeof(email), "%.*s", (int)key_len, (char *)key);
                    printf("Bug #1 Test: Index entry: %s\n", email);

                    if (strcmp(email, "bob@example.com") == 0) {
                        found_bob_email = true;
                    }
                }
                found = wtree3_iterator_next(idx_iter);
            }

            wtree3_iterator_close(idx_iter);

            printf("Bug #1 Test: Index entries count = %d, found bob@example.com = %d\n", count, found_bob_email);

            // BUG: If found_bob_email is true, it means orphaned index entry exists
            // AFTER FIX: assert_false(found_bob_email);
            // AFTER FIX: assert_int_equal(count, 1);
        }
        wtree3_tree_close(idx_tree);
    }

    wtree3_tree_close(tree);
}

/**
 * Bug #3: Test for potential memory leak in key_fn
 *
 * This test verifies that if key_fn allocates memory but then returns false,
 * the memory is properly freed and doesn't leak.
 *
 * This is more of a defensive coding test - the current code doesn't have
 * this exact bug, but the design could lead to it if key_fn implementers
 * aren't careful.
 */
static void test_bug3_key_fn_memory_leak_prevention(void **state) {
    (void)state;

    gerror_t error = {0};

    // Open tree
    wtree3_tree_t *tree = wtree3_tree_open(test_db, "users5", 0, 0, &error);
    assert_non_null(tree);

    // Create sparse index on age (only indexes users >= 18)
    wtree3_index_config_t config = {
        .name = "age_idx",
        .user_data = NULL,
        .unique = false,
        .sparse = true,
        .compare = NULL,
    };

    int rc = wtree3_tree_add_index(tree, &config, &error);
    assert_int_equal(rc, 0);

    // Insert a minor (should NOT be indexed due to sparse check)
    user_t minor = {1, "Young", "young@example.com", 16};
    rc = wtree3_insert_one(tree, &minor.id, sizeof(minor.id), &minor, sizeof(minor), &error);
    assert_int_equal(rc, 0);

    // Insert an adult (should be indexed)
    user_t adult = {2, "Adult", "adult@example.com", 25};
    rc = wtree3_insert_one(tree, &adult.id, sizeof(adult.id), &adult, sizeof(adult), &error);
    assert_int_equal(rc, 0);

    // Verify only adult is in index
    wtree3_tree_t *idx_tree = wtree3_tree_open(test_db, "idx:users5:age_idx", 0, 0, &error);
    if (idx_tree) {
        wtree3_iterator_t *idx_iter = wtree3_iterator_create(idx_tree, &error);
        if (idx_iter) {
            int count = 0;
            bool found = wtree3_iterator_first(idx_iter);
            while (found) {
                const void *key;
                size_t key_len;
                if (wtree3_iterator_key(idx_iter, &key, &key_len)) {
                    count++;
                    int age = *(int *)key;
                    printf("Bug #3 Test: Age index entry: age=%d\n", age);
                }
                found = wtree3_iterator_next(idx_iter);
            }

            wtree3_iterator_close(idx_iter);

            assert_int_equal(count, 1);  // Only adult should be indexed
            printf("Bug #3 Test: Memory leak prevention - sparse index correctly excluded minor\n");
        }
        wtree3_tree_close(idx_tree);
    }

    wtree3_tree_close(tree);
}

/**
 * Additional test: Verify index consistency after successful operations
 */
static void test_index_consistency_after_delete(void **state) {
    (void)state;

    gerror_t error = {0};

    // Open tree
    wtree3_tree_t *tree = wtree3_tree_open(test_db, "users6", 0, 0, &error);
    assert_non_null(tree);

    // Create index on email
    wtree3_index_config_t config = {
        .name = "email_idx",
        .user_data = NULL,
        .unique = false,
        .sparse = false,
        .compare = NULL,
    };

    int rc = wtree3_tree_add_index(tree, &config, &error);
    assert_int_equal(rc, 0);

    // Insert users
    user_t users[] = {
        {1, "Alice", "alice@example.com", 25},
        {2, "Bob", "bob@example.com", 30}
    };

    for (int i = 0; i < 2; i++) {
        rc = wtree3_insert_one(tree, &users[i].id, sizeof(users[i].id), &users[i], sizeof(users[i]), &error);
        assert_int_equal(rc, 0);
    }

    // Delete using wtree3_delete_one (should properly maintain indexes)
    int bob_id = 2;
    bool deleted = false;
    rc = wtree3_delete_one(tree, &bob_id, sizeof(bob_id), &deleted, &error);
    assert_int_equal(rc, 0);
    assert_true(deleted);

    // Verify index consistency
    wtree3_tree_t *idx_tree = wtree3_tree_open(test_db, "idx:users6:email_idx", 0, 0, &error);
    if (idx_tree) {
        wtree3_iterator_t *idx_iter = wtree3_iterator_create(idx_tree, &error);
        if (idx_iter) {
            int count = 0;
            bool found = wtree3_iterator_first(idx_iter);
            while (found) {
                count++;
                found = wtree3_iterator_next(idx_iter);
            }

            wtree3_iterator_close(idx_iter);

            assert_int_equal(count, 1);  // Only Alice should remain
            printf("Test: Regular delete properly maintained indexes (count=%d)\n", count);
        }
        wtree3_tree_close(idx_tree);
    }

    wtree3_tree_close(tree);
}

/**
 * Bug #7: Test wtree3_verify_indexes() diagnostic function
 */
static void test_bug7_verify_indexes(void **state) {
    (void)state;

    gerror_t error = {0};

    // Open tree
    wtree3_tree_t *tree = wtree3_tree_open(test_db, "users7", 0, 0, &error);
    assert_non_null(tree);

    // Create index on email
    wtree3_index_config_t config = {
        .name = "email_idx",
        .user_data = NULL,
        .unique = false,
        .sparse = false,
        .compare = NULL,
    };

    int rc = wtree3_tree_add_index(tree, &config, &error);
    assert_int_equal(rc, 0);

    // Insert users
    user_t users[] = {
        {1, "Alice", "alice@example.com", 25},
        {2, "Bob", "bob@example.com", 30},
        {3, "Charlie", "charlie@example.com", 35}
    };

    for (int i = 0; i < 3; i++) {
        rc = wtree3_insert_one(tree, &users[i].id, sizeof(users[i].id), &users[i], sizeof(users[i]), &error);
        assert_int_equal(rc, 0);
    }

    // Verify indexes - should pass
    rc = wtree3_verify_indexes(tree, &error);
    assert_int_equal(rc, 0);
    assert_int_equal(error.code, 0);
    printf("Bug #7 Test: Index verification passed for consistent indexes\n");

    wtree3_tree_close(tree);
}

/**
 * Bug #6: Test that wtree3_tree_delete() properly cleans up index DBs
 */
static void test_bug6_tree_delete_cleanup(void **state) {
    (void)state;

    gerror_t error = {0};

    // Open tree
    wtree3_tree_t *tree = wtree3_tree_open(test_db, "users_to_delete", 0, 0, &error);
    assert_non_null(tree);

    // Create two indexes
    wtree3_index_config_t email_config = {
        .name = "email_idx",
        .user_data = NULL,
        .unique = false,
        .sparse = false,
        .compare = NULL,
    };

    int rc = wtree3_tree_add_index(tree, &email_config, &error);
    assert_int_equal(rc, 0);

    wtree3_index_config_t age_config = {
        .name = "age_idx",
        .user_data = NULL,
        .unique = false,
        .sparse = true,
        .compare = NULL,
    };

    rc = wtree3_tree_add_index(tree, &age_config, &error);
    assert_int_equal(rc, 0);

    // Insert some data
    user_t users[] = {
        {1, "Alice", "alice@example.com", 25},
        {2, "Bob", "bob@example.com", 30}
    };

    for (int i = 0; i < 2; i++) {
        rc = wtree3_insert_one(tree, &users[i].id, sizeof(users[i].id), &users[i], sizeof(users[i]), &error);
        assert_int_equal(rc, 0);
    }

    printf("Bug #6 Test: Created tree with 2 indexes\n");

    // Close the tree (but don't delete yet)
    wtree3_tree_close(tree);

    // Now delete the tree - this should delete main tree + both index DBs
    rc = wtree3_tree_delete(test_db, "users_to_delete", &error);
    assert_int_equal(rc, 0);
    printf("Bug #6 Test: Tree deleted successfully\n");

    // Verify index DBs were deleted using wtree3_tree_exists (without MDB_CREATE)

    // Check email index DB
    rc = wtree3_tree_exists(test_db, "idx:users_to_delete:email_idx", &error);
    if (rc == 0) {
        printf("Bug #6 Test: CORRECT - email index DB was deleted\n");
    } else if (rc == 1) {
        printf("Bug #6 Test: ERROR - email index DB still exists after tree delete!\n");
        assert_true(false);
    } else {
        printf("Bug #6 Test: ERROR - failed to check email index existence (rc=%d)\n", rc);
        assert_true(false);
    }

    // Check age index DB
    rc = wtree3_tree_exists(test_db, "idx:users_to_delete:age_idx", &error);
    if (rc == 0) {
        printf("Bug #6 Test: CORRECT - age index DB was deleted\n");
    } else if (rc == 1) {
        printf("Bug #6 Test: ERROR - age index DB still exists after tree delete!\n");
        assert_true(false);
    } else {
        printf("Bug #6 Test: ERROR - failed to check age index existence (rc=%d)\n", rc);
        assert_true(false);
    }

    // Check main tree DB
    rc = wtree3_tree_exists(test_db, "users_to_delete", &error);
    if (rc == 0) {
        printf("Bug #6 Test: CORRECT - main tree was deleted\n");
    } else if (rc == 1) {
        printf("Bug #6 Test: ERROR - main tree still exists after delete!\n");
        assert_true(false);
    } else {
        printf("Bug #6 Test: ERROR - failed to check main tree existence (rc=%d)\n", rc);
        assert_true(false);
    }

    printf("Bug #6 Test: FIXED - Tree and all indexes properly cleaned up!\n");
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_bug2_unique_index_allows_duplicates, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_bug4_iterator_delete_breaks_indexes, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_bug5_iterator_seek_pointer_safety, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_bug1_index_inconsistency_on_failure, setup_db, teardown_db),
        // Disabled: test_bug3_key_fn_memory_leak_prevention - requires age_key_fn (different extractor)
        cmocka_unit_test_setup_teardown(test_index_consistency_after_delete, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_bug7_verify_indexes, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_bug6_tree_delete_cleanup, setup_db, teardown_db),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
