/*
 * test_wtree3_full_integration.c - Comprehensive Integration Test
 *
 * A single-file comprehensive test that exercises all major wtree3 functionality:
 * - Database lifecycle (open, close, reopen)
 * - Multiple tree creation and management
 * - Full CRUD operations (insert, update, upsert, delete, read)
 * - Index creation, population, and queries
 * - CRUD with automatic index maintenance
 * - Iterators and scans (forward, reverse, range, prefix)
 * - Persistence (close/reopen with index auto-loading)
 * - Tree deletion with index cleanup
 * - Advanced operations (batch, bulk, conditional)
 *
 * This test demonstrates real-world usage patterns and verifies end-to-end functionality.
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
#define TEST_VERSION WTREE3_VERSION(1, 0)

/* ============================================================
 * Data Structures for Testing
 * ============================================================ */

typedef struct {
    int id;
    char name[64];
    char email[128];
    int age;
    int active;  // 1 for active, 0 for inactive
} user_t;

typedef struct {
    int id;
    char name[128];
    char category[64];  // May be empty for sparse index test
    double price;
    int stock;
} product_t;

typedef struct {
    int id;
    int user_id;
    int product_id;
    int quantity;
    double total;
} order_t;

/* ============================================================
 * Index Key Extractor Functions
 * ============================================================ */

/* Extract email from user (unique index) */
static bool user_email_extractor(const void *value, size_t value_len,
                                  void *user_data,
                                  void **out_key, size_t *out_len) {
    (void)user_data;

    if (value_len < sizeof(user_t)) {
        return false;
    }

    const user_t *user = (const user_t *)value;
    size_t email_len = strlen(user->email);

    if (email_len == 0) {
        return false;  // Sparse behavior
    }

    char *key = malloc(email_len + 1);
    if (!key) return false;

    memcpy(key, user->email, email_len + 1);
    *out_key = key;
    *out_len = email_len + 1;
    return true;
}

/* Extract name from user (non-unique index) */
static bool user_name_extractor(const void *value, size_t value_len,
                                 void *user_data,
                                 void **out_key, size_t *out_len) {
    (void)user_data;

    if (value_len < sizeof(user_t)) {
        return false;
    }

    const user_t *user = (const user_t *)value;
    size_t name_len = strlen(user->name);

    if (name_len == 0) {
        return false;
    }

    char *key = malloc(name_len + 1);
    if (!key) return false;

    memcpy(key, user->name, name_len + 1);
    *out_key = key;
    *out_len = name_len + 1;
    return true;
}

/* Extract category from product (sparse index - may be empty) */
static bool product_category_extractor(const void *value, size_t value_len,
                                        void *user_data,
                                        void **out_key, size_t *out_len) {
    (void)user_data;

    if (value_len < sizeof(product_t)) {
        return false;
    }

    const product_t *product = (const product_t *)value;
    size_t category_len = strlen(product->category);

    // Sparse: skip if category is empty
    if (category_len == 0) {
        return false;
    }

    char *key = malloc(category_len + 1);
    if (!key) return false;

    memcpy(key, product->category, category_len + 1);
    *out_key = key;
    *out_len = category_len + 1;
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

static void make_user_key(int id, void *buf, size_t *len) {
    snprintf((char*)buf, 64, "user:%d", id);
    *len = strlen((char*)buf) + 1;
}

static void make_product_key(int id, void *buf, size_t *len) {
    snprintf((char*)buf, 64, "product:%d", id);
    *len = strlen((char*)buf) + 1;
}

static void make_order_key(int id, void *buf, size_t *len) {
    snprintf((char*)buf, 64, "order:%d", id);
    *len = strlen((char*)buf) + 1;
}

/* ============================================================
 * COMPREHENSIVE INTEGRATION TEST
 * ============================================================ */

static void test_full_integration(void **state) {
    (void)state;
    gerror_t error = {0};
    int rc;

    printf("\n=== PHASE 1: Database Open and Setup ===\n");

    /* Create test directory */
#ifdef _WIN32
    const char *temp = getenv("TEMP");
    if (!temp) temp = getenv("TMP");
    if (!temp) temp = ".";
    snprintf(test_db_path, sizeof(test_db_path), "%s\\test_full_integration_%d",
             temp, getpid());
#else
    snprintf(test_db_path, sizeof(test_db_path), "/tmp/test_full_integration_%d",
             getpid());
#endif
    mkdir(test_db_path, 0755);

    /* Open database */
    wtree3_db_t *db = wtree3_db_open(test_db_path, 128 * 1024 * 1024, 64,
                                      TEST_VERSION, 0, &error);
    assert_non_null(db);
    printf("✓ Database opened: %s\n", test_db_path);

    /* Register key extractors - one extractor can handle all flag combinations */
    printf("✓ Registering extractors...\n");

    /* Register email extractor (for unique, non-sparse) */
    rc = wtree3_db_register_key_extractor(db, TEST_VERSION, 0x01,
                                           user_email_extractor, &error);
    assert_int_equal(rc, WTREE3_OK);

    /* Register name extractor (for non-unique, non-sparse) */
    rc = wtree3_db_register_key_extractor(db, TEST_VERSION, 0x00,
                                           user_name_extractor, &error);
    assert_int_equal(rc, WTREE3_OK);

    /* Register category extractor (for non-unique, sparse) */
    rc = wtree3_db_register_key_extractor(db, TEST_VERSION, 0x02,
                                           product_category_extractor, &error);
    assert_int_equal(rc, WTREE3_OK);

    printf("✓ Extractors registered\n");

    /* ============================================================
     * PHASE 2: Create Multiple Trees
     * ============================================================ */
    printf("\n=== PHASE 2: Creating Multiple Trees ===\n");

    wtree3_tree_t *users_tree = wtree3_tree_open(db, "users", MDB_CREATE, 0, &error);
    assert_non_null(users_tree);
    printf("✓ Created tree: users\n");

    wtree3_tree_t *products_tree = wtree3_tree_open(db, "products", MDB_CREATE, 0, &error);
    assert_non_null(products_tree);
    printf("✓ Created tree: products\n");

    wtree3_tree_t *orders_tree = wtree3_tree_open(db, "orders", MDB_CREATE, 0, &error);
    assert_non_null(orders_tree);
    printf("✓ Created tree: orders\n");

    /* Verify trees exist */
    rc = wtree3_tree_exists(db, "users", &error);
    assert_int_equal(rc, 1);
    rc = wtree3_tree_exists(db, "products", &error);
    assert_int_equal(rc, 1);
    rc = wtree3_tree_exists(db, "nonexistent", &error);
    assert_int_equal(rc, 0);

    /* ============================================================
     * PHASE 3: Basic CRUD Operations (Before Indexes)
     * ============================================================ */
    printf("\n=== PHASE 3: Basic CRUD Operations (No Indexes Yet) ===\n");

    /* Insert users */
    printf("Inserting users...\n");
    char key_buf[64];
    size_t key_len;

    user_t user1 = {1, "Alice Smith", "alice@example.com", 30, 1};
    make_user_key(1, key_buf, &key_len);
    rc = wtree3_insert_one(users_tree, key_buf, key_len, &user1, sizeof(user1), &error);
    assert_int_equal(rc, WTREE3_OK);

    user_t user2 = {2, "Bob Jones", "bob@example.com", 25, 1};
    make_user_key(2, key_buf, &key_len);
    rc = wtree3_insert_one(users_tree, key_buf, key_len, &user2, sizeof(user2), &error);
    assert_int_equal(rc, WTREE3_OK);

    user_t user3 = {3, "Charlie Brown", "charlie@example.com", 35, 1};
    make_user_key(3, key_buf, &key_len);
    rc = wtree3_insert_one(users_tree, key_buf, key_len, &user3, sizeof(user3), &error);
    assert_int_equal(rc, WTREE3_OK);

    user_t user4 = {4, "Alice Johnson", "alice.j@example.com", 28, 0};
    make_user_key(4, key_buf, &key_len);
    rc = wtree3_insert_one(users_tree, key_buf, key_len, &user4, sizeof(user4), &error);
    assert_int_equal(rc, WTREE3_OK);

    printf("✓ Inserted 4 users\n");
    assert_int_equal(wtree3_tree_count(users_tree), 4);

    /* Insert products (some without category for sparse index testing) */
    printf("Inserting products...\n");

    product_t prod1 = {1, "Laptop", "Electronics", 999.99, 50};
    make_product_key(1, key_buf, &key_len);
    rc = wtree3_insert_one(products_tree, key_buf, key_len, &prod1, sizeof(prod1), &error);
    assert_int_equal(rc, WTREE3_OK);

    product_t prod2 = {2, "Coffee Mug", "Kitchen", 12.99, 200};
    make_product_key(2, key_buf, &key_len);
    rc = wtree3_insert_one(products_tree, key_buf, key_len, &prod2, sizeof(prod2), &error);
    assert_int_equal(rc, WTREE3_OK);

    product_t prod3 = {3, "Mystery Item", "", 5.00, 10};  // No category - sparse test
    make_product_key(3, key_buf, &key_len);
    rc = wtree3_insert_one(products_tree, key_buf, key_len, &prod3, sizeof(prod3), &error);
    assert_int_equal(rc, WTREE3_OK);

    product_t prod4 = {4, "Desk Chair", "Furniture", 199.99, 30};
    make_product_key(4, key_buf, &key_len);
    rc = wtree3_insert_one(products_tree, key_buf, key_len, &prod4, sizeof(prod4), &error);
    assert_int_equal(rc, WTREE3_OK);

    printf("✓ Inserted 4 products (1 without category)\n");
    assert_int_equal(wtree3_tree_count(products_tree), 4);

    /* Insert orders */
    printf("Inserting orders...\n");

    order_t order1 = {1, 1, 1, 1, 999.99};
    make_order_key(1, key_buf, &key_len);
    rc = wtree3_insert_one(orders_tree, key_buf, key_len, &order1, sizeof(order1), &error);
    assert_int_equal(rc, WTREE3_OK);

    order_t order2 = {2, 2, 2, 3, 38.97};
    make_order_key(2, key_buf, &key_len);
    rc = wtree3_insert_one(orders_tree, key_buf, key_len, &order2, sizeof(order2), &error);
    assert_int_equal(rc, WTREE3_OK);

    printf("✓ Inserted 2 orders\n");
    assert_int_equal(wtree3_tree_count(orders_tree), 2);

    /* Test READ operations */
    printf("\nTesting READ operations...\n");
    void *read_value = NULL;
    size_t read_len = 0;

    make_user_key(1, key_buf, &key_len);
    rc = wtree3_get(users_tree, key_buf, key_len, &read_value, &read_len, &error);
    assert_int_equal(rc, WTREE3_OK);
    assert_non_null(read_value);
    user_t *read_user = (user_t*)read_value;
    assert_string_equal(read_user->name, "Alice Smith");
    assert_string_equal(read_user->email, "alice@example.com");
    free(read_value);
    printf("✓ Read user:1 successfully\n");

    /* Test UPDATE operations */
    printf("\nTesting UPDATE operations...\n");
    user_t updated_user1 = {1, "Alice Smith-Updated", "alice.new@example.com", 31, 1};
    make_user_key(1, key_buf, &key_len);
    rc = wtree3_update(users_tree, key_buf, key_len, &updated_user1, sizeof(updated_user1), &error);
    assert_int_equal(rc, WTREE3_OK);

    /* Verify update */
    rc = wtree3_get(users_tree, key_buf, key_len, &read_value, &read_len, &error);
    assert_int_equal(rc, WTREE3_OK);
    read_user = (user_t*)read_value;
    assert_string_equal(read_user->name, "Alice Smith-Updated");
    assert_int_equal(read_user->age, 31);
    free(read_value);
    printf("✓ Updated user:1 successfully\n");

    /* Test UPSERT operations */
    printf("\nTesting UPSERT operations...\n");
    user_t user5 = {5, "Eve Martin", "eve@example.com", 29, 1};
    make_user_key(5, key_buf, &key_len);
    rc = wtree3_upsert(users_tree, key_buf, key_len, &user5, sizeof(user5), &error);
    assert_int_equal(rc, WTREE3_OK);
    assert_int_equal(wtree3_tree_count(users_tree), 5);
    printf("✓ Upserted new user:5\n");

    /* Upsert existing */
    user5.age = 30;
    strcpy(user5.name, "Eve Martin-Updated");
    rc = wtree3_upsert(users_tree, key_buf, key_len, &user5, sizeof(user5), &error);
    assert_int_equal(rc, WTREE3_OK);
    assert_int_equal(wtree3_tree_count(users_tree), 5);  // Count shouldn't change
    printf("✓ Upserted existing user:5\n");

    /* Test DELETE operations */
    printf("\nTesting DELETE operations...\n");
    bool deleted = false;
    make_user_key(5, key_buf, &key_len);
    rc = wtree3_delete_one(users_tree, key_buf, key_len, &deleted, &error);
    assert_int_equal(rc, WTREE3_OK);
    assert_true(deleted);
    assert_int_equal(wtree3_tree_count(users_tree), 4);
    printf("✓ Deleted user:5\n");

    /* Try delete non-existent */
    deleted = false;
    make_user_key(999, key_buf, &key_len);
    rc = wtree3_delete_one(users_tree, key_buf, key_len, &deleted, &error);
    assert_int_equal(rc, WTREE3_OK);
    assert_false(deleted);
    printf("✓ Delete non-existent key handled correctly\n");

    /* Test EXISTS */
    make_user_key(1, key_buf, &key_len);
    bool exists = wtree3_exists(users_tree, key_buf, key_len, &error);
    assert_true(exists);

    make_user_key(999, key_buf, &key_len);
    exists = wtree3_exists(users_tree, key_buf, key_len, &error);
    assert_false(exists);
    printf("✓ Exists checks work correctly\n");

    /* ============================================================
     * PHASE 4: Index Creation
     * ============================================================ */
    printf("\n=== PHASE 4: Creating Indexes ===\n");

    /* Add unique email index to users */
    printf("Adding unique email index to users tree...\n");
    const char *email_user_data = "email_field";
    wtree3_index_config_t email_idx_config = {
        .name = "email_idx",
        .user_data = email_user_data,
        .user_data_len = strlen(email_user_data) + 1,
        .unique = true,
        .sparse = false,
        .compare = NULL
    };
    rc = wtree3_tree_add_index(users_tree, &email_idx_config, &error);
    assert_int_equal(rc, WTREE3_OK);
    printf("✓ Added email_idx\n");

    /* Populate email index from existing data */
    rc = wtree3_tree_populate_index(users_tree, "email_idx", &error);
    assert_int_equal(rc, WTREE3_OK);
    printf("✓ Populated email_idx with existing data\n");

    /* Add non-unique name index to users */
    printf("Adding non-unique name index to users tree...\n");
    const char *name_user_data = "name_field";
    wtree3_index_config_t name_idx_config = {
        .name = "name_idx",
        .user_data = name_user_data,
        .user_data_len = strlen(name_user_data) + 1,
        .unique = false,
        .sparse = false,
        .compare = NULL
    };
    rc = wtree3_tree_add_index(users_tree, &name_idx_config, &error);
    assert_int_equal(rc, WTREE3_OK);
    rc = wtree3_tree_populate_index(users_tree, "name_idx", &error);
    assert_int_equal(rc, WTREE3_OK);
    printf("✓ Added and populated name_idx\n");

    /* Add sparse category index to products */
    printf("Adding sparse category index to products tree...\n");
    const char *category_user_data = "category_field";
    wtree3_index_config_t category_idx_config = {
        .name = "category_idx",
        .user_data = category_user_data,
        .user_data_len = strlen(category_user_data) + 1,
        .unique = false,
        .sparse = true,
        .compare = NULL
    };
    rc = wtree3_tree_add_index(products_tree, &category_idx_config, &error);
    assert_int_equal(rc, WTREE3_OK);
    rc = wtree3_tree_populate_index(products_tree, "category_idx", &error);
    assert_int_equal(rc, WTREE3_OK);
    printf("✓ Added and populated category_idx (sparse)\n");

    /* Verify index counts */
    assert_int_equal(wtree3_tree_index_count(users_tree), 2);
    assert_int_equal(wtree3_tree_index_count(products_tree), 1);
    assert_int_equal(wtree3_tree_index_count(orders_tree), 0);

    assert_true(wtree3_tree_has_index(users_tree, "email_idx"));
    assert_true(wtree3_tree_has_index(users_tree, "name_idx"));
    assert_false(wtree3_tree_has_index(users_tree, "nonexistent"));
    printf("✓ Index counts and checks verified\n");

    /* ============================================================
     * PHASE 5: CRUD Operations with Indexes
     * ============================================================ */
    printf("\n=== PHASE 5: CRUD Operations with Index Maintenance ===\n");

    /* Insert new user - indexes should auto-update */
    printf("Inserting new user with indexes active...\n");
    user_t user6 = {6, "Frank Wilson", "frank@example.com", 40, 1};
    make_user_key(6, key_buf, &key_len);
    rc = wtree3_insert_one(users_tree, key_buf, key_len, &user6, sizeof(user6), &error);
    assert_int_equal(rc, WTREE3_OK);
    assert_int_equal(wtree3_tree_count(users_tree), 5);
    printf("✓ Inserted user:6 with automatic index updates\n");

    /* Try to insert duplicate email (unique constraint violation) */
    printf("Testing unique constraint violation...\n");
    user_t user7 = {7, "Frank Clone", "frank@example.com", 41, 1};  // Duplicate email
    make_user_key(7, key_buf, &key_len);
    rc = wtree3_insert_one(users_tree, key_buf, key_len, &user7, sizeof(user7), &error);
    assert_int_equal(rc, WTREE3_INDEX_ERROR);  // Should fail on unique constraint
    printf("✓ Unique constraint violation detected correctly\n");

    /* Update user - indexes should update */
    printf("Updating user with indexes active...\n");
    user_t updated_user2 = {2, "Bob Jones-Updated", "bob.new@example.com", 26, 1};
    make_user_key(2, key_buf, &key_len);
    rc = wtree3_update(users_tree, key_buf, key_len, &updated_user2, sizeof(updated_user2), &error);
    assert_int_equal(rc, WTREE3_OK);
    printf("✓ Updated user:2 with automatic index maintenance\n");

    /* Upsert with indexes */
    printf("Upserting with indexes active...\n");
    user_t user8 = {8, "Grace Lee", "grace@example.com", 27, 1};
    make_user_key(8, key_buf, &key_len);
    rc = wtree3_upsert(users_tree, key_buf, key_len, &user8, sizeof(user8), &error);
    assert_int_equal(rc, WTREE3_OK);
    assert_int_equal(wtree3_tree_count(users_tree), 6);
    printf("✓ Upserted new user:8\n");

    /* Delete with indexes */
    printf("Deleting user with indexes active...\n");
    make_user_key(4, key_buf, &key_len);
    rc = wtree3_delete_one(users_tree, key_buf, key_len, &deleted, &error);
    assert_int_equal(rc, WTREE3_OK);
    assert_true(deleted);
    assert_int_equal(wtree3_tree_count(users_tree), 5);
    printf("✓ Deleted user:4 with automatic index cleanup\n");

    /* Insert product without category (sparse index test) */
    printf("Testing sparse index with missing field...\n");
    product_t prod5 = {5, "Another Mystery", "", 7.50, 5};  // No category
    make_product_key(5, key_buf, &key_len);
    rc = wtree3_insert_one(products_tree, key_buf, key_len, &prod5, sizeof(prod5), &error);
    assert_int_equal(rc, WTREE3_OK);
    printf("✓ Inserted product without category (sparse index skip)\n");

    /* ============================================================
     * PHASE 6: Index Queries
     * ============================================================ */
    printf("\n=== PHASE 6: Index Queries ===\n");

    /* Query by unique email index */
    printf("Querying by email index...\n");
    wtree3_iterator_t *idx_iter = wtree3_index_seek(users_tree, "email_idx",
                                                     "frank@example.com",
                                                     strlen("frank@example.com") + 1,
                                                     &error);
    assert_non_null(idx_iter);
    assert_true(wtree3_iterator_valid(idx_iter));

    const void *main_key_ptr;
    size_t main_key_len;
    bool has_key = wtree3_index_iterator_main_key(idx_iter, &main_key_ptr, &main_key_len);
    assert_true(has_key);

    /* Get the actual user data using main key */
    wtree3_txn_t *txn = wtree3_iterator_get_txn(idx_iter);
    const void *user_value_ptr;
    size_t user_value_len;
    rc = wtree3_get_txn(txn, users_tree, main_key_ptr, main_key_len,
                        &user_value_ptr, &user_value_len, &error);
    assert_int_equal(rc, WTREE3_OK);
    const user_t *found_user = (const user_t*)user_value_ptr;
    assert_string_equal(found_user->name, "Frank Wilson");
    printf("✓ Found user by email: %s\n", found_user->name);
    wtree3_iterator_close(idx_iter);

    /* Query by non-unique name index (should find Alice) */
    printf("Querying by name index (non-unique)...\n");
    idx_iter = wtree3_index_seek(users_tree, "name_idx",
                                  "Alice Smith-Updated",
                                  strlen("Alice Smith-Updated") + 1,
                                  &error);
    assert_non_null(idx_iter);
    assert_true(wtree3_iterator_valid(idx_iter));

    has_key = wtree3_index_iterator_main_key(idx_iter, &main_key_ptr, &main_key_len);
    assert_true(has_key);
    txn = wtree3_iterator_get_txn(idx_iter);
    rc = wtree3_get_txn(txn, users_tree, main_key_ptr, main_key_len,
                        &user_value_ptr, &user_value_len, &error);
    assert_int_equal(rc, WTREE3_OK);
    found_user = (const user_t*)user_value_ptr;
    assert_string_equal(found_user->email, "alice.new@example.com");
    printf("✓ Found user by name: %s\n", found_user->name);
    wtree3_iterator_close(idx_iter);

    /* Query sparse index for category */
    printf("Querying sparse category index...\n");
    idx_iter = wtree3_index_seek(products_tree, "category_idx",
                                  "Electronics", strlen("Electronics") + 1,
                                  &error);
    assert_non_null(idx_iter);
    assert_true(wtree3_iterator_valid(idx_iter));

    has_key = wtree3_index_iterator_main_key(idx_iter, &main_key_ptr, &main_key_len);
    assert_true(has_key);
    txn = wtree3_iterator_get_txn(idx_iter);
    const void *prod_value_ptr;
    size_t prod_value_len;
    rc = wtree3_get_txn(txn, products_tree, main_key_ptr, main_key_len,
                        &prod_value_ptr, &prod_value_len, &error);
    assert_int_equal(rc, WTREE3_OK);
    const product_t *found_prod = (const product_t*)prod_value_ptr;
    assert_string_equal(found_prod->name, "Laptop");
    printf("✓ Found product by category: %s\n", found_prod->name);
    wtree3_iterator_close(idx_iter);

    /* ============================================================
     * PHASE 7: Iterators and Scans
     * ============================================================ */
    printf("\n=== PHASE 7: Iterators and Scans ===\n");

    /* Forward iteration */
    printf("Testing forward iteration...\n");
    wtree3_iterator_t *iter = wtree3_iterator_create(users_tree, &error);
    assert_non_null(iter);

    int count = 0;
    for (bool valid = wtree3_iterator_first(iter); valid; valid = wtree3_iterator_next(iter)) {
        const void *k, *v;
        size_t klen, vlen;
        wtree3_iterator_key(iter, &k, &klen);
        wtree3_iterator_value(iter, &v, &vlen);
        count++;
    }
    assert_int_equal(count, 5);  // Should have 5 users now
    printf("✓ Forward iteration: %d users\n", count);
    wtree3_iterator_close(iter);

    /* Reverse iteration */
    printf("Testing reverse iteration...\n");
    iter = wtree3_iterator_create(users_tree, &error);
    assert_non_null(iter);

    count = 0;
    for (bool valid = wtree3_iterator_last(iter); valid; valid = wtree3_iterator_prev(iter)) {
        count++;
    }
    assert_int_equal(count, 5);
    printf("✓ Reverse iteration: %d users\n", count);
    wtree3_iterator_close(iter);

    /* Seek to specific key */
    printf("Testing seek operation...\n");
    iter = wtree3_iterator_create(users_tree, &error);
    make_user_key(2, key_buf, &key_len);
    bool found = wtree3_iterator_seek(iter, key_buf, key_len);
    assert_true(found);

    const void *found_value;
    size_t found_len;
    wtree3_iterator_value(iter, &found_value, &found_len);
    const user_t *seeked_user = (const user_t*)found_value;
    assert_string_equal(seeked_user->name, "Bob Jones-Updated");
    printf("✓ Seek found: %s\n", seeked_user->name);
    wtree3_iterator_close(iter);

    /* Range scan with callback */
    printf("Testing range scan with callback...\n");

    typedef struct {
        int count;
    } scan_context_t;

    scan_context_t scan_ctx = {0};

    bool scan_callback(const void *k, size_t klen, const void *v, size_t vlen, void *user_data) {
        (void)k; (void)klen; (void)v; (void)vlen;
        scan_context_t *ctx = (scan_context_t*)user_data;
        ctx->count++;
        return true;  // Continue
    }

    txn = wtree3_txn_begin(db, false, &error);
    assert_non_null(txn);
    rc = wtree3_scan_range_txn(txn, users_tree, NULL, 0, NULL, 0,
                                scan_callback, &scan_ctx, &error);
    assert_int_equal(rc, WTREE3_OK);
    wtree3_txn_abort(txn);
    assert_int_equal(scan_ctx.count, 5);
    printf("✓ Range scan counted %d users\n", scan_ctx.count);

    /* Prefix scan */
    printf("Testing prefix scan...\n");
    scan_ctx.count = 0;
    const char *prefix = "user:";
    txn = wtree3_txn_begin(db, false, &error);
    rc = wtree3_scan_prefix_txn(txn, users_tree, prefix, strlen(prefix),
                                 scan_callback, &scan_ctx, &error);
    assert_int_equal(rc, WTREE3_OK);
    wtree3_txn_abort(txn);
    assert_int_equal(scan_ctx.count, 5);
    printf("✓ Prefix scan found %d matching users\n", scan_ctx.count);

    /* ============================================================
     * PHASE 8: Advanced Operations
     * ============================================================ */
    printf("\n=== PHASE 8: Advanced Operations ===\n");

    /* Batch insert */
    printf("Testing batch insert...\n");
    user_t user9 = {9, "Hannah Davis", "hannah@example.com", 32, 1};
    user_t user10 = {10, "Ian Foster", "ian@example.com", 29, 1};

    char key9[64], key10[64];
    size_t key9_len, key10_len;
    make_user_key(9, key9, &key9_len);
    make_user_key(10, key10, &key10_len);

    wtree3_kv_t kvs[] = {
        {key9, key9_len, &user9, sizeof(user9)},
        {key10, key10_len, &user10, sizeof(user10)}
    };

    txn = wtree3_txn_begin(db, true, &error);
    assert_non_null(txn);
    rc = wtree3_insert_many_txn(txn, users_tree, kvs, 2, &error);
    assert_int_equal(rc, WTREE3_OK);
    rc = wtree3_txn_commit(txn, &error);
    assert_int_equal(rc, WTREE3_OK);
    assert_int_equal(wtree3_tree_count(users_tree), 7);
    printf("✓ Batch inserted 2 users (now %d total)\n", (int)wtree3_tree_count(users_tree));

    /* Batch read */
    printf("Testing batch read...\n");
    wtree3_kv_t read_keys[] = {
        {key9, key9_len, NULL, 0},
        {key10, key10_len, NULL, 0}
    };
    const void *values[2];
    size_t value_lens[2];

    txn = wtree3_txn_begin(db, false, &error);
    rc = wtree3_get_many_txn(txn, users_tree, read_keys, 2, values, value_lens, &error);
    assert_int_equal(rc, WTREE3_OK);
    assert_non_null(values[0]);
    assert_non_null(values[1]);
    const user_t *u9 = (const user_t*)values[0];
    const user_t *u10 = (const user_t*)values[1];
    assert_string_equal(u9->name, "Hannah Davis");
    assert_string_equal(u10->name, "Ian Foster");
    wtree3_txn_abort(txn);
    printf("✓ Batch read 2 users successfully\n");

    /* Conditional delete */
    printf("Testing conditional delete (delete inactive users)...\n");

    bool delete_predicate(const void *k, size_t klen, const void *v, size_t vlen, void *user_data) {
        (void)k; (void)klen; (void)user_data;
        if (vlen < sizeof(user_t)) return false;
        const user_t *u = (const user_t*)v;
        return u->active == 0;  // Delete inactive users
    }

    size_t deleted_count = 0;
    txn = wtree3_txn_begin(db, true, &error);
    rc = wtree3_delete_if_txn(txn, users_tree, NULL, 0, NULL, 0,
                               delete_predicate, NULL, &deleted_count, &error);
    assert_int_equal(rc, WTREE3_OK);
    rc = wtree3_txn_commit(txn, &error);
    assert_int_equal(rc, WTREE3_OK);
    printf("✓ Conditional delete removed %d inactive users\n", (int)deleted_count);

    /* Collect range */
    printf("Testing collect range...\n");
    void **collected_keys = NULL;
    size_t *collected_key_lens = NULL;
    void **collected_values = NULL;
    size_t *collected_value_lens = NULL;
    size_t collected_count = 0;

    txn = wtree3_txn_begin(db, false, &error);
    rc = wtree3_collect_range_txn(txn, users_tree, NULL, 0, NULL, 0,
                                   NULL, NULL,
                                   &collected_keys, &collected_key_lens,
                                   &collected_values, &collected_value_lens,
                                   &collected_count, 3, &error);  // Max 3
    assert_int_equal(rc, WTREE3_OK);
    wtree3_txn_abort(txn);
    assert_int_equal(collected_count, 3);

    /* Free collected data */
    for (size_t i = 0; i < collected_count; i++) {
        free(collected_keys[i]);
        free(collected_values[i]);
    }
    free(collected_keys);
    free(collected_key_lens);
    free(collected_values);
    free(collected_value_lens);
    printf("✓ Collected %d entries\n", (int)collected_count);

    /* Verify index consistency */
    printf("Testing index verification...\n");
    rc = wtree3_verify_indexes(users_tree, &error);
    assert_int_equal(rc, WTREE3_OK);
    printf("✓ All indexes verified consistent\n");

    /* ============================================================
     * PHASE 9: Database Close and Reopen (Persistence Test)
     * ============================================================ */
    printf("\n=== PHASE 9: Close and Reopen Database (Persistence) ===\n");

    int64_t users_count_before = wtree3_tree_count(users_tree);
    int64_t products_count_before = wtree3_tree_count(products_tree);

    printf("Closing trees and database...\n");
    wtree3_tree_close(users_tree);
    wtree3_tree_close(products_tree);
    wtree3_tree_close(orders_tree);
    wtree3_db_close(db);
    printf("✓ Database closed\n");

    printf("\nReopening database...\n");
    db = wtree3_db_open(test_db_path, 128 * 1024 * 1024, 64,
                        TEST_VERSION, 0, &error);
    assert_non_null(db);
    printf("✓ Database reopened\n");

    /* Re-register extractors */
    printf("Re-registering extractors...\n");
    wtree3_db_register_key_extractor(db, TEST_VERSION, 0x01, user_email_extractor, &error);
    wtree3_db_register_key_extractor(db, TEST_VERSION, 0x00, user_name_extractor, &error);
    wtree3_db_register_key_extractor(db, TEST_VERSION, 0x02, product_category_extractor, &error);

    /* Reopen trees - indexes should auto-load */
    printf("Reopening trees (indexes should auto-load)...\n");
    users_tree = wtree3_tree_open(db, "users", 0, users_count_before, &error);
    assert_non_null(users_tree);
    printf("✓ Reopened users tree\n");

    products_tree = wtree3_tree_open(db, "products", 0, products_count_before, &error);
    assert_non_null(products_tree);
    printf("✓ Reopened products tree\n");

    orders_tree = wtree3_tree_open(db, "orders", 0, 2, &error);
    assert_non_null(orders_tree);
    printf("✓ Reopened orders tree\n");

    /* Verify data persisted */
    printf("\nVerifying persisted data...\n");
    assert_int_equal(wtree3_tree_count(users_tree), users_count_before);
    assert_int_equal(wtree3_tree_count(products_tree), products_count_before);
    printf("✓ Entry counts match\n");

    /* Verify indexes auto-loaded */
    assert_int_equal(wtree3_tree_index_count(users_tree), 2);
    assert_int_equal(wtree3_tree_index_count(products_tree), 1);
    assert_true(wtree3_tree_has_index(users_tree, "email_idx"));
    assert_true(wtree3_tree_has_index(users_tree, "name_idx"));
    assert_true(wtree3_tree_has_index(products_tree, "category_idx"));
    printf("✓ Indexes auto-loaded from metadata\n");

    /* Test that indexes work after reload */
    printf("Testing index queries after reload...\n");
    idx_iter = wtree3_index_seek(users_tree, "email_idx",
                                  "hannah@example.com",
                                  strlen("hannah@example.com") + 1,
                                  &error);
    assert_non_null(idx_iter);
    assert_true(wtree3_iterator_valid(idx_iter));

    has_key = wtree3_index_iterator_main_key(idx_iter, &main_key_ptr, &main_key_len);
    assert_true(has_key);
    txn = wtree3_iterator_get_txn(idx_iter);
    rc = wtree3_get_txn(txn, users_tree, main_key_ptr, main_key_len,
                        &user_value_ptr, &user_value_len, &error);
    assert_int_equal(rc, WTREE3_OK);
    found_user = (const user_t*)user_value_ptr;
    assert_string_equal(found_user->name, "Hannah Davis");
    printf("✓ Index query works after reload: found %s\n", found_user->name);
    wtree3_iterator_close(idx_iter);

    /* Insert new data with persisted indexes */
    printf("Inserting new data with persisted indexes...\n");
    user_t user11 = {11, "Jack Smith", "jack@example.com", 33, 1};
    make_user_key(11, key_buf, &key_len);
    rc = wtree3_insert_one(users_tree, key_buf, key_len, &user11, sizeof(user11), &error);
    assert_int_equal(rc, WTREE3_OK);
    printf("✓ New insert works with persisted indexes\n");

    /* Verify new data is indexed */
    idx_iter = wtree3_index_seek(users_tree, "email_idx",
                                  "jack@example.com",
                                  strlen("jack@example.com") + 1,
                                  &error);
    assert_non_null(idx_iter);
    assert_true(wtree3_iterator_valid(idx_iter));
    printf("✓ Newly inserted data is indexed correctly\n");
    wtree3_iterator_close(idx_iter);

    /* ============================================================
     * PHASE 10: Tree Deletion and Index Cleanup
     * ============================================================ */
    printf("\n=== PHASE 10: Tree Deletion with Index Cleanup ===\n");

    /* Delete products tree (should remove category_idx) */
    printf("Deleting products tree (with index)...\n");
    wtree3_tree_close(products_tree);
    rc = wtree3_tree_delete(db, "products", &error);
    assert_int_equal(rc, WTREE3_OK);
    printf("✓ Deleted products tree\n");

    /* Verify products tree no longer exists */
    rc = wtree3_tree_exists(db, "products", &error);
    assert_int_equal(rc, 0);
    printf("✓ Products tree verified deleted\n");

    /* Verify products index tree is also deleted */
    rc = wtree3_tree_exists(db, "idx:products:category_idx", &error);
    assert_int_equal(rc, 0);
    printf("✓ Category index tree verified deleted\n");

    /* Delete users tree (should remove both email_idx and name_idx) */
    printf("Deleting users tree (with 2 indexes)...\n");
    wtree3_tree_close(users_tree);
    rc = wtree3_tree_delete(db, "users", &error);
    assert_int_equal(rc, WTREE3_OK);
    printf("✓ Deleted users tree\n");

    /* Verify users index trees are deleted */
    rc = wtree3_tree_exists(db, "idx:users:email_idx", &error);
    assert_int_equal(rc, 0);
    rc = wtree3_tree_exists(db, "idx:users:name_idx", &error);
    assert_int_equal(rc, 0);
    printf("✓ Both user index trees verified deleted\n");

    /* Orders tree should still work */
    printf("Verifying orders tree still exists and works...\n");
    rc = wtree3_tree_exists(db, "orders", &error);
    assert_int_equal(rc, 1);
    assert_int_equal(wtree3_tree_count(orders_tree), 2);
    printf("✓ Orders tree still functional\n");

    /* Delete last tree */
    wtree3_tree_close(orders_tree);
    rc = wtree3_tree_delete(db, "orders", &error);
    assert_int_equal(rc, WTREE3_OK);
    printf("✓ Deleted orders tree\n");

    /* ============================================================
     * PHASE 11: Additional Coverage - Index Verification
     * ============================================================ */
    printf("\n=== PHASE 11: Index Verification (Extended) ===\n");

    /* Create a test tree for verification */
    wtree3_tree_t *verify_tree = wtree3_tree_open(db, "verify_test", MDB_CREATE, 0, &error);
    assert_non_null(verify_tree);

    /* Add an index */
    const char *verify_user_data = "test_field";
    wtree3_index_config_t verify_idx_config = {
        .name = "verify_idx",
        .user_data = verify_user_data,
        .user_data_len = strlen(verify_user_data) + 1,
        .unique = false,
        .sparse = false,
        .compare = NULL
    };
    rc = wtree3_tree_add_index(verify_tree, &verify_idx_config, &error);
    assert_int_equal(rc, WTREE3_OK);

    /* Add some data */
    user_t verify_user = {100, "Verify User", "verify@test.com", 25, 1};
    make_user_key(100, key_buf, &key_len);
    rc = wtree3_insert_one(verify_tree, key_buf, key_len, &verify_user, sizeof(verify_user), &error);
    assert_int_equal(rc, WTREE3_OK);

    /* Verify indexes are consistent */
    printf("Running comprehensive index verification...\n");
    rc = wtree3_verify_indexes(verify_tree, &error);
    assert_int_equal(rc, WTREE3_OK);
    printf("✓ Index verification passed\n");

    wtree3_tree_close(verify_tree);
    wtree3_tree_delete(db, "verify_test", &error);

    /* ============================================================
     * PHASE 12: Scan Reverse with Boundaries
     * ============================================================ */
    printf("\n=== PHASE 12: Scan Reverse with Boundaries ===\n");

    /* Create a fresh tree for reverse scan testing */
    wtree3_tree_t *scan_tree = wtree3_tree_open(db, "scan_test", MDB_CREATE, 0, &error);
    assert_non_null(scan_tree);

    /* Insert ordered data */
    for (int i = 1; i <= 10; i++) {
        user_t u = {i, "User", "email", i * 10, 1};
        snprintf(u.name, sizeof(u.name), "User%02d", i);
        snprintf(u.email, sizeof(u.email), "user%02d@test.com", i);
        make_user_key(i, key_buf, &key_len);
        wtree3_insert_one(scan_tree, key_buf, key_len, &u, sizeof(u), &error);
    }
    printf("✓ Inserted 10 ordered users\n");

    /* Test reverse scan with range boundaries */
    printf("Testing reverse scan with range [user:03, user:07]...\n");
    scan_ctx.count = 0;

    char start_key[64], end_key[64];
    size_t start_len, end_len;
    make_user_key(3, start_key, &start_len);
    make_user_key(7, end_key, &end_len);

    txn = wtree3_txn_begin(db, false, &error);
    assert_non_null(txn);
    rc = wtree3_scan_reverse_txn(txn, scan_tree, end_key, end_len, start_key, start_len,
                                  scan_callback, &scan_ctx, &error);
    assert_int_equal(rc, WTREE3_OK);
    wtree3_txn_abort(txn);
    assert_int_equal(scan_ctx.count, 5);  // Should scan 7, 6, 5, 4, 3
    printf("✓ Reverse scan with boundaries: %d entries\n", scan_ctx.count);

    /* Test reverse scan with NULL boundaries (full scan) */
    printf("Testing full reverse scan (NULL boundaries)...\n");
    scan_ctx.count = 0;
    txn = wtree3_txn_begin(db, false, &error);
    rc = wtree3_scan_reverse_txn(txn, scan_tree, NULL, 0, NULL, 0,
                                  scan_callback, &scan_ctx, &error);
    assert_int_equal(rc, WTREE3_OK);
    wtree3_txn_abort(txn);
    assert_int_equal(scan_ctx.count, 10);
    printf("✓ Full reverse scan: %d entries\n", scan_ctx.count);

    /* Test early termination in reverse scan */
    printf("Testing early termination in reverse scan...\n");

    bool early_stop_callback(const void *k, size_t klen, const void *v, size_t vlen, void *user_data) {
        (void)k; (void)klen; (void)v; (void)vlen;
        int *count = (int*)user_data;
        (*count)++;
        return (*count < 3);  // Stop after 3
    }

    int early_count = 0;
    txn = wtree3_txn_begin(db, false, &error);
    rc = wtree3_scan_reverse_txn(txn, scan_tree, NULL, 0, NULL, 0,
                                  early_stop_callback, &early_count, &error);
    assert_int_equal(rc, WTREE3_OK);
    wtree3_txn_abort(txn);
    assert_int_equal(early_count, 3);
    printf("✓ Early termination worked: stopped after %d entries\n", early_count);

    wtree3_tree_close(scan_tree);
    wtree3_tree_delete(db, "scan_test", &error);

    /* ============================================================
     * PHASE 13: Collect Range Edge Cases
     * ============================================================ */
    printf("\n=== PHASE 13: Collect Range Edge Cases ===\n");

    /* Create tree for collect testing */
    wtree3_tree_t *collect_tree = wtree3_tree_open(db, "collect_test", MDB_CREATE, 0, &error);
    assert_non_null(collect_tree);

    /* Insert 20 entries */
    for (int i = 1; i <= 20; i++) {
        user_t u = {i, "Collect", "collect@test.com", i, 1};
        snprintf(u.name, sizeof(u.name), "Collect%02d", i);
        make_user_key(i, key_buf, &key_len);
        wtree3_insert_one(collect_tree, key_buf, key_len, &u, sizeof(u), &error);
    }
    printf("✓ Inserted 20 users for collect testing\n");

    /* Test collect with max_count limit */
    printf("Testing collect with max_count=5...\n");
    void **coll_keys = NULL;
    size_t *coll_key_lens = NULL;
    void **coll_values = NULL;
    size_t *coll_value_lens = NULL;
    size_t coll_count = 0;

    txn = wtree3_txn_begin(db, false, &error);
    rc = wtree3_collect_range_txn(txn, collect_tree, NULL, 0, NULL, 0,
                                   NULL, NULL,
                                   &coll_keys, &coll_key_lens,
                                   &coll_values, &coll_value_lens,
                                   &coll_count, 5, &error);
    assert_int_equal(rc, WTREE3_OK);
    wtree3_txn_abort(txn);
    assert_int_equal(coll_count, 5);
    for (size_t i = 0; i < coll_count; i++) {
        free(coll_keys[i]);
        free(coll_values[i]);
    }
    free(coll_keys);
    free(coll_key_lens);
    free(coll_values);
    free(coll_value_lens);
    printf("✓ Collected %d entries (limited by max_count)\n", (int)coll_count);

    /* Test collect with predicate filter */
    printf("Testing collect with predicate (age > 10)...\n");

    bool age_filter(const void *k, size_t klen, const void *v, size_t vlen, void *user_data) {
        (void)k; (void)klen; (void)user_data;
        if (vlen < sizeof(user_t)) return false;
        const user_t *u = (const user_t*)v;
        return u->age > 10;
    }

    txn = wtree3_txn_begin(db, false, &error);
    rc = wtree3_collect_range_txn(txn, collect_tree, NULL, 0, NULL, 0,
                                   age_filter, NULL,
                                   &coll_keys, &coll_key_lens,
                                   &coll_values, &coll_value_lens,
                                   &coll_count, 0, &error);  // No limit
    assert_int_equal(rc, WTREE3_OK);
    wtree3_txn_abort(txn);
    assert_int_equal(coll_count, 10);  // Should collect age 11-20
    for (size_t i = 0; i < coll_count; i++) {
        free(coll_keys[i]);
        free(coll_values[i]);
    }
    free(coll_keys);
    free(coll_key_lens);
    free(coll_values);
    free(coll_value_lens);
    printf("✓ Collected %d entries matching predicate\n", (int)coll_count);

    /* Test collect with range boundaries */
    printf("Testing collect with range boundaries...\n");
    make_user_key(15, start_key, &start_len);
    make_user_key(18, end_key, &end_len);

    txn = wtree3_txn_begin(db, false, &error);
    rc = wtree3_collect_range_txn(txn, collect_tree, start_key, start_len, end_key, end_len,
                                   NULL, NULL,
                                   &coll_keys, &coll_key_lens,
                                   &coll_values, &coll_value_lens,
                                   &coll_count, 0, &error);
    assert_int_equal(rc, WTREE3_OK);
    wtree3_txn_abort(txn);
    assert_int_equal(coll_count, 4);  // Should collect 15-18 inclusive
    for (size_t i = 0; i < coll_count; i++) {
        free(coll_keys[i]);
        free(coll_values[i]);
    }
    free(coll_keys);
    free(coll_key_lens);
    free(coll_values);
    free(coll_value_lens);
    printf("✓ Collected %d entries in range\n", (int)coll_count);

    wtree3_tree_close(collect_tree);
    wtree3_tree_delete(db, "collect_test", &error);

    /* ============================================================
     * PHASE 14: Iterator Edge Cases
     * ============================================================ */
    printf("\n=== PHASE 14: Iterator Edge Cases ===\n");

    /* Create tree for iterator testing */
    wtree3_tree_t *iter_tree = wtree3_tree_open(db, "iter_test", MDB_CREATE, 0, &error);
    assert_non_null(iter_tree);

    /* Insert a few entries */
    for (int i = 1; i <= 5; i++) {
        user_t u = {i, "Iter", "iter@test.com", i, 1};
        make_user_key(i, key_buf, &key_len);
        wtree3_insert_one(iter_tree, key_buf, key_len, &u, sizeof(u), &error);
    }

    /* Test seek_range with non-existent key (should find next) */
    printf("Testing seek_range with non-existent key...\n");
    iter = wtree3_iterator_create(iter_tree, &error);
    assert_non_null(iter);

    char seek_key[64];
    snprintf(seek_key, sizeof(seek_key), "user:2.5");  // Between user:2 and user:3
    found = wtree3_iterator_seek_range(iter, seek_key, strlen(seek_key) + 1);
    assert_true(found);  // Should find user:3

    const void *iter_key;
    size_t iter_key_len;
    wtree3_iterator_key(iter, &iter_key, &iter_key_len);
    make_user_key(3, key_buf, &key_len);
    assert_memory_equal(iter_key, key_buf, key_len);
    printf("✓ seek_range found next key correctly\n");
    wtree3_iterator_close(iter);

    /* Test iterator key/value copy */
    printf("Testing iterator copy functions...\n");
    iter = wtree3_iterator_create(iter_tree, &error);
    wtree3_iterator_first(iter);

    void *copied_key = NULL;
    size_t copied_key_len = 0;
    void *copied_value = NULL;
    size_t copied_value_len = 0;

    bool copy_ok = wtree3_iterator_key_copy(iter, &copied_key, &copied_key_len);
    assert_true(copy_ok);
    assert_non_null(copied_key);

    copy_ok = wtree3_iterator_value_copy(iter, &copied_value, &copied_value_len);
    assert_true(copy_ok);
    assert_non_null(copied_value);

    free(copied_key);
    free(copied_value);
    printf("✓ Iterator copy functions work\n");
    wtree3_iterator_close(iter);

    /* Test iterator on empty tree after deletes */
    printf("Testing iterator on empty tree...\n");
    for (int i = 1; i <= 5; i++) {
        make_user_key(i, key_buf, &key_len);
        wtree3_delete_one(iter_tree, key_buf, key_len, &deleted, &error);
    }

    iter = wtree3_iterator_create(iter_tree, &error);
    assert_non_null(iter);
    bool has_first = wtree3_iterator_first(iter);
    assert_false(has_first);
    assert_false(wtree3_iterator_valid(iter));
    printf("✓ Iterator on empty tree handled correctly\n");
    wtree3_iterator_close(iter);

    wtree3_tree_close(iter_tree);
    wtree3_tree_delete(db, "iter_test", &error);

    /* ============================================================
     * PHASE 15: Atomic Modify Operations
     * ============================================================ */
    printf("\n=== PHASE 15: Atomic Modify Operations ===\n");

    /* Create tree for modify testing */
    wtree3_tree_t *modify_tree = wtree3_tree_open(db, "modify_test", MDB_CREATE, 0, &error);
    assert_non_null(modify_tree);

    /* Insert initial value */
    user_t mod_user = {1, "Original", "original@test.com", 20, 1};
    make_user_key(1, key_buf, &key_len);
    wtree3_insert_one(modify_tree, key_buf, key_len, &mod_user, sizeof(mod_user), &error);

    /* Test modify: increment age */
    printf("Testing atomic modify (increment age)...\n");

    void* increment_age_modifier(const void *existing_value, size_t existing_len,
                                  void *user_data, size_t *out_len) {
        (void)user_data;
        if (!existing_value || existing_len < sizeof(user_t)) return NULL;

        user_t *new_user = malloc(sizeof(user_t));
        if (!new_user) return NULL;

        memcpy(new_user, existing_value, sizeof(user_t));
        new_user->age += 1;
        *out_len = sizeof(user_t);
        return new_user;
    }

    txn = wtree3_txn_begin(db, true, &error);
    rc = wtree3_modify_txn(txn, modify_tree, key_buf, key_len,
                           increment_age_modifier, NULL, &error);
    assert_int_equal(rc, WTREE3_OK);
    wtree3_txn_commit(txn, &error);

    /* Verify modification */
    rc = wtree3_get(modify_tree, key_buf, key_len, &read_value, &read_len, &error);
    assert_int_equal(rc, WTREE3_OK);
    read_user = (user_t*)read_value;
    assert_int_equal(read_user->age, 21);
    free(read_value);
    printf("✓ Atomic modify incremented age: 20 → 21\n");

    /* Test modify: delete via returning NULL */
    printf("Testing atomic modify (delete by returning NULL)...\n");

    void* delete_modifier(const void *existing_value, size_t existing_len,
                          void *user_data, size_t *out_len) {
        (void)existing_value; (void)existing_len; (void)user_data; (void)out_len;
        return NULL;  // Returning NULL deletes the entry
    }

    txn = wtree3_txn_begin(db, true, &error);
    rc = wtree3_modify_txn(txn, modify_tree, key_buf, key_len,
                           delete_modifier, NULL, &error);
    assert_int_equal(rc, WTREE3_OK);
    wtree3_txn_commit(txn, &error);

    /* Verify deletion */
    exists = wtree3_exists(modify_tree, key_buf, key_len, &error);
    assert_false(exists);
    printf("✓ Atomic modify deleted entry\n");

    /* Test modify: insert via modify on non-existent key */
    printf("Testing atomic modify (insert new)...\n");

    void* insert_modifier(const void *existing_value, size_t existing_len,
                          void *user_data, size_t *out_len) {
        (void)existing_len;
        if (existing_value) return NULL;  // Should not happen

        user_t *new_user = malloc(sizeof(user_t));
        if (!new_user) return NULL;

        *new_user = *(user_t*)user_data;
        *out_len = sizeof(user_t);
        return new_user;
    }

    user_t insert_user = {2, "Inserted", "inserted@test.com", 25, 1};
    make_user_key(2, key_buf, &key_len);

    txn = wtree3_txn_begin(db, true, &error);
    rc = wtree3_modify_txn(txn, modify_tree, key_buf, key_len,
                           insert_modifier, &insert_user, &error);
    assert_int_equal(rc, WTREE3_OK);
    wtree3_txn_commit(txn, &error);

    /* Verify insertion */
    exists = wtree3_exists(modify_tree, key_buf, key_len, &error);
    assert_true(exists);
    printf("✓ Atomic modify inserted new entry\n");

    wtree3_tree_close(modify_tree);
    wtree3_tree_delete(db, "modify_test", &error);

    /* ============================================================
     * PHASE 16: Exists Many Batch Operation
     * ============================================================ */
    printf("\n=== PHASE 16: Exists Many (Batch Existence Check) ===\n");

    wtree3_tree_t *exists_tree = wtree3_tree_open(db, "exists_test", MDB_CREATE, 0, &error);
    assert_non_null(exists_tree);

    /* Insert some entries */
    for (int i = 1; i <= 10; i += 2) {  // Insert odd IDs only: 1, 3, 5, 7, 9
        user_t u = {i, "Exists", "exists@test.com", i, 1};
        make_user_key(i, key_buf, &key_len);
        wtree3_insert_one(exists_tree, key_buf, key_len, &u, sizeof(u), &error);
    }

    /* Check existence of multiple keys at once */
    printf("Testing batch existence check...\n");
    char keys_buf[10][64];
    size_t keys_lens[10];
    wtree3_kv_t check_keys[10];
    bool results[10];

    for (int i = 0; i < 10; i++) {
        make_user_key(i + 1, keys_buf[i], &keys_lens[i]);
        check_keys[i].key = keys_buf[i];
        check_keys[i].key_len = keys_lens[i];
        check_keys[i].value = NULL;
        check_keys[i].value_len = 0;
    }

    txn = wtree3_txn_begin(db, false, &error);
    rc = wtree3_exists_many_txn(txn, exists_tree, check_keys, 10, results, &error);
    assert_int_equal(rc, WTREE3_OK);
    wtree3_txn_abort(txn);

    /* Verify results: odd keys exist, even keys don't */
    for (int i = 0; i < 10; i++) {
        bool expected = ((i + 1) % 2 == 1);  // Odd numbers exist
        assert_int_equal(results[i], expected);
    }
    printf("✓ Batch existence check: 5 exist, 5 don't exist\n");

    wtree3_tree_close(exists_tree);
    wtree3_tree_delete(db, "exists_test", &error);

    /* ============================================================
     * PHASE 17: Cleanup
     * ============================================================ */
    printf("\n=== PHASE 17: Final Cleanup ===\n");

    /* Close database */
    wtree3_db_close(db);
    printf("✓ Database closed\n");

    /* Remove test directory */
    remove_directory_recursive(test_db_path);
    printf("✓ Test directory removed\n");

    printf("\n=== ALL TESTS PASSED ===\n");
    printf("Successfully tested:\n");
    printf("  ✓ Database open/close/reopen\n");
    printf("  ✓ Multiple tree creation\n");
    printf("  ✓ CRUD operations (insert, read, update, upsert, delete)\n");
    printf("  ✓ Index creation (unique, non-unique, sparse)\n");
    printf("  ✓ Index population\n");
    printf("  ✓ CRUD with automatic index maintenance\n");
    printf("  ✓ Unique constraint enforcement\n");
    printf("  ✓ Sparse index behavior\n");
    printf("  ✓ Index queries\n");
    printf("  ✓ Iterators (forward, reverse, seek, seek_range, copy)\n");
    printf("  ✓ Scans (range, prefix, reverse with boundaries)\n");
    printf("  ✓ Advanced operations (batch, bulk, conditional)\n");
    printf("  ✓ Index persistence and auto-loading\n");
    printf("  ✓ Tree deletion with index cleanup\n");
    printf("  ✓ Transaction handling\n");
    printf("  ✓ Index verification (comprehensive)\n");
    printf("  ✓ Scan reverse with boundaries and early termination\n");
    printf("  ✓ Collect range with limits, predicates, and ranges\n");
    printf("  ✓ Iterator edge cases (empty tree, seek_range)\n");
    printf("  ✓ Atomic modify operations (increment, delete, insert)\n");
    printf("  ✓ Batch existence checks\n");
}

/* ============================================================
 * Test Runner
 * ============================================================ */

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_full_integration),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
