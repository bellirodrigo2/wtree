# WTree3 Usage Examples

This document provides comprehensive examples for using the WTree3 library, from basic operations to advanced patterns.

## Table of Contents

1. [Quick Start](#quick-start)
2. [Database Lifecycle](#database-lifecycle)
3. [Basic CRUD Operations](#basic-crud-operations)
4. [Secondary Indexes](#secondary-indexes)
5. [Advanced Querying](#advanced-querying)
6. [Batch Operations](#batch-operations)
7. [Atomic Operations](#atomic-operations)
8. [Range Scans and Iteration](#range-scans-and-iteration)
9. [Memory Optimization](#memory-optimization)
10. [Error Handling](#error-handling)
11. [Multi-Collection Applications](#multi-collection-applications)
12. [Performance Tips](#performance-tips)

---

## Quick Start

### Minimal Example: User Management

```c
#include "wtree3.h"
#include <stdio.h>
#include <string.h>

typedef struct {
    int id;
    char name[64];
    char email[128];
    int age;
} user_t;

// Index key extractor for email field
bool extract_email(const void *value, size_t value_len,
                   void *user_data,
                   void **out_key, size_t *out_len) {
    const user_t *user = (const user_t *)value;
    size_t len = strlen(user->email);

    if (len == 0) return false;  // Sparse: skip empty emails

    *out_key = malloc(len + 1);
    memcpy(*out_key, user->email, len + 1);
    *out_len = len + 1;
    return true;
}

int main() {
    gerror_t error = {0};
    int rc;

    // 1. Open database
    wtree3_db_t *db = wtree3_db_open(
        "./userdb",                  // Directory path
        128 * 1024 * 1024,          // 128MB map size
        64,                          // Max 64 named trees
        WTREE3_VERSION(1, 0),       // Schema version
        0,                           // Flags (0 = defaults)
        &error
    );

    if (!db) {
        fprintf(stderr, "Failed to open DB: %s\n", error.message);
        return 1;
    }

    // 2. Register extractor for version 1.0, unique flag (0x01)
    rc = wtree3_db_register_key_extractor(
        db,
        WTREE3_VERSION(1, 0),
        0x01,  // Flags: unique=0x01, sparse=0x02
        extract_email,
        &error
    );

    // 3. Open/create "users" tree
    wtree3_tree_t *users = wtree3_tree_open(
        db,
        "users",
        MDB_CREATE,  // Create if doesn't exist
        0,           // Initial entry count (0 for new tree)
        &error
    );

    // 4. Add unique email index
    wtree3_index_config_t idx_config = {
        .name = "email_idx",
        .user_data = "email",
        .user_data_len = 6,
        .unique = true,
        .sparse = true,
        .compare = NULL,
        .dupsort_compare = NULL
    };

    rc = wtree3_tree_add_index(users, &idx_config, &error);

    // 5. Insert a user
    user_t alice = {1, "Alice Smith", "alice@example.com", 30};
    char key[32];
    snprintf(key, sizeof(key), "user:%d", alice.id);

    rc = wtree3_insert_one(
        users,
        key, strlen(key) + 1,
        &alice, sizeof(alice),
        &error
    );

    if (rc != WTREE3_OK) {
        fprintf(stderr, "Insert failed: %s\n", error.message);
    } else {
        printf("Inserted Alice (count: %ld)\n", wtree3_tree_count(users));
    }

    // 6. Query by email index
    wtree3_iterator_t *iter = wtree3_index_seek(
        users,
        "email_idx",
        "alice@example.com",
        strlen("alice@example.com") + 1,
        &error
    );

    if (wtree3_iterator_valid(iter)) {
        const void *main_key;
        size_t main_key_len;
        wtree3_index_iterator_main_key(iter, &main_key, &main_key_len);

        wtree3_txn_t *txn = wtree3_iterator_get_txn(iter);
        const void *value;
        size_t value_len;
        wtree3_get_txn(txn, users, main_key, main_key_len, &value, &value_len, &error);

        const user_t *found = (const user_t*)value;
        printf("Found by email: %s, age %d\n", found->name, found->age);
    }

    wtree3_iterator_close(iter);

    // 7. Cleanup
    wtree3_tree_close(users);
    wtree3_db_close(db);

    return 0;
}
```

---

## Database Lifecycle

### Opening and Configuring a Database

```c
#include "wtree3.h"

gerror_t error = {0};

// Open with custom settings
wtree3_db_t *db = wtree3_db_open(
    "/var/data/myapp",                    // Path (must exist)
    10UL * 1024 * 1024 * 1024,           // 10GB map size
    128,                                  // Max 128 DBs
    WTREE3_VERSION(2, 1),                // Schema version 2.1
    MDB_NOSYNC | MDB_WRITEMAP,           // LMDB flags (faster, riskier)
    &error
);

if (!db) {
    fprintf(stderr, "Error: %s\n", error.message);
    exit(1);
}

// Get current map size
size_t mapsize = wtree3_db_get_mapsize(db);
printf("Database map size: %zu MB\n", mapsize / (1024 * 1024));

// Get database stats
MDB_stat stat;
wtree3_db_stats(db, &stat, &error);
printf("Page size: %u, Depth: %u, Entries: %zu\n",
       stat.ms_psize, stat.ms_depth, stat.ms_entries);
```

### Handling Database Growth (Map Full)

```c
// Insert operation that might fill database
int rc = wtree3_insert_one(tree, key, klen, val, vlen, &error);

if (rc == WTREE3_MAP_FULL) {
    printf("Database full, resizing...\n");

    size_t current_size = wtree3_db_get_mapsize(db);
    size_t new_size = current_size + (1UL * 1024 * 1024 * 1024);  // +1GB

    rc = wtree3_db_resize(db, new_size, &error);
    if (rc == WTREE3_OK) {
        // Retry the insert
        rc = wtree3_insert_one(tree, key, klen, val, vlen, &error);
    }
}
```

### Closing and Syncing

```c
// Force sync to disk before closing
wtree3_db_sync(db, true, &error);

// Close trees first
wtree3_tree_close(users);
wtree3_tree_close(products);

// Then close database
wtree3_db_close(db);
```

---

## Basic CRUD Operations

### Create (Insert)

```c
// Simple insert
user_t user = {123, "Bob Jones", "bob@example.com", 28};
char key[32];
snprintf(key, sizeof(key), "user:%d", user.id);

int rc = wtree3_insert_one(users, key, strlen(key)+1, &user, sizeof(user), &error);

if (rc == WTREE3_KEY_EXISTS) {
    printf("User already exists\n");
} else if (rc == WTREE3_INDEX_ERROR) {
    printf("Duplicate email (unique constraint violation)\n");
} else if (rc == WTREE3_OK) {
    printf("User inserted successfully\n");
}
```

### Read (Get)

```c
void *value = NULL;
size_t value_len = 0;

int rc = wtree3_get(users, key, strlen(key)+1, &value, &value_len, &error);

if (rc == WTREE3_OK) {
    user_t *user = (user_t *)value;
    printf("Name: %s, Email: %s\n", user->name, user->email);
    free(value);  // MUST free - wtree3_get allocates
} else if (rc == WTREE3_NOT_FOUND) {
    printf("User not found\n");
}
```

### Update

```c
// Update existing user
user.age = 29;  // Birthday!

int rc = wtree3_update(users, key, strlen(key)+1, &user, sizeof(user), &error);

if (rc == WTREE3_NOT_FOUND) {
    printf("Cannot update - user doesn't exist\n");
} else if (rc == WTREE3_OK) {
    printf("User updated\n");
}
```

### Upsert (Insert or Update)

```c
// Upsert: insert if new, update if exists
user_t user = {456, "Charlie", "charlie@example.com", 35};
char key[32];
snprintf(key, sizeof(key), "user:%d", user.id);

int rc = wtree3_upsert(users, key, strlen(key)+1, &user, sizeof(user), &error);
// Always succeeds (unless index violation or other error)
```

### Delete

```c
bool deleted = false;

int rc = wtree3_delete_one(users, key, strlen(key)+1, &deleted, &error);

if (rc == WTREE3_OK) {
    if (deleted) {
        printf("User deleted (count: %ld)\n", wtree3_tree_count(users));
    } else {
        printf("User didn't exist\n");
    }
}
```

### Exists Check

```c
bool exists = wtree3_exists(users, key, strlen(key)+1, &error);

if (exists) {
    printf("User exists\n");
} else {
    printf("User does not exist\n");
}
```

---

## Secondary Indexes

### Non-Unique Index Example (Name)

```c
// Extractor for name field (non-unique)
bool extract_name(const void *value, size_t value_len,
                  void *user_data,
                  void **out_key, size_t *out_len) {
    const user_t *user = (const user_t *)value;
    size_t len = strlen(user->name);

    if (len == 0) return false;

    *out_key = malloc(len + 1);
    memcpy(*out_key, user->name, len + 1);
    *out_len = len + 1;
    return true;
}

// Register extractor for non-unique (flags = 0x00)
wtree3_db_register_key_extractor(db, WTREE3_VERSION(1, 0), 0x00, extract_name, &error);

// Add non-unique name index
wtree3_index_config_t name_config = {
    .name = "name_idx",
    .user_data = "name",
    .user_data_len = 5,
    .unique = false,        // Allow duplicates
    .sparse = false,
    .compare = NULL
};

wtree3_tree_add_index(users, &name_config, &error);

// Populate from existing data
wtree3_tree_populate_index(users, "name_idx", &error);
```

### Sparse Index Example (Optional Category)

```c
typedef struct {
    int id;
    char name[128];
    char category[64];  // May be empty
    double price;
} product_t;

// Extractor returns false for empty category
bool extract_category(const void *value, size_t value_len,
                      void *user_data,
                      void **out_key, size_t *out_len) {
    const product_t *product = (const product_t *)value;
    size_t len = strlen(product->category);

    // Sparse: return false if category is empty
    if (len == 0) return false;

    *out_key = malloc(len + 1);
    memcpy(*out_key, product->category, len + 1);
    *out_len = len + 1;
    return true;
}

// Register for sparse flag (0x02)
wtree3_db_register_key_extractor(db, WTREE3_VERSION(1, 0), 0x02, extract_category, &error);

// Add sparse index
wtree3_index_config_t cat_config = {
    .name = "category_idx",
    .user_data = NULL,
    .user_data_len = 0,
    .unique = false,
    .sparse = true,         // Skip entries without category
    .compare = NULL
};

wtree3_tree_add_index(products, &cat_config, &error);
```

### Querying by Index

```c
// Find all users named "Alice" (non-unique index)
wtree3_iterator_t *iter = wtree3_index_seek(
    users,
    "name_idx",
    "Alice",
    strlen("Alice") + 1,
    &error
);

wtree3_txn_t *txn = wtree3_iterator_get_txn(iter);

while (wtree3_iterator_valid(iter)) {
    // Get main tree key from index
    const void *main_key;
    size_t main_key_len;
    wtree3_index_iterator_main_key(iter, &main_key, &main_key_len);

    // Fetch user data
    const void *value;
    size_t value_len;
    wtree3_get_txn(txn, users, main_key, main_key_len, &value, &value_len, &error);

    const user_t *user = (const user_t*)value;
    printf("Found Alice: %s, age %d\n", user->email, user->age);

    // Move to next duplicate
    if (!wtree3_iterator_next(iter)) break;

    // Check if still on same index key
    const void *idx_key;
    size_t idx_key_len;
    wtree3_iterator_key(iter, &idx_key, &idx_key_len);
    if (strcmp((const char*)idx_key, "Alice") != 0) break;
}

wtree3_iterator_close(iter);
```

### Index Verification

```c
// Verify all indexes are consistent
int rc = wtree3_verify_indexes(users, &error);

if (rc == WTREE3_OK) {
    printf("All indexes consistent\n");
} else {
    printf("Index corruption detected: %s\n", error.message);
}
```

---

## Advanced Querying

### Range Query with Index

```c
// Find all users with emails between "alice" and "bob"
wtree3_iterator_t *iter = wtree3_index_seek_range(
    users,
    "email_idx",
    "alice",
    strlen("alice") + 1,
    &error
);

wtree3_txn_t *txn = wtree3_iterator_get_txn(iter);

while (wtree3_iterator_valid(iter)) {
    const void *email_key;
    size_t email_len;
    wtree3_iterator_key(iter, &email_key, &email_len);

    // Stop if past "bob"
    if (strcmp((const char*)email_key, "bob") > 0) break;

    // Get user data...
    printf("Email in range: %s\n", (const char*)email_key);

    wtree3_iterator_next(iter);
}

wtree3_iterator_close(iter);
```

### Prefix Scan

```c
// Find all users whose primary keys start with "user:admin:"
wtree3_txn_t *txn = wtree3_txn_begin(db, false, &error);

bool scan_callback(const void *k, size_t klen, const void *v, size_t vlen, void *ud) {
    int *count = (int*)ud;
    const user_t *user = (const user_t*)v;
    printf("Admin user: %s\n", user->name);
    (*count)++;
    return true;  // Continue
}

int count = 0;
const char *prefix = "user:admin:";

wtree3_scan_prefix_txn(
    txn,
    users,
    prefix, strlen(prefix),
    scan_callback,
    &count,
    &error
);

wtree3_txn_abort(txn);
printf("Found %d admin users\n", count);
```

---

## Batch Operations

### Batch Insert

```c
user_t users_data[] = {
    {100, "User 100", "u100@example.com", 25},
    {101, "User 101", "u101@example.com", 26},
    {102, "User 102", "u102@example.com", 27}
};

char keys[3][32];
wtree3_kv_t kvs[3];

for (int i = 0; i < 3; i++) {
    snprintf(keys[i], sizeof(keys[i]), "user:%d", users_data[i].id);
    kvs[i].key = keys[i];
    kvs[i].key_len = strlen(keys[i]) + 1;
    kvs[i].value = &users_data[i];
    kvs[i].value_len = sizeof(user_t);
}

// Single transaction for all inserts
wtree3_txn_t *txn = wtree3_txn_begin(db, true, &error);
int rc = wtree3_insert_many_txn(txn, users, kvs, 3, &error);

if (rc == WTREE3_OK) {
    wtree3_txn_commit(txn, &error);
    printf("Batch inserted 3 users\n");
} else {
    wtree3_txn_abort(txn);
    printf("Batch insert failed: %s\n", error.message);
}
```

### Batch Read

```c
char keys[3][32] = {"user:1", "user:2", "user:3"};
wtree3_kv_t read_keys[3];
const void *values[3];
size_t value_lens[3];

for (int i = 0; i < 3; i++) {
    read_keys[i].key = keys[i];
    read_keys[i].key_len = strlen(keys[i]) + 1;
}

wtree3_txn_t *txn = wtree3_txn_begin(db, false, &error);
int rc = wtree3_get_many_txn(txn, users, read_keys, 3, values, value_lens, &error);

for (int i = 0; i < 3; i++) {
    if (values[i]) {
        const user_t *user = (const user_t*)values[i];
        printf("User %d: %s\n", i+1, user->name);
    } else {
        printf("User %d: not found\n", i+1);
    }
}

wtree3_txn_abort(txn);
```

### Batch Existence Check

```c
char keys[5][32] = {"user:1", "user:2", "user:3", "user:999", "user:1000"};
wtree3_kv_t check_keys[5];
bool results[5];

for (int i = 0; i < 5; i++) {
    check_keys[i].key = keys[i];
    check_keys[i].key_len = strlen(keys[i]) + 1;
}

wtree3_txn_t *txn = wtree3_txn_begin(db, false, &error);
wtree3_exists_many_txn(txn, users, check_keys, 5, results, &error);
wtree3_txn_abort(txn);

for (int i = 0; i < 5; i++) {
    printf("%s: %s\n", keys[i], results[i] ? "exists" : "missing");
}
```

---

## Atomic Operations

### Atomic Counter Increment

```c
typedef struct {
    int count;
} counter_t;

void* increment_counter(const void *existing_value, size_t existing_len,
                        void *user_data, size_t *out_len) {
    counter_t *new = malloc(sizeof(counter_t));

    if (existing_value) {
        const counter_t *old = (const counter_t*)existing_value;
        new->count = old->count + 1;
    } else {
        new->count = 1;  // Initialize
    }

    *out_len = sizeof(counter_t);
    return new;
}

// Atomically increment counter
wtree3_txn_t *txn = wtree3_txn_begin(db, true, &error);
wtree3_modify_txn(txn, counters, "page_views", 11, increment_counter, NULL, &error);
wtree3_txn_commit(txn, &error);
```

### Conditional Bulk Delete

```c
// Delete all inactive users
bool is_inactive(const void *k, size_t klen, const void *v, size_t vlen, void *ud) {
    const user_t *user = (const user_t*)v;
    return user->age > *(int*)ud;  // Delete users older than threshold
}

int age_threshold = 65;
size_t deleted_count = 0;

wtree3_txn_t *txn = wtree3_txn_begin(db, true, &error);
wtree3_delete_if_txn(
    txn,
    users,
    NULL, 0,          // Start key (NULL = beginning)
    NULL, 0,          // End key (NULL = end)
    is_inactive,
    &age_threshold,
    &deleted_count,
    &error
);
wtree3_txn_commit(txn, &error);

printf("Deleted %zu users over age %d\n", deleted_count, age_threshold);
```

### Collect with Predicate

```c
// Collect all users between age 25 and 35
bool age_filter(const void *k, size_t klen, const void *v, size_t vlen, void *ud) {
    const user_t *user = (const user_t*)v;
    return user->age >= 25 && user->age <= 35;
}

void **keys = NULL;
size_t *key_lens = NULL;
void **values = NULL;
size_t *value_lens = NULL;
size_t count = 0;

wtree3_txn_t *txn = wtree3_txn_begin(db, false, &error);
wtree3_collect_range_txn(
    txn,
    users,
    NULL, 0, NULL, 0,  // Full range
    age_filter,
    NULL,
    &keys, &key_lens,
    &values, &value_lens,
    &count,
    100,  // Max 100 entries
    &error
);
wtree3_txn_abort(txn);

printf("Collected %zu users aged 25-35\n", count);

// Process collected data
for (size_t i = 0; i < count; i++) {
    const user_t *user = (const user_t*)values[i];
    printf("  %s, age %d\n", user->name, user->age);
    free(keys[i]);
    free(values[i]);
}

free(keys);
free(key_lens);
free(values);
free(value_lens);
```

---

## Range Scans and Iteration

### Forward Range Scan

```c
typedef struct {
    int count;
    int max;
} scan_ctx_t;

bool count_callback(const void *k, size_t klen, const void *v, size_t vlen, void *ud) {
    scan_ctx_t *ctx = (scan_ctx_t*)ud;
    ctx->count++;
    return ctx->count < ctx->max;  // Stop after max entries
}

scan_ctx_t ctx = {0, 10};

wtree3_txn_t *txn = wtree3_txn_begin(db, false, &error);
wtree3_scan_range_txn(
    txn,
    users,
    "user:100", 9,   // Start
    "user:200", 9,   // End
    count_callback,
    &ctx,
    &error
);
wtree3_txn_abort(txn);

printf("Scanned %d users (stopped at max %d)\n", ctx.count, ctx.max);
```

### Reverse Iteration

```c
wtree3_iterator_t *iter = wtree3_iterator_create(users, &error);

// Start from last entry
for (bool valid = wtree3_iterator_last(iter); valid; valid = wtree3_iterator_prev(iter)) {
    const void *key, *value;
    size_t key_len, value_len;

    wtree3_iterator_key(iter, &key, &key_len);
    wtree3_iterator_value(iter, &value, &value_len);

    const user_t *user = (const user_t*)value;
    printf("User (reverse): %s\n", user->name);
}

wtree3_iterator_close(iter);
```

### Iterator with Seek

```c
wtree3_iterator_t *iter = wtree3_iterator_create(users, &error);

// Seek to specific key
if (wtree3_iterator_seek(iter, "user:150", 9)) {
    const void *value;
    size_t value_len;
    wtree3_iterator_value(iter, &value, &value_len);
    const user_t *user = (const user_t*)value;
    printf("Found user:150 - %s\n", user->name);
}

// Seek to key or next greater
if (wtree3_iterator_seek_range(iter, "user:175", 9)) {
    const void *key;
    size_t key_len;
    wtree3_iterator_key(iter, &key, &key_len);
    printf("Found key >= user:175: %s\n", (const char*)key);
}

wtree3_iterator_close(iter);
```

---

## Memory Optimization

### Setting Access Pattern Hints

```c
// Hint: database will be accessed randomly (disable readahead)
wtree3_db_madvise(db, WTREE3_MADV_RANDOM, &error);

// Hint: database will be scanned sequentially (aggressive readahead)
wtree3_db_madvise(db, WTREE3_MADV_SEQUENTIAL, &error);

// Prefetch pages into cache
wtree3_db_madvise(db, WTREE3_MADV_WILLNEED, &error);
```

### Locking Database in RAM

```c
// Lock database in physical memory (prevent swapping)
// Requires elevated privileges on some systems
int rc = wtree3_db_mlock(db, WTREE3_MLOCK_CURRENT, &error);

if (rc == WTREE3_OK) {
    printf("Database locked in RAM\n");
} else {
    printf("Failed to lock: %s\n", error.message);
}

// Later: unlock to allow swapping
wtree3_db_munlock(db, &error);
```

### Prefetching Specific Ranges

```c
// Get memory map info
void *map_addr;
size_t map_size;
wtree3_db_get_mapinfo(db, &map_addr, &map_size, &error);

printf("Database mapped at %p, size %zu MB\n",
       map_addr, map_size / (1024 * 1024));

// Prefetch first 100MB into cache
wtree3_db_prefetch(db, 0, 100 * 1024 * 1024, &error);
```

---

## Error Handling

### Complete Error Handling Pattern

```c
gerror_t error = {0};
int rc = wtree3_insert_one(users, key, klen, value, vlen, &error);

switch (rc) {
    case WTREE3_OK:
        printf("Success\n");
        break;

    case WTREE3_KEY_EXISTS:
        printf("Key already exists in main tree\n");
        break;

    case WTREE3_INDEX_ERROR:
        printf("Index constraint violation: %s\n", error.message);
        break;

    case WTREE3_MAP_FULL:
        printf("Database full, need to resize\n");
        // Attempt resize and retry...
        break;

    case WTREE3_TXN_FULL:
        printf("Transaction too large, split into smaller batches\n");
        break;

    case WTREE3_ENOMEM:
        printf("Out of memory\n");
        break;

    default:
        printf("Error: %s (code %d)\n", error.message, rc);
        break;
}
```

### Checking Error Recoverability

```c
int rc = wtree3_insert_one(users, key, klen, value, vlen, &error);

if (rc != WTREE3_OK) {
    if (wtree3_error_recoverable(rc)) {
        printf("Recoverable error: %s\n", wtree3_strerror(rc));
        // Can retry or take corrective action
    } else {
        printf("Fatal error: %s\n", wtree3_strerror(rc));
        // Should abort operation
    }
}
```

---

## Multi-Collection Applications

### E-Commerce System with Multiple Trees

```c
// Product catalog
typedef struct {
    int id;
    char name[128];
    char category[64];
    double price;
    int stock;
} product_t;

// Customer orders
typedef struct {
    int order_id;
    int user_id;
    int product_id;
    int quantity;
    double total;
    int64_t timestamp;
} order_t;

// Open database
wtree3_db_t *db = wtree3_db_open("./ecommerce",
                                  512 * 1024 * 1024, 128,
                                  WTREE3_VERSION(1, 0), 0, &error);

// Register extractors
wtree3_db_register_key_extractor(db, WTREE3_VERSION(1, 0), 0x00,
                                 category_extractor, &error);
wtree3_db_register_key_extractor(db, WTREE3_VERSION(1, 0), 0x01,
                                 user_id_extractor, &error);

// Create multiple trees
wtree3_tree_t *products = wtree3_tree_open(db, "products", MDB_CREATE, 0, &error);
wtree3_tree_t *orders = wtree3_tree_open(db, "orders", MDB_CREATE, 0, &error);
wtree3_tree_t *users = wtree3_tree_open(db, "users", MDB_CREATE, 0, &error);

// Add indexes
wtree3_index_config_t product_category_idx = {
    .name = "category_idx",
    .user_data = "category",
    .user_data_len = 9,
    .unique = false,
    .sparse = true,
    .compare = NULL
};
wtree3_tree_add_index(products, &product_category_idx, &error);

wtree3_index_config_t order_user_idx = {
    .name = "user_idx",
    .user_data = "user_id",
    .user_data_len = 8,
    .unique = false,
    .sparse = false,
    .compare = NULL
};
wtree3_tree_add_index(orders, &order_user_idx, &error);

// Cross-tree transaction
wtree3_txn_t *txn = wtree3_txn_begin(db, true, &error);

// Reduce product stock
product_t product;
// ... get product, reduce stock ...
wtree3_update_txn(txn, products, product_key, pklen, &product, sizeof(product), &error);

// Create order
order_t order = {1001, 42, 123, 2, 199.98, time(NULL)};
wtree3_insert_one_txn(txn, orders, order_key, oklen, &order, sizeof(order), &error);

// Commit both changes atomically
wtree3_txn_commit(txn, &error);
```

---

## Performance Tips

### 1. Use Transactions Wisely

```c
// BAD: Many small transactions
for (int i = 0; i < 1000; i++) {
    wtree3_insert_one(tree, keys[i], klens[i], vals[i], vlens[i], &error);
}

// GOOD: Batch in single transaction
wtree3_txn_t *txn = wtree3_txn_begin(db, true, &error);
for (int i = 0; i < 1000; i++) {
    wtree3_insert_one_txn(txn, tree, keys[i], klens[i], vals[i], vlens[i], &error);
}
wtree3_txn_commit(txn, &error);

// BEST: Use batch insert
wtree3_txn_t *txn = wtree3_txn_begin(db, true, &error);
wtree3_insert_many_txn(txn, tree, kvs, 1000, &error);
wtree3_txn_commit(txn, &error);
```

### 2. Avoid Unnecessary Copies

```c
// BAD: Extra copy
void *value;
size_t value_len;
wtree3_get(tree, key, klen, &value, &value_len, &error);
user_t user_copy = *(user_t*)value;  // Copy
free(value);
use_user(&user_copy);

// GOOD: Use transaction for zero-copy
wtree3_txn_t *txn = wtree3_txn_begin(db, false, &error);
const void *value;
size_t value_len;
wtree3_get_txn(txn, tree, key, klen, &value, &value_len, &error);
use_user((const user_t*)value);  // Zero-copy, valid during txn
wtree3_txn_abort(txn);
```

### 3. Index Only What You Query

```c
// Don't create indexes "just in case"
// Each index adds overhead to insert/update/delete

// Create indexes based on actual query patterns
// Monitor query patterns and add indexes as needed
```

### 4. Use Sparse Indexes When Appropriate

```c
// If 90% of products don't have a "sale_end_date", use sparse index
// Saves space and maintains performance

wtree3_index_config_t sale_idx = {
    .name = "sale_end_idx",
    .sparse = true,  // Only index products with sale_end_date
    // ...
};
```

### 5. Size Database Appropriately

```c
// Start with generous mapsize (grows easily but doesn't shrink)
// For production: 2-5x expected dataset size
wtree3_db_t *db = wtree3_db_open(
    path,
    10UL * 1024 * 1024 * 1024,  // 10GB even if data is 2GB
    // ...
);

// Monitor usage
MDB_envinfo info;
mdb_env_info(wtree3_db_get_env(db), &info);
double usage = (double)info.me_last_pgno * info.me_mapsize /
               (info.me_mapsize * 100.0);
printf("Database %.1f%% full\n", usage);
```

---

## Conclusion

This guide covers the most common WTree3 usage patterns. For complete API reference, see the Doxygen-generated documentation. For implementation details and edge cases, consult the test suite in `tests/test_wtree3_full_integration.c`.

**Key Takeaways:**
- Always use transactions for consistency
- Leverage indexes for query performance
- Batch operations when possible
- Handle errors appropriately
- Size database generously
- Monitor and optimize based on real usage patterns
