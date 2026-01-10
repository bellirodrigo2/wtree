/**
 * @file wtree3.h
 * @brief Unified Storage Layer with Index Support - High-Performance LMDB Wrapper
 *
 * @mainpage WTree3 - Advanced Key-Value Storage with Secondary Indexes
 *
 * @section intro_sec Introduction
 *
 * WTree3 is a sophisticated, production-ready abstraction layer over LMDB that provides
 * a complete database solution with minimal overhead. It combines the performance of
 * LMDB with modern database features including automatic index maintenance, ACID
 * transactions, and advanced query capabilities.
 *
 * @section features_sec Key Features
 *
 * - **Zero-Copy Architecture**: Direct access to memory-mapped data without serialization overhead
 * - **ACID Transactions**: Full transactional support with MVCC (Multi-Version Concurrency Control)
 * - **Secondary Indexes**: Automatic maintenance of unique and non-unique indexes with sparse support
 * - **Named Collections**: Multiple independent trees (collections) within a single database
 * - **Advanced Operations**: Batch operations, range scans, prefix queries, atomic modify
 * - **Memory Optimization**: Fine-grained control over memory access patterns (madvise, mlock, prefetch)
 * - **Index Persistence**: Indexes are automatically saved and reloaded across database sessions
 * - **Entry Counting**: O(1) access to collection sizes without full scans
 *
 * @section arch_sec Architecture
 *
 * WTree3 is organized into three conceptual layers:
 *
 * @subsection layer_core Core Layer (Database & Transactions)
 * - Database environment management (wtree3_db_*)
 * - Transaction lifecycle (wtree3_txn_*)
 * - Memory optimization (madvise, mlock, prefetch)
 *
 * @subsection layer_trees Tree Layer (Collections)
 * - Named tree (collection) management (wtree3_tree_*)
 * - Basic CRUD operations (insert, update, upsert, delete, get)
 * - Iterators for sequential access
 *
 * @subsection layer_indexes Index Layer (Secondary Indexes)
 * - Index creation and population
 * - Automatic index maintenance during CRUD operations
 * - Index queries and verification
 * - Sparse and unique constraint support
 *
 * @section quick_start Quick Start Example
 *
 * @code{.c}
 * #include "wtree3.h"
 *
 * // 1. Define your data structure
 * typedef struct {
 *     int id;
 *     char email[128];
 *     char name[64];
 *     int age;
 * } user_t;
 *
 * // 2. Define index key extractor
 * bool email_extractor(const void *value, size_t value_len,
 *                      void *user_data,
 *                      void **out_key, size_t *out_len) {
 *     const user_t *user = (const user_t *)value;
 *     size_t len = strlen(user->email);
 *     if (len == 0) return false;  // Sparse: skip empty emails
 *
 *     *out_key = malloc(len + 1);
 *     memcpy(*out_key, user->email, len + 1);
 *     *out_len = len + 1;
 *     return true;
 * }
 *
 * int main() {
 *     gerror_t error = {0};
 *
 *     // 3. Open database
 *     wtree3_db_t *db = wtree3_db_open("./mydb",
 *                                       128 * 1024 * 1024,  // 128MB
 *                                       64,                  // max 64 trees
 *                                       WTREE3_VERSION(1, 0),
 *                                       0, &error);
 *
 *     // 4. Register extractor (library maintainer step)
 *     wtree3_db_register_key_extractor(db, WTREE3_VERSION(1, 0), 0x01,
 *                                      email_extractor, &error);
 *
 *     // 5. Open/create a tree (collection)
 *     wtree3_tree_t *users = wtree3_tree_open(db, "users", MDB_CREATE, 0, &error);
 *
 *     // 6. Add unique email index
 *     wtree3_index_config_t idx_config = {
 *         .name = "email_idx",
 *         .user_data = "email_field",
 *         .user_data_len = 12,
 *         .unique = true,
 *         .sparse = true,
 *         .compare = NULL
 *     };
 *     wtree3_tree_add_index(users, &idx_config, &error);
 *
 *     // 7. Insert data (index automatically updated)
 *     user_t user = {1, "alice@example.com", "Alice", 30};
 *     char key[32];
 *     snprintf(key, sizeof(key), "user:%d", user.id);
 *     wtree3_insert_one(users, key, strlen(key)+1, &user, sizeof(user), &error);
 *
 *     // 8. Query by index
 *     wtree3_iterator_t *iter = wtree3_index_seek(users, "email_idx",
 *                                                  "alice@example.com", 18, &error);
 *     if (wtree3_iterator_valid(iter)) {
 *         // Found! Get main tree key
 *         const void *main_key;
 *         size_t main_key_len;
 *         wtree3_index_iterator_main_key(iter, &main_key, &main_key_len);
 *
 *         // Fetch user data
 *         wtree3_txn_t *txn = wtree3_iterator_get_txn(iter);
 *         const void *value;
 *         size_t value_len;
 *         wtree3_get_txn(txn, users, main_key, main_key_len, &value, &value_len, &error);
 *
 *         const user_t *found = (const user_t*)value;
 *         printf("Found: %s, age %d\n", found->name, found->age);
 *     }
 *     wtree3_iterator_close(iter);
 *
 *     // 9. Cleanup
 *     wtree3_tree_close(users);
 *     wtree3_db_close(db);
 *     return 0;
 * }
 * @endcode
 *
 * @section persistence_sec Index Persistence
 *
 * Indexes are automatically persisted to a metadata database and reloaded on tree open:
 *
 * @code{.c}
 * // First run: Create and populate index
 * wtree3_tree_add_index(tree, &config, &error);
 * wtree3_tree_populate_index(tree, "email_idx", &error);
 * wtree3_tree_close(tree);
 * wtree3_db_close(db);
 *
 * // Later run: Index automatically reloaded
 * db = wtree3_db_open(...);
 * wtree3_db_register_key_extractor(db, ...);  // Must re-register extractors
 * tree = wtree3_tree_open(db, "users", 0, prev_count, &error);
 * // Index "email_idx" is now available without recreation!
 * @endcode
 *
 * @section advanced_sec Advanced Features
 *
 * @subsection scan_ops Scanning and Iteration
 * - wtree3_scan_range_txn(): Forward range scan with callback
 * - wtree3_scan_reverse_txn(): Reverse range scan
 * - wtree3_scan_prefix_txn(): Prefix-based scan
 * - wtree3_iterator_*(): Manual cursor-based iteration
 *
 * @subsection batch_ops Batch Operations
 * - wtree3_insert_many_txn(): Batch insert for performance
 * - wtree3_get_many_txn(): Batch read
 * - wtree3_exists_many_txn(): Batch existence check
 * - wtree3_collect_range_txn(): Collect range into arrays
 *
 * @subsection atomic_ops Atomic Operations
 * - wtree3_modify_txn(): Atomic read-modify-write
 * - wtree3_delete_if_txn(): Conditional bulk delete
 * - wtree3_upsert_txn(): Insert or update with custom merge
 *
 * @subsection mem_ops Memory Optimization
 * - wtree3_db_madvise(): Hint access patterns (random, sequential, willneed)
 * - wtree3_db_mlock(): Lock pages in RAM (prevent swapping)
 * - wtree3_db_prefetch(): Async prefetch for specific ranges
 *
 * @section error_sec Error Handling
 *
 * All functions that can fail return an error code and optionally populate a gerror_t:
 *
 * @code{.c}
 * gerror_t error = {0};
 * int rc = wtree3_insert_one(tree, key, klen, val, vlen, &error);
 * if (rc != WTREE3_OK) {
 *     fprintf(stderr, "Error: %s\n", error.message);
 *     if (rc == WTREE3_MAP_FULL) {
 *         // Recoverable: resize database
 *         wtree3_db_resize(db, new_size, &error);
 *     } else if (rc == WTREE3_INDEX_ERROR) {
 *         // Unique constraint violation
 *         fprintf(stderr, "Duplicate index key\n");
 *     }
 * }
 * @endcode
 *
 * @section thread_safety Thread Safety
 *
 * - Database (wtree3_db_t): Thread-safe, can be shared
 * - Transactions (wtree3_txn_t): NOT thread-safe, one per thread
 * - Trees (wtree3_tree_t): Thread-safe when used with proper transactions
 * - Iterators (wtree3_iterator_t): NOT thread-safe, single-threaded use only
 *
 * Read transactions can run concurrently. Write transactions are serialized by LMDB.
 *
 * @section perf_sec Performance Characteristics
 *
 * - **Insert**: O(log n) for main tree + O(log n) per index
 * - **Lookup**: O(log n) direct key, O(log n) index seek
 * - **Range Scan**: O(k) where k is result size (zero-copy)
 * - **Index Population**: O(n log n) where n is entry count
 * - **Tree Count**: O(1) maintained incrementally
 *
 * @section compat_sec Compatibility
 *
 * - **C Standard**: C99 or later
 * - **Platforms**: Linux, macOS, Windows, BSD
 * - **LMDB Version**: 0.9.14 or later recommended
 * - **Compilers**: GCC, Clang, MSVC
 *
 * @section license_sec License
 *
 * See project LICENSE file for details.
 *
 * @author WTree3 Development Team
 * @version 3.0
 */

#ifndef WTREE3_H
#define WTREE3_H

#include <lmdb.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include "gerror.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @defgroup error_codes Error Codes
 * @brief Return codes for WTree3 operations
 *
 * All error codes are negative integers in the range -3000 to -3099.
 * Error code 0 (WTREE3_OK) indicates success.
 *
 * @{
 */

/** @brief Success */
#define WTREE3_OK            0

/** @brief Generic error - check gerror_t message for details */
#define WTREE3_ERROR        -3000

/** @brief Invalid argument passed to function */
#define WTREE3_EINVAL       -3001

/** @brief Out of memory (malloc/calloc failed) */
#define WTREE3_ENOMEM       -3002

/** @brief Key already exists in main tree (MDB_KEYEXIST) */
#define WTREE3_KEY_EXISTS   -3003

/** @brief Key not found (MDB_NOTFOUND) */
#define WTREE3_NOT_FOUND    -3004

/**
 * @brief Database map is full, needs resize
 *
 * Recoverable error. Call wtree3_db_resize() with larger mapsize.
 */
#define WTREE3_MAP_FULL     -3005

/**
 * @brief Transaction has too many dirty pages
 *
 * Occurs when a single write transaction modifies too much data.
 * Split operation into smaller transactions.
 */
#define WTREE3_TXN_FULL     -3006

/**
 * @brief Index operation failed
 *
 * Common causes:
 * - Unique constraint violation during insert/update
 * - Index corruption detected during verification
 * - Extractor function failed or returned inconsistent results
 */
#define WTREE3_INDEX_ERROR  -3007

/** @} */ /* end of error_codes group */

/**
 * @defgroup opaque_types Opaque Handle Types
 * @brief Opaque pointers representing internal structures
 *
 * These types are intentionally opaque to enforce encapsulation.
 * Access to internal state is provided through documented API functions only.
 *
 * @{
 */

/**
 * @brief Database environment handle
 *
 * Represents an LMDB environment containing one or more named trees.
 * Thread-safe for concurrent access. Create with wtree3_db_open(),
 * destroy with wtree3_db_close().
 *
 * @see wtree3_db_open()
 * @see wtree3_db_close()
 */
typedef struct wtree3_db_t wtree3_db_t;

/**
 * @brief Transaction handle
 *
 * Represents a database transaction (read-only or read-write).
 * NOT thread-safe - each thread must have its own transaction.
 * Create with wtree3_txn_begin(), finalize with wtree3_txn_commit()
 * or wtree3_txn_abort().
 *
 * @see wtree3_txn_begin()
 * @see wtree3_txn_commit()
 * @see wtree3_txn_abort()
 */
typedef struct wtree3_txn_t wtree3_txn_t;

/**
 * @brief Tree (collection) handle
 *
 * Represents a named key-value collection within a database, optionally
 * with secondary indexes. Thread-safe when accessed through transactions.
 * Create with wtree3_tree_open(), destroy with wtree3_tree_close().
 *
 * @see wtree3_tree_open()
 * @see wtree3_tree_close()
 */
typedef struct wtree3_tree_t wtree3_tree_t;

/**
 * @brief Iterator (cursor) handle
 *
 * Provides sequential or seek-based access to tree entries.
 * NOT thread-safe - single-threaded use only. Can own its own
 * read transaction or share an existing one.
 *
 * @see wtree3_iterator_create()
 * @see wtree3_iterator_close()
 */
typedef struct wtree3_iterator_t wtree3_iterator_t;

/** @} */ /* end of opaque_types group */

/**
 * @defgroup callbacks Callback Function Types
 * @brief User-provided callback functions for custom behaviors
 * @{
 */

/**
 * @brief Index key extraction callback
 *
 * Called automatically during insert/update/delete to extract secondary index
 * keys from main tree values. This is the core mechanism for maintaining
 * secondary indexes.
 *
 * **Extraction Process:**
 * 1. WTree3 calls this function during CRUD operations
 * 2. Function inspects the value and extracts the index key
 * 3. Function allocates and returns the key (or returns false for sparse)
 * 4. WTree3 uses the key to update the index tree
 * 5. WTree3 frees the key after index update
 *
 * **Sparse Index Behavior:**
 * For sparse indexes, return `false` when the indexed field is missing or null.
 * The entry will be skipped in the index, saving space.
 *
 * **Memory Management:**
 * - The callback MUST allocate `out_key` using malloc()
 * - WTree3 will free the key after using it
 * - Return false if extraction fails or field is missing (sparse)
 *
 * @param value       Raw value bytes from main tree (e.g., serialized struct, BSON document)
 * @param value_len   Length of value in bytes
 * @param user_data   User context from wtree3_index_config_t (e.g., field name, path)
 * @param[out] out_key  Allocated key data (caller must malloc, WTree3 will free)
 * @param[out] out_len  Length of extracted key
 *
 * @return true if key extracted successfully (index this entry)
 * @return false if field missing/null (skip for sparse index)
 *
 * @see wtree3_tree_add_index()
 * @see wtree3_db_register_key_extractor()
 *
 * @par Example: Extract email field from user struct
 * @code{.c}
 * typedef struct { int id; char email[128]; } user_t;
 *
 * bool email_extractor(const void *value, size_t value_len,
 *                      void *user_data, void **out_key, size_t *out_len) {
 *     const user_t *user = (const user_t *)value;
 *     size_t email_len = strlen(user->email);
 *
 *     if (email_len == 0) return false;  // Sparse: skip empty
 *
 *     *out_key = malloc(email_len + 1);
 *     memcpy(*out_key, user->email, email_len + 1);
 *     *out_len = email_len + 1;
 *     return true;
 * }
 * @endcode
 */
typedef bool (*wtree3_index_key_fn)(
    const void *value,
    size_t value_len,
    void *user_data,
    void **out_key,
    size_t *out_len
);

/**
 * @brief Merge callback for upsert operations
 *
 * Called when wtree3_upsert() or wtree3_upsert_txn() encounters an existing key.
 * Allows custom merge logic instead of simple overwrite.
 *
 * **Common Use Cases:**
 * - Incrementing counters (merge: add new value to existing)
 * - Updating partial fields (merge: update specific struct fields)
 * - Conflict resolution (merge: choose newer timestamp)
 *
 * **Memory Management:**
 * - Function MUST allocate return value using malloc()
 * - WTree3 will free the merged value after writing
 * - Return NULL to abort the upsert operation
 *
 * @param existing_value Current value in database
 * @param existing_len   Current value length
 * @param new_value      New value being upserted
 * @param new_len        New value length
 * @param user_data      User context from wtree3_tree_set_merge_fn()
 * @param[out] out_len   Length of merged value
 *
 * @return Pointer to merged value (malloc'd, WTree3 frees)
 * @return NULL on error (upsert will fail)
 *
 * @note If no merge function is set, upsert simply overwrites with new_value
 *
 * @see wtree3_tree_set_merge_fn()
 * @see wtree3_upsert_txn()
 *
 * @par Example: Increment counter on upsert
 * @code{.c}
 * typedef struct { int count; } counter_t;
 *
 * void* merge_counter(const void *existing_value, size_t existing_len,
 *                     const void *new_value, size_t new_len,
 *                     void *user_data, size_t *out_len) {
 *     counter_t *merged = malloc(sizeof(counter_t));
 *     const counter_t *old = (const counter_t*)existing_value;
 *     const counter_t *new = (const counter_t*)new_value;
 *     merged->count = old->count + new->count;
 *     *out_len = sizeof(counter_t);
 *     return merged;
 * }
 * @endcode
 */
typedef void* (*wtree3_merge_fn)(
    const void *existing_value,
    size_t existing_len,
    const void *new_value,
    size_t new_len,
    void *user_data,
    size_t *out_len
);

/**
 * @brief Scan callback for range iteration
 *
 * Called for each key-value pair during range scan operations. Allows
 * processing entries without manual iterator management.
 *
 * **Zero-Copy Design:**
 * - Key and value pointers are valid ONLY during the callback
 * - Do NOT store these pointers - copy data if needed beyond callback
 * - Pointers become invalid after callback returns
 *
 * **Early Termination:**
 * Return `false` to stop the scan early. Useful for "find first N" queries.
 *
 * @param key       Current key (zero-copy, valid during callback only)
 * @param key_len   Key length
 * @param value     Current value (zero-copy, valid during callback only)
 * @param value_len Value length
 * @param user_data User context passed to scan function
 *
 * @return true to continue scanning
 * @return false to stop scanning (early termination)
 *
 * @see wtree3_scan_range_txn()
 * @see wtree3_scan_prefix_txn()
 *
 * @par Example: Find and print first 10 users
 * @code{.c}
 * typedef struct { int count; } scan_ctx_t;
 *
 * bool print_user(const void *k, size_t klen, const void *v, size_t vlen, void *ud) {
 *     scan_ctx_t *ctx = (scan_ctx_t*)ud;
 *     const user_t *user = (const user_t*)v;
 *     printf("User: %s\n", user->name);
 *     return ++ctx->count < 10;  // Stop after 10
 * }
 * @endcode
 */
typedef bool (*wtree3_scan_fn)(
    const void *key,
    size_t key_len,
    const void *value,
    size_t value_len,
    void *user_data
);

/**
 * @brief Modify callback for atomic read-modify-write operations
 *
 * Called by wtree3_modify_txn() to atomically transform a value within a transaction.
 * This is the preferred way to implement atomic updates (e.g., increment counter).
 *
 * **Operation Modes:**
 * - **Update**: existing_value is not NULL → return modified value
 * - **Insert**: existing_value is NULL → return new value (creates entry)
 * - **Delete**: existing_value is not NULL, return NULL → deletes entry
 * - **Abort**: existing_value is NULL, return NULL → no operation
 *
 * **Memory Management:**
 * - Function MUST allocate return value using malloc()
 * - WTree3 will free the returned value after writing
 * - Return NULL to delete (if exists) or abort (if doesn't exist)
 *
 * **Atomicity:**
 * The entire read-modify-write happens within a single transaction, ensuring
 * no other transaction can modify the value between read and write.
 *
 * @param existing_value Current value (NULL if key doesn't exist)
 * @param existing_len   Current value length (0 if key doesn't exist)
 * @param user_data      User context passed to wtree3_modify_txn()
 * @param[out] out_len   Length of returned value
 *
 * @return Pointer to new value (malloc'd, WTree3 frees)
 * @return NULL to delete existing entry or abort if no entry
 *
 * @see wtree3_modify_txn()
 *
 * @par Example: Atomically increment counter
 * @code{.c}
 * void* increment(const void *existing_value, size_t existing_len,
 *                 void *user_data, size_t *out_len) {
 *     counter_t *new = malloc(sizeof(counter_t));
 *     if (existing_value) {
 *         const counter_t *old = (const counter_t*)existing_value;
 *         new->count = old->count + 1;
 *     } else {
 *         new->count = 1;  // Initialize if doesn't exist
 *     }
 *     *out_len = sizeof(counter_t);
 *     return new;
 * }
 * @endcode
 */
typedef void* (*wtree3_modify_fn)(
    const void *existing_value,
    size_t existing_len,
    void *user_data,
    size_t *out_len
);

/**
 * @brief Predicate callback for conditional operations
 *
 * Used to test whether a key-value pair matches a condition. Powers
 * conditional bulk delete and filtered collection operations.
 *
 * **Zero-Copy Design:**
 * - Key and value pointers are valid ONLY during the callback
 * - Do NOT store these pointers - copy data if needed
 *
 * @param key       Current key (zero-copy, valid during callback only)
 * @param key_len   Key length
 * @param value     Current value (zero-copy, valid during callback only)
 * @param value_len Value length
 * @param user_data User context passed to operation
 *
 * @return true if predicate matches (delete/collect this entry)
 * @return false if predicate doesn't match (skip this entry)
 *
 * @see wtree3_delete_if_txn()
 * @see wtree3_collect_range_txn()
 *
 * @par Example: Delete inactive users
 * @code{.c}
 * bool is_inactive(const void *k, size_t klen, const void *v, size_t vlen, void *ud) {
 *     const user_t *user = (const user_t*)v;
 *     return user->active == 0;
 * }
 *
 * // Usage: wtree3_delete_if_txn(txn, tree, NULL, 0, NULL, 0, is_inactive, NULL, &deleted, &err);
 * @endcode
 */
typedef bool (*wtree3_predicate_fn)(
    const void *key,
    size_t key_len,
    const void *value,
    size_t value_len,
    void *user_data
);

/** @} */ /* end of callbacks group */

/**
 * @defgroup config_types Configuration Structures
 * @brief Configuration and data structures for WTree3 operations
 * @{
 */

/**
 * @brief Helper macro to build version identifier
 *
 * Combines major and minor version into a single 32-bit identifier.
 * Upper 16 bits: major version, Lower 16 bits: minor version.
 *
 * Used for schema versioning and extractor registration.
 *
 * @param major Major version number (0-65535)
 * @param minor Minor version number (0-65535)
 * @return 32-bit version identifier
 *
 * @par Example:
 * @code{.c}
 * uint32_t v1_0 = WTREE3_VERSION(1, 0);  // 0x00010000
 * uint32_t v2_5 = WTREE3_VERSION(2, 5);  // 0x00020005
 * @endcode
 */
#define WTREE3_VERSION(major, minor) \
    (((uint32_t)(major) << 16) | (uint16_t)(minor))

/**
 * @brief Index configuration structure
 *
 * Defines the properties of a secondary index including name, uniqueness,
 * sparseness, and custom comparators.
 *
 * **Index Naming:**
 * - Index name must be unique within a tree
 * - Internal tree name: `idx:<tree_name>:<index_name>`
 * - Examples: "email_idx", "name_1", "category_sparse"
 *
 * **User Data:**
 * - Persisted in metadata database
 * - Passed to extractor callback for context
 * - Can contain field names, paths, BSON specs, etc.
 *
 * **Unique vs Non-Unique:**
 * - Unique: Each index key can appear at most once (enforced)
 * - Non-Unique: Multiple entries can have same index key (allows duplicates)
 *
 * **Sparse vs Dense:**
 * - Sparse: Entries are indexed only if extractor returns true
 * - Dense: All entries must be indexed (extractor returning false is an error)
 *
 * @see wtree3_tree_add_index()
 * @see wtree3_index_key_fn
 */
typedef struct wtree3_index_config {
    /** Index name (e.g., "email_1", "category_idx") - must be unique per tree */
    const char *name;

    /** User context for extractor callback - persisted in metadata (can be NULL) */
    const void *user_data;

    /** Length of user_data in bytes (0 if user_data is NULL) */
    size_t user_data_len;

    /** Unique constraint - true: at most one entry per index key */
    bool unique;

    /** Sparse index - true: skip entries where extractor returns false */
    bool sparse;

    /** Custom key comparator function (NULL for lexicographic) */
    MDB_cmp_func *compare;

    /**
     * Custom duplicate value comparator (NULL for default)
     * Only applies to non-unique indexes
     */
    MDB_cmp_func *dupsort_compare;
} wtree3_index_config_t;

/**
 * @brief Key-value pair structure for batch operations
 *
 * Used in batch insert, batch read, and batch existence check operations.
 * Provides a convenient way to group multiple key-value pairs.
 *
 * **Usage Notes:**
 * - All pointers must remain valid for the duration of the operation
 * - For batch reads, value/value_len are outputs (pass NULL)
 * - For batch writes, all fields must be populated
 *
 * @see wtree3_insert_many_txn()
 * @see wtree3_get_many_txn()
 * @see wtree3_exists_many_txn()
 */
typedef struct wtree3_kv {
    const void *key;       /**< Key data */
    size_t key_len;        /**< Key length in bytes */
    const void *value;     /**< Value data (NULL for read operations) */
    size_t value_len;      /**< Value length in bytes (0 for read operations) */
} wtree3_kv_t;

/** @} */ /* end of config_types group */

/* ============================================================
 * Database Operations
 * ============================================================ */

/*
 * Open a database environment
 *
 * Parameters:
 *   path    - Directory path (must exist)
 *   mapsize - Maximum database size in bytes (can be grown later)
 *   max_dbs - Maximum number of named databases/trees (typically 128)
 *   version - Schema version (use WTREE3_VERSION macro, e.g., WTREE3_VERSION(1, 0))
 *   flags   - LMDB flags (MDB_RDONLY, MDB_NOSYNC, etc.)
 *   error   - Error output
 *
 * Returns: Database handle or NULL on error
 */
wtree3_db_t* wtree3_db_open(
    const char *path,
    size_t mapsize,
    unsigned int max_dbs,
    uint32_t version,
    unsigned int flags,
    gerror_t *error
);

/* Close database environment */
void wtree3_db_close(wtree3_db_t *db);

/* Sync database to disk */
int wtree3_db_sync(wtree3_db_t *db, bool force, gerror_t *error);

/* Resize the database map */
int wtree3_db_resize(wtree3_db_t *db, size_t new_mapsize, gerror_t *error);

/* Get current mapsize */
size_t wtree3_db_get_mapsize(wtree3_db_t *db);

/* Get database statistics */
int wtree3_db_stats(wtree3_db_t *db, MDB_stat *stat, gerror_t *error);

/* Get underlying LMDB environment (for advanced use) */
MDB_env* wtree3_db_get_env(wtree3_db_t *db);

/*
 * Register a key extractor function (library maintainer use only)
 *
 * Extractor functions must be registered for each version+flags combination.
 * This is typically done by the library maintainer after opening the database.
 *
 * Parameters:
 *   db      - Database handle
 *   version - Version identifier (use WTREE3_VERSION macro, e.g., WTREE3_VERSION(1, 0))
 *   flags   - Index flags combination (unique=0x01, sparse=0x02, etc.)
 *   key_fn  - Key extraction function for this version+flags combination
 *   error   - Error output
 *
 * Returns: 0 on success, error code on failure
 *
 * Example:
 *   // Register field extractor for v1.0, non-unique non-sparse indexes
 *   wtree3_db_register_key_extractor(db, WTREE3_VERSION(1, 0), 0x00, field_extractor_v1, &error);
 */
int wtree3_db_register_key_extractor(
    wtree3_db_t *db,
    uint32_t version,
    uint32_t flags,
    wtree3_index_key_fn key_fn,
    gerror_t *error
);

/* ============================================================
 * Memory Optimization API
 * ============================================================ */

/*
 * Memory access advice flags (portable abstraction over POSIX madvise)
 * Used with wtree3_db_madvise()
 */
#define WTREE3_MADV_NORMAL      0x00  /* Default behavior */
#define WTREE3_MADV_RANDOM      0x01  /* Expect random access (disable readahead) */
#define WTREE3_MADV_SEQUENTIAL  0x02  /* Expect sequential access (aggressive readahead) */
#define WTREE3_MADV_WILLNEED    0x04  /* Pages will be accessed soon (prefetch) */
#define WTREE3_MADV_DONTNEED    0x08  /* Pages won't be needed (can free) */

/*
 * Memory locking flags
 * Used with wtree3_db_mlock()
 */
#define WTREE3_MLOCK_CURRENT    0x01  /* Lock currently mapped pages */
#define WTREE3_MLOCK_FUTURE     0x02  /* Lock future mapped pages (POSIX only) */

/*
 * Apply memory access advice to database mapping
 *
 * Provides hints to the OS about expected access patterns.
 * Equivalent to POSIX madvise() but portable.
 *
 * @param db Database handle
 * @param advice Advice flags (WTREE3_MADV_*)
 * @param error Error output
 * @return WTREE3_OK on success, error code otherwise
 *
 * Platform support:
 * - Linux/BSD/macOS: Full support (uses madvise)
 * - Windows: Partial support (WILLNEED uses PrefetchVirtualMemory)
 * - Other: Returns error
 *
 * Common patterns:
 * - Random workloads: WTREE3_MADV_RANDOM (disables readahead)
 * - Sequential scans: WTREE3_MADV_SEQUENTIAL (aggressive readahead)
 * - Warming cache: WTREE3_MADV_WILLNEED (prefetch pages)
 */
int wtree3_db_madvise(wtree3_db_t *db, unsigned int advice, gerror_t *error);

/*
 * Lock database pages in physical memory (prevent swapping)
 *
 * Locks the database memory map in RAM, preventing it from being
 * swapped to disk. Useful for latency-sensitive applications.
 *
 * @param db Database handle
 * @param flags Locking flags (WTREE3_MLOCK_*)
 * @param error Error output
 * @return WTREE3_OK on success, error code otherwise
 *
 * Platform support:
 * - Linux/BSD/macOS: Full support (uses mlock/mlockall)
 * - Windows: Full support (uses VirtualLock)
 *
 * Notes:
 * - May require elevated privileges (CAP_IPC_LOCK on Linux)
 * - Locks only currently mapped region; grows with database
 * - Can degrade system performance if too much memory locked
 * - MLOCK_FUTURE flag uses mlockall(MCL_FUTURE) on POSIX
 *
 * Recommendation: Only lock if database < 50% of system RAM
 */
int wtree3_db_mlock(wtree3_db_t *db, unsigned int flags, gerror_t *error);

/*
 * Unlock database pages (allow swapping)
 *
 * Reverses the effect of wtree3_db_mlock(), allowing pages to be
 * swapped to disk again.
 *
 * @param db Database handle
 * @param error Error output
 * @return WTREE3_OK on success, error code otherwise
 *
 * Platform support:
 * - Linux/BSD/macOS: Full support (uses munlock)
 * - Windows: Full support (uses VirtualUnlock)
 */
int wtree3_db_munlock(wtree3_db_t *db, gerror_t *error);

/*
 * Get memory mapping information
 *
 * Returns the address and size of the database memory map.
 * Useful for advanced memory management or custom optimizations.
 *
 * @param db Database handle
 * @param out_addr Output: memory map address (can be NULL)
 * @param out_size Output: memory map size in bytes (can be NULL)
 * @param error Error output
 * @return WTREE3_OK on success, error code otherwise
 *
 * Platform support: All platforms (uses mdb_env_info)
 *
 * Note: The returned address is valid until database is closed or resized.
 */
int wtree3_db_get_mapinfo(wtree3_db_t *db,
                           void **out_addr,
                           size_t *out_size,
                           gerror_t *error);

/*
 * Prefetch a range of the database into cache
 *
 * Asynchronously brings pages into cache without blocking.
 * More granular than wtree3_db_madvise(WILLNEED).
 *
 * @param db Database handle
 * @param offset Byte offset from start of database
 * @param length Number of bytes to prefetch
 * @param error Error output
 * @return WTREE3_OK on success, error code otherwise
 *
 * Platform support:
 * - Linux/BSD/macOS: Uses madvise(MADV_WILLNEED) on range
 * - Windows: Uses PrefetchVirtualMemory (Windows 8+)
 *
 * Notes:
 * - Non-blocking, asynchronous operation
 * - Rounds offset/length to page boundaries
 * - Useful for warming specific sections before access
 */
int wtree3_db_prefetch(wtree3_db_t *db,
                       size_t offset,
                       size_t length,
                       gerror_t *error);

/* ============================================================
 * Transaction Operations
 * ============================================================ */

/*
 * Begin a transaction
 *
 * Parameters:
 *   db    - Database handle
 *   write - true for write transaction, false for read-only
 *   error - Error output
 *
 * Returns: Transaction handle or NULL on error
 */
wtree3_txn_t* wtree3_txn_begin(wtree3_db_t *db, bool write, gerror_t *error);

/* Commit transaction */
int wtree3_txn_commit(wtree3_txn_t *txn, gerror_t *error);

/* Abort transaction */
void wtree3_txn_abort(wtree3_txn_t *txn);

/* Reset read-only transaction (release snapshot, keep handle) */
void wtree3_txn_reset(wtree3_txn_t *txn);

/* Renew a reset read-only transaction */
int wtree3_txn_renew(wtree3_txn_t *txn, gerror_t *error);

/* Check if transaction is read-only */
bool wtree3_txn_is_readonly(wtree3_txn_t *txn);

/* Get underlying LMDB transaction (for advanced use) */
MDB_txn* wtree3_txn_get_mdb(wtree3_txn_t *txn);

/* Get parent database */
wtree3_db_t* wtree3_txn_get_db(wtree3_txn_t *txn);

/* ============================================================
 * Tree Operations
 * ============================================================ */

/*
 * Open/create a tree (named database)
 *
 * Parameters:
 *   db          - Database handle
 *   name        - Tree name (e.g., "users")
 *   flags       - LMDB flags (MDB_CREATE to create if not exists)
 *   entry_count - Initial entry count (0 for new, persisted value for existing)
 *   error       - Error output
 *
 * Note: This function automatically loads all persisted indexes for the tree
 *       using extractors registered via wtree3_db_register_key_extractor().
 *       Indexes with unregistered extractors will be skipped with a warning.
 *
 * Returns: Tree handle or NULL on error
 */
wtree3_tree_t* wtree3_tree_open(
    wtree3_db_t *db,
    const char *name,
    unsigned int flags,
    int64_t entry_count,
    gerror_t *error
);

/* Close tree handle (does not delete data) */
void wtree3_tree_close(wtree3_tree_t *tree);

/* Delete a tree and all its indexes */
int wtree3_tree_delete(wtree3_db_t *db, const char *name, gerror_t *error);

/* Get tree name */
const char* wtree3_tree_name(wtree3_tree_t *tree);

/* Get current entry count */
int64_t wtree3_tree_count(wtree3_tree_t *tree);

/* Get parent database from tree */
wtree3_db_t* wtree3_tree_get_db(wtree3_tree_t *tree);

/* Set custom key comparison function */
int wtree3_tree_set_compare(wtree3_tree_t *tree, MDB_cmp_func *cmp, gerror_t *error);

/* Set merge callback for upsert operations */
void wtree3_tree_set_merge_fn(wtree3_tree_t *tree, wtree3_merge_fn merge_fn, void *user_data);

/* ============================================================
 * Index Management
 * ============================================================ */

/*
 * Add an index to a tree
 *
 * Creates the index tree but does NOT populate it with existing data.
 * Call wtree3_tree_populate_index() after to build from existing entries.
 *
 * Index tree naming: idx:<tree_name>:<index_name>
 *
 * Returns: 0 on success, error code on failure
 */
int wtree3_tree_add_index(
    wtree3_tree_t *tree,
    const wtree3_index_config_t *config,
    gerror_t *error
);

/*
 * Populate an index from existing tree entries
 *
 * Scans all entries in the main tree and builds the index.
 * Use after wtree3_tree_add_index() for trees with existing data.
 *
 * Returns: 0 on success, WTREE3_INDEX_ERROR on unique violation
 */
int wtree3_tree_populate_index(
    wtree3_tree_t *tree,
    const char *index_name,
    gerror_t *error
);

/*
 * Drop an index from a tree
 */
int wtree3_tree_drop_index(
    wtree3_tree_t *tree,
    const char *index_name,
    gerror_t *error
);

/* Check if an index exists */
bool wtree3_tree_has_index(wtree3_tree_t *tree, const char *index_name);

/* Get index count */
size_t wtree3_tree_index_count(wtree3_tree_t *tree);

/*
 * Verify index consistency
 *
 * Performs integrity checks on all secondary indexes:
 * 1. Verifies all main tree entries appear in their indexes
 * 2. Verifies all index entries point to valid main tree entries
 * 3. Checks for orphaned index entries
 * 4. Validates unique constraints
 *
 * Returns: 0 if all indexes are consistent, WTREE3_INDEX_ERROR if inconsistencies found
 * The error parameter will contain details about the first inconsistency detected.
 */
int wtree3_verify_indexes(wtree3_tree_t *tree, gerror_t *error);

/*
 * Check if a tree/DB exists without creating it
 *
 * Returns: 1 if exists, 0 if doesn't exist, negative error code on failure
 */
int wtree3_tree_exists(wtree3_db_t *db, const char *name, gerror_t *error);

/* ============================================================
 * Index Introspection Operations
 * ============================================================ */

/*
 * Get extractor ID for a persisted index
 *
 * Reads the extractor ID from index metadata without loading the full index.
 * Useful for introspection and debugging.
 *
 * Parameters:
 *   tree              - Tree handle
 *   index_name        - Index name
 *   out_extractor_id  - Output: extractor ID from metadata
 *   error             - Error output
 *
 * Returns: 0 on success, WTREE3_NOT_FOUND if no metadata exists
 */
int wtree3_index_get_extractor_id(
    wtree3_tree_t *tree,
    const char *index_name,
    uint64_t *out_extractor_id,
    gerror_t *error
);

/* ============================================================
 * Data Operations (With Transaction)
 *
 * These operations automatically maintain all registered indexes.
 * ============================================================ */

/*
 * Get value by key (zero-copy)
 *
 * Returned value is valid only during transaction.
 *
 * Returns: 0 on success, WTREE3_NOT_FOUND if key doesn't exist
 */
int wtree3_get_txn(
    wtree3_txn_t *txn,
    wtree3_tree_t *tree,
    const void *key, size_t key_len,
    const void **value, size_t *value_len,
    gerror_t *error
);

/*
 * Insert a key-value pair
 *
 * Automatically:
 * - Checks unique constraints on all indexes
 * - Inserts into all index trees
 * - Increments entry count
 *
 * Returns: 0 on success, WTREE3_KEY_EXISTS on duplicate key,
 *          WTREE3_INDEX_ERROR on unique index violation
 */
int wtree3_insert_one_txn(
    wtree3_txn_t *txn,
    wtree3_tree_t *tree,
    const void *key, size_t key_len,
    const void *value, size_t value_len,
    gerror_t *error
);

/*
 * Update an existing key's value
 *
 * Automatically:
 * - Removes old index entries
 * - Checks unique constraints for new index keys
 * - Inserts new index entries
 *
 * Note: Does NOT change entry count (key already exists)
 *
 * Returns: 0 on success, WTREE3_NOT_FOUND if key doesn't exist
 */
int wtree3_update_txn(
    wtree3_txn_t *txn,
    wtree3_tree_t *tree,
    const void *key, size_t key_len,
    const void *value, size_t value_len,
    gerror_t *error
);

/*
 * Upsert a key-value pair (insert or update)
 *
 * If key doesn't exist, inserts it (same as insert_one_txn).
 * If key exists:
 *   - If tree has merge_fn set: calls merge_fn to combine existing + new values
 *   - Otherwise: overwrites with new value (same as update_txn)
 *
 * Automatically:
 * - Updates all index entries if value changes
 * - Increments entry count only on new insert
 *
 * Returns: 0 on success, error code on failure
 */
int wtree3_upsert_txn(
    wtree3_txn_t *txn,
    wtree3_tree_t *tree,
    const void *key, size_t key_len,
    const void *value, size_t value_len,
    gerror_t *error
);

/*
 * Delete a key-value pair
 *
 * Automatically:
 * - Removes from all index trees
 * - Decrements entry count
 *
 * Parameters:
 *   deleted - Output: true if key existed and was deleted
 *
 * Returns: 0 on success (even if key didn't exist)
 */
int wtree3_delete_one_txn(
    wtree3_txn_t *txn,
    wtree3_tree_t *tree,
    const void *key, size_t key_len,
    bool *deleted,
    gerror_t *error
);

/* Check if key exists */
bool wtree3_exists_txn(
    wtree3_txn_t *txn,
    wtree3_tree_t *tree,
    const void *key, size_t key_len,
    gerror_t *error
);

/* Insert multiple key-value pairs (batch operation) */
int wtree3_insert_many_txn(
    wtree3_txn_t *txn,
    wtree3_tree_t *tree,
    const wtree3_kv_t *kvs, size_t count,
    gerror_t *error
);

/* Upsert multiple key-value pairs (batch operation) */
int wtree3_upsert_many_txn(
    wtree3_txn_t *txn,
    wtree3_tree_t *tree,
    const wtree3_kv_t *kvs, size_t count,
    gerror_t *error
);

/* ============================================================
 * Data Operations (Auto-transaction)
 *
 * Convenience wrappers that create their own transaction.
 * ============================================================ */

int wtree3_get(
    wtree3_tree_t *tree,
    const void *key, size_t key_len,
    void **value, size_t *value_len,
    gerror_t *error
);

int wtree3_insert_one(
    wtree3_tree_t *tree,
    const void *key, size_t key_len,
    const void *value, size_t value_len,
    gerror_t *error
);

int wtree3_update(
    wtree3_tree_t *tree,
    const void *key, size_t key_len,
    const void *value, size_t value_len,
    gerror_t *error
);

int wtree3_upsert(
    wtree3_tree_t *tree,
    const void *key, size_t key_len,
    const void *value, size_t value_len,
    gerror_t *error
);

int wtree3_delete_one(
    wtree3_tree_t *tree,
    const void *key, size_t key_len,
    bool *deleted,
    gerror_t *error
);

bool wtree3_exists(
    wtree3_tree_t *tree,
    const void *key, size_t key_len,
    gerror_t *error
);

/* ============================================================
 * Tier 1 Operations - Generic Low-Level Primitives
 * ============================================================ */

/*
 * Scan a range of key-value pairs (forward)
 *
 * Iterates keys in ascending order within [start_key, end_key].
 * Pass NULL for start_key to scan from beginning.
 * Pass NULL for end_key to scan to end.
 *
 * Parameters:
 *   txn       - Transaction handle
 *   tree      - Tree to scan
 *   start_key - Start key (inclusive, NULL for first key)
 *   start_len - Start key length
 *   end_key   - End key (inclusive, NULL for last key)
 *   end_len   - End key length
 *   scan_fn   - Callback for each entry (return false to stop early)
 *   user_data - Context passed to callback
 *
 * Returns: 0 on success, error code on failure
 */
int wtree3_scan_range_txn(
    wtree3_txn_t *txn,
    wtree3_tree_t *tree,
    const void *start_key, size_t start_len,
    const void *end_key, size_t end_len,
    wtree3_scan_fn scan_fn,
    void *user_data,
    gerror_t *error
);

/*
 * Scan a range of key-value pairs (reverse)
 *
 * Iterates keys in descending order within [start_key, end_key].
 * Pass NULL for start_key to scan from end.
 * Pass NULL for end_key to scan to beginning.
 *
 * Returns: 0 on success, error code on failure
 */
int wtree3_scan_reverse_txn(
    wtree3_txn_t *txn,
    wtree3_tree_t *tree,
    const void *start_key, size_t start_len,
    const void *end_key, size_t end_len,
    wtree3_scan_fn scan_fn,
    void *user_data,
    gerror_t *error
);

/*
 * Scan all keys with a given prefix
 *
 * Iterates all keys that start with prefix in ascending order.
 *
 * Parameters:
 *   prefix     - Key prefix to match
 *   prefix_len - Prefix length
 *
 * Returns: 0 on success, error code on failure
 */
int wtree3_scan_prefix_txn(
    wtree3_txn_t *txn,
    wtree3_tree_t *tree,
    const void *prefix, size_t prefix_len,
    wtree3_scan_fn scan_fn,
    void *user_data,
    gerror_t *error
);

/*
 * Atomic read-modify-write operation
 *
 * Reads current value, calls modify_fn to transform it, writes result.
 * All happens atomically within transaction.
 *
 * Automatically maintains indexes:
 * - If modify_fn returns NULL for existing key: deletes entry
 * - If modify_fn returns value for missing key: inserts entry
 * - If modify_fn returns value for existing key: updates entry
 *
 * Parameters:
 *   key       - Key to modify
 *   modify_fn - Transformation callback
 *   user_data - Context passed to callback
 *
 * Returns: 0 on success, error code on failure
 */
int wtree3_modify_txn(
    wtree3_txn_t *txn,
    wtree3_tree_t *tree,
    const void *key, size_t key_len,
    wtree3_modify_fn modify_fn,
    void *user_data,
    gerror_t *error
);

/*
 * Batch read operation
 *
 * Reads multiple keys in a single transaction.
 * For each key in keys array, writes pointer to value in values array.
 *
 * Parameters:
 *   keys       - Array of keys to read
 *   key_count  - Number of keys
 *   values     - Output: array of value pointers (zero-copy, valid during txn)
 *   value_lens - Output: array of value lengths
 *
 * Note: If a key doesn't exist, corresponding value pointer is set to NULL
 *       and value_len to 0. This is NOT an error.
 *
 * Returns: 0 on success, error code on failure
 */
int wtree3_get_many_txn(
    wtree3_txn_t *txn,
    wtree3_tree_t *tree,
    const wtree3_kv_t *keys, size_t key_count,
    const void **values, size_t *value_lens,
    gerror_t *error
);

/* ============================================================
 * Tier 2 Operations - Specialized Bulk Operations
 * ============================================================ */

/*
 * Conditional bulk delete operation
 *
 * Scans a range and deletes all entries that match the predicate.
 * Useful for: expiring old data, removing by pattern, cleanup operations.
 *
 * Parameters:
 *   start_key   - Start of range (NULL for beginning)
 *   start_len   - Start key length
 *   end_key     - End of range (NULL for end)
 *   end_len     - End key length
 *   predicate   - Test function (return true to delete entry)
 *   user_data   - Context passed to predicate
 *   deleted_out - Output: number of entries deleted (can be NULL)
 *
 * Returns: 0 on success, error code on failure
 */
int wtree3_delete_if_txn(
    wtree3_txn_t *txn,
    wtree3_tree_t *tree,
    const void *start_key, size_t start_len,
    const void *end_key, size_t end_len,
    wtree3_predicate_fn predicate,
    void *user_data,
    size_t *deleted_out,
    gerror_t *error
);

/*
 * Collect key-value pairs from a range into arrays
 *
 * Scans a range and copies matching entries into output arrays.
 * Useful for: batch export, caching, snapshot operations.
 *
 * Parameters:
 *   start_key   - Start of range (NULL for beginning)
 *   start_len   - Start key length
 *   end_key     - End of range (NULL for end)
 *   end_len     - End key length
 *   predicate   - Optional filter (NULL to collect all, true to collect entry)
 *   user_data   - Context passed to predicate
 *   keys_out    - Output: array of collected keys (caller must free each key and array)
 *   key_lens    - Output: array of key lengths (caller must free)
 *   values_out  - Output: array of collected values (caller must free each value and array)
 *   value_lens  - Output: array of value lengths (caller must free)
 *   count_out   - Output: number of entries collected
 *   max_count   - Maximum entries to collect (0 for unlimited)
 *
 * Note: All output arrays are allocated with malloc. Caller must free.
 *
 * Returns: 0 on success, error code on failure
 */
int wtree3_collect_range_txn(
    wtree3_txn_t *txn,
    wtree3_tree_t *tree,
    const void *start_key, size_t start_len,
    const void *end_key, size_t end_len,
    wtree3_predicate_fn predicate,
    void *user_data,
    void ***keys_out,
    size_t **key_lens,
    void ***values_out,
    size_t **value_lens,
    size_t *count_out,
    size_t max_count,
    gerror_t *error
);

/*
 * Batch existence check
 *
 * Checks if multiple keys exist in a single transaction.
 * Much faster than individual exists() calls.
 *
 * Parameters:
 *   keys      - Array of keys to check
 *   key_count - Number of keys
 *   results   - Output: array of booleans (true if key exists)
 *
 * Returns: 0 on success, error code on failure
 */
int wtree3_exists_many_txn(
    wtree3_txn_t *txn,
    wtree3_tree_t *tree,
    const wtree3_kv_t *keys, size_t key_count,
    bool *results,
    gerror_t *error
);

/* ============================================================
 * Iterator Operations
 * ============================================================ */

/* Create iterator (creates its own read transaction) */
wtree3_iterator_t* wtree3_iterator_create(wtree3_tree_t *tree, gerror_t *error);

/* Create iterator with existing transaction */
wtree3_iterator_t* wtree3_iterator_create_with_txn(
    wtree3_tree_t *tree,
    wtree3_txn_t *txn,
    gerror_t *error
);

/* Navigation */
bool wtree3_iterator_first(wtree3_iterator_t *iter);
bool wtree3_iterator_last(wtree3_iterator_t *iter);
bool wtree3_iterator_next(wtree3_iterator_t *iter);
bool wtree3_iterator_prev(wtree3_iterator_t *iter);
bool wtree3_iterator_seek(wtree3_iterator_t *iter, const void *key, size_t key_len);
bool wtree3_iterator_seek_range(wtree3_iterator_t *iter, const void *key, size_t key_len);

/* Access current entry (zero-copy) */
bool wtree3_iterator_key(wtree3_iterator_t *iter, const void **key, size_t *key_len);
bool wtree3_iterator_value(wtree3_iterator_t *iter, const void **value, size_t *value_len);

/* Access current entry (with copy - safe after iterator closes) */
bool wtree3_iterator_key_copy(wtree3_iterator_t *iter, void **key, size_t *key_len);
bool wtree3_iterator_value_copy(wtree3_iterator_t *iter, void **value, size_t *value_len);

/* Check validity */
bool wtree3_iterator_valid(wtree3_iterator_t *iter);

/* Delete current entry (requires write transaction) */
int wtree3_iterator_delete(wtree3_iterator_t *iter, gerror_t *error);

/* Close iterator */
void wtree3_iterator_close(wtree3_iterator_t *iter);

/* Get iterator's transaction */
wtree3_txn_t* wtree3_iterator_get_txn(wtree3_iterator_t *iter);

/* ============================================================
 * Index Query Operations
 * ============================================================ */

/*
 * Create iterator positioned at index key
 *
 * The iterator yields main tree keys that match the index key.
 * Use wtree3_get_txn() to fetch the actual values.
 */
wtree3_iterator_t* wtree3_index_seek(
    wtree3_tree_t *tree,
    const char *index_name,
    const void *key, size_t key_len,
    gerror_t *error
);

/*
 * Create iterator positioned at index key range (key or next greater)
 */
wtree3_iterator_t* wtree3_index_seek_range(
    wtree3_tree_t *tree,
    const char *index_name,
    const void *key, size_t key_len,
    gerror_t *error
);

/*
 * Get main tree key from index iterator
 *
 * When iterating an index, the "value" is the main tree key.
 * This is a convenience alias for wtree3_iterator_value().
 */
bool wtree3_index_iterator_main_key(
    wtree3_iterator_t *iter,
    const void **main_key,
    size_t *main_key_len
);

/* ============================================================
 * Utility Functions
 * ============================================================ */

/* Convert error code to string */
const char* wtree3_strerror(int error_code);

/* Check if error is recoverable (e.g., MAP_FULL) */
bool wtree3_error_recoverable(int error_code);

#ifdef __cplusplus
}
#endif

#endif /* WTREE3_H */
