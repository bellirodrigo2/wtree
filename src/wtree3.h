/*
 * wtree3.h - Unified Storage Layer with Index Support
 *
 * A single, clean abstraction over LMDB that provides:
 * - Database and transaction management
 * - Named trees (collections) with key-value storage
 * - Optional secondary index support with automatic maintenance
 * - Entry count tracking
 *
 * This layer replaces both wtree.h and wtree2.h with a unified API.
 *
 * Usage:
 *   1. Open database with wtree3_db_open()
 *   2. Create/open trees with wtree3_tree_open()
 *   3. Optionally add indexes with wtree3_tree_add_index()
 *   4. Use wtree3_insert/update/delete - indexes maintained automatically
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

/* ============================================================
 * Error Codes (-3000 to -3099)
 * ============================================================ */

#define WTREE3_OK            0
#define WTREE3_ERROR        -3000   /* Generic error */
#define WTREE3_EINVAL       -3001   /* Invalid argument */
#define WTREE3_ENOMEM       -3002   /* Out of memory */
#define WTREE3_KEY_EXISTS   -3003   /* Key already exists (unique violation) */
#define WTREE3_NOT_FOUND    -3004   /* Key not found */
#define WTREE3_MAP_FULL     -3005   /* Database map is full, needs resize */
#define WTREE3_TXN_FULL     -3006   /* Transaction is full */
#define WTREE3_INDEX_ERROR  -3007   /* Index operation failed */

/* ============================================================
 * Opaque Types
 * ============================================================ */

typedef struct wtree3_db_t wtree3_db_t;
typedef struct wtree3_txn_t wtree3_txn_t;
typedef struct wtree3_tree_t wtree3_tree_t;
typedef struct wtree3_iterator_t wtree3_iterator_t;

/* ============================================================
 * Index Support Types
 * ============================================================ */

/*
 * Index key extraction callback
 *
 * Called during insert/update/delete to extract index key from value.
 *
 * Parameters:
 *   value     - Raw value bytes (e.g., BSON document)
 *   value_len - Value length in bytes
 *   user_data - User context passed during index registration
 *   out_key   - Output: allocated key data (caller frees with free())
 *   out_len   - Output: key length
 *
 * Returns:
 *   true  - Key extracted successfully, document should be indexed
 *   false - Document should NOT be indexed (sparse index behavior)
 *
 * Note: For sparse indexes, return false when indexed field is missing/null.
 *       The callback is responsible for allocating out_key with malloc().
 */
typedef bool (*wtree3_index_key_fn)(
    const void *value,
    size_t value_len,
    void *user_data,
    void **out_key,
    size_t *out_len
);

/*
 * Cleanup callback for user_data (called when index is dropped or tree closed)
 */
typedef void (*wtree3_user_data_cleanup_fn)(void *user_data);

/*
 * User data persistence callbacks
 *
 * These callbacks enable user_data to be persisted and restored across DB sessions.
 */
typedef struct wtree3_user_data_persistence {
    /*
     * Serialize user_data to bytes
     *
     * Parameters:
     *   user_data - The user_data to serialize
     *   out_data  - Output: allocated data buffer (caller will free with free())
     *   out_len   - Output: length of serialized data
     *
     * Returns: 0 on success, non-zero on error
     */
    int (*serialize)(void *user_data, void **out_data, size_t *out_len);

    /*
     * Deserialize bytes to user_data
     *
     * Parameters:
     *   data - Serialized data
     *   len  - Length of serialized data
     *
     * Returns: Allocated user_data (must be compatible with user_data_cleanup),
     *          or NULL on error
     */
    void* (*deserialize)(const void *data, size_t len);
} wtree3_user_data_persistence_t;

/*
 * Merge callback for upsert operations
 *
 * Called when upserting a key that already exists. Allows custom merge logic.
 *
 * Parameters:
 *   existing_value - Current value in database
 *   existing_len   - Current value length
 *   new_value      - New value being inserted
 *   new_len        - New value length
 *   user_data      - User context passed during tree configuration
 *   out_len        - Output: merged value length
 *
 * Returns:
 *   Pointer to merged value (caller must allocate with malloc, freed by wtree)
 *   NULL on error (upsert will fail)
 *
 * Note: If no merge callback is set, upsert overwrites with new_value
 */
typedef void* (*wtree3_merge_fn)(
    const void *existing_value,
    size_t existing_len,
    const void *new_value,
    size_t new_len,
    void *user_data,
    size_t *out_len
);

/*
 * Scan callback for range iteration
 *
 * Called for each key-value pair during range scan operations.
 *
 * Parameters:
 *   key       - Current key (zero-copy, valid during callback only)
 *   key_len   - Key length
 *   value     - Current value (zero-copy, valid during callback only)
 *   value_len - Value length
 *   user_data - User context passed to scan function
 *
 * Returns:
 *   true  - Continue scanning
 *   false - Stop scanning (early termination)
 */
typedef bool (*wtree3_scan_fn)(
    const void *key,
    size_t key_len,
    const void *value,
    size_t value_len,
    void *user_data
);

/*
 * Modify callback for atomic read-modify-write operations
 *
 * Called to transform an existing value in-place within a transaction.
 *
 * Parameters:
 *   existing_value - Current value in database (NULL if key doesn't exist)
 *   existing_len   - Current value length (0 if key doesn't exist)
 *   user_data      - User context passed to modify function
 *   out_len        - Output: new value length
 *
 * Returns:
 *   Pointer to new value (caller must allocate with malloc, freed by wtree)
 *   NULL to delete the key (when existing_value is not NULL)
 *   NULL to abort operation (when existing_value is NULL)
 *
 * Note: Returned value replaces existing value atomically
 */
typedef void* (*wtree3_modify_fn)(
    const void *existing_value,
    size_t existing_len,
    void *user_data,
    size_t *out_len
);

/*
 * Predicate callback for conditional operations
 *
 * Used to test whether a key-value pair should be deleted or collected.
 *
 * Parameters:
 *   key       - Current key (zero-copy, valid during callback only)
 *   key_len   - Key length
 *   value     - Current value (zero-copy, valid during callback only)
 *   value_len - Value length
 *   user_data - User context passed to operation
 *
 * Returns:
 *   true  - Predicate matches (delete/collect this entry)
 *   false - Predicate doesn't match (skip this entry)
 */
typedef bool (*wtree3_predicate_fn)(
    const void *key,
    size_t key_len,
    const void *value,
    size_t value_len,
    void *user_data
);

/*
 * Index configuration
 */
typedef struct wtree3_index_config {
    const char *name;           /* Index name (e.g., "email_1") */
    wtree3_index_key_fn key_fn; /* Key extraction callback */
    void *user_data;            /* User context for callback */
    wtree3_user_data_cleanup_fn user_data_cleanup; /* Cleanup callback for user_data */
    bool unique;                /* Unique constraint */
    bool sparse;                /* Skip entries where key_fn returns false */
    MDB_cmp_func *compare;      /* Custom key comparator (NULL for default) */
    wtree3_user_data_persistence_t *persistence; /* Optional persistence callbacks */
} wtree3_index_config_t;

/* Key-Value pair for batch operations */
typedef struct wtree3_kv {
    const void *key;
    size_t key_len;
    const void *value;
    size_t value_len;
} wtree3_kv_t;

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
 *   flags   - LMDB flags (MDB_RDONLY, MDB_NOSYNC, etc.)
 *   error   - Error output
 *
 * Returns: Database handle or NULL on error
 */
wtree3_db_t* wtree3_db_open(
    const char *path,
    size_t mapsize,
    unsigned int max_dbs,
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

/* ============================================================
 * Index Persistence Operations
 * ============================================================ */

/*
 * Save index metadata to persistent storage
 *
 * Stores serialized user_data and index configuration for restoration
 * across DB sessions. Called automatically by wtree3_tree_add_index()
 * if persistence callbacks are provided.
 *
 * Returns: 0 on success, error code on failure
 */
int wtree3_index_save_metadata(
    wtree3_tree_t *tree,
    const char *index_name,
    gerror_t *error
);

/*
 * Load index metadata and reconstruct index
 *
 * Deserializes user_data and restores index configuration from persistent
 * storage. Used when reopening an existing database with indexes.
 *
 * Parameters:
 *   tree        - Tree handle
 *   index_name  - Index name to load
 *   key_fn      - Key extraction function (must match original)
 *   persistence - Persistence callbacks (deserialize will be called)
 *   error       - Error output
 *
 * Returns: 0 on success, WTREE3_NOT_FOUND if no metadata exists
 */
int wtree3_index_load_metadata(
    wtree3_tree_t *tree,
    const char *index_name,
    wtree3_index_key_fn key_fn,
    wtree3_user_data_persistence_t *persistence,
    gerror_t *error
);

/*
 * List all persisted indexes for a tree
 *
 * Returns array of index names that have persisted metadata.
 * Call wtree3_index_list_free() to free the returned array.
 *
 * Returns: NULL-terminated array of strings, or NULL on error
 */
char** wtree3_tree_list_persisted_indexes(
    wtree3_tree_t *tree,
    size_t *count,
    gerror_t *error
);

/*
 * Free array returned by wtree3_tree_list_persisted_indexes()
 */
void wtree3_index_list_free(char **list, size_t count);

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
