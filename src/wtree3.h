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
