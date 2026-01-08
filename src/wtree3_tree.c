/*
 * wtree3_tree.c - Tree Lifecycle and Configuration Management
 *
 * This module provides tree/collection management:
 * - Tree lifecycle (open, close, delete, exists)
 * - Tree configuration (set_compare, set_merge_fn)
 * - Index loader support for restoring persisted indexes
 */

#include "wtree3_internal.h"
#include "macros.h"

/* Forward declarations from wtree3_index_persist.c */
extern char** wtree3_tree_list_persisted_indexes(wtree3_tree_t *tree, size_t *count, gerror_t *error);
extern void wtree3_index_list_free(char **list, size_t count);

/* ============================================================
 * Internal Helpers
 * ============================================================ */

/*
 * Cleanup function for index vector elements
 */
WTREE_COLD
static void cleanup_index(void *element) {
    wtree3_index_t *idx = (wtree3_index_t *)element;
    if (WTREE_UNLIKELY(!idx)) return;
    free(idx->user_data);
    free(idx->name);
    free(idx->tree_name);
    free(idx);
}

/*
 * Auto-load all persisted indexes for a tree
 * Uses registered extractors from db->extractors registry
 */
WTREE_COLD
static void auto_load_indexes(wtree3_tree_t *tree) {
    if (WTREE_UNLIKELY(!tree)) return;

    gerror_t error = {0};
    size_t index_count = 0;
    char **index_names = wtree3_tree_list_persisted_indexes(tree, &index_count, &error);

    if (WTREE_UNLIKELY(!index_names || index_count == 0)) {
        return;  // No indexes to load
    }

    // Load each index
    for (size_t i = 0; i < index_count; i++) {
        int rc = load_index_metadata(tree, index_names[i], &error);
        /* load_index_metadata handles missing extractors gracefully (prints warning), ignore errors */
        (void)rc;
    }

    wtree3_index_list_free(index_names, index_count);
}

/* ============================================================
 * Tree Operations
 * ============================================================ */

/* Helper context for tree_open transaction */
typedef struct {
    const char *name;
    unsigned int flags;
    MDB_dbi *out_dbi;
} tree_open_ctx_t;

static int tree_open_txn(MDB_txn *txn, void *user_data) {
    tree_open_ctx_t *ctx = (tree_open_ctx_t *)user_data;
    int rc = mdb_dbi_open(txn, ctx->name, MDB_CREATE | ctx->flags, ctx->out_dbi);
    if (WTREE_UNLIKELY(rc != 0)) {
        return rc;  /* Will be translated by with_write_txn */
    }
    return WTREE3_OK;
}

WTREE_WARN_UNUSED
wtree3_tree_t* wtree3_tree_open(wtree3_db_t *db, const char *name,
                                 unsigned int flags, int64_t entry_count,
                                 gerror_t *error) {
    if (WTREE_UNLIKELY(!db || !name)) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Database and name are required");
        return NULL;
    }

    wtree3_tree_t *tree = calloc(1, sizeof(wtree3_tree_t));
    if (WTREE_UNLIKELY(!tree)) {
        set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Failed to allocate tree");
        return NULL;
    }

    /* Open DBI in a transaction */
    tree_open_ctx_t ctx = {
        .name = name,
        .flags = flags,
        .out_dbi = &tree->dbi
    };

    int rc = with_write_txn(db, tree_open_txn, &ctx, error);
    if (WTREE_UNLIKELY(rc != 0)) {
        free(tree);
        return NULL;
    }

    tree->name = strdup(name);
    tree->db = db;
    tree->flags = flags;
    tree->entry_count = entry_count;
    tree->indexes = wvector_create(4, cleanup_index);

    if (WTREE_UNLIKELY(!tree->name || !tree->indexes)) {
        free(tree->name);
        wvector_destroy(tree->indexes);
        free(tree);
        set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Failed to allocate tree resources");
        return NULL;
    }

    /* Auto-load persisted indexes */
    auto_load_indexes(tree);

    return tree;
}

WTREE_COLD
void wtree3_tree_close(wtree3_tree_t *tree) {
    if (WTREE_UNLIKELY(!tree)) return;

    /* Free all indexes (wvector cleanup function handles individual index cleanup) */
    wvector_destroy(tree->indexes);
    free(tree->name);
    free(tree);
}

/* ============================================================
 * Tree Delete Helper Functions
 * ============================================================ */

/*
 * Collect keys matching a prefix from a cursor scan
 * Returns: number of keys collected, or -1 on error
 */
WTREE_COLD
static int collect_keys_by_prefix(MDB_cursor *cursor, const char *prefix,
                                   char ***out_keys, size_t *out_count) {
    size_t prefix_len = strlen(prefix);
    char **keys = NULL;
    size_t count = 0;
    size_t capacity = 0;

    MDB_val key, val;
    int rc = mdb_cursor_get(cursor, &key, &val, MDB_FIRST);

    while (rc == 0) {
        if (key.mv_size >= prefix_len &&
            memcmp(key.mv_data, prefix, prefix_len) == 0) {
            /* Grow array if needed */
            if (WTREE_UNLIKELY(count >= capacity)) {
                capacity = capacity ? capacity * 2 : 4;
                char **new_arr = realloc(keys, capacity * sizeof(char*));
                if (WTREE_UNLIKELY(!new_arr)) {
                    /* Cleanup on allocation failure */
                    for (size_t i = 0; i < count; i++) {
                        free(keys[i]);
                    }
                    free(keys);
                    return -1;
                }
                keys = new_arr;
            }

            /* Allocate and copy key */
            keys[count] = malloc(key.mv_size + 1);
            if (WTREE_LIKELY(keys[count])) {
                memcpy(keys[count], key.mv_data, key.mv_size);
                keys[count][key.mv_size] = '\0';
                count++;
            }
        }
        rc = mdb_cursor_get(cursor, &key, &val, MDB_NEXT);
    }

    *out_keys = keys;
    *out_count = count;
    return 0;
}

/*
 * Delete all index DBs for a tree
 */
WTREE_COLD
static int delete_tree_index_dbs(MDB_txn *txn, const char *tree_name) {
    /* Open main (unnamed) DBI to scan for named DBs */
    MDB_dbi main_dbi;
    int rc = mdb_dbi_open(txn, NULL, 0, &main_dbi);
    if (WTREE_UNLIKELY(rc != 0)) return rc;

    /* Scan for index DBs matching "idx:<tree_name>:" */
    MDB_cursor *cursor;
    rc = mdb_cursor_open(txn, main_dbi, &cursor);
    if (WTREE_UNLIKELY(rc != 0)) return rc;

    char idx_prefix[256];
    snprintf(idx_prefix, sizeof(idx_prefix), "idx:%s:", tree_name);

    char **index_names = NULL;
    size_t index_count = 0;

    if (WTREE_UNLIKELY(collect_keys_by_prefix(cursor, idx_prefix, &index_names, &index_count) != 0)) {
        mdb_cursor_close(cursor);
        return ENOMEM;
    }
    mdb_cursor_close(cursor);

    /* Delete all found index DBs */
    for (size_t i = 0; i < index_count; i++) {
        MDB_dbi idx_dbi;
        rc = mdb_dbi_open(txn, index_names[i], 0, &idx_dbi);
        if (WTREE_LIKELY(rc == 0)) {
            mdb_drop(txn, idx_dbi, 1);
        }
        free(index_names[i]);
    }
    free(index_names);

    return 0;
}

/*
 * Delete all metadata entries for a tree
 */
WTREE_COLD
static int delete_tree_metadata(MDB_txn *txn, wtree3_db_t *db, const char *tree_name) {
    MDB_dbi metadata_dbi;
    int rc = get_metadata_dbi(db, txn, &metadata_dbi, NULL);
    if (WTREE_UNLIKELY(rc != 0)) return 0;  /* No metadata DB = nothing to delete */

    MDB_cursor *cursor;
    rc = mdb_cursor_open(txn, metadata_dbi, &cursor);
    if (WTREE_UNLIKELY(rc != 0)) return rc;

    /* Collect metadata keys matching "tree_name:" */
    char meta_prefix[256];
    snprintf(meta_prefix, sizeof(meta_prefix), "%s:", tree_name);

    char **keys = NULL;
    size_t key_count = 0;

    if (WTREE_UNLIKELY(collect_keys_by_prefix(cursor, meta_prefix, &keys, &key_count) != 0)) {
        mdb_cursor_close(cursor);
        return ENOMEM;
    }

    /* Delete collected keys */
    for (size_t i = 0; i < key_count; i++) {
        MDB_val del_key = {.mv_data = keys[i], .mv_size = strlen(keys[i])};
        MDB_val val;
        if (WTREE_LIKELY(mdb_cursor_get(cursor, &del_key, &val, MDB_SET) == 0)) {
            mdb_cursor_del(cursor, 0);
        }
        free(keys[i]);
    }
    free(keys);

    mdb_cursor_close(cursor);
    return 0;
}

/* ============================================================
 * Tree Delete (Main Function)
 * ============================================================ */

WTREE_COLD WTREE_WARN_UNUSED
int wtree3_tree_delete(wtree3_db_t *db, const char *name, gerror_t *error) {
    if (WTREE_UNLIKELY(!db || !name)) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Invalid parameters");
        return WTREE3_EINVAL;
    }

    /* Start transaction */
    MDB_txn *txn;
    int rc = mdb_txn_begin(db->env, NULL, 0, &txn);
    if (WTREE_UNLIKELY(rc != 0)) {
        return translate_mdb_error(rc, error);
    }

    /* Check if main tree exists */
    MDB_dbi dbi;
    rc = mdb_dbi_open(txn, name, 0, &dbi);
    if (WTREE_UNLIKELY(rc != 0)) {
        mdb_txn_abort(txn);
        return translate_mdb_error(rc, error);
    }

    /* Delete all associated index DBs */
    rc = delete_tree_index_dbs(txn, name);
    if (WTREE_UNLIKELY(rc != 0)) {
        mdb_txn_abort(txn);
        return translate_mdb_error(rc, error);
    }

    /* Delete all metadata entries */
    rc = delete_tree_metadata(txn, db, name);
    if (WTREE_UNLIKELY(rc != 0)) {
        mdb_txn_abort(txn);
        return translate_mdb_error(rc, error);
    }

    /* Finally, delete the main tree DBI */
    rc = mdb_drop(txn, dbi, 1);
    if (WTREE_UNLIKELY(rc != 0)) {
        mdb_txn_abort(txn);
        return translate_mdb_error(rc, error);
    }

    /* Commit transaction */
    rc = mdb_txn_commit(txn);
    if (WTREE_UNLIKELY(rc != 0)) {
        return translate_mdb_error(rc, error);
    }

    return WTREE3_OK;
}

WTREE_PURE
const char* wtree3_tree_name(wtree3_tree_t *tree) {
    return tree ? tree->name : NULL;
}

WTREE_PURE
int64_t wtree3_tree_count(wtree3_tree_t *tree) {
    return tree ? tree->entry_count : 0;
}

WTREE_PURE
wtree3_db_t* wtree3_tree_get_db(wtree3_tree_t *tree) {
    return tree ? tree->db : NULL;
}

/* Helper context for set_compare transaction */
typedef struct {
    wtree3_tree_t *tree;
    MDB_cmp_func *cmp;
} set_compare_ctx_t;

static int set_compare_txn(MDB_txn *txn, void *user_data) {
    set_compare_ctx_t *ctx = (set_compare_ctx_t *)user_data;
    int rc = mdb_set_compare(txn, ctx->tree->dbi, ctx->cmp);
    return WTREE_UNLIKELY(rc != 0) ? rc : WTREE3_OK;
}

WTREE_WARN_UNUSED
int wtree3_tree_set_compare(wtree3_tree_t *tree, MDB_cmp_func *cmp, gerror_t *error) {
    if (WTREE_UNLIKELY(!tree || !cmp)) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Invalid parameters");
        return WTREE3_EINVAL;
    }

    set_compare_ctx_t ctx = { .tree = tree, .cmp = cmp };
    return with_write_txn(tree->db, set_compare_txn, &ctx, error);
}

void wtree3_tree_set_merge_fn(wtree3_tree_t *tree, wtree3_merge_fn merge_fn, void *user_data) {
    if (WTREE_UNLIKELY(!tree)) return;
    tree->merge_fn = merge_fn;
    tree->merge_user_data = user_data;
}

/* Helper context for tree_exists transaction */
typedef struct {
    const char *name;
    int *result;
    gerror_t *error;
} tree_exists_ctx_t;

static int tree_exists_txn(MDB_txn *txn, void *user_data) {
    tree_exists_ctx_t *ctx = (tree_exists_ctx_t *)user_data;
    MDB_dbi dbi;

    /* Try to open without MDB_CREATE flag - won't create the DB */
    int rc = mdb_dbi_open(txn, ctx->name, 0, &dbi);

    if (WTREE_UNLIKELY(rc == MDB_NOTFOUND)) {
        *ctx->result = 0;  /* DB doesn't exist */
        return WTREE3_OK;
    } else if (WTREE_LIKELY(rc == 0)) {
        *ctx->result = 1;  /* DB exists */
        return WTREE3_OK;
    } else {
        return translate_mdb_error(rc, ctx->error);
    }
}

WTREE_WARN_UNUSED
int wtree3_tree_exists(wtree3_db_t *db, const char *name, gerror_t *error) {
    if (WTREE_UNLIKELY(!db || !name)) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "DB and name cannot be NULL");
        return WTREE3_EINVAL;
    }

    int result = 0;
    tree_exists_ctx_t ctx = { .name = name, .result = &result, .error = error };

    int rc = with_read_txn(db, tree_exists_txn, &ctx, error);
    if (WTREE_UNLIKELY(rc != 0)) {
        return rc;
    }

    return result;
}
