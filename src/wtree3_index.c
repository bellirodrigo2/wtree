/*
 * wtree3_index.c - Index Definition and Management
 *
 * This module provides secondary index management:
 * - Index creation and deletion (add_index, drop_index, populate_index)
 * - Index querying (has_index, index_count)
 * - Index verification (verify_indexes)
 * - Helper functions for index operations
 */

#include "wtree3_internal.h"
#include "macros.h"

/* Forward declarations from wtree3_index_persist.c */
extern int save_index_metadata(wtree3_tree_t *tree, const char *index_name, gerror_t *error);

/* ============================================================
 * Helper Functions
 * ============================================================ */

/* Build index tree name: idx:<tree_name>:<index_name> */
WTREE_MALLOC
char* build_index_tree_name(const char *tree_name, const char *index_name) {
    size_t len = strlen(WTREE3_INDEX_PREFIX) + strlen(tree_name) + 1 + strlen(index_name) + 1;
    char *name = malloc(len);
    if (WTREE_LIKELY(name)) {
        snprintf(name, len, "%s%s:%s", WTREE3_INDEX_PREFIX, tree_name, index_name);
    }
    return name;
}

/* Comparison function for finding index by name */
WTREE_PURE
static int compare_index_by_name(const void *idx_ptr, const void *name_ptr) {
    const wtree3_index_t *idx = (const wtree3_index_t *)idx_ptr;
    const char *name = (const char *)name_ptr;
    return strcmp(idx->name, name);
}

/* Find index by name */
WTREE_HOT WTREE_PURE
wtree3_index_t* find_index(wtree3_tree_t *tree, const char *name) {
    if (WTREE_UNLIKELY(!tree || !name)) return NULL;
    return (wtree3_index_t *)wvector_find(tree->indexes, name, compare_index_by_name);
}

/* Get or create metadata DBI */
int get_metadata_dbi(wtree3_db_t *db, MDB_txn *txn, MDB_dbi *out_dbi, gerror_t *error) {
    int rc = mdb_dbi_open(txn, WTREE3_META_DB, MDB_CREATE, out_dbi);
    if (WTREE_UNLIKELY(rc != 0)) {
        return translate_mdb_error(rc, error);
    }
    return WTREE3_OK;
}

/* Build metadata key: tree_name:index_name */
WTREE_MALLOC
char* build_metadata_key(const char *tree_name, const char *index_name) {
    size_t len = strlen(tree_name) + 1 + strlen(index_name) + 1;
    char *key = malloc(len);
    if (WTREE_LIKELY(key)) {
        snprintf(key, len, "%s:%s", tree_name, index_name);
    }
    return key;
}

/* ============================================================
 * High-Level Metadata Access Helpers
 * ============================================================ */

/*
 * Get metadata value for an index (within existing transaction)
 *
 * Returns: 0 on success, WTREE3_NOT_FOUND if not found, other error codes
 */
int metadata_get_txn(MDB_txn *txn, wtree3_db_t *db,
                     const char *tree_name, const char *index_name,
                     MDB_val *out_val, gerror_t *error) {
    MDB_dbi meta_dbi;
    int rc = get_metadata_dbi(db, txn, &meta_dbi, error);
    if (WTREE_UNLIKELY(rc != 0)) return rc;

    char *meta_key = build_metadata_key(tree_name, index_name);
    if (WTREE_UNLIKELY(!meta_key)) {
        set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Failed to build metadata key");
        return WTREE3_ENOMEM;
    }

    MDB_val key = {.mv_size = strlen(meta_key), .mv_data = meta_key};
    rc = mdb_get(txn, meta_dbi, &key, out_val);
    free(meta_key);

    if (WTREE_UNLIKELY(rc == MDB_NOTFOUND)) {
        set_error(error, WTREE3_LIB, WTREE3_NOT_FOUND,
                 "No metadata found for index '%s'", index_name);
        return WTREE3_NOT_FOUND;
    }

    return WTREE_UNLIKELY(rc != 0) ? translate_mdb_error(rc, error) : WTREE3_OK;
}

/*
 * Put metadata value for an index (within existing transaction)
 *
 * Returns: 0 on success, error code on failure
 */
int metadata_put_txn(MDB_txn *txn, wtree3_db_t *db,
                     const char *tree_name, const char *index_name,
                     const void *data, size_t data_len, gerror_t *error) {
    MDB_dbi meta_dbi;
    int rc = get_metadata_dbi(db, txn, &meta_dbi, error);
    if (rc != 0) return rc;

    char *meta_key = build_metadata_key(tree_name, index_name);
    if (!meta_key) {
        set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Failed to build metadata key");
        return WTREE3_ENOMEM;
    }

    MDB_val key = {.mv_size = strlen(meta_key), .mv_data = meta_key};
    MDB_val val = {.mv_size = data_len, .mv_data = (void*)data};

    rc = mdb_put(txn, meta_dbi, &key, &val, 0);
    free(meta_key);

    return rc != 0 ? translate_mdb_error(rc, error) : WTREE3_OK;
}

/*
 * Delete metadata for an index (within existing transaction)
 *
 * Returns: 0 on success, error code on failure (NOTFOUND is not an error)
 */
int metadata_delete_txn(MDB_txn *txn, wtree3_db_t *db,
                        const char *tree_name, const char *index_name,
                        gerror_t *error) {
    MDB_dbi meta_dbi;
    int rc = get_metadata_dbi(db, txn, &meta_dbi, NULL);
    if (rc != 0) return WTREE3_OK;  /* No metadata DB = nothing to delete */

    char *meta_key = build_metadata_key(tree_name, index_name);
    if (!meta_key) {
        /* Don't fail delete operation due to memory allocation */
        return WTREE3_OK;
    }

    MDB_val key = {.mv_size = strlen(meta_key), .mv_data = meta_key};
    rc = mdb_del(txn, meta_dbi, &key, NULL);
    free(meta_key);

    /* NOTFOUND is fine for delete operations */
    if (rc == MDB_NOTFOUND) {
        return WTREE3_OK;
    }

    return rc != 0 ? translate_mdb_error(rc, error) : WTREE3_OK;
}


/* ============================================================
 * Index Management - Helper Transaction Functions
 * ============================================================ */

/* Helper context for creating index DBI */
typedef struct {
    const char *tree_name;
    MDB_dbi *out_dbi;
    MDB_cmp_func *compare;
} create_index_ctx_t;

static int create_index_txn(MDB_txn *txn, void *user_data) {
    create_index_ctx_t *ctx = (create_index_ctx_t *)user_data;
    int rc = mdb_dbi_open(txn, ctx->tree_name, MDB_CREATE | MDB_DUPSORT, ctx->out_dbi);
    if (rc != 0) return rc;

    /* Set custom comparator if provided */
    if (ctx->compare) {
        rc = mdb_set_compare(txn, *ctx->out_dbi, ctx->compare);
        if (rc != 0) return rc;
    }

    return WTREE3_OK;
}

/* ============================================================
 * Index Management
 * ============================================================ */

WTREE_WARN_UNUSED
int wtree3_tree_add_index(wtree3_tree_t *tree,
                           const wtree3_index_config_t *config,
                           gerror_t *error) {
    if (WTREE_UNLIKELY(!tree || !config || !config->name || config->name[0] == '\0')) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Invalid parameters");
        return WTREE3_EINVAL;
    }

    /* Check if index already exists */
    if (WTREE_UNLIKELY(find_index(tree, config->name))) {
        set_error(error, WTREE3_LIB, WTREE3_KEY_EXISTS,
                 "Index '%s' already exists", config->name);
        return WTREE3_KEY_EXISTS;
    }

    /* Build extractor ID from db version and config flags */
    uint32_t flags = extract_index_flags(config);
    uint64_t extractor_id = build_extractor_id(tree->db->version, flags);

    /* Look up extractor function from registry */
    wtree3_index_key_fn key_fn = find_extractor(tree->db, extractor_id);
    if (WTREE_UNLIKELY(!key_fn)) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL,
                 "No extractor registered for version=%u flags=0x%02x",
                 tree->db->version, flags);
        return WTREE3_EINVAL;
    }

    /* Build index tree name */
    char *idx_tree_name = build_index_tree_name(tree->name, config->name);
    if (WTREE_UNLIKELY(!idx_tree_name)) {
        set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Failed to allocate tree name");
        return WTREE3_ENOMEM;
    }

    /* Create index DBI with DUPSORT */
    MDB_dbi idx_dbi;
    create_index_ctx_t ctx = {
        .tree_name = idx_tree_name,
        .out_dbi = &idx_dbi,
        .compare = config->compare
    };

    int rc = with_write_txn(tree->db, create_index_txn, &ctx, error);
    if (WTREE_UNLIKELY(rc != 0)) {
        goto cleanup_tree_name;
    }

    /* Allocate new index on heap */
    wtree3_index_t *idx = calloc(1, sizeof(wtree3_index_t));
    if (!idx) {
        set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Failed to allocate index");
        rc = WTREE3_ENOMEM;
        goto cleanup_tree_name;
    }

    /* Initialize index structure */
    idx->name = strdup(config->name);
    if (!idx->name) {
        set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Failed to allocate index name");
        rc = WTREE3_ENOMEM;
        goto cleanup_idx;
    }

    idx->tree_name = idx_tree_name;
    idx->dbi = idx_dbi;
    idx->extractor_id = extractor_id;
    idx->key_fn = key_fn;
    idx->unique = config->unique;
    idx->sparse = config->sparse;
    idx->compare = config->compare;

    /* Copy user_data if provided */
    if (config->user_data && config->user_data_len > 0) {
        idx->user_data = malloc(config->user_data_len);
        if (!idx->user_data) {
            set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Failed to allocate user_data");
            rc = WTREE3_ENOMEM;
            goto cleanup_idx_name;
        }
        memcpy(idx->user_data, config->user_data, config->user_data_len);
        idx->user_data_len = config->user_data_len;
    }

    /* Add to vector */
    if (!wvector_push(tree->indexes, idx)) {
        set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Failed to add index to vector");
        rc = WTREE3_ENOMEM;
        goto cleanup_user_data;
    }

    /* Save metadata (always persisted) */
    rc = save_index_metadata(tree, config->name, error);
    if (rc != 0) {
        goto rollback_vector;
    }

    return WTREE3_OK;

    /* Cleanup paths in reverse order of allocation */
rollback_vector:
    /* Roll back by removing from vector (calls cleanup_index via wvector) */
    wvector_pop(tree->indexes);
    /* Drop the index tree */
    {
        MDB_txn *drop_txn;
        if (mdb_txn_begin(tree->db->env, NULL, 0, &drop_txn) == 0) {
            mdb_drop(drop_txn, idx_dbi, 1);
            mdb_txn_commit(drop_txn);
        }
    }
    return rc;

cleanup_user_data:
    free(idx->user_data);
cleanup_idx_name:
    free(idx->name);
cleanup_idx:
    free(idx_tree_name);
    free(idx);
    return rc;

cleanup_tree_name:
    free(idx_tree_name);
    return rc;
}

WTREE_COLD WTREE_WARN_UNUSED
int wtree3_tree_populate_index(wtree3_tree_t *tree,
                                const char *index_name,
                                gerror_t *error) {
    if (WTREE_UNLIKELY(!tree || !index_name)) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Invalid parameters");
        return WTREE3_EINVAL;
    }

    wtree3_index_t *idx = find_index(tree, index_name);
    if (!idx) {
        set_error(error, WTREE3_LIB, WTREE3_NOT_FOUND,
                 "Index '%s' not found", index_name);
        return WTREE3_NOT_FOUND;
    }

    /* Begin write transaction */
    MDB_txn *txn;
    int rc = mdb_txn_begin(tree->db->env, NULL, 0, &txn);
    if (rc != 0) return translate_mdb_error(rc, error);

    /* Create cursor for main tree */
    MDB_cursor *cursor;
    rc = mdb_cursor_open(txn, tree->dbi, &cursor);
    if (rc != 0) {
        mdb_txn_abort(txn);
        return translate_mdb_error(rc, error);
    }

    MDB_val mkey, mval;
    rc = mdb_cursor_get(cursor, &mkey, &mval, MDB_FIRST);

    while (rc == 0) {
        /* Extract index key */
        void *idx_key = NULL;
        size_t idx_key_size = 0;
        bool should_index = idx->key_fn(mval.mv_data, mval.mv_size, idx->user_data,
                                        &idx_key, &idx_key_size);

        if (should_index && idx_key) {
            /* Check unique constraint */
            if (idx->unique) {
                MDB_val check_key = {.mv_size = idx_key_size, .mv_data = idx_key};
                MDB_val check_val;
                int get_rc = mdb_get(txn, idx->dbi, &check_key, &check_val);
                if (get_rc == 0) {
                    free(idx_key);
                    mdb_cursor_close(cursor);
                    mdb_txn_abort(txn);
                    set_error(error, WTREE3_LIB, WTREE3_INDEX_ERROR,
                             "Duplicate key for unique index '%s'", index_name);
                    return WTREE3_INDEX_ERROR;
                }
            }

            /* Insert: index_key -> main_key */
            MDB_val idx_k = {.mv_size = idx_key_size, .mv_data = idx_key};
            MDB_val idx_v = {.mv_size = mkey.mv_size, .mv_data = mkey.mv_data};
            rc = mdb_put(txn, idx->dbi, &idx_k, &idx_v, MDB_NODUPDATA);
            free(idx_key);

            if (rc != 0 && rc != MDB_KEYEXIST) {
                mdb_cursor_close(cursor);
                mdb_txn_abort(txn);
                return translate_mdb_error(rc, error);
            }
        }

        rc = mdb_cursor_get(cursor, &mkey, &mval, MDB_NEXT);
    }

    mdb_cursor_close(cursor);

    if (rc != MDB_NOTFOUND) {
        mdb_txn_abort(txn);
        return translate_mdb_error(rc, error);
    }

    rc = mdb_txn_commit(txn);
    if (rc != 0) return translate_mdb_error(rc, error);

    return WTREE3_OK;
}

/* Helper context for drop_index transaction */
typedef struct {
    wtree3_tree_t *tree;
    const char *index_name;
    MDB_dbi idx_dbi;
} drop_index_ctx_t;

static int drop_index_txn(MDB_txn *txn, void *user_data) {
    drop_index_ctx_t *ctx = (drop_index_ctx_t *)user_data;

    /* Drop the index tree */
    int rc = mdb_drop(txn, ctx->idx_dbi, 1);
    if (rc != 0 && rc != MDB_NOTFOUND) {
        return rc;
    }

    return WTREE3_OK;
}

static int drop_index_metadata_txn(MDB_txn *txn, void *user_data) {
    drop_index_ctx_t *ctx = (drop_index_ctx_t *)user_data;
    return metadata_delete_txn(txn, ctx->tree->db, ctx->tree->name,
                               ctx->index_name, NULL);
}

WTREE_COLD WTREE_WARN_UNUSED
int wtree3_tree_drop_index(wtree3_tree_t *tree,
                            const char *index_name,
                            gerror_t *error) {
    if (WTREE_UNLIKELY(!tree || !index_name)) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Invalid parameters");
        return WTREE3_EINVAL;
    }

    /* Find index */
    wtree3_index_t *idx = find_index(tree, index_name);
    if (!idx) {
        set_error(error, WTREE3_LIB, WTREE3_NOT_FOUND,
                 "Index '%s' not found", index_name);
        return WTREE3_NOT_FOUND;
    }

    /* Save context before removing from vector */
    drop_index_ctx_t ctx = {
        .tree = tree,
        .index_name = index_name,
        .idx_dbi = idx->dbi
    };

    /* Delete the index tree */
    int rc = with_write_txn(tree->db, drop_index_txn, &ctx, error);
    if (rc != 0) return rc;

    /* Delete metadata (ignore errors - metadata might not exist) */
    rc = with_write_txn(tree->db, drop_index_metadata_txn, &ctx, NULL);
    (void)rc;

    /* Remove from vector (this will call cleanup_index automatically) */
    if (!wvector_remove(tree->indexes, index_name, compare_index_by_name)) {
        set_error(error, WTREE3_LIB, WTREE3_ERROR, "Failed to remove index from vector");
        return WTREE3_ERROR;
    }

    return WTREE3_OK;
}

WTREE_PURE
bool wtree3_tree_has_index(wtree3_tree_t *tree, const char *index_name) {
    return tree && index_name && find_index(tree, index_name) != NULL;
}

WTREE_PURE
size_t wtree3_tree_index_count(wtree3_tree_t *tree) {
    return tree ? wvector_size(tree->indexes) : 0;
}

WTREE_COLD WTREE_WARN_UNUSED
int wtree3_verify_indexes(wtree3_tree_t *tree, gerror_t *error) {
    if (WTREE_UNLIKELY(!tree)) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Tree cannot be NULL");
        return WTREE3_EINVAL;
    }

    size_t index_count = wvector_size(tree->indexes);
    if (index_count == 0) {
        return WTREE3_OK;  // No indexes to verify
    }

    // Create read transaction
    MDB_txn *txn;
    int rc = mdb_txn_begin(tree->db->env, NULL, MDB_RDONLY, &txn);
    if (rc != 0) return translate_mdb_error(rc, error);

    // Phase 1: Verify all main tree entries appear in their indexes
    MDB_cursor *main_cursor;
    rc = mdb_cursor_open(txn, tree->dbi, &main_cursor);
    if (rc != 0) {
        mdb_txn_abort(txn);
        return translate_mdb_error(rc, error);
    }

    MDB_val key, val;
    rc = mdb_cursor_get(main_cursor, &key, &val, MDB_FIRST);

    while (rc == 0) {
        // For each main entry, check it appears in all applicable indexes
        for (size_t i = 0; i < index_count; i++) {
            wtree3_index_t *idx = (wtree3_index_t *)wvector_get(tree->indexes, i);

            // Extract index key
            void *idx_key = NULL;
            size_t idx_key_len = 0;
            bool should_index = idx->key_fn(val.mv_data, val.mv_size, idx->user_data,
                                           &idx_key, &idx_key_len);

            if (!should_index) continue;  // Sparse index - this entry not indexed

            if (!idx_key) {
                mdb_cursor_close(main_cursor);
                mdb_txn_abort(txn);
                set_error(error, WTREE3_LIB, WTREE3_INDEX_ERROR,
                         "Index '%s': key extraction failed during verification", idx->name);
                return WTREE3_INDEX_ERROR;
            }

            // Check if this entry exists in the index
            MDB_val idx_search_key = {.mv_size = idx_key_len, .mv_data = idx_key};
            MDB_val idx_val;

            MDB_cursor *idx_cursor;
            int idx_rc = mdb_cursor_open(txn, idx->dbi, &idx_cursor);
            if (idx_rc != 0) {
                free(idx_key);
                mdb_cursor_close(main_cursor);
                mdb_txn_abort(txn);
                return translate_mdb_error(idx_rc, error);
            }

            // For unique indexes, there should be exactly one entry
            // For non-unique, search for our primary key in duplicates
            idx_rc = mdb_cursor_get(idx_cursor, &idx_search_key, &idx_val, MDB_SET);

            if (idx_rc == MDB_NOTFOUND) {
                // Missing index entry!
                mdb_cursor_close(idx_cursor);
                free(idx_key);
                mdb_cursor_close(main_cursor);
                mdb_txn_abort(txn);
                set_error(error, WTREE3_LIB, WTREE3_INDEX_ERROR,
                         "Index '%s': missing entry for main tree key (index inconsistency)",
                         idx->name);
                return WTREE3_INDEX_ERROR;
            }

            if (idx_rc != 0) {
                mdb_cursor_close(idx_cursor);
                free(idx_key);
                mdb_cursor_close(main_cursor);
                mdb_txn_abort(txn);
                return translate_mdb_error(idx_rc, error);
            }

            // For non-unique indexes with DUPSORT, verify our PK is in the duplicates
            if (!idx->unique) {
                bool found_pk = false;
                do {
                    if (idx_val.mv_size == key.mv_size &&
                        memcmp(idx_val.mv_data, key.mv_data, key.mv_size) == 0) {
                        found_pk = true;
                        break;
                    }
                    idx_rc = mdb_cursor_get(idx_cursor, &idx_search_key, &idx_val, MDB_NEXT_DUP);
                } while (idx_rc == 0);

                if (!found_pk) {
                    mdb_cursor_close(idx_cursor);
                    free(idx_key);
                    mdb_cursor_close(main_cursor);
                    mdb_txn_abort(txn);
                    set_error(error, WTREE3_LIB, WTREE3_INDEX_ERROR,
                             "Index '%s': primary key not found in index duplicates (index inconsistency)",
                             idx->name);
                    return WTREE3_INDEX_ERROR;
                }
            }

            mdb_cursor_close(idx_cursor);
            free(idx_key);
        }

        rc = mdb_cursor_get(main_cursor, &key, &val, MDB_NEXT);
    }

    if (rc != MDB_NOTFOUND) {
        mdb_cursor_close(main_cursor);
        mdb_txn_abort(txn);
        return translate_mdb_error(rc, error);
    }

    mdb_cursor_close(main_cursor);

    // Phase 2: Verify all index entries point to valid main tree entries
    for (size_t i = 0; i < index_count; i++) {
        wtree3_index_t *idx = (wtree3_index_t *)wvector_get(tree->indexes, i);

        MDB_cursor *idx_cursor;
        rc = mdb_cursor_open(txn, idx->dbi, &idx_cursor);
        if (rc != 0) {
            mdb_txn_abort(txn);
            return translate_mdb_error(rc, error);
        }

        MDB_val idx_key, idx_val;
        rc = mdb_cursor_get(idx_cursor, &idx_key, &idx_val, MDB_FIRST);

        while (rc == 0) {
            // idx_val contains the primary key - verify it exists in main tree
            MDB_val main_val;
            int lookup_rc = mdb_get(txn, tree->dbi, &idx_val, &main_val);

            if (lookup_rc == MDB_NOTFOUND) {
                // Orphaned index entry!
                mdb_cursor_close(idx_cursor);
                mdb_txn_abort(txn);
                set_error(error, WTREE3_LIB, WTREE3_INDEX_ERROR,
                         "Index '%s': orphaned entry pointing to non-existent main tree key",
                         idx->name);
                return WTREE3_INDEX_ERROR;
            }

            if (lookup_rc != 0) {
                mdb_cursor_close(idx_cursor);
                mdb_txn_abort(txn);
                return translate_mdb_error(lookup_rc, error);
            }

            // For unique indexes, verify no duplicates exist
            if (idx->unique) {
                MDB_val dup_check_key = idx_key;
                MDB_val dup_val;
                int dup_rc = mdb_cursor_get(idx_cursor, &dup_check_key, &dup_val, MDB_NEXT_DUP);
                if (dup_rc == 0) {
                    // Unique constraint violated!
                    mdb_cursor_close(idx_cursor);
                    mdb_txn_abort(txn);
                    set_error(error, WTREE3_LIB, WTREE3_INDEX_ERROR,
                             "Index '%s': unique constraint violated - duplicate keys found",
                             idx->name);
                    return WTREE3_INDEX_ERROR;
                }
            }

            rc = mdb_cursor_get(idx_cursor, &idx_key, &idx_val, MDB_NEXT);
        }

        if (rc != MDB_NOTFOUND) {
            mdb_cursor_close(idx_cursor);
            mdb_txn_abort(txn);
            return translate_mdb_error(rc, error);
        }

        mdb_cursor_close(idx_cursor);
    }

    mdb_txn_abort(txn);  // Read-only transaction
    return WTREE3_OK;
}
