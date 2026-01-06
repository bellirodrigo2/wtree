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

/* Forward declaration from wtree3_index_persist.c */
extern int wtree3_index_save_metadata(wtree3_tree_t *tree, const char *index_name, gerror_t *error);

/* ============================================================
 * Helper Functions
 * ============================================================ */

/* Build index tree name: idx:<tree_name>:<index_name> */
char* build_index_tree_name(const char *tree_name, const char *index_name) {
    size_t len = strlen(WTREE3_INDEX_PREFIX) + strlen(tree_name) + 1 + strlen(index_name) + 1;
    char *name = malloc(len);
    if (name) {
        snprintf(name, len, "%s%s:%s", WTREE3_INDEX_PREFIX, tree_name, index_name);
    }
    return name;
}

/* Find index by name */
wtree3_index_t* find_index(wtree3_tree_t *tree, const char *name) {
    for (size_t i = 0; i < tree->index_count; i++) {
        if (strcmp(tree->indexes[i].name, name) == 0) {
            return &tree->indexes[i];
        }
    }
    return NULL;
}

/* Get or create metadata DBI */
int get_metadata_dbi(wtree3_db_t *db, MDB_txn *txn, MDB_dbi *out_dbi, gerror_t *error) {
    int rc = mdb_dbi_open(txn, WTREE3_META_DB, MDB_CREATE, out_dbi);
    if (rc != 0) {
        return translate_mdb_error(rc, error);
    }
    return WTREE3_OK;
}

/* Build metadata key: tree_name:index_name */
char* build_metadata_key(const char *tree_name, const char *index_name) {
    size_t len = strlen(tree_name) + 1 + strlen(index_name) + 1;
    char *key = malloc(len);
    if (key) {
        snprintf(key, len, "%s:%s", tree_name, index_name);
    }
    return key;
}


/* ============================================================
 * Index Management
 * ============================================================ */

int wtree3_tree_add_index(wtree3_tree_t *tree,
                           const wtree3_index_config_t *config,
                           gerror_t *error) {
    if (!tree || !config || !config->name || !config->key_fn) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Invalid parameters");
        return WTREE3_EINVAL;
    }

    /* Check if index already exists */
    if (find_index(tree, config->name)) {
        set_error(error, WTREE3_LIB, WTREE3_KEY_EXISTS,
                 "Index '%s' already exists", config->name);
        return WTREE3_KEY_EXISTS;
    }

    /* Expand array if needed */
    if (tree->index_count >= tree->index_capacity) {
        size_t new_capacity = tree->index_capacity * 2;
        wtree3_index_t *new_indexes = realloc(tree->indexes,
                                               new_capacity * sizeof(wtree3_index_t));
        if (!new_indexes) {
            set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Failed to expand index array");
            return WTREE3_ENOMEM;
        }
        tree->indexes = new_indexes;
        tree->index_capacity = new_capacity;
    }

    /* Build index tree name */
    char *idx_tree_name = build_index_tree_name(tree->name, config->name);
    if (!idx_tree_name) {
        set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Failed to allocate tree name");
        return WTREE3_ENOMEM;
    }

    /* Create index DBI with DUPSORT */
    MDB_txn *txn;
    int rc = mdb_txn_begin(tree->db->env, NULL, 0, &txn);
    if (rc != 0) {
        free(idx_tree_name);
        return translate_mdb_error(rc, error);
    }

    MDB_dbi idx_dbi;
    rc = mdb_dbi_open(txn, idx_tree_name, MDB_CREATE | MDB_DUPSORT, &idx_dbi);
    if (rc != 0) {
        mdb_txn_abort(txn);
        free(idx_tree_name);
        return translate_mdb_error(rc, error);
    }

    /* Set custom comparator if provided */
    if (config->compare) {
        rc = mdb_set_compare(txn, idx_dbi, config->compare);
        if (rc != 0) {
            mdb_txn_abort(txn);
            free(idx_tree_name);
            return translate_mdb_error(rc, error);
        }
    }

    rc = mdb_txn_commit(txn);
    if (rc != 0) {
        free(idx_tree_name);
        return translate_mdb_error(rc, error);
    }

    /* Add to index array */
    wtree3_index_t *idx = &tree->indexes[tree->index_count];
    idx->name = strdup(config->name);
    idx->tree_name = idx_tree_name;
    idx->dbi = idx_dbi;
    idx->key_fn = config->key_fn;
    idx->user_data = config->user_data;
    idx->user_data_cleanup = config->user_data_cleanup;
    idx->unique = config->unique;
    idx->sparse = config->sparse;
    idx->compare = config->compare;
    idx->has_persistence = (config->persistence != NULL);
    if (idx->has_persistence) {
        idx->persistence = *config->persistence;
    }

    if (!idx->name) {
        free(idx_tree_name);
        set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Failed to allocate index name");
        return WTREE3_ENOMEM;
    }

    tree->index_count++;

    /* Save metadata if persistence is configured */
    if (idx->has_persistence) {
        rc = wtree3_index_save_metadata(tree, config->name, error);
        if (rc != 0) {
            /* Failed to save metadata - roll back */
            tree->index_count--;
            if (idx->user_data_cleanup && idx->user_data) {
                idx->user_data_cleanup(idx->user_data);
            }
            free(idx->name);
            free(idx->tree_name);

            /* Drop the index tree */
            MDB_txn *drop_txn;
            if (mdb_txn_begin(tree->db->env, NULL, 0, &drop_txn) == 0) {
                mdb_drop(drop_txn, idx->dbi, 1);
                mdb_txn_commit(drop_txn);
            }

            return rc;
        }
    }

    return WTREE3_OK;
}

int wtree3_tree_populate_index(wtree3_tree_t *tree,
                                const char *index_name,
                                gerror_t *error) {
    if (!tree || !index_name) {
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

int wtree3_tree_drop_index(wtree3_tree_t *tree,
                            const char *index_name,
                            gerror_t *error) {
    if (!tree || !index_name) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Invalid parameters");
        return WTREE3_EINVAL;
    }

    /* Find index */
    size_t idx_pos = 0;
    wtree3_index_t *idx = NULL;
    for (size_t i = 0; i < tree->index_count; i++) {
        if (strcmp(tree->indexes[i].name, index_name) == 0) {
            idx = &tree->indexes[i];
            idx_pos = i;
            break;
        }
    }

    if (!idx) {
        set_error(error, WTREE3_LIB, WTREE3_NOT_FOUND,
                 "Index '%s' not found", index_name);
        return WTREE3_NOT_FOUND;
    }

    /* Delete the index tree */
    MDB_txn *txn;
    int rc = mdb_txn_begin(tree->db->env, NULL, 0, &txn);
    if (rc != 0) return translate_mdb_error(rc, error);

    rc = mdb_drop(txn, idx->dbi, 1);
    if (rc != 0 && rc != MDB_NOTFOUND) {
        mdb_txn_abort(txn);
        return translate_mdb_error(rc, error);
    }

    rc = mdb_txn_commit(txn);
    if (rc != 0) return translate_mdb_error(rc, error);

    /* Delete metadata if it exists */
    if (idx->has_persistence) {
        rc = mdb_txn_begin(tree->db->env, NULL, 0, &txn);
        if (rc == 0) {
            MDB_dbi meta_dbi;
            if (get_metadata_dbi(tree->db, txn, &meta_dbi, NULL) == 0) {
                char *meta_key = build_metadata_key(tree->name, index_name);
                if (meta_key) {
                    MDB_val key = {.mv_size = strlen(meta_key), .mv_data = meta_key};
                    mdb_del(txn, meta_dbi, &key, NULL);
                    free(meta_key);
                }
            }
            mdb_txn_commit(txn);
        }
    }

    /* Free index entry */
    if (idx->user_data_cleanup && idx->user_data) {
        idx->user_data_cleanup(idx->user_data);
    }
    free(idx->name);
    free(idx->tree_name);

    /* Remove from array by shifting */
    for (size_t i = idx_pos; i < tree->index_count - 1; i++) {
        tree->indexes[i] = tree->indexes[i + 1];
    }
    tree->index_count--;

    return WTREE3_OK;
}

bool wtree3_tree_has_index(wtree3_tree_t *tree, const char *index_name) {
    return tree && index_name && find_index(tree, index_name) != NULL;
}

size_t wtree3_tree_index_count(wtree3_tree_t *tree) {
    return tree ? tree->index_count : 0;
}

int wtree3_verify_indexes(wtree3_tree_t *tree, gerror_t *error) {
    if (!tree) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Tree cannot be NULL");
        return WTREE3_EINVAL;
    }

    if (tree->index_count == 0) {
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
        for (size_t i = 0; i < tree->index_count; i++) {
            wtree3_index_t *idx = &tree->indexes[i];

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
    for (size_t i = 0; i < tree->index_count; i++) {
        wtree3_index_t *idx = &tree->indexes[i];

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
