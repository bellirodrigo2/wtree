/*
 * wtree3_tree.c - Tree Lifecycle and Configuration Management
 *
 * This module provides tree/collection management:
 * - Tree lifecycle (open, close, delete, exists)
 * - Tree configuration (set_compare, set_merge_fn)
 * - Index loader support for restoring persisted indexes
 */

#include "wtree3_internal.h"

/* Forward declarations from wtree3_index_persist.c */
extern char** wtree3_tree_list_persisted_indexes(wtree3_tree_t *tree, size_t *count, gerror_t *error);
extern void wtree3_index_list_free(char **list, size_t count);

/* ============================================================
 * Internal Helpers
 * ============================================================ */

/*
 * Auto-load all persisted indexes for a tree
 * Uses registered extractors from db->extractors registry
 */
static void auto_load_indexes(wtree3_tree_t *tree) {
    if (!tree) return;

    gerror_t error = {0};
    size_t index_count = 0;
    char **index_names = wtree3_tree_list_persisted_indexes(tree, &index_count, &error);

    if (!index_names || index_count == 0) {
        return;  // No indexes to load
    }

    // Load each index
    for (size_t i = 0; i < index_count; i++) {
        load_index_metadata(tree, index_names[i], &error);
        /* load_index_metadata handles missing extractors gracefully (prints warning) */
    }

    wtree3_index_list_free(index_names, index_count);
}

/* ============================================================
 * Tree Operations
 * ============================================================ */

wtree3_tree_t* wtree3_tree_open(wtree3_db_t *db, const char *name,
                                 unsigned int flags, int64_t entry_count,
                                 gerror_t *error) {
    if (!db || !name) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Database and name are required");
        return NULL;
    }

    wtree3_tree_t *tree = calloc(1, sizeof(wtree3_tree_t));
    if (!tree) {
        set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Failed to allocate tree");
        return NULL;
    }

    /* Open DBI in a transaction */
    MDB_txn *txn;
    int rc = mdb_txn_begin(db->env, NULL, 0, &txn);
    if (rc != 0) {
        translate_mdb_error(rc, error);
        free(tree);
        return NULL;
    }

    rc = mdb_dbi_open(txn, name, MDB_CREATE | flags, &tree->dbi);
    if (rc != 0) {
        translate_mdb_error(rc, error);
        mdb_txn_abort(txn);
        free(tree);
        return NULL;
    }

    rc = mdb_txn_commit(txn);
    if (rc != 0) {
        translate_mdb_error(rc, error);
        free(tree);
        return NULL;
    }

    tree->name = strdup(name);
    tree->db = db;
    tree->flags = flags;
    tree->entry_count = entry_count;
    tree->index_capacity = 4;
    tree->indexes = calloc(tree->index_capacity, sizeof(wtree3_index_t));

    if (!tree->name || !tree->indexes) {
        free(tree->name);
        free(tree->indexes);
        free(tree);
        set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Failed to allocate tree resources");
        return NULL;
    }

    /* Auto-load persisted indexes */
    auto_load_indexes(tree);

    return tree;
}

void wtree3_tree_close(wtree3_tree_t *tree) {
    if (!tree) return;

    /* Free all indexes */
    for (size_t i = 0; i < tree->index_count; i++) {
        free(tree->indexes[i].user_data);
        free(tree->indexes[i].name);
        free(tree->indexes[i].tree_name);
    }
    free(tree->indexes);
    free(tree->name);
    free(tree);
}

int wtree3_tree_delete(wtree3_db_t *db, const char *name, gerror_t *error) {
    if (!db || !name) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Invalid parameters");
        return WTREE3_EINVAL;
    }

    // Start a transaction
    MDB_txn *txn;
    int rc = mdb_txn_begin(db->env, NULL, 0, &txn);
    if (rc != 0) {
        return translate_mdb_error(rc, error);
    }

    // First check if the main tree exists
    MDB_dbi dbi;
    rc = mdb_dbi_open(txn, name, 0, &dbi);
    if (rc != 0) {
        mdb_txn_abort(txn);
        return translate_mdb_error(rc, error);  // Returns WTREE3_NOT_FOUND if doesn't exist
    }

    // Scan for all index DBs matching pattern "idx:<tree_name>:*"
    // Iterate through the unnamed database to find all named DBs
    MDB_dbi main_dbi;
    rc = mdb_dbi_open(txn, NULL, 0, &main_dbi);
    if (rc != 0) {
        mdb_txn_abort(txn);
        return translate_mdb_error(rc, error);
    }

    char **index_db_names = NULL;
    size_t index_count = 0;
    size_t index_capacity = 0;

    MDB_cursor *cursor;
    rc = mdb_cursor_open(txn, main_dbi, &cursor);
    if (rc == 0) {
        MDB_val key, val;
        char idx_prefix[256];
        snprintf(idx_prefix, sizeof(idx_prefix), "idx:%s:", name);
        size_t prefix_len = strlen(idx_prefix);

        // Scan all DB names
        rc = mdb_cursor_get(cursor, &key, &val, MDB_FIRST);
        while (rc == 0) {
            if (key.mv_size >= prefix_len && memcmp(key.mv_data, idx_prefix, prefix_len) == 0) {
                // Found an index DB for this tree
                if (index_count >= index_capacity) {
                    index_capacity = index_capacity ? index_capacity * 2 : 4;
                    char **new_arr = realloc(index_db_names, index_capacity * sizeof(char*));
                    if (new_arr) {
                        index_db_names = new_arr;
                    }
                }
                if (index_count < index_capacity) {
                    index_db_names[index_count] = malloc(key.mv_size + 1);
                    if (index_db_names[index_count]) {
                        memcpy(index_db_names[index_count], key.mv_data, key.mv_size);
                        index_db_names[index_count][key.mv_size] = '\0';
                        index_count++;
                    }
                }
            }
            rc = mdb_cursor_get(cursor, &key, &val, MDB_NEXT);
        }
        mdb_cursor_close(cursor);
    }

    // Delete all found index DBs
    for (size_t i = 0; i < index_count; i++) {
        MDB_dbi idx_dbi;
        rc = mdb_dbi_open(txn, index_db_names[i], 0, &idx_dbi);
        if (rc == 0) {
            rc = mdb_drop(txn, idx_dbi, 1);
        }
        free(index_db_names[i]);
    }
    free(index_db_names);

    // Delete metadata entries for this tree (tree_name:*)
    MDB_dbi metadata_dbi;
    rc = get_metadata_dbi(db, txn, &metadata_dbi, error);
    if (rc == 0) {
        MDB_cursor *meta_cursor;
        rc = mdb_cursor_open(txn, metadata_dbi, &meta_cursor);
        if (rc == 0) {
            MDB_val key, val;
            char meta_prefix[256];
            snprintf(meta_prefix, sizeof(meta_prefix), "%s:", name);
            size_t prefix_len = strlen(meta_prefix);

            // Collect keys to delete (can't delete while iterating)
            char **keys_to_delete = NULL;
            size_t key_count = 0;
            size_t key_capacity = 0;

            rc = mdb_cursor_get(meta_cursor, &key, &val, MDB_FIRST);
            while (rc == 0) {
                if (key.mv_size >= prefix_len &&
                    memcmp(key.mv_data, meta_prefix, prefix_len) == 0) {
                    if (key_count >= key_capacity) {
                        key_capacity = key_capacity ? key_capacity * 2 : 4;
                        char **new_arr = realloc(keys_to_delete, key_capacity * sizeof(char*));
                        if (new_arr) {
                            keys_to_delete = new_arr;
                        }
                    }
                    if (key_count < key_capacity) {
                        keys_to_delete[key_count] = malloc(key.mv_size);
                        if (keys_to_delete[key_count]) {
                            memcpy(keys_to_delete[key_count], key.mv_data, key.mv_size);
                            key_count++;
                        }
                    }
                }
                rc = mdb_cursor_get(meta_cursor, &key, &val, MDB_NEXT);
            }

            // Delete collected metadata keys
            for (size_t i = 0; i < key_count; i++) {
                MDB_val del_key = {.mv_data = keys_to_delete[i], .mv_size = strlen(keys_to_delete[i])};
                mdb_cursor_get(meta_cursor, &del_key, &val, MDB_SET);
                mdb_cursor_del(meta_cursor, 0);
                free(keys_to_delete[i]);
            }
            free(keys_to_delete);

            mdb_cursor_close(meta_cursor);
        }
    }

    // Finally, delete the main tree DBI
    rc = mdb_drop(txn, dbi, 1);
    if (rc != 0) {
        mdb_txn_abort(txn);
        return translate_mdb_error(rc, error);
    }

    rc = mdb_txn_commit(txn);
    if (rc != 0) return translate_mdb_error(rc, error);

    return WTREE3_OK;
}

const char* wtree3_tree_name(wtree3_tree_t *tree) {
    return tree ? tree->name : NULL;
}

int64_t wtree3_tree_count(wtree3_tree_t *tree) {
    return tree ? tree->entry_count : 0;
}

wtree3_db_t* wtree3_tree_get_db(wtree3_tree_t *tree) {
    return tree ? tree->db : NULL;
}

int wtree3_tree_set_compare(wtree3_tree_t *tree, MDB_cmp_func *cmp, gerror_t *error) {
    if (!tree || !cmp) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Invalid parameters");
        return WTREE3_EINVAL;
    }

    MDB_txn *txn;
    int rc = mdb_txn_begin(tree->db->env, NULL, 0, &txn);
    if (rc != 0) return translate_mdb_error(rc, error);

    rc = mdb_set_compare(txn, tree->dbi, cmp);
    if (rc != 0) {
        mdb_txn_abort(txn);
        return translate_mdb_error(rc, error);
    }

    rc = mdb_txn_commit(txn);
    if (rc != 0) return translate_mdb_error(rc, error);

    return WTREE3_OK;
}

void wtree3_tree_set_merge_fn(wtree3_tree_t *tree, wtree3_merge_fn merge_fn, void *user_data) {
    if (!tree) return;
    tree->merge_fn = merge_fn;
    tree->merge_user_data = user_data;
}

int wtree3_tree_exists(wtree3_db_t *db, const char *name, gerror_t *error) {
    if (!db || !name) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "DB and name cannot be NULL");
        return WTREE3_EINVAL;
    }

    MDB_txn *txn;
    int rc = mdb_txn_begin(db->env, NULL, MDB_RDONLY, &txn);
    if (rc != 0) {
        return translate_mdb_error(rc, error);
    }

    MDB_dbi dbi;
    // Try to open without MDB_CREATE flag - this won't create the DB
    rc = mdb_dbi_open(txn, name, 0, &dbi);

    mdb_txn_abort(txn);  // Clean up read-only transaction

    if (rc == MDB_NOTFOUND) {
        return 0;  // DB doesn't exist
    } else if (rc == 0) {
        return 1;  // DB exists
    } else {
        return translate_mdb_error(rc, error);  // Error occurred
    }
}
