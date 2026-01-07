/*
 * wtree3_index_persist.c - Index Persistence and Restoration
 *
 * This module handles index metadata serialization and restoration:
 * - Save index metadata (extractor_id, flags, user_data)
 * - Load index metadata and auto-attach indexes
 * - Index introspection (get_extractor_id)
 *
 * Metadata format (16 bytes + user_data):
 *   [extractor_id:8][flags:4][user_data_len:4][user_data:N]
 */

#include "wtree3_internal.h"

/* ============================================================
 * Metadata Save/Load Operations
 * ============================================================ */

int save_index_metadata(wtree3_tree_t *tree,
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

    /* Build metadata value:
     * Format: [extractor_id:8][flags:4][user_data_len:4][user_data:N]
     */
    size_t meta_len = 8 + 4 + 4 + idx->user_data_len;
    uint8_t *meta_value = malloc(meta_len);
    if (!meta_value) {
        set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Failed to allocate metadata");
        return WTREE3_ENOMEM;
    }

    /* Write extractor_id (8 bytes, little-endian) */
    uint64_t extractor_id = idx->extractor_id;
    memcpy(meta_value, &extractor_id, 8);

    /* Write flags (4 bytes) */
    uint32_t flags = 0;
    if (idx->unique) flags |= 0x01;
    if (idx->sparse) flags |= 0x02;
    memcpy(meta_value + 8, &flags, 4);

    /* Write user_data length (4 bytes) */
    uint32_t ud_len = (uint32_t)idx->user_data_len;
    memcpy(meta_value + 12, &ud_len, 4);

    /* Write user_data */
    if (idx->user_data && idx->user_data_len > 0) {
        memcpy(meta_value + 16, idx->user_data, idx->user_data_len);
    }

    /* Store in metadata DBI */
    MDB_txn *txn;
    int rc = mdb_txn_begin(tree->db->env, NULL, 0, &txn);
    if (rc != 0) {
        free(meta_value);
        return translate_mdb_error(rc, error);
    }

    MDB_dbi meta_dbi;
    rc = get_metadata_dbi(tree->db, txn, &meta_dbi, error);
    if (rc != 0) {
        free(meta_value);
        mdb_txn_abort(txn);
        return rc;
    }

    /* Build key: tree_name:index_name */
    char *meta_key = build_metadata_key(tree->name, index_name);
    if (!meta_key) {
        free(meta_value);
        mdb_txn_abort(txn);
        set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Failed to build metadata key");
        return WTREE3_ENOMEM;
    }

    MDB_val key = {.mv_size = strlen(meta_key), .mv_data = meta_key};
    MDB_val val = {.mv_size = meta_len, .mv_data = meta_value};

    rc = mdb_put(txn, meta_dbi, &key, &val, 0);
    free(meta_key);
    free(meta_value);

    if (rc != 0) {
        mdb_txn_abort(txn);
        return translate_mdb_error(rc, error);
    }

    rc = mdb_txn_commit(txn);
    if (rc != 0) {
        return translate_mdb_error(rc, error);
    }

    return WTREE3_OK;
}

/*
 * Load index metadata and attach to tree
 * Called internally by wtree3_tree_open() for auto-loading
 */
int load_index_metadata(wtree3_tree_t *tree, const char *index_name, gerror_t *error) {
    if (!tree || !index_name) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Invalid parameters");
        return WTREE3_EINVAL;
    }

    /* Check if index already loaded */
    if (find_index(tree, index_name)) {
        /* Already loaded, skip */
        return WTREE3_OK;
    }

    /* Read metadata */
    MDB_txn *txn;
    int rc = mdb_txn_begin(tree->db->env, NULL, MDB_RDONLY, &txn);
    if (rc != 0) {
        return translate_mdb_error(rc, error);
    }

    MDB_dbi meta_dbi;
    rc = get_metadata_dbi(tree->db, txn, &meta_dbi, error);
    if (rc != 0) {
        mdb_txn_abort(txn);
        return rc;
    }

    char *meta_key = build_metadata_key(tree->name, index_name);
    if (!meta_key) {
        mdb_txn_abort(txn);
        set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Failed to build metadata key");
        return WTREE3_ENOMEM;
    }

    MDB_val key = {.mv_size = strlen(meta_key), .mv_data = meta_key};
    MDB_val val;

    rc = mdb_get(txn, meta_dbi, &key, &val);
    free(meta_key);

    if (rc != 0) {
        mdb_txn_abort(txn);
        if (rc == MDB_NOTFOUND) {
            set_error(error, WTREE3_LIB, WTREE3_NOT_FOUND,
                     "No metadata found for index '%s'", index_name);
            return WTREE3_NOT_FOUND;
        }
        return translate_mdb_error(rc, error);
    }

    /* Parse metadata */
    if (val.mv_size < 16) {
        mdb_txn_abort(txn);
        set_error(error, WTREE3_LIB, WTREE3_ERROR, "Invalid metadata format");
        return WTREE3_ERROR;
    }

    uint8_t *meta_data = (uint8_t*)val.mv_data;

    uint64_t extractor_id;
    memcpy(&extractor_id, meta_data, 8);

    uint32_t flags;
    memcpy(&flags, meta_data + 8, 4);
    bool unique = (flags & 0x01) != 0;
    bool sparse = (flags & 0x02) != 0;

    uint32_t ud_len;
    memcpy(&ud_len, meta_data + 12, 4);

    if (val.mv_size < 16 + ud_len) {
        mdb_txn_abort(txn);
        set_error(error, WTREE3_LIB, WTREE3_ERROR, "Invalid metadata format");
        return WTREE3_ERROR;
    }

    /* Look up extractor function */
    wtree3_index_key_fn key_fn = find_extractor(tree->db, extractor_id);
    if (!key_fn) {
        mdb_txn_abort(txn);
        /* Extractor not registered - log warning and skip */
        fprintf(stderr, "Warning: Skipping index '%s' - extractor 0x%016llx not registered\n",
                index_name, (unsigned long long)extractor_id);
        return WTREE3_OK;  /* Not an error - just skip */
    }

    /* Copy user_data */
    void *user_data = NULL;
    if (ud_len > 0) {
        user_data = malloc(ud_len);
        if (!user_data) {
            mdb_txn_abort(txn);
            set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Failed to allocate user_data");
            return WTREE3_ENOMEM;
        }
        memcpy(user_data, meta_data + 16, ud_len);
    }

    mdb_txn_abort(txn);  /* Done reading */

    /* Open existing index DBI */
    char *idx_tree_name = build_index_tree_name(tree->name, index_name);
    if (!idx_tree_name) {
        free(user_data);
        set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Failed to allocate tree name");
        return WTREE3_ENOMEM;
    }

    rc = mdb_txn_begin(tree->db->env, NULL, 0, &txn);
    if (rc != 0) {
        free(user_data);
        free(idx_tree_name);
        return translate_mdb_error(rc, error);
    }

    MDB_dbi idx_dbi;
    rc = mdb_dbi_open(txn, idx_tree_name, MDB_DUPSORT, &idx_dbi);
    if (rc != 0) {
        mdb_txn_abort(txn);
        free(user_data);
        free(idx_tree_name);
        return translate_mdb_error(rc, error);
    }

    rc = mdb_txn_commit(txn);
    if (rc != 0) {
        free(user_data);
        free(idx_tree_name);
        return translate_mdb_error(rc, error);
    }

    /* Expand array if needed */
    if (tree->index_count >= tree->index_capacity) {
        size_t new_capacity = tree->index_capacity * 2;
        wtree3_index_t *new_indexes = realloc(tree->indexes,
                                               new_capacity * sizeof(wtree3_index_t));
        if (!new_indexes) {
            free(user_data);
            free(idx_tree_name);
            set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Failed to expand index array");
            return WTREE3_ENOMEM;
        }
        tree->indexes = new_indexes;
        tree->index_capacity = new_capacity;
    }

    /* Add to index array */
    wtree3_index_t *idx = &tree->indexes[tree->index_count];
    idx->name = strdup(index_name);
    idx->tree_name = idx_tree_name;
    idx->dbi = idx_dbi;
    idx->extractor_id = extractor_id;
    idx->key_fn = key_fn;
    idx->user_data = user_data;
    idx->user_data_len = ud_len;
    idx->unique = unique;
    idx->sparse = sparse;
    idx->compare = NULL;  /* Not persisted */

    if (!idx->name) {
        free(user_data);
        free(idx_tree_name);
        set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Failed to allocate index name");
        return WTREE3_ENOMEM;
    }

    tree->index_count++;
    return WTREE3_OK;
}

/* ============================================================
 * Introspection Operations
 * ============================================================ */

int wtree3_index_get_extractor_id(wtree3_tree_t *tree,
                                    const char *index_name,
                                    uint64_t *out_extractor_id,
                                    gerror_t *error) {
    if (!tree || !index_name || !out_extractor_id) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Invalid parameters");
        return WTREE3_EINVAL;
    }

    /* Check if index is loaded */
    wtree3_index_t *idx = find_index(tree, index_name);
    if (idx) {
        *out_extractor_id = idx->extractor_id;
        return WTREE3_OK;
    }

    /* Read from metadata */
    MDB_txn *txn;
    int rc = mdb_txn_begin(tree->db->env, NULL, MDB_RDONLY, &txn);
    if (rc != 0) {
        return translate_mdb_error(rc, error);
    }

    MDB_dbi meta_dbi;
    rc = get_metadata_dbi(tree->db, txn, &meta_dbi, error);
    if (rc != 0) {
        mdb_txn_abort(txn);
        return rc;
    }

    char *meta_key = build_metadata_key(tree->name, index_name);
    if (!meta_key) {
        mdb_txn_abort(txn);
        set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Failed to build metadata key");
        return WTREE3_ENOMEM;
    }

    MDB_val key = {.mv_size = strlen(meta_key), .mv_data = meta_key};
    MDB_val val;

    rc = mdb_get(txn, meta_dbi, &key, &val);
    free(meta_key);

    if (rc != 0) {
        mdb_txn_abort(txn);
        if (rc == MDB_NOTFOUND) {
            return translate_mdb_error(rc, error);
        }
        return translate_mdb_error(rc, error);
    }

    if (val.mv_size < 8) {
        mdb_txn_abort(txn);
        set_error(error, WTREE3_LIB, WTREE3_ERROR, "Invalid metadata format");
        return WTREE3_ERROR;
    }

    uint8_t *meta_data = (uint8_t*)val.mv_data;
    memcpy(out_extractor_id, meta_data, 8);

    mdb_txn_abort(txn);
    return WTREE3_OK;
}

/* ============================================================
 * List Persisted Indexes
 * ============================================================ */

char** wtree3_tree_list_persisted_indexes(wtree3_tree_t *tree,
                                            size_t *count,
                                            gerror_t *error) {
    if (!tree || !count) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Invalid parameters");
        return NULL;
    }

    *count = 0;

    MDB_txn *txn;
    int rc = mdb_txn_begin(tree->db->env, NULL, MDB_RDONLY, &txn);
    if (rc != 0) {
        translate_mdb_error(rc, error);
        return NULL;
    }

    MDB_dbi meta_dbi;
    rc = get_metadata_dbi(tree->db, txn, &meta_dbi, error);
    if (rc != 0) {
        mdb_txn_abort(txn);
        return NULL;
    }

    /* Build prefix: tree_name: */
    size_t prefix_len = strlen(tree->name) + 1;
    char *prefix = malloc(prefix_len + 1);
    if (!prefix) {
        mdb_txn_abort(txn);
        set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Failed to allocate prefix");
        return NULL;
    }
    snprintf(prefix, prefix_len + 1, "%s:", tree->name);

    /* Count matching entries first */
    MDB_cursor *cursor;
    rc = mdb_cursor_open(txn, meta_dbi, &cursor);
    if (rc != 0) {
        free(prefix);
        mdb_txn_abort(txn);
        translate_mdb_error(rc, error);
        return NULL;
    }

    MDB_val key, val;
    size_t match_count = 0;
    rc = mdb_cursor_get(cursor, &key, &val, MDB_FIRST);
    while (rc == 0) {
        if (key.mv_size >= prefix_len &&
            memcmp(key.mv_data, prefix, prefix_len) == 0) {
            match_count++;
        }
        rc = mdb_cursor_get(cursor, &key, &val, MDB_NEXT);
    }

    if (match_count == 0) {
        free(prefix);
        mdb_cursor_close(cursor);
        mdb_txn_abort(txn);
        return NULL;
    }

    /* Allocate result array */
    char **result = calloc(match_count + 1, sizeof(char*));
    if (!result) {
        free(prefix);
        mdb_cursor_close(cursor);
        mdb_txn_abort(txn);
        set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Failed to allocate result array");
        return NULL;
    }

    /* Populate array */
    size_t idx = 0;
    rc = mdb_cursor_get(cursor, &key, &val, MDB_FIRST);
    while (rc == 0 && idx < match_count) {
        if (key.mv_size >= prefix_len &&
            memcmp(key.mv_data, prefix, prefix_len) == 0) {
            /* Extract index name (after prefix) */
            size_t name_len = key.mv_size - prefix_len;
            result[idx] = malloc(name_len + 1);
            if (result[idx]) {
                memcpy(result[idx], (char*)key.mv_data + prefix_len, name_len);
                result[idx][name_len] = '\0';
                idx++;
            }
        }
        rc = mdb_cursor_get(cursor, &key, &val, MDB_NEXT);
    }

    free(prefix);
    mdb_cursor_close(cursor);
    mdb_txn_abort(txn);

    *count = idx;
    return result;
}

void wtree3_index_list_free(char **list, size_t count) {
    if (!list) return;
    for (size_t i = 0; i < count; i++) {
        free(list[i]);
    }
    free(list);
}
