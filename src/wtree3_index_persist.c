/*
 * wtree3_index_persist.c - Index Persistence and Restoration
 *
 * This module handles index metadata serialization and restoration:
 * - Index metadata save/load (save_metadata, load_metadata)
 * - Index listing (list_persisted_indexes, index_list_free)
 * - Index loader support (set_index_loader)
 */

#include "wtree3_internal.h"

/* ============================================================
 * Index Persistence Operations
 * ============================================================ */

int wtree3_index_save_metadata(wtree3_tree_t *tree,
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

    if (!idx->has_persistence) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL,
                 "Index '%s' has no persistence configuration", index_name);
        return WTREE3_EINVAL;
    }

    /* Serialize user_data */
    void *serialized_data = NULL;
    size_t serialized_len = 0;
    int rc = idx->persistence.serialize(idx->user_data, &serialized_data, &serialized_len);
    if (rc != 0 || !serialized_data) {
        set_error(error, WTREE3_LIB, WTREE3_ERROR,
                 "Failed to serialize user_data for index '%s'", index_name);
        return WTREE3_ERROR;
    }

    /* Build metadata value:
     * Format: [version:4][flags:1][user_data_len:4][user_data:N]
     * flags byte: bit 0 = unique, bit 1 = sparse
     */
    size_t meta_len = 4 + 1 + 4 + serialized_len;
    uint8_t *meta_value = malloc(meta_len);
    if (!meta_value) {
        free(serialized_data);
        set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Failed to allocate metadata");
        return WTREE3_ENOMEM;
    }

    /* Write version (4 bytes, little-endian) */
    uint32_t version = WTREE3_META_VERSION;
    memcpy(meta_value, &version, 4);

    /* Write flags (1 byte) */
    uint8_t flags = 0;
    if (idx->unique) flags |= 0x01;
    if (idx->sparse) flags |= 0x02;
    meta_value[4] = flags;

    /* Write user_data length (4 bytes) */
    uint32_t ud_len = (uint32_t)serialized_len;
    memcpy(meta_value + 5, &ud_len, 4);

    /* Write user_data */
    memcpy(meta_value + 9, serialized_data, serialized_len);
    free(serialized_data);

    /* Store in metadata DBI */
    MDB_txn *txn;
    rc = mdb_txn_begin(tree->db->env, NULL, 0, &txn);
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

int wtree3_index_load_metadata(wtree3_tree_t *tree,
                                 const char *index_name,
                                 wtree3_index_key_fn key_fn,
                                 wtree3_user_data_persistence_t *persistence,
                                 gerror_t *error) {
    if (!tree || !index_name || !key_fn || !persistence || !persistence->deserialize) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Invalid parameters");
        return WTREE3_EINVAL;
    }

    /* Check if index already loaded */
    if (find_index(tree, index_name)) {
        set_error(error, WTREE3_LIB, WTREE3_KEY_EXISTS,
                 "Index '%s' already loaded", index_name);
        return WTREE3_KEY_EXISTS;
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
    if (val.mv_size < 9) {
        mdb_txn_abort(txn);
        set_error(error, WTREE3_LIB, WTREE3_ERROR, "Invalid metadata format");
        return WTREE3_ERROR;
    }

    uint8_t *meta_data = (uint8_t*)val.mv_data;
    uint32_t version;
    memcpy(&version, meta_data, 4);

    if (version != WTREE3_META_VERSION) {
        mdb_txn_abort(txn);
        set_error(error, WTREE3_LIB, WTREE3_ERROR,
                 "Metadata version mismatch (expected %d, got %d)",
                 WTREE3_META_VERSION, version);
        return WTREE3_ERROR;
    }

    uint8_t flags = meta_data[4];
    bool unique = (flags & 0x01) != 0;
    bool sparse = (flags & 0x02) != 0;

    uint32_t ud_len;
    memcpy(&ud_len, meta_data + 5, 4);

    if (val.mv_size < 9 + ud_len) {
        mdb_txn_abort(txn);
        set_error(error, WTREE3_LIB, WTREE3_ERROR, "Invalid metadata format");
        return WTREE3_ERROR;
    }

    /* Deserialize user_data */
    void *user_data = persistence->deserialize(meta_data + 9, ud_len);
    mdb_txn_abort(txn);

    if (!user_data) {
        set_error(error, WTREE3_LIB, WTREE3_ERROR,
                 "Failed to deserialize user_data for index '%s'", index_name);
        return WTREE3_ERROR;
    }

    /* Open existing index DBI */
    char *idx_tree_name = build_index_tree_name(tree->name, index_name);
    if (!idx_tree_name) {
        set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Failed to allocate tree name");
        return WTREE3_ENOMEM;
    }

    rc = mdb_txn_begin(tree->db->env, NULL, 0, &txn);
    if (rc != 0) {
        free(idx_tree_name);
        return translate_mdb_error(rc, error);
    }

    MDB_dbi idx_dbi;
    rc = mdb_dbi_open(txn, idx_tree_name, MDB_DUPSORT, &idx_dbi);
    if (rc != 0) {
        mdb_txn_abort(txn);
        free(idx_tree_name);
        return translate_mdb_error(rc, error);
    }

    rc = mdb_txn_commit(txn);
    if (rc != 0) {
        free(idx_tree_name);
        return translate_mdb_error(rc, error);
    }

    /* Expand array if needed */
    if (tree->index_count >= tree->index_capacity) {
        size_t new_capacity = tree->index_capacity * 2;
        wtree3_index_t *new_indexes = realloc(tree->indexes,
                                               new_capacity * sizeof(wtree3_index_t));
        if (!new_indexes) {
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
    idx->key_fn = key_fn;
    idx->user_data = user_data;
    idx->user_data_cleanup = NULL;
    idx->unique = unique;
    idx->sparse = sparse;
    idx->compare = NULL;
    idx->persistence = *persistence;
    idx->has_persistence = true;

    if (!idx->name) {
        free(idx_tree_name);
        set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Failed to allocate index name");
        return WTREE3_ENOMEM;
    }

    tree->index_count++;
    return WTREE3_OK;
}

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
