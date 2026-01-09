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
#include "macros.h"

/* ============================================================
 * Index Metadata Format
 * ============================================================ */

/* Field offsets in serialized metadata */
#define META_EXTRACTOR_ID_OFFSET    0
#define META_FLAGS_OFFSET           8
#define META_USERDATA_LEN_OFFSET    12
#define META_USERDATA_OFFSET        16

/* Field sizes */
#define META_EXTRACTOR_ID_SIZE      8   /* uint64_t */
#define META_FLAGS_SIZE             4   /* uint32_t */
#define META_USERDATA_LEN_SIZE      4   /* uint32_t */

/* Total header size (before variable-length user_data) */
#define META_HEADER_SIZE            16

/* Flag bits */
#define META_FLAG_UNIQUE            0x01
#define META_FLAG_SPARSE            0x02

/*
 * In-memory representation of index metadata
 * (For serialization/deserialization)
 */
typedef struct {
    uint64_t extractor_id;
    bool unique;
    bool sparse;
    void *user_data;
    size_t user_data_len;
} index_metadata_t;

/* ============================================================
 * Serialization/Deserialization Helpers
 * ============================================================ */

/*
 * Serialize index metadata to binary format
 * Returns allocated buffer (caller must free) or NULL on error
 * Sets out_len to total serialized size
 */
WTREE_MALLOC
static uint8_t* serialize_index_metadata(const index_metadata_t *meta, size_t *out_len) {
    if (WTREE_UNLIKELY(!meta || !out_len)) {
        return NULL;
    }

    size_t total_len = META_HEADER_SIZE + meta->user_data_len;
    uint8_t *buffer = malloc(total_len);
    if (WTREE_UNLIKELY(!buffer)) {
        return NULL;
    }

    /* Write extractor_id at offset 0 */
    memcpy(buffer + META_EXTRACTOR_ID_OFFSET, &meta->extractor_id, META_EXTRACTOR_ID_SIZE);

    /* Write flags at offset 8 */
    uint32_t flags = 0;
    if (meta->unique) flags |= META_FLAG_UNIQUE;
    if (meta->sparse) flags |= META_FLAG_SPARSE;
    memcpy(buffer + META_FLAGS_OFFSET, &flags, META_FLAGS_SIZE);

    /* Write user_data length at offset 12 */
    uint32_t ud_len = (uint32_t)meta->user_data_len;
    memcpy(buffer + META_USERDATA_LEN_OFFSET, &ud_len, META_USERDATA_LEN_SIZE);

    /* Write user_data at offset 16 */
    if (WTREE_LIKELY(meta->user_data && meta->user_data_len > 0)) {
        memcpy(buffer + META_USERDATA_OFFSET, meta->user_data, meta->user_data_len);
    }

    *out_len = total_len;
    return buffer;
}

/*
 * Deserialize index metadata from binary format
 * Returns 0 on success, error code on failure
 * Allocates user_data if present (caller must free)
 */
static int deserialize_index_metadata(const void *data, size_t data_len,
                                        index_metadata_t *out_meta, gerror_t *error) {
    if (WTREE_UNLIKELY(!data || !out_meta)) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Invalid parameters");
        return WTREE3_EINVAL;
    }

    if (WTREE_UNLIKELY(data_len < META_HEADER_SIZE)) {
        set_error(error, WTREE3_LIB, WTREE3_ERROR, "Invalid metadata format: too short");
        return WTREE3_ERROR;
    }

    const uint8_t *buffer = (const uint8_t*)data;

    /* Read extractor_id */
    memcpy(&out_meta->extractor_id, buffer + META_EXTRACTOR_ID_OFFSET, META_EXTRACTOR_ID_SIZE);

    /* Read flags */
    uint32_t flags;
    memcpy(&flags, buffer + META_FLAGS_OFFSET, META_FLAGS_SIZE);
    out_meta->unique = (flags & META_FLAG_UNIQUE) != 0;
    out_meta->sparse = (flags & META_FLAG_SPARSE) != 0;

    /* Read user_data length */
    uint32_t ud_len;
    memcpy(&ud_len, buffer + META_USERDATA_LEN_OFFSET, META_USERDATA_LEN_SIZE);

    /* Validate total length */
    if (WTREE_UNLIKELY(data_len < META_HEADER_SIZE + ud_len)) {
        set_error(error, WTREE3_LIB, WTREE3_ERROR, "Invalid metadata format: user_data truncated");
        return WTREE3_ERROR;
    }

    /* Copy user_data if present */
    if (WTREE_LIKELY(ud_len > 0)) {
        out_meta->user_data = malloc(ud_len);
        if (WTREE_UNLIKELY(!out_meta->user_data)) {
            set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Failed to allocate user_data");
            return WTREE3_ENOMEM;
        }
        memcpy(out_meta->user_data, buffer + META_USERDATA_OFFSET, ud_len);
        out_meta->user_data_len = ud_len;
    } else {
        out_meta->user_data = NULL;
        out_meta->user_data_len = 0;
    }

    return WTREE3_OK;
}

/* ============================================================
 * Metadata Save/Load Operations
 * ============================================================ */

/* Helper context for save_index_metadata transaction */
typedef struct {
    wtree3_tree_t *tree;
    const char *index_name;
    uint8_t *meta_value;
    size_t meta_len;
    gerror_t *error;
} save_metadata_ctx_t;

static int save_metadata_txn(MDB_txn *txn, void *user_data) {
    save_metadata_ctx_t *ctx = (save_metadata_ctx_t *)user_data;
    return metadata_put_txn(txn, ctx->tree->db, ctx->tree->name, ctx->index_name,
                           ctx->meta_value, ctx->meta_len, ctx->error);
}

WTREE_COLD
int save_index_metadata(wtree3_tree_t *tree,
                         const char *index_name,
                         gerror_t *error) {
    if (WTREE_UNLIKELY(!tree || !index_name)) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Invalid parameters");
        return WTREE3_EINVAL;
    }

    wtree3_index_t *idx = find_index(tree, index_name);
    if (WTREE_UNLIKELY(!idx)) {
        set_error(error, WTREE3_LIB, WTREE3_NOT_FOUND,
                 "Index '%s' not found", index_name);
        return WTREE3_NOT_FOUND;
    }

    /* Build metadata struct from index */
    index_metadata_t meta = {
        .extractor_id = idx->extractor_id,
        .unique = idx->unique,
        .sparse = idx->sparse,
        .user_data = idx->user_data,
        .user_data_len = idx->user_data_len
    };

    /* Serialize to binary format */
    size_t meta_len;
    uint8_t *meta_value = serialize_index_metadata(&meta, &meta_len);
    if (WTREE_UNLIKELY(!meta_value)) {
        set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Failed to allocate metadata");
        return WTREE3_ENOMEM;
    }

    /* Store in metadata DBI */
    save_metadata_ctx_t ctx = {
        .tree = tree,
        .index_name = index_name,
        .meta_value = meta_value,
        .meta_len = meta_len,
        .error = error
    };

    int rc = with_write_txn(tree->db, save_metadata_txn, &ctx, error);
    free(meta_value);

    return rc;
}

/* Helper context for reading metadata */
typedef struct {
    wtree3_tree_t *tree;
    const char *index_name;
    uint64_t extractor_id;
    bool unique;
    bool sparse;
    void *user_data;
    size_t user_data_len;
    gerror_t *error;
} read_metadata_ctx_t;

static int read_metadata_txn(MDB_txn *txn, void *user_data_param) {
    read_metadata_ctx_t *ctx = (read_metadata_ctx_t *)user_data_param;

    MDB_val val;
    int rc = metadata_get_txn(txn, ctx->tree->db, ctx->tree->name,
                              ctx->index_name, &val, ctx->error);
    if (rc != 0) {
        return rc;
    }

    /* Deserialize metadata */
    index_metadata_t meta;
    rc = deserialize_index_metadata(val.mv_data, val.mv_size, &meta, ctx->error);
    if (rc != 0) {
        return rc;
    }

    /* Copy to context */
    ctx->extractor_id = meta.extractor_id;
    ctx->unique = meta.unique;
    ctx->sparse = meta.sparse;
    ctx->user_data = meta.user_data;
    ctx->user_data_len = meta.user_data_len;

    return WTREE3_OK;
}

/* Helper context for opening index DBI */
typedef struct {
    const char *idx_tree_name;
    MDB_dbi *out_dbi;
} open_index_dbi_ctx_t;

static int open_index_dbi_txn(MDB_txn *txn, void *user_data_param) {
    open_index_dbi_ctx_t *ctx = (open_index_dbi_ctx_t *)user_data_param;
    int rc = mdb_dbi_open(txn, ctx->idx_tree_name, MDB_DUPSORT, ctx->out_dbi);
    return rc != 0 ? rc : WTREE3_OK;
}

/*
 * Load index metadata and attach to tree
 * Called internally by wtree3_tree_open() for auto-loading
 */
WTREE_COLD
int load_index_metadata(wtree3_tree_t *tree, const char *index_name, gerror_t *error) {
    if (WTREE_UNLIKELY(!tree || !index_name)) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Invalid parameters");
        return WTREE3_EINVAL;
    }

    /* Check if index already loaded */
    if (WTREE_UNLIKELY(find_index(tree, index_name))) {
        /* Already loaded, skip */
        return WTREE3_OK;
    }

    /* Read metadata */
    read_metadata_ctx_t meta_ctx = {
        .tree = tree,
        .index_name = index_name,
        .user_data = NULL,
        .user_data_len = 0,
        .error = error
    };

    int rc = with_read_txn(tree->db, read_metadata_txn, &meta_ctx, error);
    if (WTREE_UNLIKELY(rc != 0)) {
        return rc;
    }

    /* Look up extractor function */
    wtree3_index_key_fn key_fn = find_extractor(tree->db, meta_ctx.extractor_id);
    if (WTREE_UNLIKELY(!key_fn)) {
        free(meta_ctx.user_data);
        /* Extractor not registered - log warning and skip */
        fprintf(stderr, "Warning: Skipping index '%s' - extractor 0x%016llx not registered\n",
                index_name, (unsigned long long)meta_ctx.extractor_id);
        return WTREE3_OK;  /* Not an error - just skip */
    }

    /* Open existing index DBI */
    char *idx_tree_name = build_index_tree_name(tree->name, index_name);
    if (!idx_tree_name) {
        set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Failed to allocate tree name");
        rc = WTREE3_ENOMEM;
        goto cleanup_user_data;
    }

    MDB_dbi idx_dbi;
    open_index_dbi_ctx_t open_ctx = {
        .idx_tree_name = idx_tree_name,
        .out_dbi = &idx_dbi
    };

    rc = with_write_txn(tree->db, open_index_dbi_txn, &open_ctx, error);
    if (rc != 0) {
        goto cleanup_tree_name;
    }

    /* Allocate new index on heap */
    wtree3_index_t *idx = calloc(1, sizeof(wtree3_index_t));
    if (!idx) {
        set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Failed to allocate index");
        rc = WTREE3_ENOMEM;
        goto cleanup_tree_name;
    }

    idx->name = strdup(index_name);
    if (!idx->name) {
        set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Failed to allocate index name");
        rc = WTREE3_ENOMEM;
        goto cleanup_idx;
    }

    idx->tree_name = idx_tree_name;
    idx->dbi = idx_dbi;
    idx->extractor_id = meta_ctx.extractor_id;
    idx->key_fn = key_fn;
    idx->user_data = meta_ctx.user_data;
    idx->user_data_len = meta_ctx.user_data_len;
    idx->unique = meta_ctx.unique;
    idx->sparse = meta_ctx.sparse;
    idx->compare = NULL;  /* Not persisted */
    idx->dupsort_compare = NULL;  /* Not persisted */

    /* Add to vector */
    if (!wvector_push(tree->indexes, idx)) {
        set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Failed to add index to vector");
        rc = WTREE3_ENOMEM;
        goto cleanup_idx_name;
    }

    return WTREE3_OK;

    /* Cleanup paths in reverse order of allocation */
cleanup_idx_name:
    free(idx->name);
cleanup_idx:
    free(idx_tree_name);
    free(idx);
    goto cleanup_user_data;  /* user_data still needs to be freed */

cleanup_tree_name:
    free(idx_tree_name);
cleanup_user_data:
    free(meta_ctx.user_data);
    return rc;
}

/* ============================================================
 * Introspection Operations
 * ============================================================ */

/* Helper context for get_extractor_id transaction */
typedef struct {
    wtree3_tree_t *tree;
    const char *index_name;
    uint64_t *out_extractor_id;
    gerror_t *error;
} get_extractor_id_ctx_t;

static int get_extractor_id_txn(MDB_txn *txn, void *user_data) {
    get_extractor_id_ctx_t *ctx = (get_extractor_id_ctx_t *)user_data;

    MDB_val val;
    int rc = metadata_get_txn(txn, ctx->tree->db, ctx->tree->name,
                              ctx->index_name, &val, ctx->error);
    if (rc != 0) {
        return rc;
    }

    /* Deserialize metadata (we only need extractor_id) */
    index_metadata_t meta;
    rc = deserialize_index_metadata(val.mv_data, val.mv_size, &meta, ctx->error);
    if (rc != 0) {
        return rc;
    }

    *ctx->out_extractor_id = meta.extractor_id;

    /* Free user_data since we don't need it */
    free(meta.user_data);

    return WTREE3_OK;
}

WTREE_WARN_UNUSED
int wtree3_index_get_extractor_id(wtree3_tree_t *tree,
                                    const char *index_name,
                                    uint64_t *out_extractor_id,
                                    gerror_t *error) {
    if (WTREE_UNLIKELY(!tree || !index_name || !out_extractor_id)) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Invalid parameters");
        return WTREE3_EINVAL;
    }

    /* Check if index is loaded */
    wtree3_index_t *idx = find_index(tree, index_name);
    if (WTREE_LIKELY(idx)) {
        *out_extractor_id = idx->extractor_id;
        return WTREE3_OK;
    }

    /* Read from metadata */
    get_extractor_id_ctx_t ctx = {
        .tree = tree,
        .index_name = index_name,
        .out_extractor_id = out_extractor_id,
        .error = error
    };

    return with_read_txn(tree->db, get_extractor_id_txn, &ctx, error);
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
