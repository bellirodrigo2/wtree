/*
 * wtree3_crud.c - CRUD Operations with Automatic Index Maintenance
 *
 * This module provides all CRUD operations:
 * - Transactional CRUD: get_txn, insert_one_txn, update_txn, upsert_txn, delete_one_txn, exists_txn
 * - Batch operations: insert_many_txn, upsert_many_txn, get_many_txn
 * - Auto-transaction wrappers: get, insert_one, update, upsert, delete_one, exists
 * - Index maintenance helpers: indexes_insert, indexes_delete
 */

#include "wtree3_internal.h"
#include "macros.h"

/* ============================================================
 * Index Maintenance Helpers
 * ============================================================ */

WTREE_HOT
int indexes_insert(wtree3_tree_t *tree, MDB_txn *txn,
                          const void *key, size_t key_len,
                          const void *value, size_t value_len,
                          gerror_t *error) {
    size_t index_count = wvector_size(tree->indexes);
    for (size_t i = 0; i < index_count; i++) {
        wtree3_index_t *idx = (wtree3_index_t *)wvector_get(tree->indexes, i);

        void *idx_key = NULL;
        size_t idx_key_len = 0;
        bool should_index = idx->key_fn(value, value_len, idx->user_data,
                                        &idx_key, &idx_key_len);

        if (WTREE_LIKELY(!should_index)) continue;
        if (WTREE_UNLIKELY(!idx_key)) {
            set_error(error, WTREE3_LIB, WTREE3_ERROR,
                     "Index key extraction failed for '%s'", idx->name);
            return WTREE3_ERROR;
        }

        /* Check unique constraint */
        if (WTREE_UNLIKELY(idx->unique)) {
            MDB_val check_key = {.mv_size = idx_key_len, .mv_data = idx_key};
            MDB_val check_val;
            int get_rc = mdb_get(txn, idx->dbi, &check_key, &check_val);
            if (WTREE_UNLIKELY(get_rc == 0)) {
                free(idx_key);
                set_error(error, WTREE3_LIB, WTREE3_INDEX_ERROR,
                         "Duplicate key for unique index '%s'", idx->name);
                return WTREE3_INDEX_ERROR;
            }
        }

        /* Insert: index_key -> main_key */
        MDB_val mk = {.mv_size = idx_key_len, .mv_data = idx_key};
        MDB_val mv = {.mv_size = key_len, .mv_data = (void*)key};
        int rc = mdb_put(txn, idx->dbi, &mk, &mv, MDB_NODUPDATA);
        free(idx_key);

        if (WTREE_UNLIKELY(rc != 0 && rc != MDB_KEYEXIST)) {
            return translate_mdb_error(rc, error);
        }
    }

    return WTREE3_OK;
}

WTREE_HOT
int indexes_delete(wtree3_tree_t *tree, MDB_txn *txn,
                          const void *key, size_t key_len,
                          const void *value, size_t value_len,
                          gerror_t *error) {
    size_t index_count = wvector_size(tree->indexes);
    for (size_t i = 0; i < index_count; i++) {
        wtree3_index_t *idx = (wtree3_index_t *)wvector_get(tree->indexes, i);

        void *idx_key = NULL;
        size_t idx_key_len = 0;
        bool should_index = idx->key_fn(value, value_len, idx->user_data,
                                        &idx_key, &idx_key_len);

        if (WTREE_LIKELY(!should_index || !idx_key)) continue;

        /* Delete specific key+value pair from DUPSORT tree */
        MDB_val mk = {.mv_size = idx_key_len, .mv_data = idx_key};
        MDB_val mv = {.mv_size = key_len, .mv_data = (void*)key};
        int rc = mdb_del(txn, idx->dbi, &mk, &mv);
        free(idx_key);

        if (WTREE_UNLIKELY(rc != 0 && rc != MDB_NOTFOUND)) {
            return translate_mdb_error(rc, error);
        }
    }

    return WTREE3_OK;
}

/* ============================================================
 * Data Operations (With Transaction)
 * ============================================================ */

WTREE_HOT WTREE_WARN_UNUSED
int wtree3_get_txn(wtree3_txn_t *txn, wtree3_tree_t *tree,
                   const void *key, size_t key_len,
                   const void **value, size_t *value_len,
                   gerror_t *error) {
    if (WTREE_UNLIKELY(!txn || !tree || !key || !value || !value_len)) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Invalid parameters");
        return WTREE3_EINVAL;
    }

    MDB_val mkey = {.mv_size = key_len, .mv_data = (void*)key};
    MDB_val mval;

    int rc = mdb_get(txn->txn, tree->dbi, &mkey, &mval);
    if (WTREE_UNLIKELY(rc != 0)) return translate_mdb_error(rc, error);

    *value = mval.mv_data;
    *value_len = mval.mv_size;
    return WTREE3_OK;
}

WTREE_HOT WTREE_WARN_UNUSED
int wtree3_insert_one_txn(wtree3_txn_t *txn, wtree3_tree_t *tree,
                           const void *key, size_t key_len,
                           const void *value, size_t value_len,
                           gerror_t *error) {
    if (WTREE_UNLIKELY(!txn || !tree || !key || !value)) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Invalid parameters");
        return WTREE3_EINVAL;
    }

    if (WTREE_UNLIKELY(!txn->is_write)) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Write operation requires write transaction");
        return WTREE3_EINVAL;
    }

    /* Insert into indexes first (check unique constraints) */
    int rc = indexes_insert(tree, txn->txn, key, key_len, value, value_len, error);
    if (WTREE_UNLIKELY(rc != 0)) return rc;

    /* Insert into main tree */
    MDB_val mkey = {.mv_size = key_len, .mv_data = (void*)key};
    MDB_val mval = {.mv_size = value_len, .mv_data = (void*)value};

    rc = mdb_put(txn->txn, tree->dbi, &mkey, &mval, MDB_NOOVERWRITE);
    if (WTREE_UNLIKELY(rc != 0)) return translate_mdb_error(rc, error);

    tree->entry_count++;
    return WTREE3_OK;
}

WTREE_HOT WTREE_WARN_UNUSED
int wtree3_update_txn(wtree3_txn_t *txn, wtree3_tree_t *tree,
                       const void *key, size_t key_len,
                       const void *value, size_t value_len,
                       gerror_t *error) {
    if (WTREE_UNLIKELY(!txn || !tree || !key || !value)) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Invalid parameters");
        return WTREE3_EINVAL;
    }

    if (WTREE_UNLIKELY(!txn->is_write)) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Write operation requires write transaction");
        return WTREE3_EINVAL;
    }

    /* Get old value for index maintenance (if exists) */
    MDB_val mkey = {.mv_size = key_len, .mv_data = (void*)key};
    MDB_val old_val;
    int rc = mdb_get(txn->txn, tree->dbi, &mkey, &old_val);

    if (WTREE_UNLIKELY(rc == MDB_NOTFOUND)) {
        set_error(error, WTREE3_LIB, WTREE3_NOT_FOUND, "Key not found");
        return WTREE3_NOT_FOUND;
    }

    if (WTREE_UNLIKELY(rc != 0)) {
        return translate_mdb_error(rc, error);
    }

    /* Delete from indexes using old value */
    rc = indexes_delete(tree, txn->txn, key, key_len, old_val.mv_data, old_val.mv_size, error);
    if (WTREE_UNLIKELY(rc != 0)) return rc;

    /* Insert into indexes with new value */
    rc = indexes_insert(tree, txn->txn, key, key_len, value, value_len, error);
    if (WTREE_UNLIKELY(rc != 0)) return rc;

    /* Update value in main tree */
    MDB_val mval = {.mv_size = value_len, .mv_data = (void*)value};
    rc = mdb_put(txn->txn, tree->dbi, &mkey, &mval, 0);
    if (WTREE_UNLIKELY(rc != 0)) return translate_mdb_error(rc, error);

    return WTREE3_OK;
}

WTREE_HOT WTREE_WARN_UNUSED
int wtree3_upsert_txn(wtree3_txn_t *txn, wtree3_tree_t *tree,
                       const void *key, size_t key_len,
                       const void *value, size_t value_len,
                       gerror_t *error) {
    if (WTREE_UNLIKELY(!txn || !tree || !key || !value)) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Invalid parameters");
        return WTREE3_EINVAL;
    }

    if (!txn->is_write) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Write operation requires write transaction");
        return WTREE3_EINVAL;
    }

    /* Try insert first - if key doesn't exist, we're done */
    int rc = wtree3_insert_one_txn(txn, tree, key, key_len, value, value_len, error);

    if (rc == WTREE3_OK) {
        /* Insert succeeded - key didn't exist */
        return WTREE3_OK;
    }

    if (rc != WTREE3_KEY_EXISTS) {
        /* Some other error occurred */
        return rc;
    }

    /* Key exists - need to handle merge (if callback exists) or just update */
    if (tree->merge_fn) {
        /* Get old value for merge */
        MDB_val mkey = {.mv_size = key_len, .mv_data = (void*)key};
        MDB_val old_val;
        rc = mdb_get(txn->txn, tree->dbi, &mkey, &old_val);

        if (WTREE_UNLIKELY(rc != 0)) {
            return translate_mdb_error(rc, error);
        }

        /* Call merge callback */
        size_t merged_len;
        void *merged_value = tree->merge_fn(
            old_val.mv_data, old_val.mv_size,
            value, value_len,
            tree->merge_user_data,
            &merged_len
        );

        if (!merged_value) {
            set_error(error, WTREE3_LIB, WTREE3_ERROR, "Merge callback returned NULL");
            return WTREE3_ERROR;
        }

        /* Update with merged value */
        rc = wtree3_update_txn(txn, tree, key, key_len, merged_value, merged_len, error);
        free(merged_value);
        return rc;
    }

    /* No merge function - just update with new value */
    return wtree3_update_txn(txn, tree, key, key_len, value, value_len, error);
}

WTREE_HOT WTREE_WARN_UNUSED
int wtree3_delete_one_txn(wtree3_txn_t *txn, wtree3_tree_t *tree,
                           const void *key, size_t key_len,
                           bool *deleted,
                           gerror_t *error) {
    if (WTREE_UNLIKELY(!txn || !tree || !key)) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Invalid parameters");
        return WTREE3_EINVAL;
    }

    if (!txn->is_write) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Write operation requires write transaction");
        return WTREE3_EINVAL;
    }

    if (deleted) *deleted = false;

    /* Get value for index maintenance */
    MDB_val mkey = {.mv_size = key_len, .mv_data = (void*)key};
    MDB_val mval;
    int rc = mdb_get(txn->txn, tree->dbi, &mkey, &mval);
    if (rc != 0) {
        if (rc == MDB_NOTFOUND) return WTREE3_OK;  /* Not an error */
        return translate_mdb_error(rc, error);
    }

    /* Delete from indexes */
    rc = indexes_delete(tree, txn->txn, key, key_len, mval.mv_data, mval.mv_size, error);
    if (rc != 0) return rc;

    /* Delete from main tree */
    rc = mdb_del(txn->txn, tree->dbi, &mkey, NULL);
    if (rc == 0) {
        tree->entry_count--;
        if (deleted) *deleted = true;
    } else if (rc != MDB_NOTFOUND) {
        return translate_mdb_error(rc, error);
    }

    return WTREE3_OK;
}

WTREE_HOT
bool wtree3_exists_txn(wtree3_txn_t *txn, wtree3_tree_t *tree,
                        const void *key, size_t key_len,
                        gerror_t *error) {
    if (WTREE_UNLIKELY(!txn || !tree || !key)) return false;

    MDB_val mkey = {.mv_size = key_len, .mv_data = (void*)key};
    MDB_val mval;
    return mdb_get(txn->txn, tree->dbi, &mkey, &mval) == 0;
}

int wtree3_insert_many_txn(wtree3_txn_t *txn, wtree3_tree_t *tree,
                            const wtree3_kv_t *kvs, size_t count,
                            gerror_t *error) {
    if (!txn || !tree || !kvs || count == 0) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Invalid parameters");
        return WTREE3_EINVAL;
    }

    if (!txn->is_write) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Write operation requires write transaction");
        return WTREE3_EINVAL;
    }

    for (size_t i = 0; i < count; i++) {
        int rc = wtree3_insert_one_txn(txn, tree, kvs[i].key, kvs[i].key_len,
                                        kvs[i].value, kvs[i].value_len, error);
        if (rc != 0) return rc;
    }

    return WTREE3_OK;
}

int wtree3_upsert_many_txn(wtree3_txn_t *txn, wtree3_tree_t *tree,
                            const wtree3_kv_t *kvs, size_t count,
                            gerror_t *error) {
    if (!txn || !tree || !kvs || count == 0) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Invalid parameters");
        return WTREE3_EINVAL;
    }

    if (!txn->is_write) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Write operation requires write transaction");
        return WTREE3_EINVAL;
    }

    for (size_t i = 0; i < count; i++) {
        int rc = wtree3_upsert_txn(txn, tree, kvs[i].key, kvs[i].key_len,
                                    kvs[i].value, kvs[i].value_len, error);
        if (rc != 0) return rc;
    }

    return WTREE3_OK;
}

/* ============================================================
 * Data Operations (Auto-transaction)
/* ============================================================
 * Data Operations (Auto-transaction)
 * ============================================================ */

int wtree3_get(wtree3_tree_t *tree,
               const void *key, size_t key_len,
               void **value, size_t *value_len,
               gerror_t *error) {
    if (!tree || !key || !value || !value_len) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Invalid parameters");
        return WTREE3_EINVAL;
    }

    wtree3_txn_t *txn = wtree3_txn_begin(tree->db, false, error);
    if (!txn) return WTREE3_ERROR;

    const void *tmp;
    size_t tmp_len;
    int rc = wtree3_get_txn(txn, tree, key, key_len, &tmp, &tmp_len, error);

    if (rc == 0) {
        *value = malloc(tmp_len);
        if (!*value) {
            wtree3_txn_abort(txn);
            set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Failed to allocate value");
            return WTREE3_ENOMEM;
        }
        memcpy(*value, tmp, tmp_len);
        *value_len = tmp_len;
    }

    wtree3_txn_abort(txn);
    return rc;
}

int wtree3_insert_one(wtree3_tree_t *tree,
                       const void *key, size_t key_len,
                       const void *value, size_t value_len,
                       gerror_t *error) {
    if (!tree) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Tree cannot be NULL");
        return WTREE3_EINVAL;
    }

    wtree3_txn_t *txn = wtree3_txn_begin(tree->db, true, error);
    if (!txn) return WTREE3_ERROR;

    int rc = wtree3_insert_one_txn(txn, tree, key, key_len, value, value_len, error);

    if (rc == 0) {
        rc = wtree3_txn_commit(txn, error);
    } else {
        wtree3_txn_abort(txn);
    }

    return rc;
}

int wtree3_update(wtree3_tree_t *tree,
                   const void *key, size_t key_len,
                   const void *value, size_t value_len,
                   gerror_t *error) {
    if (!tree) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Tree cannot be NULL");
        return WTREE3_EINVAL;
    }

    wtree3_txn_t *txn = wtree3_txn_begin(tree->db, true, error);
    if (!txn) return WTREE3_ERROR;

    int rc = wtree3_update_txn(txn, tree, key, key_len, value, value_len, error);

    if (rc == 0) {
        rc = wtree3_txn_commit(txn, error);
    } else {
        wtree3_txn_abort(txn);
    }

    return rc;
}

int wtree3_upsert(wtree3_tree_t *tree,
                   const void *key, size_t key_len,
                   const void *value, size_t value_len,
                   gerror_t *error) {
    if (!tree) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Tree cannot be NULL");
        return WTREE3_EINVAL;
    }

    wtree3_txn_t *txn = wtree3_txn_begin(tree->db, true, error);
    if (!txn) return WTREE3_ERROR;

    int rc = wtree3_upsert_txn(txn, tree, key, key_len, value, value_len, error);

    if (rc == 0) {
        rc = wtree3_txn_commit(txn, error);
    } else {
        wtree3_txn_abort(txn);
    }

    return rc;
}

int wtree3_delete_one(wtree3_tree_t *tree,
                       const void *key, size_t key_len,
                       bool *deleted,
                       gerror_t *error) {
    if (!tree) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Tree cannot be NULL");
        return WTREE3_EINVAL;
    }

    wtree3_txn_t *txn = wtree3_txn_begin(tree->db, true, error);
    if (!txn) return WTREE3_ERROR;

    int rc = wtree3_delete_one_txn(txn, tree, key, key_len, deleted, error);

    if (rc == 0) {
        rc = wtree3_txn_commit(txn, error);
    } else {
        wtree3_txn_abort(txn);
    }

    return rc;
}

bool wtree3_exists(wtree3_tree_t *tree,
                   const void *key, size_t key_len,
                   gerror_t *error) {
    if (!tree || !key) return false;

    wtree3_txn_t *txn = wtree3_txn_begin(tree->db, false, error);
    if (!txn) return false;

    bool exists = wtree3_exists_txn(txn, tree, key, key_len, error);
    wtree3_txn_abort(txn);
    return exists;
}
