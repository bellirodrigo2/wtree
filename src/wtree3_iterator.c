/*
 * wtree3_iterator.c - Iterator and Cursor Operations
 *
 * This module provides iteration over tree entries:
 * - Iterator lifecycle: create, create_with_txn, close
 * - Navigation: first, last, next, prev, seek, seek_range
 * - Data access: key, value, key_copy, value_copy
 * - Operations: delete, valid, get_txn
 * - Index queries: index_seek, index_seek_range, index_iterator_main_key
 */

#include "wtree3_internal.h"

/* ============================================================
 * Iterator Operations
 * ============================================================ */

wtree3_iterator_t* wtree3_iterator_create(wtree3_tree_t *tree, gerror_t *error) {
    if (!tree) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Tree cannot be NULL");
        return NULL;
    }

    wtree3_txn_t *txn = wtree3_txn_begin(tree->db, false, error);
    if (!txn) return NULL;

    wtree3_iterator_t *iter = calloc(1, sizeof(wtree3_iterator_t));
    if (!iter) {
        wtree3_txn_abort(txn);
        set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Failed to allocate iterator");
        return NULL;
    }

    int rc = mdb_cursor_open(txn->txn, tree->dbi, &iter->cursor);
    if (rc != 0) {
        translate_mdb_error(rc, error);
        wtree3_txn_abort(txn);
        free(iter);
        return NULL;
    }

    iter->txn = txn;
    iter->tree = tree;
    iter->owns_txn = true;
    iter->valid = false;
    iter->is_index = false;

    return iter;
}

wtree3_iterator_t* wtree3_iterator_create_with_txn(wtree3_tree_t *tree,
                                                    wtree3_txn_t *txn,
                                                    gerror_t *error) {
    if (!tree || !txn) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Invalid parameters");
        return NULL;
    }

    wtree3_iterator_t *iter = calloc(1, sizeof(wtree3_iterator_t));
    if (!iter) {
        set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Failed to allocate iterator");
        return NULL;
    }

    int rc = mdb_cursor_open(txn->txn, tree->dbi, &iter->cursor);
    if (rc != 0) {
        translate_mdb_error(rc, error);
        free(iter);
        return NULL;
    }

    iter->txn = txn;
    iter->tree = tree;
    iter->owns_txn = false;
    iter->valid = false;
    iter->is_index = false;

    return iter;
}

bool wtree3_iterator_first(wtree3_iterator_t *iter) {
    if (!iter || !iter->cursor) return false;

    iter->current_key.mv_size = 0;
    iter->current_key.mv_data = NULL;
    iter->current_val.mv_size = 0;
    iter->current_val.mv_data = NULL;

    int rc = mdb_cursor_get(iter->cursor, &iter->current_key, &iter->current_val, MDB_FIRST);
    iter->valid = (rc == 0);
    return iter->valid;
}

bool wtree3_iterator_last(wtree3_iterator_t *iter) {
    if (!iter || !iter->cursor) return false;

    iter->current_key.mv_size = 0;
    iter->current_key.mv_data = NULL;
    iter->current_val.mv_size = 0;
    iter->current_val.mv_data = NULL;

    int rc = mdb_cursor_get(iter->cursor, &iter->current_key, &iter->current_val, MDB_LAST);
    iter->valid = (rc == 0);
    return iter->valid;
}

bool wtree3_iterator_next(wtree3_iterator_t *iter) {
    if (!iter || !iter->cursor) return false;
    int rc = mdb_cursor_get(iter->cursor, &iter->current_key, &iter->current_val, MDB_NEXT);
    iter->valid = (rc == 0);
    return iter->valid;
}

bool wtree3_iterator_prev(wtree3_iterator_t *iter) {
    if (!iter || !iter->cursor) return false;
    int rc = mdb_cursor_get(iter->cursor, &iter->current_key, &iter->current_val, MDB_PREV);
    iter->valid = (rc == 0);
    return iter->valid;
}

bool wtree3_iterator_seek(wtree3_iterator_t *iter, const void *key, size_t key_len) {
    if (!iter || !iter->cursor || !key) return false;

    MDB_val search_key = {.mv_size = key_len, .mv_data = (void*)key};
    MDB_val found_val = {0};

    int rc = mdb_cursor_get(iter->cursor, &search_key, &found_val, MDB_SET);
    if (rc == 0) {
        iter->current_key = search_key;
        iter->current_val = found_val;
        iter->valid = true;
    } else {
        iter->valid = false;
    }
    return iter->valid;
}

bool wtree3_iterator_seek_range(wtree3_iterator_t *iter, const void *key, size_t key_len) {
    if (!iter || !iter->cursor || !key) return false;

    MDB_val search_key = {.mv_size = key_len, .mv_data = (void*)key};
    MDB_val found_val = {0};

    int rc = mdb_cursor_get(iter->cursor, &search_key, &found_val, MDB_SET_RANGE);
    if (rc == 0) {
        iter->current_key = search_key;
        iter->current_val = found_val;
        iter->valid = true;
    } else {
        iter->valid = false;
    }
    return iter->valid;
}

bool wtree3_iterator_key(wtree3_iterator_t *iter, const void **key, size_t *key_len) {
    if (!iter || !iter->valid || !key || !key_len) return false;
    *key = iter->current_key.mv_data;
    *key_len = iter->current_key.mv_size;
    return true;
}

bool wtree3_iterator_value(wtree3_iterator_t *iter, const void **value, size_t *value_len) {
    if (!iter || !iter->valid || !value || !value_len) return false;
    *value = iter->current_val.mv_data;
    *value_len = iter->current_val.mv_size;
    return true;
}

bool wtree3_iterator_key_copy(wtree3_iterator_t *iter, void **key, size_t *key_len) {
    if (!iter || !iter->valid || !key || !key_len) return false;

    *key_len = iter->current_key.mv_size;
    *key = malloc(*key_len);
    if (!*key) return false;

    memcpy(*key, iter->current_key.mv_data, *key_len);
    return true;
}

bool wtree3_iterator_value_copy(wtree3_iterator_t *iter, void **value, size_t *value_len) {
    if (!iter || !iter->valid || !value || !value_len) return false;

    *value_len = iter->current_val.mv_size;
    *value = malloc(*value_len);
    if (!*value) return false;

    memcpy(*value, iter->current_val.mv_data, *value_len);
    return true;
}

bool wtree3_iterator_valid(wtree3_iterator_t *iter) {
    return iter && iter->valid;
}

/**
 * Raw iterator delete - deletes from cursor without maintaining indexes
 * Internal use only - for index trees or when indexes are not needed
 */
static int wtree3_iterator_delete_raw(wtree3_iterator_t *iter, gerror_t *error) {
    if (!iter || !iter->cursor) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Invalid iterator");
        return WTREE3_EINVAL;
    }

    if (!iter->valid) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Iterator not positioned on valid entry");
        return WTREE3_EINVAL;
    }

    if (!iter->txn || !iter->txn->is_write) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Delete requires write transaction");
        return WTREE3_EINVAL;
    }

    int rc = mdb_cursor_del(iter->cursor, 0);
    if (rc != 0) return translate_mdb_error(rc, error);

    iter->valid = false;
    rc = mdb_cursor_get(iter->cursor, &iter->current_key, &iter->current_val, MDB_GET_CURRENT);
    if (rc == MDB_NOTFOUND) {
        rc = mdb_cursor_get(iter->cursor, &iter->current_key, &iter->current_val, MDB_NEXT);
    }
    iter->valid = (rc == 0);

    return WTREE3_OK;
}

/**
 * Delete current entry via iterator - maintains secondary indexes
 *
 * NOTE: This function requires a write transaction. Standard iterators are
 * created with read-only transactions. To use this function, you must:
 * 1. Create a write transaction with wtree3_txn_begin(db, true, &error)
 * 2. Create iterator with wtree3_iterator_create_with_txn(tree, txn, &error)
 * 3. Delete entries
 * 4. Commit transaction with wtree3_txn_commit(txn, &error)
 */
int wtree3_iterator_delete(wtree3_iterator_t *iter, gerror_t *error) {
    if (!iter || !iter->cursor || !iter->tree) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Invalid iterator");
        return WTREE3_EINVAL;
    }

    if (!iter->valid) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Iterator not positioned on valid entry");
        return WTREE3_EINVAL;
    }

    if (!iter->txn || !iter->txn->is_write) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL,
                 "Delete requires write transaction - use wtree3_iterator_create_with_txn() with write transaction");
        return WTREE3_EINVAL;
    }

    /* Skip index maintenance for index iterators */
    if (iter->is_index) {
        return wtree3_iterator_delete_raw(iter, error);
    }

    /* For main tree iterators, maintain secondary indexes */
    wtree3_tree_t *tree = iter->tree;

    /* Get current key and value before deletion */
    const void *key = iter->current_key.mv_data;
    size_t key_len = iter->current_key.mv_size;
    const void *value = iter->current_val.mv_data;
    size_t value_len = iter->current_val.mv_size;

    if (!key || !value) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Iterator has invalid key/value");
        return WTREE3_EINVAL;
    }

    /* Delete from secondary indexes first */
    int rc = indexes_delete(tree, iter->txn->txn, key, key_len, value, value_len, error);
    if (rc != 0) return rc;

    /* Delete from main tree via cursor */
    rc = mdb_cursor_del(iter->cursor, 0);
    if (rc != 0) return translate_mdb_error(rc, error);

    /* Update iterator state */
    tree->entry_count--;
    iter->valid = false;

    /* Try to position on next entry */
    rc = mdb_cursor_get(iter->cursor, &iter->current_key, &iter->current_val, MDB_GET_CURRENT);
    if (rc == MDB_NOTFOUND) {
        rc = mdb_cursor_get(iter->cursor, &iter->current_key, &iter->current_val, MDB_NEXT);
    }
    iter->valid = (rc == 0);

    return WTREE3_OK;
}

void wtree3_iterator_close(wtree3_iterator_t *iter) {
    if (!iter) return;

    if (iter->cursor) {
        mdb_cursor_close(iter->cursor);
    }

    if (iter->owns_txn && iter->txn) {
        wtree3_txn_abort(iter->txn);
    }

    free(iter);
}

wtree3_txn_t* wtree3_iterator_get_txn(wtree3_iterator_t *iter) {
    return iter ? iter->txn : NULL;
}

/* ============================================================
 * Index Query Operations
 * ============================================================ */

static wtree3_iterator_t* index_seek_internal(wtree3_tree_t *tree,
                                               const char *index_name,
                                               const void *key, size_t key_len,
                                               bool range,
                                               gerror_t *error) {
    if (!tree || !index_name) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Invalid parameters");
        return NULL;
    }

    wtree3_index_t *idx = find_index(tree, index_name);
    if (!idx) {
        set_error(error, WTREE3_LIB, WTREE3_NOT_FOUND,
                 "Index '%s' not found", index_name);
        return NULL;
    }

    wtree3_txn_t *txn = wtree3_txn_begin(tree->db, false, error);
    if (!txn) return NULL;

    wtree3_iterator_t *iter = calloc(1, sizeof(wtree3_iterator_t));
    if (!iter) {
        wtree3_txn_abort(txn);
        set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Failed to allocate iterator");
        return NULL;
    }

    int rc = mdb_cursor_open(txn->txn, idx->dbi, &iter->cursor);
    if (rc != 0) {
        translate_mdb_error(rc, error);
        wtree3_txn_abort(txn);
        free(iter);
        return NULL;
    }

    iter->txn = txn;
    iter->tree = tree;
    iter->owns_txn = true;
    iter->is_index = true;
    iter->valid = false;

    /* Seek to key */
    if (key && key_len > 0) {
        MDB_val search_key = {.mv_size = key_len, .mv_data = (void*)key};
        MDB_val found_val = {0};

        if (range) {
            rc = mdb_cursor_get(iter->cursor, &search_key, &found_val, MDB_SET_RANGE);
        } else {
            rc = mdb_cursor_get(iter->cursor, &search_key, &found_val, MDB_SET);
        }

        if (rc == 0) {
            iter->current_key = search_key;
            iter->current_val = found_val;
            iter->valid = true;
        }
    }

    return iter;
}

wtree3_iterator_t* wtree3_index_seek(wtree3_tree_t *tree,
                                      const char *index_name,
                                      const void *key, size_t key_len,
                                      gerror_t *error) {
    return index_seek_internal(tree, index_name, key, key_len, false, error);
}

wtree3_iterator_t* wtree3_index_seek_range(wtree3_tree_t *tree,
                                            const char *index_name,
                                            const void *key, size_t key_len,
                                            gerror_t *error) {
    return index_seek_internal(tree, index_name, key, key_len, true, error);
}

bool wtree3_index_iterator_main_key(wtree3_iterator_t *iter,
                                     const void **main_key,
                                     size_t *main_key_len) {
    /* In index iterator, "value" is the main tree key */
    return iter && iter->is_index &&
           wtree3_iterator_value(iter, main_key, main_key_len);
}

