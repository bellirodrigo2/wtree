/*
 * wtree3_scan.c - Tier 1 and Tier 2 Bulk Operations
 *
 * This module provides bulk scanning and advanced operations:
 * - Tier 1 primitives: scan_range_txn, scan_reverse_txn, scan_prefix_txn, modify_txn, get_many_txn
 * - Tier 2 bulk ops: delete_if_txn, collect_range_txn, exists_many_txn
 */

#include "wtree3_internal.h"

/* ============================================================
 * Tier 1 Operations - Generic Low-Level Primitives
 * ============================================================ */

int wtree3_scan_range_txn(
    wtree3_txn_t *txn,
    wtree3_tree_t *tree,
    const void *start_key, size_t start_len,
    const void *end_key, size_t end_len,
    wtree3_scan_fn scan_fn,
    void *user_data,
    gerror_t *error
) {
    if (!txn || !tree || !scan_fn) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Invalid parameters");
        return WTREE3_EINVAL;
    }

    MDB_cursor *cursor;
    int rc = mdb_cursor_open(txn->txn, tree->dbi, &cursor);
    if (rc != 0) return translate_mdb_error(rc, error);

    MDB_val key, val;
    MDB_cursor_op op;

    /* Position cursor at start */
    if (start_key) {
        key.mv_size = start_len;
        key.mv_data = (void*)start_key;
        op = MDB_SET_RANGE;  /* Position at key or next greater */
    } else {
        op = MDB_FIRST;  /* Start from beginning */
    }

    rc = mdb_cursor_get(cursor, &key, &val, op);

    /* Iterate forward */
    while (rc == 0) {
        /* Check if we've passed end_key */
        if (end_key) {
            MDB_val end = {.mv_size = end_len, .mv_data = (void*)end_key};
            if (mdb_cmp(txn->txn, tree->dbi, &key, &end) > 0) {
                break;  /* Past end key */
            }
        }

        /* Call user callback */
        bool continue_scan = scan_fn(key.mv_data, key.mv_size,
                                      val.mv_data, val.mv_size,
                                      user_data);
        if (!continue_scan) {
            break;  /* Early termination */
        }

        /* Move to next entry */
        rc = mdb_cursor_get(cursor, &key, &val, MDB_NEXT);
    }

    mdb_cursor_close(cursor);

    if (rc != 0 && rc != MDB_NOTFOUND) {
        return translate_mdb_error(rc, error);
    }

    return WTREE3_OK;
}

int wtree3_scan_reverse_txn(
    wtree3_txn_t *txn,
    wtree3_tree_t *tree,
    const void *start_key, size_t start_len,
    const void *end_key, size_t end_len,
    wtree3_scan_fn scan_fn,
    void *user_data,
    gerror_t *error
) {
    if (!txn || !tree || !scan_fn) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Invalid parameters");
        return WTREE3_EINVAL;
    }

    MDB_cursor *cursor;
    int rc = mdb_cursor_open(txn->txn, tree->dbi, &cursor);
    if (rc != 0) return translate_mdb_error(rc, error);

    MDB_val key, val;
    MDB_cursor_op op;

    /* Position cursor at start (for reverse, start means "high bound") */
    if (start_key) {
        key.mv_size = start_len;
        key.mv_data = (void*)start_key;
        op = MDB_SET_RANGE;  /* Position at key or next greater */
        rc = mdb_cursor_get(cursor, &key, &val, op);

        /* If we landed past start_key, move back to start_key or previous */
        if (rc == 0) {
            MDB_val start = {.mv_size = start_len, .mv_data = (void*)start_key};
            if (mdb_cmp(txn->txn, tree->dbi, &key, &start) > 0) {
                rc = mdb_cursor_get(cursor, &key, &val, MDB_PREV);
            }
        } else if (rc == MDB_NOTFOUND) {
            /* No key >= start_key, start from last */
            rc = mdb_cursor_get(cursor, &key, &val, MDB_LAST);
        }
    } else {
        /* Start from end */
        op = MDB_LAST;
        rc = mdb_cursor_get(cursor, &key, &val, op);
    }

    /* Iterate backward */
    while (rc == 0) {
        /* Check if we've passed end_key (for reverse, end means "low bound") */
        if (end_key) {
            MDB_val end = {.mv_size = end_len, .mv_data = (void*)end_key};
            if (mdb_cmp(txn->txn, tree->dbi, &key, &end) < 0) {
                break;  /* Past end key (lower bound) */
            }
        }

        /* Call user callback */
        bool continue_scan = scan_fn(key.mv_data, key.mv_size,
                                      val.mv_data, val.mv_size,
                                      user_data);
        if (!continue_scan) {
            break;  /* Early termination */
        }

        /* Move to previous entry */
        rc = mdb_cursor_get(cursor, &key, &val, MDB_PREV);
    }

    mdb_cursor_close(cursor);

    if (rc != 0 && rc != MDB_NOTFOUND) {
        return translate_mdb_error(rc, error);
    }

    return WTREE3_OK;
}

int wtree3_scan_prefix_txn(
    wtree3_txn_t *txn,
    wtree3_tree_t *tree,
    const void *prefix, size_t prefix_len,
    wtree3_scan_fn scan_fn,
    void *user_data,
    gerror_t *error
) {
    if (!txn || !tree || !prefix || !scan_fn) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Invalid parameters");
        return WTREE3_EINVAL;
    }

    MDB_cursor *cursor;
    int rc = mdb_cursor_open(txn->txn, tree->dbi, &cursor);
    if (rc != 0) return translate_mdb_error(rc, error);

    MDB_val key, val;

    /* Position at prefix or next greater */
    key.mv_size = prefix_len;
    key.mv_data = (void*)prefix;
    rc = mdb_cursor_get(cursor, &key, &val, MDB_SET_RANGE);

    /* Iterate while keys match prefix */
    while (rc == 0) {
        /* Check if current key still matches prefix */
        if (key.mv_size < prefix_len ||
            memcmp(key.mv_data, prefix, prefix_len) != 0) {
            break;  /* No longer matches prefix */
        }

        /* Call user callback */
        bool continue_scan = scan_fn(key.mv_data, key.mv_size,
                                      val.mv_data, val.mv_size,
                                      user_data);
        if (!continue_scan) {
            break;  /* Early termination */
        }

        /* Move to next entry */
        rc = mdb_cursor_get(cursor, &key, &val, MDB_NEXT);
    }

    mdb_cursor_close(cursor);

    if (rc != 0 && rc != MDB_NOTFOUND) {
        return translate_mdb_error(rc, error);
    }

    return WTREE3_OK;
}

int wtree3_modify_txn(
    wtree3_txn_t *txn,
    wtree3_tree_t *tree,
    const void *key, size_t key_len,
    wtree3_modify_fn modify_fn,
    void *user_data,
    gerror_t *error
) {
    if (!txn || !tree || !key || !modify_fn) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Invalid parameters");
        return WTREE3_EINVAL;
    }

    if (!txn->is_write) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Write operation requires write transaction");
        return WTREE3_EINVAL;
    }

    /* Get existing value */
    MDB_val mkey = {.mv_size = key_len, .mv_data = (void*)key};
    MDB_val old_val;
    int rc = mdb_get(txn->txn, tree->dbi, &mkey, &old_val);

    const void *existing_value = NULL;
    size_t existing_len = 0;
    bool key_exists = false;

    if (rc == 0) {
        existing_value = old_val.mv_data;
        existing_len = old_val.mv_size;
        key_exists = true;
    } else if (rc != MDB_NOTFOUND) {
        return translate_mdb_error(rc, error);
    }

    /* Call modify callback */
    size_t new_len;
    void *new_value = modify_fn(existing_value, existing_len, user_data, &new_len);

    /* Handle different cases based on modify_fn result */
    if (!new_value) {
        if (key_exists) {
            /* Delete the key */
            bool deleted;
            return wtree3_delete_one_txn(txn, tree, key, key_len, &deleted, error);
        } else {
            /* Abort operation (key doesn't exist and callback returned NULL) */
            return WTREE3_OK;
        }
    }

    /* We have a new value to write */
    if (key_exists) {
        /* Update existing key */
        rc = indexes_delete(tree, txn->txn, key, key_len, old_val.mv_data, old_val.mv_size, error);
        if (rc != 0) {
            free(new_value);
            return rc;
        }

        rc = indexes_insert(tree, txn->txn, key, key_len, new_value, new_len, error);
        if (rc != 0) {
            free(new_value);
            return rc;
        }

        MDB_val mval = {.mv_size = new_len, .mv_data = new_value};
        rc = mdb_put(txn->txn, tree->dbi, &mkey, &mval, 0);
        free(new_value);

        if (rc != 0) return translate_mdb_error(rc, error);
    } else {
        /* Insert new key */
        rc = wtree3_insert_one_txn(txn, tree, key, key_len, new_value, new_len, error);
        free(new_value);
        if (rc != 0) return rc;
    }

    return WTREE3_OK;
}

int wtree3_get_many_txn(
    wtree3_txn_t *txn,
    wtree3_tree_t *tree,
    const wtree3_kv_t *keys, size_t key_count,
    const void **values, size_t *value_lens,
    gerror_t *error
) {
    if (!txn || !tree || !keys || !values || !value_lens || key_count == 0) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Invalid parameters");
        return WTREE3_EINVAL;
    }

    for (size_t i = 0; i < key_count; i++) {
        MDB_val mkey = {.mv_size = keys[i].key_len, .mv_data = (void*)keys[i].key};
        MDB_val mval;

        int rc = mdb_get(txn->txn, tree->dbi, &mkey, &mval);

        if (rc == 0) {
            values[i] = mval.mv_data;
            value_lens[i] = mval.mv_size;
        } else if (rc == MDB_NOTFOUND) {
            values[i] = NULL;
            value_lens[i] = 0;
        } else {
            return translate_mdb_error(rc, error);
        }
    }

    return WTREE3_OK;
}

/* ============================================================
 * Tier 2 Operations - Specialized Bulk Operations
 * ============================================================ */

int wtree3_delete_if_txn(
    wtree3_txn_t *txn,
    wtree3_tree_t *tree,
    const void *start_key, size_t start_len,
    const void *end_key, size_t end_len,
    wtree3_predicate_fn predicate,
    void *user_data,
    size_t *deleted_out,
    gerror_t *error
) {
    if (!txn || !tree || !predicate) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Invalid parameters");
        return WTREE3_EINVAL;
    }

    if (!txn->is_write) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Write operation requires write transaction");
        return WTREE3_EINVAL;
    }

    MDB_cursor *cursor;
    int rc = mdb_cursor_open(txn->txn, tree->dbi, &cursor);
    if (rc != 0) return translate_mdb_error(rc, error);

    MDB_val key, val;
    MDB_cursor_op op;

    /* Position cursor at start */
    if (start_key) {
        key.mv_size = start_len;
        key.mv_data = (void*)start_key;
        op = MDB_SET_RANGE;
    } else {
        op = MDB_FIRST;
    }

    rc = mdb_cursor_get(cursor, &key, &val, op);

    size_t deleted_count = 0;

    /* Iterate and delete matching entries */
    while (rc == 0) {
        /* Check if we've passed end_key */
        if (end_key) {
            MDB_val end = {.mv_size = end_len, .mv_data = (void*)end_key};
            if (mdb_cmp(txn->txn, tree->dbi, &key, &end) > 0) {
                break;
            }
        }

        /* Test predicate */
        bool should_delete = predicate(key.mv_data, key.mv_size,
                                       val.mv_data, val.mv_size,
                                       user_data);

        if (should_delete) {
            /* Need to save key for index deletion since cursor delete invalidates it */
            void *key_copy = malloc(key.mv_size);
            void *val_copy = malloc(val.mv_size);
            if (!key_copy || !val_copy) {
                free(key_copy);
                free(val_copy);
                mdb_cursor_close(cursor);
                set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Out of memory");
                return WTREE3_ENOMEM;
            }

            memcpy(key_copy, key.mv_data, key.mv_size);
            size_t key_copy_len = key.mv_size;
            memcpy(val_copy, val.mv_data, val.mv_size);
            size_t val_copy_len = val.mv_size;

            /* Delete from indexes first */
            int del_rc = indexes_delete(tree, txn->txn, key_copy, key_copy_len,
                                       val_copy, val_copy_len, error);
            if (del_rc != 0) {
                free(key_copy);
                free(val_copy);
                mdb_cursor_close(cursor);
                return del_rc;
            }

            /* Delete from main tree using cursor */
            rc = mdb_cursor_del(cursor, 0);
            free(key_copy);
            free(val_copy);

            if (rc != 0) {
                mdb_cursor_close(cursor);
                return translate_mdb_error(rc, error);
            }

            tree->entry_count--;
            deleted_count++;

            /* After mdb_cursor_del, we need to reposition the cursor
             * Try to get current position (next entry after delete) */
            rc = mdb_cursor_get(cursor, &key, &val, MDB_NEXT);
            /* If NOTFOUND, we deleted the last entry - continue to exit loop */
        } else {
            /* Move to next entry */
            rc = mdb_cursor_get(cursor, &key, &val, MDB_NEXT);
        }
    }

    mdb_cursor_close(cursor);

    if (rc != 0 && rc != MDB_NOTFOUND) {
        return translate_mdb_error(rc, error);
    }

    if (deleted_out) {
        *deleted_out = deleted_count;
    }

    return WTREE3_OK;
}

int wtree3_collect_range_txn(
    wtree3_txn_t *txn,
    wtree3_tree_t *tree,
    const void *start_key, size_t start_len,
    const void *end_key, size_t end_len,
    wtree3_predicate_fn predicate,
    void *user_data,
    void ***keys_out,
    size_t **key_lens,
    void ***values_out,
    size_t **value_lens,
    size_t *count_out,
    size_t max_count,
    gerror_t *error
) {
    if (!txn || !tree || !keys_out || !key_lens || !values_out || !value_lens || !count_out) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Invalid parameters");
        return WTREE3_EINVAL;
    }

    /* Initialize outputs */
    *keys_out = NULL;
    *key_lens = NULL;
    *values_out = NULL;
    *value_lens = NULL;
    *count_out = 0;

    /* Dynamic arrays for collection */
    size_t capacity = max_count > 0 ? max_count : 16;
    void **keys = malloc(sizeof(void*) * capacity);
    size_t *k_lens = malloc(sizeof(size_t) * capacity);
    void **values = malloc(sizeof(void*) * capacity);
    size_t *v_lens = malloc(sizeof(size_t) * capacity);

    if (!keys || !k_lens || !values || !v_lens) {
        free(keys);
        free(k_lens);
        free(values);
        free(v_lens);
        set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Out of memory");
        return WTREE3_ENOMEM;
    }

    MDB_cursor *cursor;
    int rc = mdb_cursor_open(txn->txn, tree->dbi, &cursor);
    if (rc != 0) {
        free(keys);
        free(k_lens);
        free(values);
        free(v_lens);
        return translate_mdb_error(rc, error);
    }

    MDB_val key, val;
    MDB_cursor_op op;

    /* Position cursor at start */
    if (start_key) {
        key.mv_size = start_len;
        key.mv_data = (void*)start_key;
        op = MDB_SET_RANGE;
    } else {
        op = MDB_FIRST;
    }

    rc = mdb_cursor_get(cursor, &key, &val, op);
    size_t count = 0;

    /* Iterate and collect matching entries */
    while (rc == 0) {
        /* Check if we've passed end_key */
        if (end_key) {
            MDB_val end = {.mv_size = end_len, .mv_data = (void*)end_key};
            if (mdb_cmp(txn->txn, tree->dbi, &key, &end) > 0) {
                break;
            }
        }

        /* Check max count limit */
        if (max_count > 0 && count >= max_count) {
            break;
        }

        /* Test predicate (if provided) */
        bool should_collect = true;
        if (predicate) {
            should_collect = predicate(key.mv_data, key.mv_size,
                                      val.mv_data, val.mv_size,
                                      user_data);
        }

        if (should_collect) {
            /* Grow arrays if needed */
            if (count >= capacity) {
                size_t new_capacity = capacity * 2;
                void **new_keys = realloc(keys, sizeof(void*) * new_capacity);
                size_t *new_k_lens = realloc(k_lens, sizeof(size_t) * new_capacity);
                void **new_values = realloc(values, sizeof(void*) * new_capacity);
                size_t *new_v_lens = realloc(v_lens, sizeof(size_t) * new_capacity);

                if (!new_keys || !new_k_lens || !new_values || !new_v_lens) {
                    /* Cleanup on allocation failure */
                    for (size_t i = 0; i < count; i++) {
                        free(keys[i]);
                        free(values[i]);
                    }
                    free(keys);
                    free(k_lens);
                    free(values);
                    free(v_lens);
                    mdb_cursor_close(cursor);
                    set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Out of memory");
                    return WTREE3_ENOMEM;
                }

                keys = new_keys;
                k_lens = new_k_lens;
                values = new_values;
                v_lens = new_v_lens;
                capacity = new_capacity;
            }

            /* Copy key and value */
            keys[count] = malloc(key.mv_size);
            values[count] = malloc(val.mv_size);

            if (!keys[count] || !values[count]) {
                free(keys[count]);
                free(values[count]);
                /* Cleanup */
                for (size_t i = 0; i < count; i++) {
                    free(keys[i]);
                    free(values[i]);
                }
                free(keys);
                free(k_lens);
                free(values);
                free(v_lens);
                mdb_cursor_close(cursor);
                set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Out of memory");
                return WTREE3_ENOMEM;
            }

            memcpy(keys[count], key.mv_data, key.mv_size);
            k_lens[count] = key.mv_size;
            memcpy(values[count], val.mv_data, val.mv_size);
            v_lens[count] = val.mv_size;

            count++;
        }

        /* Move to next entry */
        rc = mdb_cursor_get(cursor, &key, &val, MDB_NEXT);
    }

    mdb_cursor_close(cursor);

    if (rc != 0 && rc != MDB_NOTFOUND) {
        /* Cleanup on error */
        for (size_t i = 0; i < count; i++) {
            free(keys[i]);
            free(values[i]);
        }
        free(keys);
        free(k_lens);
        free(values);
        free(v_lens);
        return translate_mdb_error(rc, error);
    }

    /* Set outputs */
    *keys_out = keys;
    *key_lens = k_lens;
    *values_out = values;
    *value_lens = v_lens;
    *count_out = count;

    return WTREE3_OK;
}

int wtree3_exists_many_txn(
    wtree3_txn_t *txn,
    wtree3_tree_t *tree,
    const wtree3_kv_t *keys, size_t key_count,
    bool *results,
    gerror_t *error
) {
    if (!txn || !tree || !keys || !results || key_count == 0) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Invalid parameters");
        return WTREE3_EINVAL;
    }

    for (size_t i = 0; i < key_count; i++) {
        MDB_val mkey = {.mv_size = keys[i].key_len, .mv_data = (void*)keys[i].key};
        MDB_val mval;

        int rc = mdb_get(txn->txn, tree->dbi, &mkey, &mval);

        if (rc == 0) {
            results[i] = true;
        } else if (rc == MDB_NOTFOUND) {
            results[i] = false;
        } else {
            return translate_mdb_error(rc, error);
        }
    }

    return WTREE3_OK;
}
