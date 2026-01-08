/*
 * wtree3_core.c - Core Database and Transaction Management
 *
 * This module provides the foundation layer for wtree3:
 * - Database lifecycle (open, close, sync, resize, stats)
 * - Transaction management (begin, commit, abort, reset, renew)
 * - Error translation from LMDB to wtree3 error codes
 * - Utility functions (strerror, error_recoverable)
 */

#include "wtree3_internal.h"
#include "wtree3_extractor_registry.h"

/* ============================================================
 * Error Translation
 * ============================================================ */

int translate_mdb_error(int mdb_rc, gerror_t *error) {
    switch (mdb_rc) {
        case 0:
            return WTREE3_OK;
        case MDB_MAP_FULL:
            set_error(error, WTREE3_LIB, WTREE3_MAP_FULL,
                     "Database map is full, resize needed");
            return WTREE3_MAP_FULL;
        case MDB_TXN_FULL:
            set_error(error, WTREE3_LIB, WTREE3_TXN_FULL,
                     "Transaction has too many dirty pages");
            return WTREE3_TXN_FULL;
        case MDB_NOTFOUND:
            set_error(error, WTREE3_LIB, WTREE3_NOT_FOUND, "Key not found");
            return WTREE3_NOT_FOUND;
        case MDB_KEYEXIST:
            set_error(error, WTREE3_LIB, WTREE3_KEY_EXISTS, "Key already exists");
            return WTREE3_KEY_EXISTS;
        default:
            set_error(error, WTREE3_LIB, WTREE3_ERROR, "%s", mdb_strerror(mdb_rc));
            return WTREE3_ERROR;
    }
}

/* ============================================================
 * Database Operations
 * ============================================================ */

wtree3_db_t* wtree3_db_open(const char *path, size_t mapsize,
                             unsigned int max_dbs, uint32_t version,
                             unsigned int flags,
                             gerror_t *error) {
    if (!path) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Path cannot be NULL");
        return NULL;
    }

    /* Check if directory exists */
    struct stat st = {0};
    if (stat(path, &st) == -1) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL,
                 "Directory does not exist: %s", path);
        return NULL;
    }
    if (!S_ISDIR(st.st_mode)) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL,
                 "Path is not a directory: %s", path);
        return NULL;
    }

    wtree3_db_t *db = calloc(1, sizeof(wtree3_db_t));
    if (!db) {
        set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Failed to allocate database");
        return NULL;
    }

    /* Store version */
    db->version = version;

    /* Initialize extractor registry */
    db->extractor_registry = wtree3_extractor_registry_create();
    if (!db->extractor_registry) {
        set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Failed to create extractor registry");
        free(db);
        return NULL;
    }

    int rc = mdb_env_create(&db->env);
    if (rc != 0) {
        set_error(error, WTREE3_LIB, WTREE3_ERROR,
                 "Failed to create environment: %s", mdb_strerror(rc));
        free(db);
        return NULL;
    }

    /* Set defaults */
    if (mapsize == 0) mapsize = 1024UL * 1024 * 1024;  /* 1GB */
    if (max_dbs == 0) max_dbs = 128;

    rc = mdb_env_set_mapsize(db->env, mapsize);
    if (rc != 0) {
        set_error(error, WTREE3_LIB, WTREE3_ERROR,
                 "Failed to set mapsize: %s", mdb_strerror(rc));
        mdb_env_close(db->env);
        free(db);
        return NULL;
    }

    rc = mdb_env_set_maxdbs(db->env, max_dbs);
    if (rc != 0) {
        set_error(error, WTREE3_LIB, WTREE3_ERROR,
                 "Failed to set max databases: %s", mdb_strerror(rc));
        mdb_env_close(db->env);
        free(db);
        return NULL;
    }

    rc = mdb_env_open(db->env, path, flags, 0664);
    if (rc != 0) {
        set_error(error, WTREE3_LIB, WTREE3_ERROR,
                 "Failed to open environment: %s", mdb_strerror(rc));
        mdb_env_close(db->env);
        free(db);
        return NULL;
    }

    db->path = strdup(path);
    db->mapsize = mapsize;
    db->max_dbs = max_dbs;
    db->flags = flags;

    return db;
}

void wtree3_db_close(wtree3_db_t *db) {
    if (!db) return;
    if (db->env) mdb_env_close(db->env);
    free(db->path);

    /* Free extractor registry */
    wtree3_extractor_registry_destroy(db->extractor_registry);

    free(db);
}

int wtree3_db_sync(wtree3_db_t *db, bool force, gerror_t *error) {
    if (!db) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Database cannot be NULL");
        return WTREE3_EINVAL;
    }
    int rc = mdb_env_sync(db->env, force ? 1 : 0);
    if (rc != 0) return translate_mdb_error(rc, error);
    return WTREE3_OK;
}

int wtree3_db_resize(wtree3_db_t *db, size_t new_mapsize, gerror_t *error) {
    if (!db) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Database cannot be NULL");
        return WTREE3_EINVAL;
    }
    int rc = mdb_env_set_mapsize(db->env, new_mapsize);
    if (rc != 0) return translate_mdb_error(rc, error);
    db->mapsize = new_mapsize;
    return WTREE3_OK;
}

size_t wtree3_db_get_mapsize(wtree3_db_t *db) {
    return db ? db->mapsize : 0;
}

int wtree3_db_stats(wtree3_db_t *db, MDB_stat *stat, gerror_t *error) {
    if (!db || !stat) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Invalid parameters");
        return WTREE3_EINVAL;
    }
    int rc = mdb_env_stat(db->env, stat);
    if (rc != 0) return translate_mdb_error(rc, error);
    return WTREE3_OK;
}

MDB_env* wtree3_db_get_env(wtree3_db_t *db) {
    return db ? db->env : NULL;
}

/* ============================================================
 * Transaction Operations
 * ============================================================ */

wtree3_txn_t* wtree3_txn_begin(wtree3_db_t *db, bool write, gerror_t *error) {
    if (!db) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Database cannot be NULL");
        return NULL;
    }

    wtree3_txn_t *txn = calloc(1, sizeof(wtree3_txn_t));
    if (!txn) {
        set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Failed to allocate transaction");
        return NULL;
    }

    unsigned int flags = write ? 0 : MDB_RDONLY;
    int rc = mdb_txn_begin(db->env, NULL, flags, &txn->txn);
    if (rc != 0) {
        translate_mdb_error(rc, error);
        free(txn);
        return NULL;
    }

    txn->db = db;
    txn->is_write = write;
    return txn;
}

int wtree3_txn_commit(wtree3_txn_t *txn, gerror_t *error) {
    if (!txn) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Transaction cannot be NULL");
        return WTREE3_EINVAL;
    }

    int rc = mdb_txn_commit(txn->txn);
    free(txn);
    if (rc != 0) return translate_mdb_error(rc, error);
    return WTREE3_OK;
}

void wtree3_txn_abort(wtree3_txn_t *txn) {
    if (!txn) return;
    mdb_txn_abort(txn->txn);
    free(txn);
}

void wtree3_txn_reset(wtree3_txn_t *txn) {
    if (!txn || txn->is_write) return;
    mdb_txn_reset(txn->txn);
}

int wtree3_txn_renew(wtree3_txn_t *txn, gerror_t *error) {
    if (!txn) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Transaction is NULL");
        return WTREE3_EINVAL;
    }
    if (txn->is_write) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Cannot renew write transaction");
        return WTREE3_EINVAL;
    }
    int rc = mdb_txn_renew(txn->txn);
    if (rc != 0) return translate_mdb_error(rc, error);
    return WTREE3_OK;
}

bool wtree3_txn_is_readonly(wtree3_txn_t *txn) {
    return txn && !txn->is_write;
}

MDB_txn* wtree3_txn_get_mdb(wtree3_txn_t *txn) {
    return txn ? txn->txn : NULL;
}

wtree3_db_t* wtree3_txn_get_db(wtree3_txn_t *txn) {
    return txn ? txn->db : NULL;
}

/* ============================================================
 * Utility Functions
 * ============================================================ */

const char* wtree3_strerror(int error_code) {
    switch (error_code) {
        case WTREE3_OK:
            return "Success";
        case WTREE3_ERROR:
            return "Generic error";
        case WTREE3_EINVAL:
            return "Invalid argument";
        case WTREE3_ENOMEM:
            return "Out of memory";
        case WTREE3_KEY_EXISTS:
            return "Key already exists";
        case WTREE3_NOT_FOUND:
            return "Key not found";
        case WTREE3_MAP_FULL:
            return "Database map is full, resize needed";
        case WTREE3_TXN_FULL:
            return "Transaction has too many dirty pages";
        case WTREE3_INDEX_ERROR:
            return "Index error (duplicate key violation)";
        default:
            return mdb_strerror(error_code);
    }
}

bool wtree3_error_recoverable(int error_code) {
    return error_code == WTREE3_MAP_FULL ||
           error_code == WTREE3_TXN_FULL ||
           error_code == MDB_MAP_RESIZED;
}

/* ============================================================
 * Extractor Registry Operations
 * ============================================================ */

int wtree3_db_register_key_extractor(wtree3_db_t *db, uint32_t version,
                                       uint32_t flags, wtree3_index_key_fn key_fn,
                                       gerror_t *error) {
    if (!db || !key_fn) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Invalid parameters");
        return WTREE3_EINVAL;
    }

    /* Build extractor ID from version and flags */
    uint64_t extractor_id = build_extractor_id(version, flags);

    /* Register in registry */
    if (!wtree3_extractor_registry_set(db->extractor_registry, extractor_id, key_fn)) {
        set_error(error, WTREE3_LIB, WTREE3_ERROR,
                 "Failed to register extractor (version=%u, flags=0x%02x)", version, flags);
        return WTREE3_ERROR;
    }

    return WTREE3_OK;
}

wtree3_index_key_fn find_extractor(wtree3_db_t *db, uint64_t extractor_id) {
    if (!db) return NULL;

    return wtree3_extractor_registry_get(db->extractor_registry, extractor_id);
}
