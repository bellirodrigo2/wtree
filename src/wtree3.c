/*
 * wtree3.c - Unified Storage Layer Implementation
 *
 * Combines LMDB access and index management into a single layer.
 */

#include "wtree3.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <errno.h>

#ifndef S_ISDIR
#define S_ISDIR(mode) (((mode) & S_IFMT) == S_IFDIR)
#endif

#define WTREE3_LIB "wtree3"
#define WTREE3_INDEX_PREFIX "idx:"

/* ============================================================
 * Internal Structures
 * ============================================================ */

/* Database handle */
struct wtree3_db_t {
    MDB_env *env;
    char *path;
    size_t mapsize;
    unsigned int max_dbs;
    unsigned int flags;
};

/* Transaction handle */
struct wtree3_txn_t {
    MDB_txn *txn;
    wtree3_db_t *db;
    bool is_write;
};

/* Single index entry */
typedef struct wtree3_index {
    char *name;                     /* Index name */
    char *tree_name;                /* Full tree name (idx:tree:name) */
    MDB_dbi dbi;                    /* Index DBI handle */
    wtree3_index_key_fn key_fn;     /* Key extraction callback */
    void *user_data;                /* Callback user data */
    wtree3_user_data_cleanup_fn user_data_cleanup; /* Cleanup callback */
    bool unique;                    /* Unique constraint */
    bool sparse;                    /* Sparse index */
    MDB_cmp_func *compare;          /* Custom comparator */
} wtree3_index_t;

/* Tree handle with index support */
struct wtree3_tree_t {
    char *name;
    MDB_dbi dbi;                    /* Main tree DBI */
    wtree3_db_t *db;
    unsigned int flags;

    /* Indexes */
    wtree3_index_t *indexes;
    size_t index_count;
    size_t index_capacity;

    /* Entry tracking */
    int64_t entry_count;
};

/* Iterator handle */
struct wtree3_iterator_t {
    MDB_cursor *cursor;
    wtree3_txn_t *txn;
    wtree3_tree_t *tree;
    MDB_val current_key;
    MDB_val current_val;
    bool valid;
    bool owns_txn;
    bool is_index;
};

/* ============================================================
 * Error Translation
 * ============================================================ */

static int translate_mdb_error(int mdb_rc, gerror_t *error) {
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
 * Helper Functions
 * ============================================================ */

/* Build index tree name: idx:<tree_name>:<index_name> */
static char* build_index_tree_name(const char *tree_name, const char *index_name) {
    size_t len = strlen(WTREE3_INDEX_PREFIX) + strlen(tree_name) + 1 + strlen(index_name) + 1;
    char *name = malloc(len);
    if (name) {
        snprintf(name, len, "%s%s:%s", WTREE3_INDEX_PREFIX, tree_name, index_name);
    }
    return name;
}

/* Find index by name */
static wtree3_index_t* find_index(wtree3_tree_t *tree, const char *name) {
    for (size_t i = 0; i < tree->index_count; i++) {
        if (strcmp(tree->indexes[i].name, name) == 0) {
            return &tree->indexes[i];
        }
    }
    return NULL;
}

/* ============================================================
 * Database Operations
 * ============================================================ */

wtree3_db_t* wtree3_db_open(const char *path, size_t mapsize,
                             unsigned int max_dbs, unsigned int flags,
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

    return tree;
}

void wtree3_tree_close(wtree3_tree_t *tree) {
    if (!tree) return;

    /* Free all indexes */
    for (size_t i = 0; i < tree->index_count; i++) {
        /* Call user_data cleanup if provided */
        if (tree->indexes[i].user_data_cleanup && tree->indexes[i].user_data) {
            tree->indexes[i].user_data_cleanup(tree->indexes[i].user_data);
        }
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

    MDB_txn *txn;
    int rc = mdb_txn_begin(db->env, NULL, 0, &txn);
    if (rc != 0) return translate_mdb_error(rc, error);

    MDB_dbi dbi;
    rc = mdb_dbi_open(txn, name, 0, &dbi);
    if (rc != 0) {
        mdb_txn_abort(txn);
        return translate_mdb_error(rc, error);
    }

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

    if (!idx->name) {
        free(idx_tree_name);
        set_error(error, WTREE3_LIB, WTREE3_ENOMEM, "Failed to allocate index name");
        return WTREE3_ENOMEM;
    }

    tree->index_count++;
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

/* ============================================================
 * Index Maintenance Helpers
 * ============================================================ */

static int indexes_insert(wtree3_tree_t *tree, MDB_txn *txn,
                          const void *key, size_t key_len,
                          const void *value, size_t value_len,
                          gerror_t *error) {
    for (size_t i = 0; i < tree->index_count; i++) {
        wtree3_index_t *idx = &tree->indexes[i];

        void *idx_key = NULL;
        size_t idx_key_len = 0;
        bool should_index = idx->key_fn(value, value_len, idx->user_data,
                                        &idx_key, &idx_key_len);

        if (!should_index) continue;
        if (!idx_key) {
            set_error(error, WTREE3_LIB, WTREE3_ERROR,
                     "Index key extraction failed for '%s'", idx->name);
            return WTREE3_ERROR;
        }

        /* Check unique constraint */
        if (idx->unique) {
            MDB_val check_key = {.mv_size = idx_key_len, .mv_data = idx_key};
            MDB_val check_val;
            int get_rc = mdb_get(txn, idx->dbi, &check_key, &check_val);
            if (get_rc == 0) {
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

        if (rc != 0 && rc != MDB_KEYEXIST) {
            return translate_mdb_error(rc, error);
        }
    }

    return WTREE3_OK;
}

static int indexes_delete(wtree3_tree_t *tree, MDB_txn *txn,
                          const void *key, size_t key_len,
                          const void *value, size_t value_len,
                          gerror_t *error) {
    for (size_t i = 0; i < tree->index_count; i++) {
        wtree3_index_t *idx = &tree->indexes[i];

        void *idx_key = NULL;
        size_t idx_key_len = 0;
        bool should_index = idx->key_fn(value, value_len, idx->user_data,
                                        &idx_key, &idx_key_len);

        if (!should_index || !idx_key) continue;

        /* Delete specific key+value pair from DUPSORT tree */
        MDB_val mk = {.mv_size = idx_key_len, .mv_data = idx_key};
        MDB_val mv = {.mv_size = key_len, .mv_data = (void*)key};
        int rc = mdb_del(txn, idx->dbi, &mk, &mv);
        free(idx_key);

        if (rc != 0 && rc != MDB_NOTFOUND) {
            return translate_mdb_error(rc, error);
        }
    }

    return WTREE3_OK;
}

/* ============================================================
 * Data Operations (With Transaction)
 * ============================================================ */

int wtree3_get_txn(wtree3_txn_t *txn, wtree3_tree_t *tree,
                   const void *key, size_t key_len,
                   const void **value, size_t *value_len,
                   gerror_t *error) {
    if (!txn || !tree || !key || !value || !value_len) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Invalid parameters");
        return WTREE3_EINVAL;
    }

    MDB_val mkey = {.mv_size = key_len, .mv_data = (void*)key};
    MDB_val mval;

    int rc = mdb_get(txn->txn, tree->dbi, &mkey, &mval);
    if (rc != 0) return translate_mdb_error(rc, error);

    *value = mval.mv_data;
    *value_len = mval.mv_size;
    return WTREE3_OK;
}

int wtree3_insert_one_txn(wtree3_txn_t *txn, wtree3_tree_t *tree,
                           const void *key, size_t key_len,
                           const void *value, size_t value_len,
                           gerror_t *error) {
    if (!txn || !tree || !key || !value) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Invalid parameters");
        return WTREE3_EINVAL;
    }

    if (!txn->is_write) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Write operation requires write transaction");
        return WTREE3_EINVAL;
    }

    /* Insert into indexes first (check unique constraints) */
    int rc = indexes_insert(tree, txn->txn, key, key_len, value, value_len, error);
    if (rc != 0) return rc;

    /* Insert into main tree */
    MDB_val mkey = {.mv_size = key_len, .mv_data = (void*)key};
    MDB_val mval = {.mv_size = value_len, .mv_data = (void*)value};

    rc = mdb_put(txn->txn, tree->dbi, &mkey, &mval, MDB_NOOVERWRITE);
    if (rc != 0) return translate_mdb_error(rc, error);

    tree->entry_count++;
    return WTREE3_OK;
}

int wtree3_update_txn(wtree3_txn_t *txn, wtree3_tree_t *tree,
                       const void *key, size_t key_len,
                       const void *value, size_t value_len,
                       gerror_t *error) {
    if (!txn || !tree || !key || !value) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Invalid parameters");
        return WTREE3_EINVAL;
    }

    if (!txn->is_write) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Write operation requires write transaction");
        return WTREE3_EINVAL;
    }

    /* Get old value for index maintenance (if exists) */
    MDB_val mkey = {.mv_size = key_len, .mv_data = (void*)key};
    MDB_val old_val;
    int rc = mdb_get(txn->txn, tree->dbi, &mkey, &old_val);
    bool exists = (rc == 0);

    if (rc != 0 && rc != MDB_NOTFOUND) {
        return translate_mdb_error(rc, error);
    }

    if (exists) {
        /* Delete from indexes using old value */
        rc = indexes_delete(tree, txn->txn, key, key_len, old_val.mv_data, old_val.mv_size, error);
        if (rc != 0) return rc;
    }

    /* Insert into indexes with new value */
    rc = indexes_insert(tree, txn->txn, key, key_len, value, value_len, error);
    if (rc != 0) return rc;

    /* Update/insert into main tree */
    MDB_val mval = {.mv_size = value_len, .mv_data = (void*)value};
    rc = mdb_put(txn->txn, tree->dbi, &mkey, &mval, 0);
    if (rc != 0) return translate_mdb_error(rc, error);

    /* Increment count only if this was an insert */
    if (!exists) {
        tree->entry_count++;
    }

    return WTREE3_OK;
}

int wtree3_delete_one_txn(wtree3_txn_t *txn, wtree3_tree_t *tree,
                           const void *key, size_t key_len,
                           bool *deleted,
                           gerror_t *error) {
    if (!txn || !tree || !key) {
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

bool wtree3_exists_txn(wtree3_txn_t *txn, wtree3_tree_t *tree,
                        const void *key, size_t key_len,
                        gerror_t *error) {
    if (!txn || !tree || !key) return false;

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

int wtree3_iterator_delete(wtree3_iterator_t *iter, gerror_t *error) {
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
