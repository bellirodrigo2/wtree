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
#define WTREE3_META_DB "__wtree3_index_meta__"
#define WTREE3_META_VERSION 1

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
    wtree3_user_data_persistence_t persistence; /* Persistence callbacks */
    bool has_persistence;           /* Whether persistence is configured */
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

    /* Upsert merge callback */
    wtree3_merge_fn merge_fn;
    void *merge_user_data;
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

/* Get or create metadata DBI */
static int get_metadata_dbi(wtree3_db_t *db, MDB_txn *txn, MDB_dbi *out_dbi, gerror_t *error) {
    int rc = mdb_dbi_open(txn, WTREE3_META_DB, MDB_CREATE, out_dbi);
    if (rc != 0) {
        return translate_mdb_error(rc, error);
    }
    return WTREE3_OK;
}

/* Build metadata key: tree_name:index_name */
static char* build_metadata_key(const char *tree_name, const char *index_name) {
    size_t len = strlen(tree_name) + 1 + strlen(index_name) + 1;
    char *key = malloc(len);
    if (key) {
        snprintf(key, len, "%s:%s", tree_name, index_name);
    }
    return key;
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

void wtree3_tree_set_merge_fn(wtree3_tree_t *tree, wtree3_merge_fn merge_fn, void *user_data) {
    if (!tree) return;
    tree->merge_fn = merge_fn;
    tree->merge_user_data = user_data;
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

int wtree3_upsert_txn(wtree3_txn_t *txn, wtree3_tree_t *tree,
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

    /* Check if key exists */
    MDB_val mkey = {.mv_size = key_len, .mv_data = (void*)key};
    MDB_val old_val;
    int rc = mdb_get(txn->txn, tree->dbi, &mkey, &old_val);

    if (rc == MDB_NOTFOUND) {
        /* Key doesn't exist - perform insert */
        return wtree3_insert_one_txn(txn, tree, key, key_len, value, value_len, error);
    } else if (rc != 0) {
        return translate_mdb_error(rc, error);
    }

    /* Key exists - need to merge or overwrite */
    const void *final_value = value;
    size_t final_value_len = value_len;
    void *merged_value = NULL;

    if (tree->merge_fn) {
        /* Call merge callback */
        size_t merged_len;
        merged_value = tree->merge_fn(
            old_val.mv_data, old_val.mv_size,
            value, value_len,
            tree->merge_user_data,
            &merged_len
        );

        if (!merged_value) {
            set_error(error, WTREE3_LIB, WTREE3_ERROR, "Merge callback returned NULL");
            return WTREE3_ERROR;
        }

        final_value = merged_value;
        final_value_len = merged_len;
    }

    /* Delete old index entries */
    rc = indexes_delete(tree, txn->txn, key, key_len, old_val.mv_data, old_val.mv_size, error);
    if (rc != 0) {
        free(merged_value);
        return rc;
    }

    /* Insert new index entries */
    rc = indexes_insert(tree, txn->txn, key, key_len, final_value, final_value_len, error);
    if (rc != 0) {
        free(merged_value);
        return rc;
    }

    /* Update main tree */
    MDB_val mval = {.mv_size = final_value_len, .mv_data = (void*)final_value};
    rc = mdb_put(txn->txn, tree->dbi, &mkey, &mval, 0);

    free(merged_value);

    if (rc != 0) return translate_mdb_error(rc, error);

    /* Note: entry_count doesn't change (key already existed) */
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
