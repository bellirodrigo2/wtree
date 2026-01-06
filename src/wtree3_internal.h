/*
 * wtree3_internal.h - Internal structures and helpers for wtree3 modules
 *
 * This header is shared across all wtree3_*.c implementation files
 * and should NOT be included by external code.
 */

#ifndef WTREE3_INTERNAL_H
#define WTREE3_INTERNAL_H

#include "wtree3.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <errno.h>

#ifndef S_ISDIR
#define S_ISDIR(mode) (((mode) & S_IFMT) == S_IFDIR)
#endif

/* ============================================================
 * Internal Constants
 * ============================================================ */

#define WTREE3_LIB "wtree3"
#define WTREE3_INDEX_PREFIX "idx:"
#define WTREE3_META_DB "__wtree3_index_meta__"
#define WTREE3_META_VERSION 1

/* ============================================================
 * Internal Structure Definitions
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
 * Internal Helper Functions (implemented in wtree3_core.c)
 * ============================================================ */

/* Translate LMDB error codes to wtree3 error codes */
int translate_mdb_error(int mdb_rc, gerror_t *error);

/* ============================================================
 * Index Helper Functions (implemented in wtree3_index.c)
 * ============================================================ */

/* Find index by name in tree's index array */
wtree3_index_t* find_index(wtree3_tree_t *tree, const char *name);

/* Build index tree name: idx:<tree_name>:<index_name> */
char* build_index_tree_name(const char *tree_name, const char *index_name);

/* Build metadata key: tree_name:index_name */
char* build_metadata_key(const char *tree_name, const char *index_name);

/* Get or create metadata DBI */
int get_metadata_dbi(wtree3_db_t *db, MDB_txn *txn, MDB_dbi *out_dbi, gerror_t *error);

/* ============================================================
 * Index Maintenance Functions (implemented in wtree3_crud.c)
 * ============================================================ */

/* Insert entry into all indexes (called during insert/update) */
int indexes_insert(wtree3_tree_t *tree, MDB_txn *txn,
                   const void *key, size_t key_len,
                   const void *value, size_t value_len,
                   gerror_t *error);

/* Delete entry from all indexes (called during delete/update) */
int indexes_delete(wtree3_tree_t *tree, MDB_txn *txn,
                   const void *key, size_t key_len,
                   const void *value, size_t value_len,
                   gerror_t *error);

#endif /* WTREE3_INTERNAL_H */
