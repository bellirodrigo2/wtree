/*
 * wtree3_internal.h - Internal structures and helpers for wtree3 modules
 *
 * This header is shared across all wtree3_*.c implementation files
 * and should NOT be included by external code.
 */

#ifndef WTREE3_INTERNAL_H
#define WTREE3_INTERNAL_H

#include "wtree3.h"
#include "wvector.h"
#include "macros.h"
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

/* ============================================================
 * Internal Structure Definitions
 * ============================================================ */

/* Forward declare registry */
typedef struct wtree3_extractor_registry wtree3_extractor_registry_t;

/* Database handle */
struct wtree3_db_t {
    MDB_env *env;
    char *path;
    size_t mapsize;
    unsigned int max_dbs;
    uint32_t version;               /* Schema version (WTREE3_VERSION) */
    unsigned int flags;

    /* Extractor registry (version+flags → key_fn) */
    wtree3_extractor_registry_t *extractor_registry;
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
    uint64_t extractor_id;          /* Extractor ID */
    wtree3_index_key_fn key_fn;     /* Key extraction callback (looked up from registry) */
    void *user_data;                /* Callback user data (owned by index, copied from config) */
    size_t user_data_len;           /* Length of user_data */
    bool unique;                    /* Unique constraint */
    bool sparse;                    /* Sparse index */
    MDB_cmp_func *compare;          /* Custom key comparator */
    MDB_cmp_func *dupsort_compare;  /* Custom duplicate value comparator */
} wtree3_index_t;

/* Tree handle with index support */
struct wtree3_tree_t {
    char *name;
    MDB_dbi dbi;                    /* Main tree DBI */
    wtree3_db_t *db;
    unsigned int flags;

    /* Indexes (vector of wtree3_index_t*) */
    wvector_t *indexes;

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
WTREE_COLD
int translate_mdb_error(int mdb_rc, gerror_t *error);

/* Extractor registry lookup */
WTREE_PURE
wtree3_index_key_fn find_extractor(wtree3_db_t *db, uint64_t extractor_id);

/* Transaction wrapper helpers */
WTREE_HOT WTREE_WARN_UNUSED
int with_write_txn(wtree3_db_t *db,
                   int (*fn)(MDB_txn*, void*),
                   void *user_data,
                   gerror_t *error);

WTREE_HOT WTREE_WARN_UNUSED
int with_read_txn(wtree3_db_t *db,
                  int (*fn)(MDB_txn*, void*),
                  void *user_data,
                  gerror_t *error);

/* Build extractor ID from version and flags */
static inline uint64_t build_extractor_id(uint32_t version, uint32_t flags) {
    return ((uint64_t)version << 32) | flags;
}

/* Extract flags from index config */
static inline uint32_t extract_index_flags(const wtree3_index_config_t *config) {
    uint32_t flags = 0;
    if (config->unique) flags |= 0x01;
    if (config->sparse) flags |= 0x02;
    return flags;
}

/* ============================================================
 * Index Helper Functions (implemented in wtree3_index.c)
 * ============================================================ */

/* Find index by name in tree's index array */
WTREE_HOT WTREE_PURE
wtree3_index_t* find_index(wtree3_tree_t *tree, const char *name);

/* Build index tree name: idx:<tree_name>:<index_name> */
WTREE_MALLOC
char* build_index_tree_name(const char *tree_name, const char *index_name);

/* Build metadata key: tree_name:index_name */
WTREE_MALLOC
char* build_metadata_key(const char *tree_name, const char *index_name);

/* Get or create metadata DBI */
WTREE_WARN_UNUSED
int get_metadata_dbi(wtree3_db_t *db, MDB_txn *txn, MDB_dbi *out_dbi, gerror_t *error);

/* High-level metadata access helpers (within transaction) */
WTREE_WARN_UNUSED
int metadata_get_txn(MDB_txn *txn, wtree3_db_t *db,
                     const char *tree_name, const char *index_name,
                     MDB_val *out_val, gerror_t *error);

WTREE_WARN_UNUSED
int metadata_put_txn(MDB_txn *txn, wtree3_db_t *db,
                     const char *tree_name, const char *index_name,
                     const void *data, size_t data_len, gerror_t *error);

WTREE_WARN_UNUSED
int metadata_delete_txn(MDB_txn *txn, wtree3_db_t *db,
                        const char *tree_name, const char *index_name,
                        gerror_t *error);

/* Load index metadata (from wtree3_index_persist.c) */
WTREE_COLD WTREE_WARN_UNUSED
int load_index_metadata(wtree3_tree_t *tree, const char *index_name, gerror_t *error);

/* ============================================================
 * Index Maintenance Functions (implemented in wtree3_crud.c)
 * ============================================================ */

/* Insert entry into all indexes (called during insert/update) */
WTREE_HOT
int indexes_insert(wtree3_tree_t *tree, MDB_txn *txn,
                   const void *key, size_t key_len,
                   const void *value, size_t value_len,
                   gerror_t *error);

/* Delete entry from all indexes (called during delete/update) */
WTREE_HOT
int indexes_delete(wtree3_tree_t *tree, MDB_txn *txn,
                   const void *key, size_t key_len,
                   const void *value, size_t value_len,
                   gerror_t *error);

#endif /* WTREE3_INTERNAL_H */
