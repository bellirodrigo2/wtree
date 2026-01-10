# WTree3 Documentation Guide

## Overview

WTree3 is a high-performance LMDB wrapper library providing:
- **Zero-copy key-value storage** with ACID transactions
- **Automatic secondary index maintenance** (unique, non-unique, sparse)
- **Advanced query capabilities** (range scans, prefix queries, index lookups)
- **Batch operations** for optimal performance
- **Memory optimization** controls (madvise, mlock, prefetch)

---

## Documentation Structure

The WTree3 library documentation is organized into three complementary components:

### 1. API Reference (Doxygen)

**Location**: `src/wtree3.h`

**Purpose**: Complete API reference with detailed function signatures, parameters, return values, and technical specifications.

**Content**:
- Main page with architecture overview
- Error code definitions
- Opaque type descriptions
- Callback function types with detailed explanations
- Configuration structures
- All public API functions (database, transaction, tree, index, CRUD, scan, iterator operations)

**Generation**:
```bash
# Create Doxyfile if you don't have one
doxygen -g

# Edit Doxyfile settings:
# PROJECT_NAME = "WTree3"
# INPUT = src/wtree3.h
# RECURSIVE = NO
# EXTRACT_ALL = YES
# HAVE_DOT = YES (if graphviz installed)
# GENERATE_HTML = YES
# OUTPUT_DIRECTORY = docs

# Generate documentation
doxygen Doxyfile

# View documentation
open docs/html/index.html  # macOS/Linux
start docs/html/index.html # Windows
```

### 2. Usage Examples (EXAMPLES.md)

**Location**: `EXAMPLES.md`

**Purpose**: Practical, copy-paste-ready code examples for common use cases.

**Content**:
- Quick start guide
- Database lifecycle management
- Basic CRUD operations
- Secondary index usage (unique, non-unique, sparse)
- Advanced querying (range, prefix, index lookups)
- Batch operations
- Atomic operations (modify, conditional delete, collect)
- Range scans and iteration patterns
- Memory optimization techniques
- Error handling patterns
- Multi-collection application example
- Performance tips and best practices

**View**:
```bash
# Markdown viewer
mdless EXAMPLES.md

# Or open in any text editor/browser
```

### 3. Integration Tests (test_wtree3_full_integration.c)

**Location**: `tests/test_wtree3_full_integration.c`

**Purpose**: Comprehensive integration test demonstrating end-to-end functionality and serving as executable documentation.

**Content** (17 test phases):
1. Database open and setup
2. Multiple tree creation
3. Basic CRUD (no indexes)
4. Index creation
5. CRUD with automatic index maintenance
6. Index queries
7. Iterators and scans
8. Advanced operations (batch, bulk, conditional)
9. Database close and reopen (persistence test)
10. Tree deletion with index cleanup
11. Index verification
12. Scan reverse with boundaries
13. Collect range edge cases
14. Iterator edge cases
15. Atomic modify operations
16. Batch existence checks
17. Cleanup

**Build and Run**:
```bash
# Using CMake (if configured)
mkdir build && cd build
cmake ..
make
./test_wtree3_full_integration

# Or compile directly
gcc -o test_integration \
    tests/test_wtree3_full_integration.c \
    src/wtree3_*.c src/gerror.c src/wvector.c \
    -I. -Isrc \
    -llmdb -lcmocka \
    -std=c99
./test_integration
```

---

## Key Concepts

### Architecture Layers

```
┌─────────────────────────────────────────────┐
│         Application Layer                    │
└─────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────┐
│  WTree3 Layer (wtree3.h)                    │
│  ┌──────────────┬──────────────┬─────────┐ │
│  │ Index Layer  │ Tree Layer   │ Core    │ │
│  │ - Indexes    │ - Trees      │ Layer   │ │
│  │ - Extractors │ - CRUD       │ - DB    │ │
│  │ - Queries    │ - Iterators  │ - Txn   │ │
│  └──────────────┴──────────────┴─────────┘ │
└─────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────┐
│         LMDB Layer (Lightning MDB)          │
│  - Memory-mapped B+tree                     │
│  - ACID transactions                        │
│  - MVCC                                     │
└─────────────────────────────────────────────┘
```

### Workflow: Creating and Using an Index

```
1. Define Data Structure
   ↓
2. Write Index Key Extractor Function
   ↓
3. Register Extractor with Database
   (version + flags → extractor function)
   ↓
4. Create Tree/Collection
   ↓
5. Add Index to Tree
   (provides config: name, unique, sparse, etc.)
   ↓
6. Populate Index from Existing Data
   (optional if tree is new)
   ↓
7. Use CRUD Operations
   (indexes automatically maintained)
   ↓
8. Query by Index
   (wtree3_index_seek returns iterator)
   ↓
9. Close Database
   (index metadata persisted)
   ↓
10. Reopen Database
    (indexes auto-loaded if extractors registered)
```

### Index Types

| Type | Unique | Sparse | Use Case | Example |
|------|--------|--------|----------|---------|
| **Unique, Dense** | ✓ | ✗ | Primary keys, email addresses | User email (all users must have email) |
| **Unique, Sparse** | ✓ | ✓ | Optional unique fields | SSN (not all users have SSN) |
| **Non-unique, Dense** | ✗ | ✗ | Categorical data | User name, product category |
| **Non-unique, Sparse** | ✗ | ✓ | Optional categorization | Product sale category (only sale items) |

### Memory Management

| Operation | Who Allocates | Who Frees | Lifetime |
|-----------|---------------|-----------|----------|
| `wtree3_get()` | WTree3 | User (with `free()`) | Until freed |
| `wtree3_get_txn()` | LMDB (zero-copy) | Automatic | Until transaction ends |
| Index extractor `out_key` | User (with `malloc()`) | WTree3 | Short-lived |
| Merge callback return | User (with `malloc()`) | WTree3 | Short-lived |
| Modify callback return | User (with `malloc()`) | WTree3 | Short-lived |
| `wtree3_collect_range_txn()` | WTree3 | User (arrays + each entry) | Until freed |
| Iterator key/value | LMDB (zero-copy) | Automatic | Until iterator moves |
| `wtree3_iterator_key_copy()` | WTree3 | User (with `free()`) | Until freed |

---

## Common Patterns

### Pattern 1: Basic CRUD with Index

```c
// Setup
wtree3_db_t *db = wtree3_db_open(...);
wtree3_db_register_key_extractor(db, WTREE3_VERSION(1,0), 0x01, extractor, &err);
wtree3_tree_t *tree = wtree3_tree_open(db, "collection", MDB_CREATE, 0, &err);

wtree3_index_config_t idx_cfg = { .name = "email_idx", .unique = true, ... };
wtree3_tree_add_index(tree, &idx_cfg, &err);

// Insert (index auto-maintained)
wtree3_insert_one(tree, key, klen, &data, sizeof(data), &err);

// Query by index
wtree3_iterator_t *iter = wtree3_index_seek(tree, "email_idx", email, elen, &err);
wtree3_txn_t *txn = wtree3_iterator_get_txn(iter);
wtree3_index_iterator_main_key(iter, &main_key, &main_key_len);
wtree3_get_txn(txn, tree, main_key, main_key_len, &value, &value_len, &err);
wtree3_iterator_close(iter);
```

### Pattern 2: Batch Operations in Transaction

```c
wtree3_txn_t *txn = wtree3_txn_begin(db, true, &err);

// Multiple operations in single transaction
for (int i = 0; i < N; i++) {
    wtree3_insert_one_txn(txn, tree, keys[i], klens[i], vals[i], vlens[i], &err);
}

// Or use batch API
wtree3_insert_many_txn(txn, tree, kvs, count, &err);

wtree3_txn_commit(txn, &err);
```

### Pattern 3: Range Scan with Callback

```c
bool scan_callback(const void *k, size_t klen,
                   const void *v, size_t vlen, void *ud) {
    // Process entry (zero-copy)
    my_data_t *data = (my_data_t*)v;
    process(data);
    return true;  // Continue or false to stop
}

wtree3_txn_t *txn = wtree3_txn_begin(db, false, &err);
wtree3_scan_range_txn(txn, tree, start_key, slen, end_key, elen,
                      scan_callback, user_data, &err);
wtree3_txn_abort(txn);
```

### Pattern 4: Atomic Increment

```c
void* increment_fn(const void *existing, size_t elen,
                   void *ud, size_t *out_len) {
    counter_t *new = malloc(sizeof(counter_t));
    new->count = existing ? ((counter_t*)existing)->count + 1 : 1;
    *out_len = sizeof(counter_t);
    return new;
}

wtree3_txn_t *txn = wtree3_txn_begin(db, true, &err);
wtree3_modify_txn(txn, tree, key, klen, increment_fn, NULL, &err);
wtree3_txn_commit(txn, &err);
```

---

## Performance Tuning

### 1. Database Configuration

```c
// Generous initial mapsize (can grow, can't shrink easily)
size_t mapsize = 10UL * 1024 * 1024 * 1024;  // 10GB

// Use flags for performance vs safety trade-off
unsigned int flags = 0;            // Safe (default)
// flags = MDB_NOSYNC;             // Faster writes, risk data loss on crash
// flags = MDB_WRITEMAP;           // Better perf on some platforms
// flags = MDB_NOSYNC | MDB_WRITEMAP;  // Maximum speed, minimum safety
```

### 2. Transaction Size

```c
// Keep transactions reasonably sized
// Too small: overhead of many commits
// Too large: risk WTREE3_TXN_FULL error

// Good practice: 1000-10000 operations per transaction
#define BATCH_SIZE 5000

for (int i = 0; i < total; i += BATCH_SIZE) {
    wtree3_txn_t *txn = wtree3_txn_begin(db, true, &err);

    for (int j = 0; j < BATCH_SIZE && (i+j) < total; j++) {
        wtree3_insert_one_txn(txn, tree, ...);
    }

    wtree3_txn_commit(txn, &err);
}
```

### 3. Index Strategy

```c
// Only create indexes you actually use
// Each index adds overhead to writes

// For read-heavy workloads: more indexes OK
// For write-heavy workloads: minimal indexes

// Consider sparse indexes to reduce space
```

### 4. Memory Hints

```c
// Random access pattern
wtree3_db_madvise(db, WTREE3_MADV_RANDOM, &err);

// Sequential scan
wtree3_db_madvise(db, WTREE3_MADV_SEQUENTIAL, &err);

// Lock in RAM for latency-sensitive applications
wtree3_db_mlock(db, WTREE3_MLOCK_CURRENT, &err);
```

---

## Error Handling Best Practices

```c
gerror_t error = {0};
int rc = wtree3_operation(..., &error);

if (rc != WTREE3_OK) {
    // Log error with context
    fprintf(stderr, "Operation failed: %s (code: %d)\n",
            error.message, error.code);

    // Handle specific errors
    if (rc == WTREE3_MAP_FULL) {
        // Recoverable: resize database
        size_t new_size = wtree3_db_get_mapsize(db) + (1UL << 30);  // +1GB
        wtree3_db_resize(db, new_size, &error);
        // Retry operation...
    } else if (rc == WTREE3_INDEX_ERROR) {
        // Unique constraint violation or index corruption
        // Check if duplicate or call wtree3_verify_indexes()
    } else if (rc == WTREE3_TXN_FULL) {
        // Transaction too large, split into smaller batches
    } else {
        // Other errors: log and abort
        return -1;
    }
}
```

---

## Thread Safety Guidelines

| Component | Thread Safety | Notes |
|-----------|---------------|-------|
| `wtree3_db_t` | ✓ Thread-safe | Can be shared across threads |
| `wtree3_txn_t` | ✗ NOT thread-safe | One per thread |
| `wtree3_tree_t` | ✓ Thread-safe (with proper txns) | Access through transactions |
| `wtree3_iterator_t` | ✗ NOT thread-safe | Single-threaded use only |

**Correct Multi-threaded Pattern:**

```c
// Global/shared
wtree3_db_t *db;          // Shared
wtree3_tree_t *tree;      // Shared

// Per-thread
void worker_thread(void *arg) {
    gerror_t error = {0};

    // Each thread creates its own transaction
    wtree3_txn_t *txn = wtree3_txn_begin(db, true, &error);

    // Operate on shared tree through thread's transaction
    wtree3_insert_one_txn(txn, tree, key, klen, val, vlen, &error);

    wtree3_txn_commit(txn, &error);
}
```

---

## Migration and Versioning

When upgrading schema or extractor logic:

```c
// Old version (1.0)
wtree3_db_register_key_extractor(db, WTREE3_VERSION(1, 0), 0x01,
                                 old_extractor_v1, &err);

// New version (2.0) with different extraction logic
wtree3_db_register_key_extractor(db, WTREE3_VERSION(2, 0), 0x01,
                                 new_extractor_v2, &err);

// Open old tree with v1.0 schema
wtree3_tree_t *old_tree = wtree3_tree_open_with_version(db, "data",
                                                         WTREE3_VERSION(1, 0), &err);

// Migrate data to new tree with v2.0 schema
wtree3_tree_t *new_tree = wtree3_tree_open(db, "data_v2",
                                            WTREE3_VERSION(2, 0), &err);

// Copy and re-index...
```

---

## Debugging Tips

### Enable Verbose Error Messages

```c
gerror_t error = {0};
int rc = wtree3_operation(..., &error);

if (rc != WTREE3_OK) {
    fprintf(stderr, "Error in %s: %s\n", error.lib, error.message);
    fprintf(stderr, "Error code: %d (%s)\n", error.code, wtree3_strerror(error.code));
}
```

### Verify Index Integrity

```c
// Periodically verify index consistency
int rc = wtree3_verify_indexes(tree, &error);

if (rc != WTREE3_OK) {
    fprintf(stderr, "Index corruption detected: %s\n", error.message);
    // Rebuild indexes:
    // 1. Drop corrupted index
    // 2. Re-add index
    // 3. Populate from main tree
}
```

### Monitor Database Usage

```c
MDB_envinfo info;
mdb_env_info(wtree3_db_get_env(db), &info);

printf("Last used page: %zu\n", info.me_last_pgno);
printf("Map size: %zu MB\n", info.me_mapsize / (1024*1024));
printf("Usage: %.1f%%\n",
       (double)info.me_last_pgno * 4096 / info.me_mapsize * 100);
```

---

## Quick Reference Card

### Database Operations
```c
wtree3_db_open()          // Open database
wtree3_db_close()         // Close database
wtree3_db_sync()          // Sync to disk
wtree3_db_resize()        // Grow database
wtree3_db_register_key_extractor()  // Register index extractor
```

### Transaction Operations
```c
wtree3_txn_begin()        // Start transaction
wtree3_txn_commit()       // Commit transaction
wtree3_txn_abort()        // Abort transaction
wtree3_txn_reset()        // Reset read-only txn
wtree3_txn_renew()        // Renew read-only txn
```

### Tree Operations
```c
wtree3_tree_open()        // Open/create tree
wtree3_tree_close()       // Close tree handle
wtree3_tree_delete()      // Delete tree and indexes
wtree3_tree_count()       // Get entry count O(1)
wtree3_tree_exists()      // Check if tree exists
```

### Index Operations
```c
wtree3_tree_add_index()      // Create index
wtree3_tree_populate_index() // Build index from data
wtree3_tree_drop_index()     // Remove index
wtree3_tree_has_index()      // Check if index exists
wtree3_verify_indexes()      // Verify integrity
```

### CRUD Operations
```c
wtree3_insert_one()       // Insert (auto-txn)
wtree3_update()           // Update (auto-txn)
wtree3_upsert()           // Insert or update (auto-txn)
wtree3_delete_one()       // Delete (auto-txn)
wtree3_get()              // Get (auto-txn, allocates)
wtree3_exists()           // Check existence
wtree3_insert_one_txn()   // Insert (with txn)
wtree3_get_txn()          // Get (with txn, zero-copy)
```

### Query Operations
```c
wtree3_index_seek()       // Exact match on index
wtree3_index_seek_range() // Range match on index
wtree3_scan_range_txn()   // Forward scan
wtree3_scan_reverse_txn() // Reverse scan
wtree3_scan_prefix_txn()  // Prefix scan
```

### Iterator Operations
```c
wtree3_iterator_create()  // Create iterator
wtree3_iterator_first()   // Go to first
wtree3_iterator_last()    // Go to last
wtree3_iterator_next()    // Next entry
wtree3_iterator_prev()    // Previous entry
wtree3_iterator_seek()    // Seek exact
wtree3_iterator_seek_range()  // Seek range
wtree3_iterator_valid()   // Check if valid
wtree3_iterator_close()   // Close iterator
```

### Advanced Operations
```c
wtree3_modify_txn()       // Atomic read-modify-write
wtree3_delete_if_txn()    // Conditional bulk delete
wtree3_collect_range_txn() // Collect to arrays
wtree3_insert_many_txn()  // Batch insert
wtree3_get_many_txn()     // Batch read
wtree3_exists_many_txn()  // Batch exists check
```

---

## Additional Resources

- **LMDB Documentation**: http://www.lmdb.tech/doc/
- **Source Code**: `src/wtree3.h` (API), `src/wtree3_internal.h` (implementation)
- **Tests**: `tests/test_wtree3_full_integration.c` (comprehensive test suite)
- **Examples**: `EXAMPLES.md` (this file)

---

## Generating and Viewing Documentation

### Full Documentation Generation

```bash
# 1. Generate Doxygen HTML docs
doxygen Doxyfile

# 2. View API reference
open docs/html/index.html

# 3. View examples
cat EXAMPLES.md

# 4. Run integration tests
make test_wtree3_full_integration
./test_wtree3_full_integration
```

### Quick Reference

```bash
# View main header (API)
less src/wtree3.h

# Search for function
grep -n "wtree3_insert" src/wtree3.h

# View example patterns
grep -A 20 "Pattern 1" EXAMPLES.md
```

---

**Document Version**: 1.0
**Last Updated**: 2026-01-10
**WTree3 Version**: 3.0
