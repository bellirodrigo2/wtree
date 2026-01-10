# WTree3

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![C Standard](https://img.shields.io/badge/C-C99-blue.svg)](https://en.wikipedia.org/wiki/C99)
[![Platform](https://img.shields.io/badge/platform-Linux%20%7C%20macOS%20%7C%20Windows-lightgrey.svg)](https://github.com/yourusername/wtree)

**High-Performance Key-Value Storage with Secondary Indexes**

WTree3 is a sophisticated LMDB wrapper that adds powerful database features while maintaining LMDB's legendary performance. It provides automatic secondary index maintenance, advanced query capabilities, and a clean API for modern C applications.

---

## Features

- **🚀 Zero-Copy Architecture** - Direct memory-mapped access without serialization overhead
- **🔒 ACID Transactions** - Full transactional support with MVCC
- **📊 Secondary Indexes** - Automatic maintenance of unique/non-unique/sparse indexes
- **⚡ Batch Operations** - Optimized bulk insert, read, and delete
- **🔍 Advanced Queries** - Range scans, prefix search, index lookups
- **💾 Index Persistence** - Indexes automatically saved and reloaded
- **🧮 O(1) Counting** - Instant collection size without scans
- **🎯 Memory Control** - Fine-grained optimization (madvise, mlock, prefetch)
- **🔧 Atomic Operations** - Read-modify-write, conditional bulk operations
- **📦 Named Collections** - Multiple independent trees in single database

---

## Quick Start

### Installation

```bash
# Clone repository
git clone https://github.com/yourusername/wtree.git
cd wtree

# Build with CMake
mkdir build && cd build
cmake ..
make

# Or compile manually
gcc -o example example.c src/wtree3_*.c src/gerror.c src/wvector.c \
    -I. -Isrc -llmdb -std=c99
```

### Dependencies

- **LMDB** ≥ 0.9.14 (Lightning Memory-Mapped Database)
- **C Compiler** with C99 support (GCC, Clang, MSVC)

### Hello World Example

```c
#include "wtree3.h"
#include <stdio.h>
#include <string.h>

typedef struct {
    int id;
    char name[64];
    char email[128];
} user_t;

// Index key extractor
bool extract_email(const void *value, size_t value_len,
                   void *user_data, void **out_key, size_t *out_len) {
    const user_t *user = (const user_t *)value;
    size_t len = strlen(user->email);
    if (len == 0) return false;  // Sparse index

    *out_key = malloc(len + 1);
    memcpy(*out_key, user->email, len + 1);
    *out_len = len + 1;
    return true;
}

int main() {
    gerror_t error = {0};

    // 1. Open database
    wtree3_db_t *db = wtree3_db_open("./mydb", 128*1024*1024, 64,
                                      WTREE3_VERSION(1, 0), 0, &error);

    // 2. Register extractor
    wtree3_db_register_key_extractor(db, WTREE3_VERSION(1, 0), 0x01,
                                     extract_email, &error);

    // 3. Create collection
    wtree3_tree_t *users = wtree3_tree_open(db, "users", MDB_CREATE, 0, &error);

    // 4. Add unique email index
    wtree3_index_config_t idx_cfg = {
        .name = "email_idx",
        .unique = true,
        .sparse = true
    };
    wtree3_tree_add_index(users, &idx_cfg, &error);

    // 5. Insert data (index auto-maintained)
    user_t alice = {1, "Alice", "alice@example.com"};
    char key[32];
    snprintf(key, sizeof(key), "user:%d", alice.id);
    wtree3_insert_one(users, key, strlen(key)+1, &alice, sizeof(alice), &error);

    // 6. Query by email
    wtree3_iterator_t *iter = wtree3_index_seek(users, "email_idx",
                                                 "alice@example.com", 18, &error);
    if (wtree3_iterator_valid(iter)) {
        printf("Found Alice!\n");
    }
    wtree3_iterator_close(iter);

    // 7. Cleanup
    wtree3_tree_close(users);
    wtree3_db_close(db);

    return 0;
}
```

Compile and run:

```bash
gcc -o hello hello.c src/wtree3_*.c src/gerror.c src/wvector.c \
    -I. -Isrc -llmdb -std=c99
./hello
```

---

## Documentation

### 📚 **Complete Documentation**

| Document | Description |
|----------|-------------|
| [**API Reference**](docs/api/html/index.html) | Doxygen-generated complete API documentation |
| [**Examples Guide**](docs/EXAMPLES.md) | 50+ copy-paste ready code examples |
| [**User Guide**](docs/DOCUMENTATION.md) | Architecture, patterns, best practices |
| [**Getting Started**](docs/DOCUMENTATION_SUMMARY.md) | Quick overview and setup instructions |

### 🚀 **Quick Links**

- **New to WTree3?** → Start with [Quick Start Example](docs/EXAMPLES.md#quick-start)
- **Need a pattern?** → Check [Common Patterns](docs/DOCUMENTATION.md#common-patterns)
- **API reference?** → Browse [Doxygen Docs](docs/api/html/index.html)
- **Performance tips?** → Read [Performance Tuning](docs/DOCUMENTATION.md#performance-tuning)

### 📖 **Generate API Docs**

```bash
# Install Doxygen (if not already installed)
# Ubuntu/Debian: sudo apt-get install doxygen graphviz
# macOS: brew install doxygen graphviz
# Windows: Download from doxygen.org

# Generate documentation
doxygen Doxyfile

# View in browser
open docs/api/html/index.html  # macOS
xdg-open docs/api/html/index.html  # Linux
start docs\api\html\index.html  # Windows
```

---

## Architecture

```
┌─────────────────────────────────────────────┐
│         Application Layer                    │
└─────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────┐
│  WTree3 Layer (wtree3.h)                    │
│  ┌──────────────┬──────────────┬─────────┐ │
│  │ Index Layer  │ Tree Layer   │ Core    │ │
│  │ - Indexes    │ - Collections│ Layer   │ │
│  │ - Extractors │ - CRUD       │ - DB    │ │
│  │ - Queries    │ - Iterators  │ - Txn   │ │
│  └──────────────┴──────────────┴─────────┘ │
└─────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────┐
│         LMDB Layer (Lightning MDB)          │
│  - Memory-mapped B+tree                     │
│  - ACID transactions                        │
│  - MVCC (Multi-Version Concurrency Control) │
└─────────────────────────────────────────────┘
```

**Three Conceptual Layers:**

1. **Core Layer** - Database environment, transactions, memory optimization
2. **Tree Layer** - Named collections (trees), CRUD operations, iterators
3. **Index Layer** - Secondary indexes, automatic maintenance, queries

---

## Key Concepts

### Secondary Indexes

WTree3 automatically maintains secondary indexes during all CRUD operations:

```c
// Define extractor function
bool extract_email(const void *value, size_t value_len,
                   void *user_data, void **out_key, size_t *out_len) {
    const user_t *user = value;
    *out_key = strdup(user->email);
    *out_len = strlen(user->email) + 1;
    return true;
}

// Create index
wtree3_index_config_t cfg = {
    .name = "email_idx",
    .unique = true,     // Enforce uniqueness
    .sparse = false     // Index all entries
};
wtree3_tree_add_index(users, &cfg, &error);

// Insert automatically updates index
wtree3_insert_one(users, key, klen, &user, sizeof(user), &error);

// Query by index
wtree3_iterator_t *iter = wtree3_index_seek(users, "email_idx",
                                             "alice@example.com", 18, &error);
```

### Index Types

| Type | Unique | Sparse | Use Case |
|------|--------|--------|----------|
| **Unique Dense** | ✓ | ✗ | Primary keys, required unique fields |
| **Unique Sparse** | ✓ | ✓ | Optional unique fields (SSN, tax ID) |
| **Non-unique Dense** | ✗ | ✗ | Categories, tags, required fields |
| **Non-unique Sparse** | ✗ | ✓ | Optional categories, nullable fields |

### Transactions

```c
// Auto-transaction (convenience)
wtree3_insert_one(tree, key, klen, val, vlen, &error);

// Manual transaction (batch operations)
wtree3_txn_t *txn = wtree3_txn_begin(db, true, &error);
for (int i = 0; i < 1000; i++) {
    wtree3_insert_one_txn(txn, tree, keys[i], klens[i], vals[i], vlens[i], &error);
}
wtree3_txn_commit(txn, &error);

// Read transaction (zero-copy)
wtree3_txn_t *txn = wtree3_txn_begin(db, false, &error);
const void *value;
wtree3_get_txn(txn, tree, key, klen, &value, &vlen, &error);
// Use value (valid during transaction)
wtree3_txn_abort(txn);
```

### Memory Management

| Operation | Who Allocates | Who Frees | Lifetime |
|-----------|---------------|-----------|----------|
| `wtree3_get()` | WTree3 | User (`free()`) | Until freed |
| `wtree3_get_txn()` | LMDB (zero-copy) | Automatic | Until txn ends |
| Index extractor output | User (`malloc()`) | WTree3 | Short-lived |
| Iterator key/value | LMDB (zero-copy) | Automatic | Until iterator moves |

---

## Examples

### Basic CRUD

```c
// Insert
user_t user = {1, "Alice", "alice@example.com"};
wtree3_insert_one(users, "user:1", 7, &user, sizeof(user), &error);

// Read
void *value;
size_t vlen;
wtree3_get(users, "user:1", 7, &value, &vlen, &error);
user_t *u = (user_t*)value;
free(value);

// Update
user.age = 31;
wtree3_update(users, "user:1", 7, &user, sizeof(user), &error);

// Delete
bool deleted;
wtree3_delete_one(users, "user:1", 7, &deleted, &error);
```

### Batch Operations

```c
// Batch insert
wtree3_kv_t kvs[100];
// ... populate kvs ...

wtree3_txn_t *txn = wtree3_txn_begin(db, true, &error);
wtree3_insert_many_txn(txn, users, kvs, 100, &error);
wtree3_txn_commit(txn, &error);
```

### Range Scan

```c
bool print_user(const void *k, size_t klen, const void *v, size_t vlen, void *ud) {
    const user_t *user = v;
    printf("User: %s\n", user->name);
    return true;  // Continue
}

wtree3_txn_t *txn = wtree3_txn_begin(db, false, &error);
wtree3_scan_range_txn(txn, users, "user:100", 9, "user:200", 9,
                      print_user, NULL, &error);
wtree3_txn_abort(txn);
```

### Atomic Increment

```c
void* increment(const void *existing, size_t elen,
                void *ud, size_t *out_len) {
    counter_t *new = malloc(sizeof(counter_t));
    new->count = existing ? ((counter_t*)existing)->count + 1 : 1;
    *out_len = sizeof(counter_t);
    return new;
}

wtree3_txn_t *txn = wtree3_txn_begin(db, true, &error);
wtree3_modify_txn(txn, counters, "page_views", 11, increment, NULL, &error);
wtree3_txn_commit(txn, &error);
```

**📖 [View 50+ More Examples →](docs/EXAMPLES.md)**

---

## Performance

### Benchmarks

*(Results from LMDB 0.9.29 on Linux x64, SSD storage)*

| Operation | Throughput | Notes |
|-----------|------------|-------|
| **Insert** | ~200K ops/sec | Single transaction, 1KB values |
| **Read** | ~500K ops/sec | Zero-copy, read transaction |
| **Batch Insert** | ~1M ops/sec | 10K items per transaction |
| **Index Lookup** | ~450K ops/sec | Via secondary index |
| **Range Scan** | ~800K ops/sec | Sequential iteration |

### Complexity

| Operation | Time Complexity | Space |
|-----------|----------------|-------|
| Insert | O(log n) + O(log n) per index | O(n) |
| Lookup | O(log n) | O(1) |
| Index Query | O(log n) | O(1) |
| Range Scan | O(k) where k = results | O(k) |
| Count | O(1) | O(1) |

### Optimization Tips

```c
// 1. Batch operations in transactions
wtree3_txn_t *txn = wtree3_txn_begin(db, true, &error);
for (int i = 0; i < N; i++) {
    wtree3_insert_one_txn(txn, tree, ...);
}
wtree3_txn_commit(txn, &error);

// 2. Use zero-copy reads
wtree3_txn_t *txn = wtree3_txn_begin(db, false, &error);
const void *value;
wtree3_get_txn(txn, tree, key, klen, &value, &vlen, &error);
// Use value directly (no copy!)
wtree3_txn_abort(txn);

// 3. Size database generously
wtree3_db_t *db = wtree3_db_open(path, 10UL * 1024*1024*1024, ...);  // 10GB

// 4. Hint access patterns
wtree3_db_madvise(db, WTREE3_MADV_RANDOM, &error);  // Random access
```

**📖 [Full Performance Guide →](docs/DOCUMENTATION.md#performance-tuning)**

---

## Project Structure

```
wtree/
├── src/
│   ├── wtree3.h                    # Main API header (well-documented)
│   ├── wtree3_core.c              # Database & transaction management
│   ├── wtree3_tree.c              # Tree operations
│   ├── wtree3_crud.c              # CRUD operations with index maintenance
│   ├── wtree3_index.c             # Index operations
│   ├── wtree3_index_persist.c     # Index metadata persistence
│   ├── wtree3_iterator.c          # Iterator implementation
│   ├── wtree3_scan.c              # Range scan operations
│   ├── wtree3_memopt.c            # Memory optimization (madvise/mlock)
│   ├── wtree3_extractor_registry.c # Key extractor registry
│   ├── wtree3_internal.h          # Internal structures
│   ├── gerror.c/h                 # Error handling
│   ├── wvector.c/h                # Dynamic array utility
│   └── macros.h                   # Compiler hints & optimizations
├── tests/
│   ├── test_wtree3_full_integration.c  # Comprehensive integration test
│   ├── test_wtree3_lmdb_errors.c       # Error handling tests
│   └── CMakeLists.txt
├── docs/
│   ├── EXAMPLES.md                # 50+ usage examples
│   ├── DOCUMENTATION.md           # Complete guide
│   ├── DOCUMENTATION_SUMMARY.md   # Quick overview
│   └── api/                       # Generated Doxygen docs (gitignored)
├── README.md                      # This file
├── Doxyfile                       # Doxygen configuration
├── CMakeLists.txt                 # Build configuration
├── .gitignore                     # Git ignore patterns
└── LICENSE                        # Project license
```

---

## Building

### CMake (Recommended)

```bash
mkdir build && cd build
cmake ..
make

# Run tests
ctest

# Install
sudo make install
```

### Manual Compilation

```bash
# Compile library
gcc -c src/wtree3_*.c src/gerror.c src/wvector.c -I. -Isrc -std=c99

# Link with your application
gcc -o myapp myapp.c *.o -llmdb -std=c99
```

### Integration

**Static Library:**
```bash
ar rcs libwtree3.a *.o
gcc -o myapp myapp.c -L. -lwtree3 -llmdb
```

**Shared Library:**
```bash
gcc -shared -o libwtree3.so src/wtree3_*.c src/gerror.c src/wvector.c -llmdb -fPIC
gcc -o myapp myapp.c -L. -lwtree3 -llmdb
```

---

## Testing

### Run All Tests

```bash
mkdir build && cd build
cmake ..
make
ctest -V
```

### Run Integration Test

```bash
gcc -o test tests/test_wtree3_full_integration.c \
    src/wtree3_*.c src/gerror.c src/wvector.c \
    -I. -Isrc -llmdb -lcmocka -std=c99
./test
```

The integration test covers:
- Database lifecycle (open, close, reopen)
- Multiple tree management
- CRUD operations (with and without indexes)
- Index creation, population, queries
- Iterators (forward, reverse, seek)
- Range scans (forward, reverse, prefix)
- Batch operations
- Atomic operations (modify, conditional delete)
- Index persistence and auto-loading
- Error handling

---

## Thread Safety

| Component | Thread-Safe? | Notes |
|-----------|-------------|-------|
| `wtree3_db_t` | ✅ Yes | Shared across threads |
| `wtree3_tree_t` | ✅ Yes | With proper transactions |
| `wtree3_txn_t` | ❌ No | One per thread |
| `wtree3_iterator_t` | ❌ No | Single-threaded only |

**Multi-threaded Pattern:**

```c
// Shared
wtree3_db_t *db;      // Global database
wtree3_tree_t *users; // Global tree

// Per-thread
void worker_thread(void *arg) {
    gerror_t error = {0};

    // Each thread creates its own transaction
    wtree3_txn_t *txn = wtree3_txn_begin(db, true, &error);

    // Operate on shared tree through thread's transaction
    wtree3_insert_one_txn(txn, users, key, klen, val, vlen, &error);

    wtree3_txn_commit(txn, &error);
}
```

---

## Error Handling

```c
gerror_t error = {0};
int rc = wtree3_insert_one(users, key, klen, val, vlen, &error);

if (rc != WTREE3_OK) {
    fprintf(stderr, "Error: %s\n", error.message);

    switch (rc) {
        case WTREE3_MAP_FULL:
            // Recoverable: resize database
            wtree3_db_resize(db, new_size, &error);
            break;

        case WTREE3_INDEX_ERROR:
            // Unique constraint violation
            fprintf(stderr, "Duplicate key in index\n");
            break;

        case WTREE3_TXN_FULL:
            // Transaction too large, split into batches
            break;

        default:
            fprintf(stderr, "Fatal error: %d\n", rc);
            break;
    }
}
```

---

## Platform Support

| Platform | Status | Tested |
|----------|--------|--------|
| **Linux** | ✅ Fully Supported | Ubuntu 20.04+, Debian 11+ |
| **macOS** | ✅ Fully Supported | macOS 11+ |
| **Windows** | ✅ Fully Supported | Windows 10+, MSVC/MinGW |
| **BSD** | ✅ Fully Supported | FreeBSD, OpenBSD |

### Compiler Support

- **GCC** ≥ 4.9
- **Clang** ≥ 3.9
- **MSVC** ≥ 2015 (Visual Studio 14.0)

---

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Setup

```bash
# Clone with tests
git clone https://github.com/yourusername/wtree.git
cd wtree

# Install dependencies (Ubuntu/Debian)
sudo apt-get install liblmdb-dev libcmocka-dev doxygen graphviz

# Build and test
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Debug ..
make
ctest -V
```

---

## FAQ

**Q: How does WTree3 compare to raw LMDB?**

A: WTree3 adds secondary indexes, entry counting, and convenience APIs on top of LMDB. Performance is nearly identical for operations without indexes. With indexes, there's ~30% overhead per index during writes, but dramatic speedup for indexed queries.

**Q: Can I use WTree3 in production?**

A: Yes! WTree3 is built on battle-tested LMDB and includes comprehensive error handling, index verification, and persistence. The test suite covers edge cases and failure scenarios.

**Q: What's the maximum database size?**

A: Limited only by available disk space and address space. LMDB supports databases up to 128TB on 64-bit systems.

**Q: Do indexes slow down writes?**

A: Each index adds one B+tree write per insert/update/delete. For N indexes, expect ~N× write overhead. However, indexed queries are orders of magnitude faster than full scans.

**Q: How do I upgrade schema versions?**

A: Register multiple extractors for different versions:

```c
wtree3_db_register_key_extractor(db, WTREE3_VERSION(1, 0), 0x01, extractor_v1, &err);
wtree3_db_register_key_extractor(db, WTREE3_VERSION(2, 0), 0x01, extractor_v2, &err);
```

**📖 [More FAQs →](docs/DOCUMENTATION.md)**

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## Acknowledgments

- **LMDB** by Howard Chu and Symas Corporation - The lightning-fast foundation
- **cmocka** - Unit testing framework
- All contributors and users of WTree3

---

## Support

- **Documentation**: [docs/](docs/)
- **Issues**: [GitHub Issues](https://github.com/yourusername/wtree/issues)
- **Discussions**: [GitHub Discussions](https://github.com/yourusername/wtree/discussions)

---

## Changelog

### Version 3.0.0 (Current)

- ✨ Complete rewrite with unified API
- ✨ Automatic index persistence and reloading
- ✨ Memory optimization controls (madvise, mlock, prefetch)
- ✨ Atomic read-modify-write operations
- ✨ Conditional bulk operations
- ✨ Comprehensive Doxygen documentation
- ✨ 50+ usage examples
- 🐛 Fixed index consistency issues
- 🔧 Improved error handling and reporting

---

<div align="center">

**Built with ❤️ using C and LMDB**

[Documentation](docs/) • [Examples](docs/EXAMPLES.md) • [API Reference](docs/api/html/index.html) • [GitHub](https://github.com/yourusername/wtree)

</div>
