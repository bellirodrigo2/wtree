/*
 * wtree3_memopt.c - Portable Memory Optimization API
 *
 * Provides OS-level memory optimizations for wtree3 databases:
 * - Memory access hints (madvise)
 * - Memory locking (mlock/munlock)
 * - Page prefetching
 * - Memory map introspection
 *
 * Platform support:
 * - POSIX (Linux, BSD, macOS): Full support
 * - Windows: Partial support (limited madvise equivalent)
 * - Other: Graceful degradation with error messages
 */

#include "wtree3_internal.h"
#include "macros.h"

#if WTREE_OS_POSIX
    #include <sys/mman.h>
    #include <errno.h>
    #include <string.h>
#elif WTREE_OS_WINDOWS
    #include <windows.h>
    #include <memoryapi.h>
#endif

/* ============================================================
 * Memory Access Advice (madvise)
 * ============================================================ */

WTREE_WARN_UNUSED
int wtree3_db_madvise(wtree3_db_t *db, unsigned int advice, gerror_t *error) {
    if (WTREE_UNLIKELY(!db)) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Database cannot be NULL");
        return WTREE3_EINVAL;
    }

    /* Get memory map info from LMDB */
    MDB_envinfo info;
    int rc = mdb_env_info(db->env, &info);
    if (WTREE_UNLIKELY(rc != 0)) {
        return translate_mdb_error(rc, error);
    }

    void *addr = info.me_mapaddr;
    size_t size = info.me_mapsize;

    /* On Windows, memory map may not be available until first transaction */
    if (WTREE_UNLIKELY(!addr)) {
        /* Not an error - just can't apply optimization yet */
        return WTREE3_OK;
    }

#if WTREE_OS_POSIX
    /* Map portable flags to POSIX madvise constants */
    int posix_advice;
    switch (advice) {
        case WTREE3_MADV_RANDOM:
            posix_advice = MADV_RANDOM;
            break;
        case WTREE3_MADV_SEQUENTIAL:
            posix_advice = MADV_SEQUENTIAL;
            break;
        case WTREE3_MADV_WILLNEED:
            posix_advice = MADV_WILLNEED;
            break;
        case WTREE3_MADV_DONTNEED:
            posix_advice = MADV_DONTNEED;
            break;
        case WTREE3_MADV_NORMAL:
        default:
            posix_advice = MADV_NORMAL;
            break;
    }

    rc = madvise(addr, size, posix_advice);
    if (WTREE_UNLIKELY(rc != 0)) {
        set_error(error, WTREE3_LIB, WTREE3_ERROR,
                 "madvise failed: %s", strerror(errno));
        return WTREE3_ERROR;
    }

#elif WTREE_OS_WINDOWS
    /* Windows has limited madvise equivalent */
    if (advice == WTREE3_MADV_WILLNEED) {
        /* Try PrefetchVirtualMemory (Windows 8+) */
        #if defined(PrefetchVirtualMemory)
        WIN32_MEMORY_RANGE_ENTRY range = {addr, size};
        if (!PrefetchVirtualMemory(GetCurrentProcess(), 1, &range, 0)) {
            /* Prefetch failed - not critical, just warn */
            set_error(error, WTREE3_LIB, WTREE3_ERROR,
                     "PrefetchVirtualMemory failed (code %lu)", GetLastError());
            return WTREE3_ERROR;
        }
        #else
        set_error(error, WTREE3_LIB, WTREE3_ERROR,
                 "Memory prefetch not available (requires Windows 8+)");
        return WTREE3_ERROR;
        #endif
    } else {
        /* Other advice types not supported on Windows */
        set_error(error, WTREE3_LIB, WTREE3_ERROR,
                 "Memory advice type not supported on Windows");
        return WTREE3_ERROR;
    }

#else
    set_error(error, WTREE3_LIB, WTREE3_ERROR,
             "Memory advice not supported on this platform");
    return WTREE3_ERROR;
#endif

    return WTREE3_OK;
}

/* ============================================================
 * Memory Locking (mlock/munlock)
 * ============================================================ */

WTREE_WARN_UNUSED
int wtree3_db_mlock(wtree3_db_t *db, unsigned int flags, gerror_t *error) {
    if (WTREE_UNLIKELY(!db)) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Database cannot be NULL");
        return WTREE3_EINVAL;
    }

    /* Get memory map info */
    MDB_envinfo info;
    int rc = mdb_env_info(db->env, &info);
    if (WTREE_UNLIKELY(rc != 0)) {
        return translate_mdb_error(rc, error);
    }

    void *addr = info.me_mapaddr;
    size_t size = info.me_mapsize;

    /* On Windows, memory map may not be available until first transaction */
    if (WTREE_UNLIKELY(!addr)) {
        /* Not an error - just can't apply optimization yet */
        return WTREE3_OK;
    }

#if WTREE_OS_POSIX
    /* Lock current pages */
    if (flags & WTREE3_MLOCK_CURRENT) {
        rc = mlock(addr, size);
        if (WTREE_UNLIKELY(rc != 0)) {
            set_error(error, WTREE3_LIB, WTREE3_ERROR,
                     "mlock failed: %s (may need CAP_IPC_LOCK)", strerror(errno));
            return WTREE3_ERROR;
        }
    }

    /* Lock future pages (use mlockall) */
    if (flags & WTREE3_MLOCK_FUTURE) {
        rc = mlockall(MCL_FUTURE);
        if (WTREE_UNLIKELY(rc != 0)) {
            set_error(error, WTREE3_LIB, WTREE3_ERROR,
                     "mlockall(MCL_FUTURE) failed: %s", strerror(errno));
            return WTREE3_ERROR;
        }
    }

#elif WTREE_OS_WINDOWS
    /* Windows VirtualLock */
    if (!VirtualLock(addr, size)) {
        DWORD err = GetLastError();
        set_error(error, WTREE3_LIB, WTREE3_ERROR,
                 "VirtualLock failed (code %lu)", err);
        return WTREE3_ERROR;
    }
    /* Note: Windows doesn't have separate "future pages" concept */

#else
    set_error(error, WTREE3_LIB, WTREE3_ERROR,
             "Memory locking not supported on this platform");
    return WTREE3_ERROR;
#endif

    return WTREE3_OK;
}

WTREE_WARN_UNUSED
int wtree3_db_munlock(wtree3_db_t *db, gerror_t *error) {
    if (WTREE_UNLIKELY(!db)) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Database cannot be NULL");
        return WTREE3_EINVAL;
    }

    /* Get memory map info */
    MDB_envinfo info;
    int rc = mdb_env_info(db->env, &info);
    if (WTREE_UNLIKELY(rc != 0)) {
        return translate_mdb_error(rc, error);
    }

    void *addr = info.me_mapaddr;
    size_t size = info.me_mapsize;

    /* On Windows, memory map may not be available until first transaction */
    if (WTREE_UNLIKELY(!addr)) {
        /* Not an error - just can't apply optimization yet */
        return WTREE3_OK;
    }

#if WTREE_OS_POSIX
    rc = munlock(addr, size);
    if (WTREE_UNLIKELY(rc != 0)) {
        set_error(error, WTREE3_LIB, WTREE3_ERROR,
                 "munlock failed: %s", strerror(errno));
        return WTREE3_ERROR;
    }

#elif WTREE_OS_WINDOWS
    if (!VirtualUnlock(addr, size)) {
        DWORD err = GetLastError();
        set_error(error, WTREE3_LIB, WTREE3_ERROR,
                 "VirtualUnlock failed (code %lu)", err);
        return WTREE3_ERROR;
    }

#else
    set_error(error, WTREE3_LIB, WTREE3_ERROR,
             "Memory unlocking not supported on this platform");
    return WTREE3_ERROR;
#endif

    return WTREE3_OK;
}

/* ============================================================
 * Memory Map Information
 * ============================================================ */

WTREE_WARN_UNUSED
int wtree3_db_get_mapinfo(wtree3_db_t *db, void **out_addr, size_t *out_size, gerror_t *error) {
    if (WTREE_UNLIKELY(!db)) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Database cannot be NULL");
        return WTREE3_EINVAL;
    }

    MDB_envinfo info;
    int rc = mdb_env_info(db->env, &info);
    if (WTREE_UNLIKELY(rc != 0)) {
        return translate_mdb_error(rc, error);
    }

    if (out_addr) *out_addr = info.me_mapaddr;
    if (out_size) *out_size = info.me_mapsize;

    return WTREE3_OK;
}

/* ============================================================
 * Page Prefetching
 * ============================================================ */

WTREE_WARN_UNUSED
int wtree3_db_prefetch(wtree3_db_t *db, size_t offset, size_t length, gerror_t *error) {
    if (WTREE_UNLIKELY(!db)) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Database cannot be NULL");
        return WTREE3_EINVAL;
    }

    /* Get memory map info */
    MDB_envinfo info;
    int rc = mdb_env_info(db->env, &info);
    if (WTREE_UNLIKELY(rc != 0)) {
        return translate_mdb_error(rc, error);
    }

    void *addr = info.me_mapaddr;
    size_t size = info.me_mapsize;

    /* Validate range first (can validate even without active map) */
    if (WTREE_UNLIKELY(offset >= size)) {
        set_error(error, WTREE3_LIB, WTREE3_EINVAL, "Offset beyond map size");
        return WTREE3_EINVAL;
    }

    /* On Windows, memory map may not be available until first transaction */
    if (WTREE_UNLIKELY(!addr)) {
        /* Not an error - just can't prefetch yet */
        return WTREE3_OK;
    }

    /* Clamp length to available data */
    if (offset + length > size) {
        length = size - offset;
    }

    void *prefetch_addr = (char*)addr + offset;

#if WTREE_OS_POSIX
    rc = madvise(prefetch_addr, length, MADV_WILLNEED);
    if (WTREE_UNLIKELY(rc != 0)) {
        set_error(error, WTREE3_LIB, WTREE3_ERROR,
                 "madvise(MADV_WILLNEED) failed: %s", strerror(errno));
        return WTREE3_ERROR;
    }

#elif WTREE_OS_WINDOWS
    #if defined(PrefetchVirtualMemory)
    WIN32_MEMORY_RANGE_ENTRY range = {prefetch_addr, length};
    if (!PrefetchVirtualMemory(GetCurrentProcess(), 1, &range, 0)) {
        set_error(error, WTREE3_LIB, WTREE3_ERROR,
                 "PrefetchVirtualMemory failed (code %lu)", GetLastError());
        return WTREE3_ERROR;
    }
    #else
    set_error(error, WTREE3_LIB, WTREE3_ERROR,
             "Memory prefetch not available (requires Windows 8+)");
    return WTREE3_ERROR;
    #endif

#else
    set_error(error, WTREE3_LIB, WTREE3_ERROR,
             "Memory prefetch not supported on this platform");
    return WTREE3_ERROR;
#endif

    return WTREE3_OK;
}
