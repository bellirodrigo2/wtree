/*
 * wt_atomic.h - Portable atomic operations wrapper
 *
 * Provides cross-platform atomic operations:
 * - C11 stdatomic.h on GCC/Clang/MinGW
 * - Windows Interlocked API on MSVC
 */

#ifndef WT_ATOMIC_H
#define WT_ATOMIC_H

#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================
 * Detect compiler and platform
 * ============================================================ */

#if defined(_MSC_VER) && !defined(__clang__)
    /* Microsoft Visual C++ */
    #define WT_ATOMIC_MSVC 1
    #include <windows.h>
#elif defined(__STDC_VERSION__) && __STDC_VERSION__ >= 201112L && !defined(__STDC_NO_ATOMICS__)
    /* C11 with stdatomic.h */
    #define WT_ATOMIC_C11 1
    #include <stdatomic.h>
#else
    /* Fallback: use compiler builtins (GCC/Clang) */
    #define WT_ATOMIC_BUILTIN 1
#endif

/* ============================================================
 * Atomic types
 * ============================================================ */

#if WT_ATOMIC_C11
    typedef atomic_uint_fast64_t wt_atomic_uint64_t;
    typedef atomic_bool wt_atomic_bool_t;
#elif WT_ATOMIC_MSVC
    typedef volatile LONG64 wt_atomic_uint64_t;
    typedef volatile LONG wt_atomic_bool_t;
#else
    typedef volatile uint64_t wt_atomic_uint64_t;
    typedef volatile int wt_atomic_bool_t;
#endif

/* ============================================================
 * Atomic operations
 * ============================================================ */

/* Initialize atomic variable */
static inline void wt_atomic_init_uint64(wt_atomic_uint64_t *var, uint64_t value) {
#if WT_ATOMIC_C11
    atomic_init(var, value);
#elif WT_ATOMIC_MSVC
    *var = (LONG64)value;
#else
    *var = value;
#endif
}

static inline void wt_atomic_init_bool(wt_atomic_bool_t *var, bool value) {
#if WT_ATOMIC_C11
    atomic_init(var, value);
#elif WT_ATOMIC_MSVC
    *var = value ? 1 : 0;
#else
    *var = value ? 1 : 0;
#endif
}

/* Load atomic variable */
static inline uint64_t wt_atomic_load_uint64(wt_atomic_uint64_t *var) {
#if WT_ATOMIC_C11
    return atomic_load(var);
#elif WT_ATOMIC_MSVC
    return (uint64_t)InterlockedOr64(var, 0);  /* Atomic read */
#else
    return __atomic_load_n(var, __ATOMIC_SEQ_CST);
#endif
}

static inline bool wt_atomic_load_bool(wt_atomic_bool_t *var) {
#if WT_ATOMIC_C11
    return atomic_load(var);
#elif WT_ATOMIC_MSVC
    return InterlockedOr((volatile LONG*)var, 0) != 0;
#else
    return __atomic_load_n(var, __ATOMIC_SEQ_CST) != 0;
#endif
}

/* Store atomic variable */
static inline void wt_atomic_store_uint64(wt_atomic_uint64_t *var, uint64_t value) {
#if WT_ATOMIC_C11
    atomic_store(var, value);
#elif WT_ATOMIC_MSVC
    InterlockedExchange64(var, (LONG64)value);
#else
    __atomic_store_n(var, value, __ATOMIC_SEQ_CST);
#endif
}

static inline void wt_atomic_store_bool(wt_atomic_bool_t *var, bool value) {
#if WT_ATOMIC_C11
    atomic_store(var, value);
#elif WT_ATOMIC_MSVC
    InterlockedExchange((volatile LONG*)var, value ? 1 : 0);
#else
    __atomic_store_n(var, value ? 1 : 0, __ATOMIC_SEQ_CST);
#endif
}

/* Fetch and add */
static inline uint64_t wt_atomic_fetch_add_uint64(wt_atomic_uint64_t *var, uint64_t value) {
#if WT_ATOMIC_C11
    return atomic_fetch_add(var, value);
#elif WT_ATOMIC_MSVC
    return (uint64_t)InterlockedExchangeAdd64(var, (LONG64)value);
#else
    return __atomic_fetch_add(var, value, __ATOMIC_SEQ_CST);
#endif
}

/* Fetch and sub */
static inline uint64_t wt_atomic_fetch_sub_uint64(wt_atomic_uint64_t *var, uint64_t value) {
#if WT_ATOMIC_C11
    return atomic_fetch_sub(var, value);
#elif WT_ATOMIC_MSVC
    return (uint64_t)InterlockedExchangeAdd64(var, -(LONG64)value);
#else
    return __atomic_fetch_sub(var, value, __ATOMIC_SEQ_CST);
#endif
}

#ifdef __cplusplus
}
#endif

#endif /* WT_ATOMIC_H */
