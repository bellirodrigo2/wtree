/**
 * macros.h - Compiler hints and optimization macros
 *
 * Provides portable macros for:
 * - Branch prediction hints (likely/unlikely)
 * - Function attributes (pure, const, malloc, nonnull, etc.)
 * - Inline hints
 * - Alignment
 */

#ifndef MACROS_H
#define MACROS_H

/* ============================================================
 * Compiler Detection
 * ============================================================ */

#if defined(__GNUC__) || defined(__clang__)
#define WTREE_GCC_LIKE 1
#else
#define WTREE_GCC_LIKE 0
#endif

#if defined(_MSC_VER)
#define WTREE_MSVC 1
#else
#define WTREE_MSVC 0
#endif

/* ============================================================
 * OS Detection
 * ============================================================ */

#if defined(_WIN32) || defined(_WIN64) || defined(__WIN32__)
    #define WTREE_OS_WINDOWS 1
    #define WTREE_OS_POSIX 0
#elif defined(__linux__) || defined(__unix__) || defined(__APPLE__) || \
      defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__) || \
      defined(__sun)
    #define WTREE_OS_POSIX 1
    #define WTREE_OS_WINDOWS 0
#else
    /* Unknown platform - disable memory optimizations */
    #define WTREE_OS_POSIX 0
    #define WTREE_OS_WINDOWS 0
#endif

/* ============================================================
 * Branch Prediction Hints
 * ============================================================ */

#if WTREE_GCC_LIKE
/**
 * WTREE_LIKELY(x) - Hint that condition is likely true
 * Use for common/fast paths
 */
#define WTREE_LIKELY(x)   __builtin_expect(!!(x), 1)

/**
 * WTREE_UNLIKELY(x) - Hint that condition is unlikely true
 * Use for error paths, rare conditions
 */
#define WTREE_UNLIKELY(x) __builtin_expect(!!(x), 0)
#else
#define WTREE_LIKELY(x)   (x)
#define WTREE_UNLIKELY(x) (x)
#endif

/* ============================================================
 * Function Attributes
 * ============================================================ */

#if WTREE_GCC_LIKE

/**
 * WTREE_PURE - Function has no side effects, only reads memory
 * Result depends only on arguments and global state.
 * Example: strlen, strcmp
 */
#define WTREE_PURE __attribute__((pure))

/**
 * WTREE_CONST - Function is pure AND doesn't read global memory
 * Result depends ONLY on arguments. Most restrictive.
 * Example: abs, sin (without errno)
 */
#define WTREE_CONST __attribute__((const))

/**
 * WTREE_MALLOC - Function returns newly allocated memory
 * Returned pointer doesn't alias anything.
 */
#define WTREE_MALLOC __attribute__((malloc))

/**
 * WTREE_NONNULL(...) - Specified arguments must not be NULL
 * Arguments are 1-indexed.
 * Example: WTREE_NONNULL(1, 2) = args 1 and 2 must be non-null
 */
#define WTREE_NONNULL(...) __attribute__((nonnull(__VA_ARGS__)))

/**
 * WTREE_NONNULL_ALL - All pointer arguments must not be NULL
 */
#define WTREE_NONNULL_ALL __attribute__((nonnull))

/**
 * WTREE_RETURNS_NONNULL - Function never returns NULL
 */
#define WTREE_RETURNS_NONNULL __attribute__((returns_nonnull))

/**
 * WTREE_WARN_UNUSED - Warn if return value is ignored
 */
#define WTREE_WARN_UNUSED __attribute__((warn_unused_result))

/**
 * WTREE_HOT - Function is called frequently (optimize for speed)
 */
#define WTREE_HOT __attribute__((hot))

/**
 * WTREE_COLD - Function is rarely called (optimize for size)
 * Use for error handlers, initialization code
 */
#define WTREE_COLD __attribute__((cold))

/**
 * WTREE_FLATTEN - Inline all calls within this function
 * Aggressive inlining for critical paths
 */
#define WTREE_FLATTEN __attribute__((flatten))

/**
 * WTREE_ALWAYS_INLINE - Force inlining
 */
#define WTREE_ALWAYS_INLINE __attribute__((always_inline)) inline

/**
 * WTREE_NOINLINE - Never inline this function
 * Use for cold paths to reduce code size
 */
#define WTREE_NOINLINE __attribute__((noinline))

/**
 * WTREE_ALIGNED(n) - Align to n bytes
 */
#define WTREE_ALIGNED(n) __attribute__((aligned(n)))

/**
 * WTREE_PACKED - Remove padding from struct
 */
#define WTREE_PACKED __attribute__((packed))

/**
 * WTREE_PREFETCH(addr, rw, locality)
 * rw: 0 = read, 1 = write
 * locality: 0 = no temporal locality, 3 = high temporal locality
 */
#define WTREE_PREFETCH(addr, rw, locality) __builtin_prefetch(addr, rw, locality)

#else /* MSVC or other */

#define WTREE_PURE
#define WTREE_CONST
#define WTREE_MALLOC
#define WTREE_NONNULL(...)
#define WTREE_NONNULL_ALL
#define WTREE_RETURNS_NONNULL
#define WTREE_WARN_UNUSED
#define WTREE_HOT
#define WTREE_COLD
#define WTREE_FLATTEN
#define WTREE_ALWAYS_INLINE __forceinline
#define WTREE_NOINLINE __declspec(noinline)
#define WTREE_ALIGNED(n) __declspec(align(n))
#define WTREE_PACKED
#define WTREE_PREFETCH(addr, rw, locality)

#endif

/* ============================================================
 * Restrict Pointer (C99)
 * ============================================================ */

#if defined(__STDC_VERSION__) && __STDC_VERSION__ >= 199901L
#define WTREE_RESTRICT restrict
#elif WTREE_GCC_LIKE
#define WTREE_RESTRICT __restrict__
#elif WTREE_MSVC
#define WTREE_RESTRICT __restrict
#else
#define WTREE_RESTRICT
#endif

/* ============================================================
 * Unreachable Code
 * ============================================================ */

#if WTREE_GCC_LIKE
#define WTREE_UNREACHABLE() __builtin_unreachable()
#elif WTREE_MSVC
#define WTREE_UNREACHABLE() __assume(0)
#else
#define WTREE_UNREACHABLE() ((void)0)
#endif

/* ============================================================
 * Assume (Optimization Hint)
 * ============================================================ */

#if WTREE_GCC_LIKE && defined(__clang__)
#define WTREE_ASSUME(cond) __builtin_assume(cond)
#elif WTREE_MSVC
#define WTREE_ASSUME(cond) __assume(cond)
#else
#define WTREE_ASSUME(cond) ((void)0)
#endif

/* ============================================================
 * Common Patterns
 * ============================================================ */

/**
 * Error check pattern - for paths that should rarely fail
 */
#define WTREE_CHECK(cond) if (WTREE_UNLIKELY(!(cond)))

/**
 * Success check pattern - for paths that usually succeed
 */
#define WTREE_SUCCESS(cond) if (WTREE_LIKELY(cond))

#endif /* MACROS */
