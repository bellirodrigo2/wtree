/*
 * wvector.h - Generic dynamic array (vector) implementation
 *
 * A simple, reusable dynamic array that stores void* elements.
 * Suitable for small to medium collections (< 1000 items).
 */

#ifndef WVECTOR_H
#define WVECTOR_H

#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================
 * Data Types
 * ============================================================ */

typedef struct wvector wvector_t;

/* Comparison function for find operations
 * Returns: 0 if equal, non-zero otherwise
 */
typedef int (*wvector_cmp_fn)(const void *a, const void *b);

/* Cleanup function called when destroying vector or removing elements
 * Can be NULL if elements don't need cleanup
 */
typedef void (*wvector_cleanup_fn)(void *element);

/* ============================================================
 * Lifecycle
 * ============================================================ */

/**
 * Create a new vector with specified initial capacity
 *
 * @param initial_capacity Initial number of elements to allocate space for (0 = default)
 * @param cleanup_fn Optional function to call when freeing elements (can be NULL)
 * @return New vector or NULL on allocation failure
 */
wvector_t* wvector_create(size_t initial_capacity, wvector_cleanup_fn cleanup_fn);

/**
 * Destroy vector and optionally cleanup all elements
 *
 * @param vec Vector to destroy (can be NULL)
 */
void wvector_destroy(wvector_t *vec);

/* ============================================================
 * Basic Operations
 * ============================================================ */

/**
 * Append element to end of vector
 *
 * @param vec Vector
 * @param element Element to append (stored as void*)
 * @return true on success, false on allocation failure
 */
bool wvector_push(wvector_t *vec, void *element);

/**
 * Remove and return last element
 *
 * @param vec Vector
 * @return Last element or NULL if empty
 * Note: Cleanup function is NOT called - caller owns the returned pointer
 */
void* wvector_pop(wvector_t *vec);

/**
 * Get element at index without removing it
 *
 * @param vec Vector
 * @param index Index (0-based)
 * @return Element at index or NULL if index out of bounds
 */
void* wvector_get(const wvector_t *vec, size_t index);

/**
 * Set element at index (replaces existing element)
 *
 * @param vec Vector
 * @param index Index (0-based)
 * @param element New element
 * @return true on success, false if index out of bounds
 * Note: Cleanup function IS called on old element if set
 */
bool wvector_set(wvector_t *vec, size_t index, void *element);

/**
 * Get current number of elements
 *
 * @param vec Vector
 * @return Number of elements (0 if vec is NULL)
 */
size_t wvector_size(const wvector_t *vec);

/**
 * Check if vector is empty
 *
 * @param vec Vector
 * @return true if empty or NULL
 */
bool wvector_is_empty(const wvector_t *vec);

/**
 * Remove all elements from vector
 *
 * @param vec Vector
 * Note: Cleanup function IS called on all elements
 */
void wvector_clear(wvector_t *vec);

/* ============================================================
 * Search Operations
 * ============================================================ */

/**
 * Find first element matching key using comparison function
 *
 * @param vec Vector
 * @param key Key to search for
 * @param cmp Comparison function (returns 0 for match)
 * @return Matching element or NULL if not found
 */
void* wvector_find(const wvector_t *vec, const void *key, wvector_cmp_fn cmp);

/**
 * Find index of first element matching key
 *
 * @param vec Vector
 * @param key Key to search for
 * @param cmp Comparison function (returns 0 for match)
 * @param out_index Output parameter for index (can be NULL)
 * @return true if found, false otherwise
 */
bool wvector_find_index(const wvector_t *vec, const void *key, wvector_cmp_fn cmp, size_t *out_index);

/**
 * Remove first element matching key
 *
 * @param vec Vector
 * @param key Key to search for
 * @param cmp Comparison function (returns 0 for match)
 * @return true if element was found and removed, false otherwise
 * Note: Cleanup function IS called on removed element
 */
bool wvector_remove(wvector_t *vec, const void *key, wvector_cmp_fn cmp);

/* ============================================================
 * Iteration
 * ============================================================ */

/**
 * Iterator function for foreach operation
 *
 * @param element Current element
 * @param index Current index
 * @param user_data User-provided data
 * @return true to continue iteration, false to stop
 */
typedef bool (*wvector_foreach_fn)(void *element, size_t index, void *user_data);

/**
 * Iterate over all elements
 *
 * @param vec Vector
 * @param fn Function to call for each element
 * @param user_data User data passed to function
 */
void wvector_foreach(const wvector_t *vec, wvector_foreach_fn fn, void *user_data);

#ifdef __cplusplus
}
#endif

#endif /* WVECTOR_H */
