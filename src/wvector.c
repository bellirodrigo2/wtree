/*
 * wvector.c - Generic dynamic array implementation
 */

#include "wvector.h"
#include <stdlib.h>
#include <string.h>

/* ============================================================
 * Constants
 * ============================================================ */

#define WVECTOR_DEFAULT_CAPACITY 8
#define WVECTOR_GROWTH_FACTOR 2

/* ============================================================
 * Internal Structure
 * ============================================================ */

struct wvector {
    void **data;                    /* Array of element pointers */
    size_t size;                    /* Current number of elements */
    size_t capacity;                /* Allocated capacity */
    wvector_cleanup_fn cleanup_fn;  /* Optional cleanup function */
};

/* ============================================================
 * Lifecycle
 * ============================================================ */

wvector_t* wvector_create(size_t initial_capacity, wvector_cleanup_fn cleanup_fn) {
    wvector_t *vec = malloc(sizeof(wvector_t));
    if (!vec) return NULL;

    if (initial_capacity == 0) {
        initial_capacity = WVECTOR_DEFAULT_CAPACITY;
    }

    vec->data = malloc(sizeof(void*) * initial_capacity);
    if (!vec->data) {
        free(vec);
        return NULL;
    }

    vec->size = 0;
    vec->capacity = initial_capacity;
    vec->cleanup_fn = cleanup_fn;

    return vec;
}

void wvector_destroy(wvector_t *vec) {
    if (!vec) return;

    /* Cleanup all elements if cleanup function provided */
    if (vec->cleanup_fn) {
        for (size_t i = 0; i < vec->size; i++) {
            if (vec->data[i]) {
                vec->cleanup_fn(vec->data[i]);
            }
        }
    }

    free(vec->data);
    free(vec);
}

/* ============================================================
 * Internal Helpers
 * ============================================================ */

static bool wvector_grow(wvector_t *vec) {
    size_t new_capacity = vec->capacity * WVECTOR_GROWTH_FACTOR;
    void **new_data = realloc(vec->data, sizeof(void*) * new_capacity);
    if (!new_data) return false;

    vec->data = new_data;
    vec->capacity = new_capacity;
    return true;
}

/* ============================================================
 * Basic Operations
 * ============================================================ */

bool wvector_push(wvector_t *vec, void *element) {
    if (!vec) return false;

    /* Grow if needed */
    if (vec->size >= vec->capacity) {
        if (!wvector_grow(vec)) {
            return false;
        }
    }

    vec->data[vec->size++] = element;
    return true;
}

void* wvector_pop(wvector_t *vec) {
    if (!vec || vec->size == 0) return NULL;

    return vec->data[--vec->size];
}

void* wvector_get(const wvector_t *vec, size_t index) {
    if (!vec || index >= vec->size) return NULL;

    return vec->data[index];
}

bool wvector_set(wvector_t *vec, size_t index, void *element) {
    if (!vec || index >= vec->size) return false;

    /* Cleanup old element if needed */
    if (vec->cleanup_fn && vec->data[index]) {
        vec->cleanup_fn(vec->data[index]);
    }

    vec->data[index] = element;
    return true;
}

size_t wvector_size(const wvector_t *vec) {
    return vec ? vec->size : 0;
}

bool wvector_is_empty(const wvector_t *vec) {
    return !vec || vec->size == 0;
}

void wvector_clear(wvector_t *vec) {
    if (!vec) return;

    /* Cleanup all elements if needed */
    if (vec->cleanup_fn) {
        for (size_t i = 0; i < vec->size; i++) {
            if (vec->data[i]) {
                vec->cleanup_fn(vec->data[i]);
            }
        }
    }

    vec->size = 0;
}

/* ============================================================
 * Search Operations
 * ============================================================ */

void* wvector_find(const wvector_t *vec, const void *key, wvector_cmp_fn cmp) {
    if (!vec || !cmp) return NULL;

    for (size_t i = 0; i < vec->size; i++) {
        if (cmp(vec->data[i], key) == 0) {
            return vec->data[i];
        }
    }

    return NULL;
}

bool wvector_find_index(const wvector_t *vec, const void *key, wvector_cmp_fn cmp, size_t *out_index) {
    if (!vec || !cmp) return false;

    for (size_t i = 0; i < vec->size; i++) {
        if (cmp(vec->data[i], key) == 0) {
            if (out_index) *out_index = i;
            return true;
        }
    }

    return false;
}

bool wvector_remove(wvector_t *vec, const void *key, wvector_cmp_fn cmp) {
    if (!vec || !cmp) return false;

    size_t index;
    if (!wvector_find_index(vec, key, cmp, &index)) {
        return false;
    }

    /* Cleanup element if needed */
    if (vec->cleanup_fn && vec->data[index]) {
        vec->cleanup_fn(vec->data[index]);
    }

    /* Shift elements down to fill gap */
    if (index < vec->size - 1) {
        memmove(&vec->data[index], &vec->data[index + 1],
                sizeof(void*) * (vec->size - index - 1));
    }

    vec->size--;
    return true;
}

/* ============================================================
 * Iteration
 * ============================================================ */

void wvector_foreach(const wvector_t *vec, wvector_foreach_fn fn, void *user_data) {
    if (!vec || !fn) return;

    for (size_t i = 0; i < vec->size; i++) {
        if (!fn(vec->data[i], i, user_data)) {
            break;  /* Stop if function returns false */
        }
    }
}
