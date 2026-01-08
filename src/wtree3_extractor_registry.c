/*
 * wtree3_extractor_registry.c - Key extractor function registry implementation
 */

#include "wtree3_extractor_registry.h"
#include "wvector.h"
#include <stdlib.h>
#include <string.h>

/* ============================================================
 * Internal Types
 * ============================================================ */

typedef struct {
    uint64_t extractor_id;
    wtree3_index_key_fn key_fn;
} extractor_entry_t;

struct wtree3_extractor_registry {
    wvector_t *entries;  /* Vector of extractor_entry_t* */
};

/* ============================================================
 * Helper Functions
 * ============================================================ */

static void cleanup_entry(void *element) {
    free(element);
}

static int compare_entry_by_id(const void *entry_ptr, const void *id_ptr) {
    const extractor_entry_t *entry = (const extractor_entry_t *)entry_ptr;
    const uint64_t *id = (const uint64_t *)id_ptr;

    if (entry->extractor_id == *id) return 0;
    return (entry->extractor_id < *id) ? -1 : 1;
}

/* ============================================================
 * Lifecycle
 * ============================================================ */

wtree3_extractor_registry_t* wtree3_extractor_registry_create(void) {
    wtree3_extractor_registry_t *registry = malloc(sizeof(wtree3_extractor_registry_t));
    if (!registry) return NULL;

    registry->entries = wvector_create(8, cleanup_entry);
    if (!registry->entries) {
        free(registry);
        return NULL;
    }

    return registry;
}

void wtree3_extractor_registry_destroy(wtree3_extractor_registry_t *registry) {
    if (!registry) return;

    wvector_destroy(registry->entries);
    free(registry);
}

/* ============================================================
 * Operations
 * ============================================================ */

bool wtree3_extractor_registry_set(wtree3_extractor_registry_t *registry,
                                     uint64_t extractor_id,
                                     wtree3_index_key_fn key_fn) {
    if (!registry || !key_fn) return false;

    /* Check if already registered */
    if (wtree3_extractor_registry_has(registry, extractor_id)) {
        return false;  /* Don't allow overwriting */
    }

    /* Create new entry */
    extractor_entry_t *entry = malloc(sizeof(extractor_entry_t));
    if (!entry) return false;

    entry->extractor_id = extractor_id;
    entry->key_fn = key_fn;

    /* Add to vector */
    if (!wvector_push(registry->entries, entry)) {
        free(entry);
        return false;
    }

    return true;
}

wtree3_index_key_fn wtree3_extractor_registry_get(const wtree3_extractor_registry_t *registry,
                                                    uint64_t extractor_id) {
    if (!registry) return NULL;

    extractor_entry_t *entry = wvector_find(registry->entries, &extractor_id, compare_entry_by_id);
    return entry ? entry->key_fn : NULL;
}

bool wtree3_extractor_registry_has(const wtree3_extractor_registry_t *registry,
                                     uint64_t extractor_id) {
    if (!registry) return false;

    return wvector_find(registry->entries, &extractor_id, compare_entry_by_id) != NULL;
}

size_t wtree3_extractor_registry_count(const wtree3_extractor_registry_t *registry) {
    if (!registry) return 0;

    return wvector_size(registry->entries);
}
