/*
 * wtree3_extractor_registry.h - Key extractor function registry
 *
 * Maps extractor IDs (version + flags) to key extraction functions.
 * Maintained by library, transparent to users.
 */

#ifndef WTREE3_EXTRACTOR_REGISTRY_H
#define WTREE3_EXTRACTOR_REGISTRY_H

#include "wtree3.h"
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================
 * Types
 * ============================================================ */

typedef struct wtree3_extractor_registry wtree3_extractor_registry_t;

/* ============================================================
 * Lifecycle
 * ============================================================ */

/**
 * Create a new extractor registry
 *
 * @return New registry or NULL on allocation failure
 */
wtree3_extractor_registry_t* wtree3_extractor_registry_create(void);

/**
 * Destroy registry and free all resources
 *
 * @param registry Registry to destroy (can be NULL)
 */
void wtree3_extractor_registry_destroy(wtree3_extractor_registry_t *registry);

/* ============================================================
 * Operations
 * ============================================================ */

/**
 * Register a key extractor function for a given ID
 *
 * @param registry Registry
 * @param extractor_id Extractor ID (version + flags combination)
 * @param key_fn Key extraction function
 * @return true on success, false on allocation failure or if ID already registered
 */
bool wtree3_extractor_registry_set(wtree3_extractor_registry_t *registry,
                                     uint64_t extractor_id,
                                     wtree3_index_key_fn key_fn);

/**
 * Lookup key extractor function by ID
 *
 * @param registry Registry
 * @param extractor_id Extractor ID to look up
 * @return Key extraction function or NULL if not found
 */
wtree3_index_key_fn wtree3_extractor_registry_get(const wtree3_extractor_registry_t *registry,
                                                    uint64_t extractor_id);

/**
 * Check if an extractor ID is registered
 *
 * @param registry Registry
 * @param extractor_id Extractor ID to check
 * @return true if registered, false otherwise
 */
bool wtree3_extractor_registry_has(const wtree3_extractor_registry_t *registry,
                                     uint64_t extractor_id);

/**
 * Get number of registered extractors
 *
 * @param registry Registry
 * @return Number of registered extractors
 */
size_t wtree3_extractor_registry_count(const wtree3_extractor_registry_t *registry);

#ifdef __cplusplus
}
#endif

#endif /* WTREE3_EXTRACTOR_REGISTRY_H */
