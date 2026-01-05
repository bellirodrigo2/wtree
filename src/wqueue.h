#ifndef WQUEUE_H
#define WQUEUE_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include "wbuffer.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @file wqueue.h
 * @brief Simplified MPSC queue implementation
 *
 * USAGE:
 *   1. Create a wqueue with entry size and capacity
 *   2. Push entry pointers (user transfers ownership)
 *   3. Start consumer thread that processes entries via callbacks
 *   4. Stop consumer thread and destroy queue when done
 *
 * EXAMPLE:
 *   // Free callback for entries
 *   void free_entry(void *entry_ptr, void *arg) {
 *       free(entry_ptr);
 *   }
 *
 *   // Create queue (stores pointers to entries)
 *   wqueue_t *q = wqueue_create(sizeof(void*), 100, free_entry, NULL);
 *
 *   // Start consumer thread
 *   wqueue_start_consumer_thread(q, ctx, consumer_fn, err_ctx, err_handler);
 *
 *   // Push entries (transfer ownership)
 *   my_entry_t *entry = malloc(sizeof(my_entry_t));
 *   wqueue_push(q, entry);
 *
 *   // In consumer_fn: entry parameter points to the stored pointer
 *   int consumer_fn(void *ctx, const void *entry) {
 *       void *entry_ptr = *(void**)entry;  // Extract the pointer
 *       my_entry_t *e = (my_entry_t*)entry_ptr;
 *       // Process e...
 *       free(e);  // Consumer owns the entry
 *       return 0;  // Success (entry discarded)
 *   }
 *
 *   // In error_handler: handle failed entries (must discard)
 *   int error_handler(void *ctx, const void *entry) {
 *       void *entry_ptr = *(void**)entry;
 *       my_entry_t *e = (my_entry_t*)entry_ptr;
 *       // Log error, then discard
 *       free(e);
 *       return 0;  // Always discard
 *   }
 *
 *   // Cleanup (stops consumer thread and frees all buffers)
 *   wqueue_destroy(q);
 */

typedef void (*wqueue_on_full_fn)(void *arg);
typedef void (*wqueue_free_fn)(void *entry_ptr, void *arg);

typedef struct wqueue wqueue_t;

/**
 * Create a new queue with internal buffer management
 * @param entry_size Size of each entry (typically sizeof(void*))
 * @param capacity Maximum number of entries in each buffer
 * @param free_fn Callback to free entries when queue is destroyed (can be NULL)
 * @param free_arg Argument passed to free_fn
 * @return New queue, or NULL on error
 */
wqueue_t *wqueue_create(size_t entry_size, size_t capacity, wqueue_free_fn free_fn, void *free_arg);

/**
 * Destroy queue and free all internal resources
 * Stops consumer thread if running
 */
void wqueue_destroy(wqueue_t *q);

/**
 * Push an entry to the queue (transfers ownership)
 * @param q Queue
 * @param entry Pointer to entry (will be copied)
 * @return true on success, false if queue is full
 */
bool wqueue_push(wqueue_t *q, void* entry);

/**
 * Get current queue depth
 * @return Number of items in queue
 */
uint64_t wqueue_depth(wqueue_t *q);

/**
 * Start consumer thread
 * @param q Queue
 * @param ctx Context for consumer callback
 * @param consumer Callback to process entries (return 0 on success, non-zero on error)
 * @param error_ctx Context for error handler
 * @param error_handler Callback for failed entries (must discard entries - no retry logic)
 * @return 0 on success, -1 on error
 */
int wqueue_start_consumer_thread(wqueue_t *q, void *ctx, wbuffer_consumer_fn consumer,
                                  void *error_ctx, wbuffer_consumer_fn error_handler);

/**
 * Stop consumer thread (waits for thread to finish)
 */
void wqueue_stop_consumer_thread(wqueue_t *q);

#ifdef __cplusplus
}
#endif

#endif /* WQUEUE_H */