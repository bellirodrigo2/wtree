/*
 * wt_queue.c - MPSC queue implementation (ring buffer, zero malloc/free per operation)
 */

#include "wt_queue.h"
#include "wt_atomic.h"
#include "wt_sync.h"
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

/* Ring buffer entry (pre-allocated array) */
typedef struct {
    const void *key;
    size_t key_len;
    const void *value;
    size_t val_len;
} wtq_entry_t;

/* Queue structure */
struct wtq {
    /* Ring buffer (pre-allocated) */
    wtq_entry_t *entries;  /* Array of capacity entries */
    size_t capacity;       /* Power of 2 */
    size_t mask;           /* capacity - 1, for fast modulo */

    /* Offsets (producer increments tail, consumer swaps entire buffer) */
    uint64_t head;                /* Consumer read position (only used in swap, protected by mutex) */
    wt_atomic_uint64_t tail;      /* Producer write position (atomic for MPSC) */

    /* Free callbacks */
    wtq_free_fn free_key;
    void *fk_arg;
    wtq_free_fn free_value;
    void *fv_arg;

    /* Trigger callbacks */
    wtq_on_full_fn on_full;
    void *fof_arg;
    wtq_on_batch_flush_fn on_batch_flush;
    void *bfl_arg;

    /* Synchronization primitives */
    wt_mutex_t lock;
    wt_event_t nonempty_event;
    wt_event_t empty_event;
    wt_cond_t nonempty_cond;
    wt_cond_t empty_cond;

    /* State (protected by mutex, no atomics needed for swap-only consumer) */
    uint64_t depth;               /* Number of items in queue */
    wt_atomic_bool_t flushed;     /* Shutdown flag (atomic for fast check) */
};

/* Helper: round up to next power of 2 */
static size_t next_power_of_2(size_t n) {
    if (n == 0) return 1;
    n--;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
#if SIZE_MAX > 0xFFFFFFFF
    n |= n >> 32;
#endif
    return n + 1;
}

wtq_t *wtq_create(size_t capacity,
                  wtq_free_fn free_key, void *fk_arg,
                  wtq_free_fn free_value, void *fv_arg,
                  wtq_on_full_fn on_full, void *fof_arg,
                  wtq_on_batch_flush_fn on_batch_flush, void *bfl_arg) {
    if (capacity == 0) return NULL;

    wtq_t *q = calloc(1, sizeof(wtq_t));
    if (!q) return NULL;

    /* Round capacity to next power of 2 for efficient ring buffer indexing */
    q->capacity = next_power_of_2(capacity);
    q->mask = q->capacity - 1;

    /* Allocate ring buffer (all entries at once, no per-operation malloc) */
    q->entries = calloc(q->capacity, sizeof(wtq_entry_t));
    if (!q->entries) {
        free(q);
        return NULL;
    }

    /* Store callbacks */
    q->free_key = free_key;
    q->fk_arg = fk_arg;
    q->free_value = free_value;
    q->fv_arg = fv_arg;
    q->on_full = on_full;
    q->fof_arg = fof_arg;
    q->on_batch_flush = on_batch_flush;
    q->bfl_arg = bfl_arg;

    /* Initialize synchronization primitives */
    wt_mutex_init(&q->lock);
    wt_event_init(&q->nonempty_event, false);  /* Initially non-signaled */
    wt_event_init(&q->empty_event, true);      /* Initially signaled (queue is empty) */
    wt_cond_init(&q->nonempty_cond);
    wt_cond_init(&q->empty_cond);

    /* Initialize state */
    q->head = 0;
    wt_atomic_init_uint64(&q->tail, 0);
    q->depth = 0;
    wt_atomic_init_bool(&q->flushed, false);

    return q;
}

void wtq_destroy(wtq_t *q) {
    if (!q) return;

    /* Clean up synchronization primitives */
    wt_event_destroy(&q->nonempty_event);
    wt_event_destroy(&q->empty_event);
    wt_cond_destroy(&q->nonempty_cond);
    wt_cond_destroy(&q->empty_cond);
    wt_mutex_destroy(&q->lock);

    /* Free ring buffer */
    free(q->entries);
    free(q);
}

bool wtq_enqueue(wtq_t *q,
                 const void *key, size_t key_len,
                 const void *value, size_t val_len) {
    if (!q || !key) return false;

    /* Check if flushed */
    if (wt_atomic_load_bool(&q->flushed)) {
        return false;
    }

    wt_mutex_lock(&q->lock);

    /* Check capacity (ring buffer full when depth == capacity) */
    if (q->depth >= q->capacity) {
        wt_mutex_unlock(&q->lock);
        /* Invoke on_full callback if configured */
        if (q->on_full) {
            q->on_full(q->fof_arg);
        }
        return false;
    }

    /* Get tail position and store entry in ring buffer */
    uint64_t tail_pos = wt_atomic_load_uint64(&q->tail);
    size_t index = tail_pos & q->mask;

    q->entries[index].key = key;
    q->entries[index].key_len = key_len;
    q->entries[index].value = value;
    q->entries[index].val_len = val_len;

    /* Advance tail and depth (both protected by mutex) */
    wt_atomic_fetch_add_uint64(&q->tail, 1);
    q->depth++;

    /* Signal nonempty */
    wt_event_set(&q->nonempty_event);
    wt_cond_signal(&q->nonempty_cond);

    wt_mutex_unlock(&q->lock);

    return true;
}

void wtq_release(wtq_t *q, void *key, void *value) {
    if (!q) return;

    if (key && q->free_key) {
        q->free_key(key, q->fk_arg);
    }
    if (value && q->free_value) {
        q->free_value(value, q->fv_arg);
    }
}

bool wtq_wait_nonempty(wtq_t *q) {
    if (!q) return false;

    /* Check if flushed */
    if (wt_atomic_load_bool(&q->flushed)) {
        return false;
    }

    /* Wait for nonempty condition */
    wt_mutex_lock(&q->lock);
    while (q->depth == 0 && !wt_atomic_load_bool(&q->flushed)) {
        wt_cond_wait(&q->nonempty_cond, &q->lock);
    }
    bool has_items = (q->depth > 0);
    wt_mutex_unlock(&q->lock);

    return has_items;
}

void wtq_flush(wtq_t *q) {
    if (!q) return;

    /* Set flushed flag */
    wt_atomic_store_bool(&q->flushed, true);

    /* Wake all waiters */
    wt_event_set(&q->nonempty_event);
    wt_event_set(&q->empty_event);
    wt_mutex_lock(&q->lock);
    wt_cond_broadcast(&q->nonempty_cond);
    wt_cond_broadcast(&q->empty_cond);
    wt_mutex_unlock(&q->lock);
}

void wtq_drain(wtq_t *q) {
    if (!q) return;

    /* Wait until depth becomes 0 */
    wt_mutex_lock(&q->lock);
    while (q->depth > 0) {
        wt_cond_wait(&q->empty_cond, &q->lock);
    }
    wt_mutex_unlock(&q->lock);
}

uint64_t wtq_depth(wtq_t *q) {
    if (!q) return 0;

    wt_mutex_lock(&q->lock);
    uint64_t depth = q->depth;
    wt_mutex_unlock(&q->lock);

    return depth;
}

wtq_buffer_t wtq_swap_buffer(wtq_t *q, size_t new_capacity) {
    wtq_buffer_t result = {0};

    if (!q) return result;

    /* If new_capacity == 0, use same capacity */
    if (new_capacity == 0) {
        new_capacity = q->capacity;
    } else {
        new_capacity = next_power_of_2(new_capacity);
    }

    /* Allocate new buffer */
    wtq_entry_t *new_entries = calloc(new_capacity, sizeof(wtq_entry_t));
    if (!new_entries) return result;

    wt_mutex_lock(&q->lock);

    /* Swap buffer - ONLY swap pointers! */
    result.entries = q->entries;
    result.capacity = q->capacity;
    result.count = q->depth;
    result.head_offset = q->head;

    /* Install new empty buffer */
    q->entries = new_entries;
    q->capacity = new_capacity;
    q->mask = new_capacity - 1;
    q->head = 0;
    wt_atomic_store_uint64(&q->tail, 0);
    q->depth = 0;

    /* Signal that queue is now empty */
    wt_event_set(&q->empty_event);
    wt_event_reset(&q->nonempty_event);
    wt_cond_broadcast(&q->empty_cond);

    wt_mutex_unlock(&q->lock);

    return result;
}

void wtq_buffer_free(wtq_t *q, wtq_buffer_t *buf) {
    if (!buf || !buf->entries) return;

    /* Free the buffer array (entries themselves are already released by consumer) */
    free(buf->entries);

    /* Clear the struct */
    buf->entries = NULL;
    buf->count = 0;
    buf->capacity = 0;
    buf->head_offset = 0;
}
