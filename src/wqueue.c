/*
 * wqueue.c - MPSC queue implementation (stores pointers to entries)
 */

#include "wqueue.h"
#include "wbuffer.h"
#include "wt_sync.h"
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

/* Queue structure */
struct wqueue {
    /* Double-buffering: queue owns both buffers */
    wbuffer_t* active_buffer;   /* Buffer for push operations */
    wbuffer_t* spare_buffer;    /* Spare buffer for consumer thread */

    /* Trigger callback */
    wqueue_on_full_fn on_full;
    void *fof_arg;

    /* Free callback */
    wqueue_free_fn free_fn;
    void *free_arg;

    /* Synchronization primitives */
    wt_mutex_t lock;
    wt_event_t nonempty_event;
    wt_event_t empty_event;
    wt_cond_t nonempty_cond;
    wt_cond_t empty_cond;

    /* State (protected by mutex) */
    bool consumer_running;

    /* Consumer thread */
    wt_thread_t consumer_thread;
    void *consumer_ctx;
    wbuffer_consumer_fn consumer_fn;
    void *error_ctx;
    wbuffer_consumer_fn error_handler;
};

wqueue_t *wqueue_create(size_t entry_size, size_t capacity, wqueue_free_fn free_fn, void *free_arg) {
    if (entry_size == 0 || capacity == 0) return NULL;

    wqueue_t *q = calloc(1, sizeof(wqueue_t));
    if (!q) return NULL;

    /* Create both buffers */
    q->active_buffer = wbuffer_create(entry_size, capacity);
    if (!q->active_buffer) {
        free(q);
        return NULL;
    }

    q->spare_buffer = wbuffer_create(entry_size, capacity);
    if (!q->spare_buffer) {
        wbuffer_free(q->active_buffer);
        free(q);
        return NULL;
    }

    /* Store callbacks */
    q->on_full = NULL;
    q->fof_arg = NULL;
    q->free_fn = free_fn;
    q->free_arg = free_arg;

    /* Initialize synchronization primitives */
    wt_mutex_init(&q->lock);
    wt_event_init(&q->nonempty_event, false);  /* Initially non-signaled */
    wt_event_init(&q->empty_event, true);      /* Initially signaled (queue is empty) */
    wt_cond_init(&q->nonempty_cond);
    wt_cond_init(&q->empty_cond);

    /* Initialize state */
    q->consumer_running = false;
    q->consumer_thread = 0;

    return q;
}

/* Helper to free all entries in a buffer */
static void free_buffer_entries(wqueue_t *q, wbuffer_t *buf) {
    if (!buf || !q->free_fn) return;

    size_t count = wbuffer_count(buf);
    for (size_t i = 0; i < count; i++) {
        void *entry_storage = wbuffer_get_entry(buf, i);
        if (entry_storage) {
            void *entry_ptr = *(void**)entry_storage;
            if (entry_ptr) {
                q->free_fn(entry_ptr, q->free_arg);
            }
        }
    }
}

void wqueue_destroy(wqueue_t *q) {
    if (!q) return;

    /* Stop consumer thread if running */
    wt_mutex_lock(&q->lock);
    bool is_running = q->consumer_running;
    wt_mutex_unlock(&q->lock);

    if (is_running) {
        wqueue_stop_consumer_thread(q);
    }

    /* Free any remaining entries in both buffers */
    free_buffer_entries(q, q->active_buffer);
    free_buffer_entries(q, q->spare_buffer);

    /* Clean up synchronization primitives */
    wt_event_destroy(&q->nonempty_event);
    wt_event_destroy(&q->empty_event);
    wt_cond_destroy(&q->nonempty_cond);
    wt_cond_destroy(&q->empty_cond);
    wt_mutex_destroy(&q->lock);

    /* Free both buffers */
    if (q->active_buffer) {
        wbuffer_free(q->active_buffer);
    }
    if (q->spare_buffer) {
        wbuffer_free(q->spare_buffer);
    }

    free(q);
}

bool wqueue_push(wqueue_t *q, void* entry){
    if (!q || !entry) return false;

    wt_mutex_lock(&q->lock);

    /* Check capacity */
    if (wbuffer_is_full(q->active_buffer)) {
        wt_mutex_unlock(&q->lock);
        /* Invoke on_full callback if configured */
        if (q->on_full) {
            q->on_full(q->fof_arg);
        }
        return false;
    }

    /* Push entry into buffer */
    int ret = wbuffer_push(q->active_buffer, &entry);
    if (ret == 0) {
        /* Signal nonempty */
        wt_event_set(&q->nonempty_event);
        wt_cond_signal(&q->nonempty_cond);
    }

    wt_mutex_unlock(&q->lock);

    return (ret == 0);
}

/* Internal function: wait for queue to become non-empty
 * Caller must NOT hold lock
 * Returns true if queue has items, false if consumer should stop
 */
static bool internal_wait_nonempty(wqueue_t *q) {
    wt_mutex_lock(&q->lock);

    /* Wait for nonempty condition or stop signal */
    while (wbuffer_is_empty(q->active_buffer) && q->consumer_running) {
        wt_cond_wait(&q->nonempty_cond, &q->lock);
    }

    bool has_items = !wbuffer_is_empty(q->active_buffer);
    bool should_continue = q->consumer_running;
    wt_mutex_unlock(&q->lock);

    return has_items && should_continue;
}


uint64_t wqueue_depth(wqueue_t *q) {
    if (!q) return 0;

    wt_mutex_lock(&q->lock);
    uint64_t depth = wbuffer_count(q->active_buffer);
    wt_mutex_unlock(&q->lock);

    return depth;
}

/* Internal function: swap active and spare buffers
 * Caller must hold lock
 * Returns pointer to the filled buffer (was active, now spare)
 */
static wbuffer_t* internal_swap_buffers(wqueue_t *q) {
    wbuffer_t* filled_buffer = q->active_buffer;
    q->active_buffer = q->spare_buffer;
    q->spare_buffer = filled_buffer;

    /* Signal that queue active buffer is now empty */
    wt_event_set(&q->empty_event);
    wt_event_reset(&q->nonempty_event);
    wt_cond_broadcast(&q->empty_cond);

    return filled_buffer;  /* This is now the spare, for processing */
}

/* ============================================================
 * Consumer Thread Implementation
 * ============================================================ */

static WT_THREAD_FUNC(consumer_thread_func) {
    wqueue_t *q = (wqueue_t*)arg;

    /* Main consumer loop */
    while (true) {
        /* Wait for items or stop signal */
        if (!internal_wait_nonempty(q)) {
            /* Consumer stopped */
            break;
        }

        /* Swap buffers internally: get filled buffer */
        wt_mutex_lock(&q->lock);
        wbuffer_t *processing_buffer = internal_swap_buffers(q);
        wt_mutex_unlock(&q->lock);

        /* Process entries outside of lock */
        if (processing_buffer && !wbuffer_is_empty(processing_buffer)) {
            wbuffer_consume(processing_buffer,
                            q->consumer_ctx,
                            q->consumer_fn,
                            q->error_ctx,
                            q->error_handler);

            /* By contract: wbuffer_consume processes ALL entries.
             * - Successful entries are removed
             * - Failed entries are passed to error_handler which must discard
             * - After consume, processing_buffer (spare) is ALWAYS empty
             */
        }

        /* Signal empty condition if queue is now empty */
        wt_mutex_lock(&q->lock);
        if (wbuffer_is_empty(q->active_buffer)) {
            wt_cond_broadcast(&q->empty_cond);
        }
        wt_mutex_unlock(&q->lock);
    }

    WT_THREAD_RETURN(0);
}

int wqueue_start_consumer_thread(wqueue_t *q, void *ctx, wbuffer_consumer_fn consumer,
                                  void *error_ctx, wbuffer_consumer_fn error_handler) {
    if (!q || !consumer || !error_handler) return -1;

    wt_mutex_lock(&q->lock);

    /* Check if already running */
    if (q->consumer_running) {
        wt_mutex_unlock(&q->lock);
        return -1;
    }

    /* Store consumer parameters */
    q->consumer_ctx = ctx;
    q->consumer_fn = consumer;
    q->error_ctx = error_ctx;
    q->error_handler = error_handler;

    /* Set running flag */
    q->consumer_running = true;

    wt_mutex_unlock(&q->lock);

    /* Create thread */
    if (wt_thread_create(&q->consumer_thread, (wt_thread_func_t)consumer_thread_func, q) != 0) {
        wt_mutex_lock(&q->lock);
        q->consumer_running = false;
        wt_mutex_unlock(&q->lock);
        return -1;
    }

    return 0;
}

void wqueue_stop_consumer_thread(wqueue_t *q) {
    if (!q) return;

    wt_mutex_lock(&q->lock);

    /* Check if running */
    if (!q->consumer_running) {
        wt_mutex_unlock(&q->lock);
        return;
    }

    /* Signal thread to stop */
    q->consumer_running = false;

    /* Wake up consumer if waiting */
    wt_cond_broadcast(&q->nonempty_cond);

    wt_mutex_unlock(&q->lock);

    /* Wait for thread to finish */
    wt_thread_join(&q->consumer_thread);
}