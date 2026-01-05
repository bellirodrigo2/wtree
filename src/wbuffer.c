
#include "wbuffer.h"
#include <stdlib.h>
#include <string.h>

struct wbuffer {
    size_t capacity;
    size_t count;
    size_t entry_size;
    void *entries;
};

wbuffer_t *wbuffer_create(size_t entry_size, size_t capacity) {
    wbuffer_t *buf = malloc(sizeof(wbuffer_t));
    if (!buf) return NULL;

    if (wbuffer_init(buf, entry_size, capacity) != 0) {
        free(buf);
        return NULL;
    }

    return buf;
}

int wbuffer_init(wbuffer_t *buf, size_t entry_size, size_t capacity) {
    buf->capacity = capacity;
    buf->count = 0;
    buf->entry_size = entry_size;
    buf->entries = malloc(entry_size * capacity);
    if (!buf->entries) {
        return -1; // Memory allocation failed
    }
    return 0; // Success
}

void wbuffer_destroy(wbuffer_t *buf) {
    if (!buf) return;
    free(buf->entries);
    buf->entries = NULL;
    buf->capacity = 0;
    buf->count = 0;
}

void wbuffer_free(wbuffer_t *buf) {
    if (!buf) return;
    wbuffer_destroy(buf);
    free(buf);
}

bool wbuffer_is_full(const wbuffer_t *buf) {
    return buf->count >= buf->capacity;
}

bool wbuffer_is_empty(const wbuffer_t *buf) {
    return buf->count == 0;
}

size_t wbuffer_capacity(const wbuffer_t *buf) {
    return buf->capacity;
}

size_t wbuffer_entry_size(const wbuffer_t *buf) {
    return buf->entry_size;
}

size_t wbuffer_count(const wbuffer_t *buf) {
    return buf->count;
}

int wbuffer_push(wbuffer_t *buf, const void *entry) {
    if (wbuffer_is_full(buf)) {
        return -1; // Buffer is full
    }
    void *dest = (char *)buf->entries + (buf->count * buf->entry_size);
    memcpy(dest, entry, buf->entry_size);
    buf->count++;
    return 0; // Success
}

int wbuffer_consume(wbuffer_t *buf, void *ctx, wbuffer_consumer_fn consumer, void* error_ctx, wbuffer_consumer_fn error_handler) {
    size_t failed_count = 0;
    void *temp_buffer = NULL;

    if (buf->count == 0) {
        return 0;
    }

    // Allocate temporary buffer to hold failed entries
    temp_buffer = malloc(buf->entry_size * buf->count);
    if (!temp_buffer) {
        return -1;
    }

    // Iterate over all entries
    for (size_t i = 0; i < buf->count; i++) {
        void *entry = (char *)buf->entries + (i * buf->entry_size);
        int result = consumer(ctx, entry);

        if (result != 0) {
            // Consumer failed, call error handler
            int error_result = error_handler(error_ctx, entry);

            // If error_handler returns non-zero, add entry back to buffer
            if (error_result != 0) {
                void *dest = (char *)temp_buffer + (failed_count * buf->entry_size);
                memcpy(dest, entry, buf->entry_size);
                failed_count++;
            }
        }
    }

    // Copy failed entries back to beginning of buffer
    if (failed_count > 0) {
        memcpy(buf->entries, temp_buffer, failed_count * buf->entry_size);
    }
    buf->count = failed_count;

    free(temp_buffer);
    return failed_count;
}

void* wbuffer_get_entry(wbuffer_t *buf, size_t index) {
    if (!buf || index >= buf->count) return NULL;
    return (char *)buf->entries + (index * buf->entry_size);
}