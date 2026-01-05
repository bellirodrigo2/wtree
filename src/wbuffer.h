#ifndef WBUFFER_H
#define WBUFFER_H

#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct wbuffer wbuffer_t;

typedef int (*wbuffer_consumer_fn)(void *ctx, const void *entry);

wbuffer_t *wbuffer_create(size_t entry_size, size_t capacity);

int wbuffer_init(wbuffer_t *buf, size_t entry_size, size_t capacity);

void wbuffer_destroy(wbuffer_t *buf);

void wbuffer_free(wbuffer_t *buf);

bool wbuffer_is_full(const wbuffer_t *buf);
bool wbuffer_is_empty(const wbuffer_t *buf);

size_t wbuffer_capacity(const wbuffer_t *buf);
size_t wbuffer_entry_size(const wbuffer_t *buf);
size_t wbuffer_count(const wbuffer_t *buf);

int wbuffer_push(wbuffer_t *buf, const void *entry);

int wbuffer_consume(wbuffer_t *buf, void *ctx, wbuffer_consumer_fn consumer, void* error_ctx, wbuffer_consumer_fn error_handler);

/* Get pointer to entry at index i (for testing purposes) */
void* wbuffer_get_entry(wbuffer_t *buf, size_t index);

#ifdef __cplusplus
}
#endif

#endif /* WBUFFER_H */