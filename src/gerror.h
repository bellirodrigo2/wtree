#ifndef GERROR_H
#define GERROR_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct gerror_t {
    int code;
    char lib[64];
    char message[256];
} gerror_t;

/* Set error with printf-style formatting */
void set_error(gerror_t *error, const char *lib, int code, const char *format, ...);

/* Get error message (returns "No error" if none) */
const char* error_message(const gerror_t *error);

/* Get formatted error message: "lib: message" */
const char* error_message_ex(const gerror_t *error, char *buffer, size_t buffer_size);

/* Clear error state */
void error_clear(gerror_t *error);

#ifdef __cplusplus
}
#endif

#endif /* GERROR_H */
