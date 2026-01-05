#ifndef WT_SYNC_H
#define WT_SYNC_H

/**
 * @file wt_sync.h
 * @brief Portable synchronization primitives (mutex, condition variables, events)
 *
 * Provides a unified API for synchronization across platforms:
 * - Windows: CRITICAL_SECTION, CONDITION_VARIABLE, HANDLE events
 * - POSIX: pthread_mutex_t, pthread_cond_t
 * - Future: Android, iOS, RISC-V, embedded systems
 *
 * Design goals:
 * - Zero overhead abstraction (inlined where possible)
 * - Consistent semantics across platforms
 * - Easy migration to external libraries if needed
 */

#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================
 * Platform Detection
 * ============================================================ */

#ifdef _WIN32
    #include <windows.h>
#else
    #include <pthread.h>
#endif

/* ============================================================
 * Mutex (recursive mutex on all platforms)
 * ============================================================ */

typedef struct {
#ifdef _WIN32
    CRITICAL_SECTION cs;
#else
    pthread_mutex_t mutex;
#endif
} wt_mutex_t;

/**
 * @brief Initialize a mutex (recursive on all platforms)
 * @param mutex Pointer to mutex structure
 */
void wt_mutex_init(wt_mutex_t *mutex);

/**
 * @brief Destroy a mutex
 * @param mutex Pointer to mutex structure
 */
void wt_mutex_destroy(wt_mutex_t *mutex);

/**
 * @brief Lock a mutex (blocks until acquired)
 * @param mutex Pointer to mutex structure
 */
void wt_mutex_lock(wt_mutex_t *mutex);

/**
 * @brief Unlock a mutex
 * @param mutex Pointer to mutex structure
 */
void wt_mutex_unlock(wt_mutex_t *mutex);

/* ============================================================
 * Condition Variable (for wait/signal patterns)
 * ============================================================ */

typedef struct {
#ifdef _WIN32
    CONDITION_VARIABLE cv;
#else
    pthread_cond_t cond;
#endif
} wt_cond_t;

/**
 * @brief Initialize a condition variable
 * @param cond Pointer to condition variable structure
 */
void wt_cond_init(wt_cond_t *cond);

/**
 * @brief Destroy a condition variable
 * @param cond Pointer to condition variable structure
 */
void wt_cond_destroy(wt_cond_t *cond);

/**
 * @brief Wait on a condition variable (atomically unlocks mutex and waits)
 * @param cond Pointer to condition variable structure
 * @param mutex Pointer to locked mutex (must be locked by caller)
 *
 * @note Mutex is automatically re-locked when function returns
 */
void wt_cond_wait(wt_cond_t *cond, wt_mutex_t *mutex);

/**
 * @brief Signal one waiting thread
 * @param cond Pointer to condition variable structure
 */
void wt_cond_signal(wt_cond_t *cond);

/**
 * @brief Broadcast to all waiting threads
 * @param cond Pointer to condition variable structure
 */
void wt_cond_broadcast(wt_cond_t *cond);

/* ============================================================
 * Event (manual-reset events for cross-platform signaling)
 * ============================================================ */

typedef struct {
#ifdef _WIN32
    HANDLE event;
#else
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    bool signaled;
#endif
} wt_event_t;

/**
 * @brief Initialize a manual-reset event
 * @param event Pointer to event structure
 * @param initial_state Initial signal state (true = signaled, false = non-signaled)
 */
void wt_event_init(wt_event_t *event, bool initial_state);

/**
 * @brief Destroy an event
 * @param event Pointer to event structure
 */
void wt_event_destroy(wt_event_t *event);

/**
 * @brief Wait for event to become signaled (blocks until signaled)
 * @param event Pointer to event structure
 */
void wt_event_wait(wt_event_t *event);

/**
 * @brief Set event to signaled state (wakes all waiters)
 * @param event Pointer to event structure
 */
void wt_event_set(wt_event_t *event);

/**
 * @brief Reset event to non-signaled state
 * @param event Pointer to event structure
 */
void wt_event_reset(wt_event_t *event);

/* ============================================================
 * Thread (cross-platform thread creation and management)
 * ============================================================ */

#ifdef _WIN32
typedef HANDLE wt_thread_t;
typedef unsigned int (__stdcall *wt_thread_func_t)(void *arg);
#define WT_THREAD_FUNC(name) unsigned int __stdcall name(void *arg)
#define WT_THREAD_RETURN(val) return (unsigned int)(val)
#else
typedef pthread_t wt_thread_t;
typedef void *(*wt_thread_func_t)(void *arg);
#define WT_THREAD_FUNC(name) void *name(void *arg)
#define WT_THREAD_RETURN(val) return (void*)(val)
#endif

/**
 * @brief Create a new thread
 * @param thread Pointer to thread handle
 * @param func Thread function
 * @param arg Argument to pass to thread function
 * @return 0 on success, -1 on failure
 */
int wt_thread_create(wt_thread_t *thread, wt_thread_func_t func, void *arg);

/**
 * @brief Wait for thread to finish
 * @param thread Thread handle
 */
void wt_thread_join(wt_thread_t *thread);

#ifdef __cplusplus
}
#endif

#endif /* WT_SYNC_H */
