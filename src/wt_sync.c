/*
 * wt_sync.c - Portable synchronization primitives implementation
 */

#include "wt_sync.h"

#ifdef _WIN32
#include <process.h>  /* For _beginthreadex */
#endif

/* ============================================================
 * Mutex Implementation
 * ============================================================ */

void wt_mutex_init(wt_mutex_t *mutex) {
    if (!mutex) return;

#ifdef _WIN32
    InitializeCriticalSection(&mutex->cs);
#else
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&mutex->mutex, &attr);
    pthread_mutexattr_destroy(&attr);
#endif
}

void wt_mutex_destroy(wt_mutex_t *mutex) {
    if (!mutex) return;

#ifdef _WIN32
    DeleteCriticalSection(&mutex->cs);
#else
    pthread_mutex_destroy(&mutex->mutex);
#endif
}

void wt_mutex_lock(wt_mutex_t *mutex) {
    if (!mutex) return;

#ifdef _WIN32
    EnterCriticalSection(&mutex->cs);
#else
    pthread_mutex_lock(&mutex->mutex);
#endif
}

void wt_mutex_unlock(wt_mutex_t *mutex) {
    if (!mutex) return;

#ifdef _WIN32
    LeaveCriticalSection(&mutex->cs);
#else
    pthread_mutex_unlock(&mutex->mutex);
#endif
}

/* ============================================================
 * Condition Variable Implementation
 * ============================================================ */

void wt_cond_init(wt_cond_t *cond) {
    if (!cond) return;

#ifdef _WIN32
    InitializeConditionVariable(&cond->cv);
#else
    pthread_cond_init(&cond->cond, NULL);
#endif
}

void wt_cond_destroy(wt_cond_t *cond) {
    if (!cond) return;

#ifdef _WIN32
    /* CONDITION_VARIABLE has no cleanup on Windows */
#else
    pthread_cond_destroy(&cond->cond);
#endif
}

void wt_cond_wait(wt_cond_t *cond, wt_mutex_t *mutex) {
    if (!cond || !mutex) return;

#ifdef _WIN32
    SleepConditionVariableCS(&cond->cv, &mutex->cs, INFINITE);
#else
    pthread_cond_wait(&cond->cond, &mutex->mutex);
#endif
}

void wt_cond_signal(wt_cond_t *cond) {
    if (!cond) return;

#ifdef _WIN32
    WakeConditionVariable(&cond->cv);
#else
    pthread_cond_signal(&cond->cond);
#endif
}

void wt_cond_broadcast(wt_cond_t *cond) {
    if (!cond) return;

#ifdef _WIN32
    WakeAllConditionVariable(&cond->cv);
#else
    pthread_cond_broadcast(&cond->cond);
#endif
}

/* ============================================================
 * Event Implementation (Manual-Reset Events)
 * ============================================================ */

void wt_event_init(wt_event_t *event, bool initial_state) {
    if (!event) return;

#ifdef _WIN32
    event->event = CreateEvent(NULL, TRUE, initial_state ? TRUE : FALSE, NULL);
#else
    pthread_mutex_init(&event->mutex, NULL);
    pthread_cond_init(&event->cond, NULL);
    event->signaled = initial_state;
#endif
}

void wt_event_destroy(wt_event_t *event) {
    if (!event) return;

#ifdef _WIN32
    if (event->event) {
        CloseHandle(event->event);
        event->event = NULL;
    }
#else
    pthread_mutex_destroy(&event->mutex);
    pthread_cond_destroy(&event->cond);
#endif
}

void wt_event_wait(wt_event_t *event) {
    if (!event) return;

#ifdef _WIN32
    if (event->event) {
        WaitForSingleObject(event->event, INFINITE);
    }
#else
    pthread_mutex_lock(&event->mutex);
    while (!event->signaled) {
        pthread_cond_wait(&event->cond, &event->mutex);
    }
    pthread_mutex_unlock(&event->mutex);
#endif
}

void wt_event_set(wt_event_t *event) {
    if (!event) return;

#ifdef _WIN32
    if (event->event) {
        SetEvent(event->event);
    }
#else
    pthread_mutex_lock(&event->mutex);
    event->signaled = true;
    pthread_cond_broadcast(&event->cond);
    pthread_mutex_unlock(&event->mutex);
#endif
}

void wt_event_reset(wt_event_t *event) {
    if (!event) return;

#ifdef _WIN32
    if (event->event) {
        ResetEvent(event->event);
    }
#else
    pthread_mutex_lock(&event->mutex);
    event->signaled = false;
    pthread_mutex_unlock(&event->mutex);
#endif
}

/* ============================================================
 * Thread Implementation
 * ============================================================ */

int wt_thread_create(wt_thread_t *thread, wt_thread_func_t func, void *arg) {
    if (!thread || !func) return -1;

#ifdef _WIN32
    *thread = (HANDLE)_beginthreadex(NULL, 0, func, arg, 0, NULL);
    return (*thread == NULL) ? -1 : 0;
#else
    return pthread_create(thread, NULL, func, arg);
#endif
}

void wt_thread_join(wt_thread_t *thread) {
    if (!thread) return;

#ifdef _WIN32
    if (*thread) {
        WaitForSingleObject(*thread, INFINITE);
        CloseHandle(*thread);
        *thread = NULL;
    }
#else
    if (*thread) {
        pthread_join(*thread, NULL);
        *thread = 0;
    }
#endif
}
