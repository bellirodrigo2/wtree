/*
 * test_wtree3_memopt.c - Tests for wtree3 memory optimization API
 */

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <setjmp.h>
#include <cmocka.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
    #include <direct.h>
    #include <process.h>
    #define mkdir(path, mode) _mkdir(path)
    #define getpid() _getpid()
#else
    #include <sys/stat.h>
    #include <unistd.h>
#endif

#include "wtree3.h"

/* Test database path */
static char test_db_path[256];
static wtree3_db_t *test_db = NULL;

/* ============================================================
 * Test Fixtures
 * ============================================================ */

static int setup_db(void **state) {
    (void)state;

    /* Create temp directory */
#ifdef _WIN32
    snprintf(test_db_path, sizeof(test_db_path), "%s\\test_memopt_%d", getenv("TEMP"), getpid());
#else
    snprintf(test_db_path, sizeof(test_db_path), "/tmp/test_memopt_%d", getpid());
#endif
    mkdir(test_db_path, 0755);

    gerror_t error = {0};
    test_db = wtree3_db_open(test_db_path, 1024 * 1024, 32, WTREE3_VERSION(1, 0), 0, &error);
    if (!test_db) {
        fprintf(stderr, "Failed to create test database: %s\n", error.message);
        return -1;
    }

    return 0;
}

static int teardown_db(void **state) {
    (void)state;

    if (test_db) {
        wtree3_db_close(test_db);
        test_db = NULL;
    }

    return 0;
}

/* ============================================================
 * Memory Advice Tests (madvise)
 * ============================================================ */

static void test_madvise_random(void **state) {
    (void)state;

    gerror_t error = {0};
    int rc = wtree3_db_madvise(test_db, WTREE3_MADV_RANDOM, &error);

    /* Should succeed on POSIX, may fail on Windows */
#if defined(__linux__) || defined(__APPLE__) || defined(__unix__) || \
    defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
    assert_int_equal(rc, WTREE3_OK);
#else
    /* On Windows, RANDOM advice is not supported */
    (void)rc; /* May succeed or fail depending on platform */
#endif
}

static void test_madvise_sequential(void **state) {
    (void)state;

    gerror_t error = {0};
    int rc = wtree3_db_madvise(test_db, WTREE3_MADV_SEQUENTIAL, &error);

    /* Should succeed on POSIX, may fail on Windows */
#if defined(__linux__) || defined(__APPLE__) || defined(__unix__) || \
    defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
    assert_int_equal(rc, WTREE3_OK);
#endif
}

static void test_madvise_normal(void **state) {
    (void)state;

    gerror_t error = {0};
    int rc = wtree3_db_madvise(test_db, WTREE3_MADV_NORMAL, &error);

    /* Should succeed on POSIX */
#if defined(__linux__) || defined(__APPLE__) || defined(__unix__) || \
    defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
    assert_int_equal(rc, WTREE3_OK);
#endif
}

static void test_madvise_null_db(void **state) {
    (void)state;

    gerror_t error = {0};
    int rc = wtree3_db_madvise(NULL, WTREE3_MADV_RANDOM, &error);

    /* NULL database should return error */
    assert_int_equal(rc, WTREE3_EINVAL);
}

/* ============================================================
 * Memory Locking Tests (mlock/munlock)
 * ============================================================ */

static void test_mlock_munlock(void **state) {
    (void)state;

    gerror_t error = {0};

    /* Try to lock - may fail due to privilege requirements */
    int rc = wtree3_db_mlock(test_db, WTREE3_MLOCK_CURRENT, &error);

    if (rc == WTREE3_OK) {
        /* Lock succeeded - now unlock */
        rc = wtree3_db_munlock(test_db, &error);
        assert_int_equal(rc, WTREE3_OK);
    } else {
        /* Lock failed - expected if lacking privileges */
        /* Just verify it returned an error, not a crash */
        assert_true(rc != WTREE3_OK);
        printf("Note: mlock failed (may lack CAP_IPC_LOCK): %s\n", error.message);
    }
}

static void test_mlock_null_db(void **state) {
    (void)state;

    gerror_t error = {0};
    int rc = wtree3_db_mlock(NULL, WTREE3_MLOCK_CURRENT, &error);

    /* NULL database should return error */
    assert_int_equal(rc, WTREE3_EINVAL);
}

static void test_munlock_null_db(void **state) {
    (void)state;

    gerror_t error = {0};
    int rc = wtree3_db_munlock(NULL, &error);

    /* NULL database should return error */
    assert_int_equal(rc, WTREE3_EINVAL);
}

/* ============================================================
 * Memory Map Info Tests
 * ============================================================ */

static void test_get_mapinfo(void **state) {
    (void)state;

    gerror_t error = {0};
    void *addr = NULL;
    size_t size = 0;

    int rc = wtree3_db_get_mapinfo(test_db, &addr, &size, &error);

    /* Should always succeed on all platforms */
    assert_int_equal(rc, WTREE3_OK);

    /* On Windows, addr might be NULL if no transaction has been created yet */
    /* Just verify size is correct, which is always available */
    assert_true(size >= 1024 * 1024);  /* At least our requested size */
}

static void test_get_mapinfo_partial(void **state) {
    (void)state;

    gerror_t error = {0};
    void *addr = NULL;

    /* Test getting only address */
    int rc = wtree3_db_get_mapinfo(test_db, &addr, NULL, &error);
    assert_int_equal(rc, WTREE3_OK);
    /* addr may be NULL on Windows before first transaction - that's OK */

    /* Test getting only size */
    size_t size = 0;
    rc = wtree3_db_get_mapinfo(test_db, NULL, &size, &error);
    assert_int_equal(rc, WTREE3_OK);
    assert_true(size >= 1024 * 1024);
}

static void test_get_mapinfo_null_db(void **state) {
    (void)state;

    gerror_t error = {0};
    void *addr = NULL;
    size_t size = 0;

    int rc = wtree3_db_get_mapinfo(NULL, &addr, &size, &error);

    /* NULL database should return error */
    assert_int_equal(rc, WTREE3_EINVAL);
}

/* ============================================================
 * Prefetch Tests
 * ============================================================ */

static void test_prefetch_basic(void **state) {
    (void)state;

    gerror_t error = {0};

    /* Prefetch first 64KB */
    int rc = wtree3_db_prefetch(test_db, 0, 65536, &error);

    /* Should succeed on POSIX, may fail on older Windows */
#if defined(__linux__) || defined(__APPLE__) || defined(__unix__) || \
    defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
    assert_int_equal(rc, WTREE3_OK);
#endif
}

static void test_prefetch_range_clamp(void **state) {
    (void)state;

    gerror_t error = {0};

    /* Try to prefetch beyond map size - should clamp */
    int rc = wtree3_db_prefetch(test_db, 0, 1024 * 1024 * 1024, &error);

    /* Should succeed and clamp to actual size on POSIX */
#if defined(__linux__) || defined(__APPLE__) || defined(__unix__) || \
    defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
    assert_int_equal(rc, WTREE3_OK);
#endif
}

static void test_prefetch_invalid_offset(void **state) {
    (void)state;

    gerror_t error = {0};

    /* Offset beyond map size should fail */
    int rc = wtree3_db_prefetch(test_db, 1024 * 1024 * 1024, 1024, &error);

    /* Should return EINVAL - offset validation happens before map check */
    assert_int_equal(rc, WTREE3_EINVAL);
}

static void test_prefetch_null_db(void **state) {
    (void)state;

    gerror_t error = {0};
    int rc = wtree3_db_prefetch(NULL, 0, 65536, &error);

    /* NULL database should return error */
    assert_int_equal(rc, WTREE3_EINVAL);
}

/* ============================================================
 * Main Test Runner
 * ============================================================ */

int main(void) {
    const struct CMUnitTest tests[] = {
        /* Memory advice tests */
        cmocka_unit_test_setup_teardown(test_madvise_random, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_madvise_sequential, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_madvise_normal, setup_db, teardown_db),
        cmocka_unit_test(test_madvise_null_db),

        /* Memory locking tests */
        cmocka_unit_test_setup_teardown(test_mlock_munlock, setup_db, teardown_db),
        cmocka_unit_test(test_mlock_null_db),
        cmocka_unit_test(test_munlock_null_db),

        /* Memory map info tests */
        cmocka_unit_test_setup_teardown(test_get_mapinfo, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_get_mapinfo_partial, setup_db, teardown_db),
        cmocka_unit_test(test_get_mapinfo_null_db),

        /* Prefetch tests */
        cmocka_unit_test_setup_teardown(test_prefetch_basic, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_prefetch_range_clamp, setup_db, teardown_db),
        cmocka_unit_test_setup_teardown(test_prefetch_invalid_offset, setup_db, teardown_db),
        cmocka_unit_test(test_prefetch_null_db),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
