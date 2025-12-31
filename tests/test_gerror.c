// test_gerror.c - Unit tests for gerror module (cmocka)

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <setjmp.h>
#include <cmocka.h>
#include <string.h>

#include "gerror.h"

static void test_error_initialization(void **state) {
    (void)state;
    gerror_t error = {0};
    
    assert_int_equal(0, error.code);
    assert_string_equal("", error.lib);
    assert_string_equal("", error.message);
}

static void test_set_error_basic(void **state) {
    (void)state;
    gerror_t error = {0};
    
    set_error(&error, "mylib", 42, "Test error: %d", 123);
    
    assert_int_equal(42, error.code);
    assert_string_equal("mylib", error.lib);
    assert_string_equal("Test error: 123", error.message);
}

static void test_set_error_null_lib(void **state) {
    (void)state;
    gerror_t error = {0};
    
    set_error(&error, NULL, 100, "Error without lib");
    
    assert_int_equal(100, error.code);
    assert_string_equal("unknown", error.lib);
    assert_string_equal("Error without lib", error.message);
}

static void test_set_error_null_error(void **state) {
    (void)state;
    // Should not crash when error is NULL
    set_error(NULL, "lib", 1, "test");
    // If we get here, it didn't crash
    assert_true(1);
}

static void test_error_message_basic(void **state) {
    (void)state;
    gerror_t error = {0};
    set_error(&error, "test", 1, "Simple message");
    
    const char *msg = error_message(&error);
    assert_string_equal("Simple message", msg);
}

static void test_error_message_empty(void **state) {
    (void)state;
    gerror_t error = {0};
    
    const char *msg = error_message(&error);
    assert_string_equal("No error", msg);
}

static void test_error_message_null(void **state) {
    (void)state;
    const char *msg = error_message(NULL);
    assert_string_equal("No error", msg);
}

static void test_error_message_ex_with_lib(void **state) {
    (void)state;
    gerror_t error = {0};
    char buffer[256];
    
    set_error(&error, "mylib", 42, "Something failed");
    
    const char *msg = error_message_ex(&error, buffer, sizeof(buffer));
    assert_string_equal("mylib: Something failed", msg);
    assert_string_equal("mylib: Something failed", buffer);
}

static void test_error_message_ex_no_lib(void **state) {
    (void)state;
    gerror_t error = {0};
    char buffer[256];
    
    error.code = 1;
    strcpy(error.message, "Just a message");
    error.lib[0] = '\0';
    
    const char *msg = error_message_ex(&error, buffer, sizeof(buffer));
    assert_string_equal("Just a message", msg);
}

static void test_error_message_ex_empty(void **state) {
    (void)state;
    gerror_t error = {0};
    char buffer[256];
    
    const char *msg = error_message_ex(&error, buffer, sizeof(buffer));
    assert_string_equal("No error", msg);
}

static void test_error_message_ex_invalid_buffer(void **state) {
    (void)state;
    gerror_t error = {0};

    const char *msg = error_message_ex(&error, NULL, 0);
    assert_string_equal("Invalid buffer", msg);
}

static void test_error_message_ex_null_error(void **state) {
    (void)state;
    char buffer[256];

    const char *msg = error_message_ex(NULL, buffer, sizeof(buffer));
    assert_string_equal("No error", msg);
    assert_string_equal("No error", buffer);
}

static void test_error_message_ex_null_buffer_size_zero(void **state) {
    (void)state;
    gerror_t error = {0};
    char buffer[256];

    set_error(&error, "lib", 1, "test");
    const char *msg = error_message_ex(&error, buffer, 0);
    assert_string_equal("Invalid buffer", msg);
}

static void test_error_clear_test(void **state) {
    (void)state;
    gerror_t error = {0};
    
    set_error(&error, "lib", 99, "Error message");
    assert_int_equal(99, error.code);
    
    error_clear(&error);
    
    assert_int_equal(0, error.code);
    assert_string_equal("", error.lib);
    assert_string_equal("", error.message);
}

static void test_error_clear_null(void **state) {
    (void)state;
    // Should not crash
    error_clear(NULL);
    assert_true(1);
}

static void test_error_overwrite(void **state) {
    (void)state;
    gerror_t error = {0};
    
    set_error(&error, "lib1", 1, "First error");
    assert_string_equal("First error", error.message);
    
    set_error(&error, "lib2", 2, "Second error");
    assert_string_equal("lib2", error.lib);
    assert_int_equal(2, error.code);
    assert_string_equal("Second error", error.message);
}

static void test_error_long_message(void **state) {
    (void)state;
    gerror_t error = {0};
    
    // Create a message that would overflow if not handled properly
    set_error(&error, "lib", 1, "%s", 
        "This is a very long message that goes on and on and on "
        "and should be truncated if it exceeds the buffer size "
        "which is 256 characters according to the structure definition "
        "so let's make it even longer to ensure proper truncation "
        "because we want to test boundary conditions properly "
        "and make sure nothing bad happens when limits are exceeded");
    
    // Just check it didn't crash and has some content
    assert_non_null(error.message);
    assert_true(strlen(error.message) > 0);
    assert_true(strlen(error.message) < 256);
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_error_initialization),
        cmocka_unit_test(test_set_error_basic),
        cmocka_unit_test(test_set_error_null_lib),
        cmocka_unit_test(test_set_error_null_error),
        cmocka_unit_test(test_error_message_basic),
        cmocka_unit_test(test_error_message_empty),
        cmocka_unit_test(test_error_message_null),
        cmocka_unit_test(test_error_message_ex_with_lib),
        cmocka_unit_test(test_error_message_ex_no_lib),
        cmocka_unit_test(test_error_message_ex_empty),
        cmocka_unit_test(test_error_message_ex_invalid_buffer),
        cmocka_unit_test(test_error_message_ex_null_error),
        cmocka_unit_test(test_error_message_ex_null_buffer_size_zero),
        cmocka_unit_test(test_error_clear_test),
        cmocka_unit_test(test_error_clear_null),
        cmocka_unit_test(test_error_overwrite),
        cmocka_unit_test(test_error_long_message),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
