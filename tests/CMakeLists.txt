# Download and build cmocka using FetchContent
include(FetchContent)

FetchContent_Declare(
    cmocka
    GIT_REPOSITORY https://git.cryptomilk.org/projects/cmocka.git
    GIT_TAG        cmocka-1.1.7
)

# Configure cmocka options
set(WITH_STATIC_LIB ON CACHE BOOL "Build static library" FORCE)
set(WITH_EXAMPLES OFF CACHE BOOL "Build examples" FORCE)
set(UNIT_TESTING OFF CACHE BOOL "Build cmocka unit tests" FORCE)

FetchContent_MakeAvailable(cmocka)

# Determine the correct cmocka library target
if(TARGET cmocka-static)
    set(CMOCKA_LIBRARY cmocka-static)
elseif(TARGET cmocka::cmocka)
    set(CMOCKA_LIBRARY cmocka::cmocka)
else()
    # On Windows with MSVC, the library might just be named 'cmocka'
    set(CMOCKA_LIBRARY cmocka)
endif()

# Test for gerror module
add_executable(test_gerror test_gerror.c)
target_include_directories(test_gerror PRIVATE ${cmocka_SOURCE_DIR}/include)
target_link_libraries(test_gerror PRIVATE wtree3 ${CMOCKA_LIBRARY})
add_test(NAME test_gerror COMMAND test_gerror)

# Test for wtree3 module
add_executable(test_wtree3 test_wtree3.c)
target_include_directories(test_wtree3 PRIVATE ${cmocka_SOURCE_DIR}/include)
target_link_libraries(test_wtree3 PRIVATE wtree3 ${CMOCKA_LIBRARY})
add_test(NAME test_wtree3 COMMAND test_wtree3)

# Test for wtree3 error cases and edge cases
add_executable(test_wtree3_errors test_wtree3_errors.c)
target_include_directories(test_wtree3_errors PRIVATE ${cmocka_SOURCE_DIR}/include)
target_link_libraries(test_wtree3_errors PRIVATE wtree3 ${CMOCKA_LIBRARY})
add_test(NAME test_wtree3_errors COMMAND test_wtree3_errors)

# Test for wtree3 advanced edge cases
add_executable(test_wtree3_advanced test_wtree3_advanced.c)
target_include_directories(test_wtree3_advanced PRIVATE ${cmocka_SOURCE_DIR}/include)
target_link_libraries(test_wtree3_advanced PRIVATE wtree3 ${CMOCKA_LIBRARY})
add_test(NAME test_wtree3_advanced COMMAND test_wtree3_advanced)

# Test for wtree3 upsert functionality
add_executable(test_wtree3_upsert test_wtree3_upsert.c)
target_include_directories(test_wtree3_upsert PRIVATE ${cmocka_SOURCE_DIR}/include)
target_link_libraries(test_wtree3_upsert PRIVATE wtree3 ${CMOCKA_LIBRARY})
add_test(NAME test_wtree3_upsert COMMAND test_wtree3_upsert)

# Test for LMDB error translation
add_executable(test_error_translation test_error_translation.c)
target_include_directories(test_error_translation PRIVATE ${cmocka_SOURCE_DIR}/include)
target_link_libraries(test_error_translation PRIVATE wtree3 ${CMOCKA_LIBRARY})
add_test(NAME test_error_translation COMMAND test_error_translation)

# Stress test for wtree3
add_executable(test_wtree3_stress test_wtree3_stress.c)
target_include_directories(test_wtree3_stress PRIVATE ${cmocka_SOURCE_DIR}/include)
target_link_libraries(test_wtree3_stress PRIVATE wtree3 ${CMOCKA_LIBRARY})
add_test(NAME test_wtree3_stress COMMAND test_wtree3_stress)

# Test for wtree3 Tier 1 operations
add_executable(test_wtree3_tier1 test_wtree3_tier1.c)
target_include_directories(test_wtree3_tier1 PRIVATE ${cmocka_SOURCE_DIR}/include)
target_link_libraries(test_wtree3_tier1 PRIVATE wtree3 ${CMOCKA_LIBRARY})
add_test(NAME test_wtree3_tier1 COMMAND test_wtree3_tier1)

# Test for wtree3 Tier 2 operations
add_executable(test_wtree3_tier2 test_wtree3_tier2.c)
target_include_directories(test_wtree3_tier2 PRIVATE ${cmocka_SOURCE_DIR}/include)
target_link_libraries(test_wtree3_tier2 PRIVATE wtree3 ${CMOCKA_LIBRARY})
add_test(NAME test_wtree3_tier2 COMMAND test_wtree3_tier2)

# Test for wtree3 coverage improvement (NULL checks and edge cases)
add_executable(test_wtree3_coverage test_wtree3_coverage.c)
target_include_directories(test_wtree3_coverage PRIVATE ${cmocka_SOURCE_DIR}/include)
target_link_libraries(test_wtree3_coverage PRIVATE wtree3 ${CMOCKA_LIBRARY})
add_test(NAME test_wtree3_coverage COMMAND test_wtree3_coverage)

# Test for wtree3 persistence and metadata operations
# DISABLED: test_wtree3_persistence - old API needs complete rewrite
# add_executable(test_wtree3_persistence test_wtree3_persistence.c)
# target_include_directories(test_wtree3_persistence PRIVATE ${cmocka_SOURCE_DIR}/include)
# target_link_libraries(test_wtree3_persistence PRIVATE wtree3 ${CMOCKA_LIBRARY})
# add_test(NAME test_wtree3_persistence COMMAND test_wtree3_persistence)

# Test for wtree3 critical bugs (Phase 1 bug fixes)
add_executable(test_wtree3_bugs test_wtree3_bugs.c)
target_include_directories(test_wtree3_bugs PRIVATE ${cmocka_SOURCE_DIR}/include)
target_link_libraries(test_wtree3_bugs PRIVATE wtree3 ${CMOCKA_LIBRARY})
add_test(NAME test_wtree3_bugs COMMAND test_wtree3_bugs)

# Test for wbuffer module
add_executable(test_wbuffer test_wbuffer.c)
target_include_directories(test_wbuffer PRIVATE ${cmocka_SOURCE_DIR}/include)
target_link_libraries(test_wbuffer PRIVATE wqueue ${CMOCKA_LIBRARY})
add_test(NAME test_wbuffer COMMAND test_wbuffer)

# Test for wqueue module (new simplified implementation)
add_executable(test_wqueue test_wqueue.c)
target_include_directories(test_wqueue PRIVATE ${cmocka_SOURCE_DIR}/include)
target_link_libraries(test_wqueue PRIVATE wqueue ${CMOCKA_LIBRARY})
add_test(NAME test_wqueue COMMAND test_wqueue)

# Test for wqueue MPSC multithreaded tests
add_executable(test_wqueue_mpsc test_wqueue_mpsc.c)
target_include_directories(test_wqueue_mpsc PRIVATE ${cmocka_SOURCE_DIR}/include)
target_link_libraries(test_wqueue_mpsc PRIVATE wqueue ${CMOCKA_LIBRARY})
add_test(NAME test_wqueue_mpsc COMMAND test_wqueue_mpsc)

# Platform-specific settings
if(WIN32)
    target_compile_definitions(test_gerror PRIVATE _CRT_SECURE_NO_WARNINGS)
    target_compile_definitions(test_wtree3 PRIVATE _CRT_SECURE_NO_WARNINGS)
    target_compile_definitions(test_wtree3_errors PRIVATE _CRT_SECURE_NO_WARNINGS)
    target_compile_definitions(test_wtree3_advanced PRIVATE _CRT_SECURE_NO_WARNINGS)
    target_compile_definitions(test_wtree3_upsert PRIVATE _CRT_SECURE_NO_WARNINGS)
    target_compile_definitions(test_error_translation PRIVATE _CRT_SECURE_NO_WARNINGS)
    target_compile_definitions(test_wtree3_stress PRIVATE _CRT_SECURE_NO_WARNINGS)
    target_compile_definitions(test_wtree3_tier1 PRIVATE _CRT_SECURE_NO_WARNINGS)
    target_compile_definitions(test_wtree3_tier2 PRIVATE _CRT_SECURE_NO_WARNINGS)
    target_compile_definitions(test_wtree3_coverage PRIVATE _CRT_SECURE_NO_WARNINGS)
    # target_compile_definitions(test_wtree3_persistence PRIVATE _CRT_SECURE_NO_WARNINGS)
    target_compile_definitions(test_wtree3_bugs PRIVATE _CRT_SECURE_NO_WARNINGS)
    target_compile_definitions(test_wbuffer PRIVATE _CRT_SECURE_NO_WARNINGS)
    target_compile_definitions(test_wqueue PRIVATE _CRT_SECURE_NO_WARNINGS)
    target_compile_definitions(test_wqueue_mpsc PRIVATE _CRT_SECURE_NO_WARNINGS)

    # Copy cmocka DLL to test directory on Windows
    add_custom_command(TARGET test_gerror POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            $<TARGET_FILE:cmocka>
            $<TARGET_FILE_DIR:test_gerror>
        COMMENT "Copying cmocka DLL to test directory"
    )

    add_custom_command(TARGET test_wtree3 POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            $<TARGET_FILE:cmocka>
            $<TARGET_FILE_DIR:test_wtree3>
        COMMENT "Copying cmocka DLL to test directory"
    )

    add_custom_command(TARGET test_wtree3_errors POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            $<TARGET_FILE:cmocka>
            $<TARGET_FILE_DIR:test_wtree3_errors>
        COMMENT "Copying cmocka DLL to test directory"
    )

    add_custom_command(TARGET test_wtree3_advanced POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            $<TARGET_FILE:cmocka>
            $<TARGET_FILE_DIR:test_wtree3_advanced>
        COMMENT "Copying cmocka DLL to test directory"
    )

    add_custom_command(TARGET test_wtree3_upsert POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            $<TARGET_FILE:cmocka>
            $<TARGET_FILE_DIR:test_wtree3_upsert>
        COMMENT "Copying cmocka DLL to test directory"
    )

    add_custom_command(TARGET test_error_translation POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            $<TARGET_FILE:cmocka>
            $<TARGET_FILE_DIR:test_error_translation>
        COMMENT "Copying cmocka DLL to test directory"
    )

    add_custom_command(TARGET test_wtree3_stress POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            $<TARGET_FILE:cmocka>
            $<TARGET_FILE_DIR:test_wtree3_stress>
        COMMENT "Copying cmocka DLL to test directory"
    )

    add_custom_command(TARGET test_wtree3_tier1 POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            $<TARGET_FILE:cmocka>
            $<TARGET_FILE_DIR:test_wtree3_tier1>
        COMMENT "Copying cmocka DLL to test directory"
    )

    add_custom_command(TARGET test_wtree3_tier2 POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            $<TARGET_FILE:cmocka>
            $<TARGET_FILE_DIR:test_wtree3_tier2>
        COMMENT "Copying cmocka DLL to test directory"
    )

    add_custom_command(TARGET test_wtree3_coverage POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            $<TARGET_FILE:cmocka>
            $<TARGET_FILE_DIR:test_wtree3_coverage>
        COMMENT "Copying cmocka DLL to test directory"
    )

    # DISABLED: test_wtree3_persistence
    # add_custom_command(TARGET test_wtree3_persistence POST_BUILD
    #    COMMAND ${CMAKE_COMMAND} -E copy_if_different
    #        $<TARGET_FILE:cmocka>
    #        $<TARGET_FILE_DIR:test_wtree3_persistence>
    #    COMMENT "Copying cmocka DLL to test directory"
    # )

    add_custom_command(TARGET test_wtree3_bugs POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            $<TARGET_FILE:cmocka>
            $<TARGET_FILE_DIR:test_wtree3_bugs>
        COMMENT "Copying cmocka DLL to test directory"
    )

    add_custom_command(TARGET test_wbuffer POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            $<TARGET_FILE:cmocka>
            $<TARGET_FILE_DIR:test_wbuffer>
        COMMENT "Copying cmocka DLL to test directory"
    )

    add_custom_command(TARGET test_wqueue POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            $<TARGET_FILE:cmocka>
            $<TARGET_FILE_DIR:test_wqueue>
        COMMENT "Copying cmocka DLL to test directory"
    )

    add_custom_command(TARGET test_wqueue_mpsc POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            $<TARGET_FILE:cmocka>
            $<TARGET_FILE_DIR:test_wqueue_mpsc>
        COMMENT "Copying cmocka DLL to test directory"
    )

    # For MinGW builds, copy required runtime DLLs
    if(MINGW)
        # Find MinGW bin directory
        get_filename_component(MINGW_BIN_DIR ${CMAKE_C_COMPILER} DIRECTORY)

        # Copy required MinGW DLLs
        set(MINGW_DLLS
            "${MINGW_BIN_DIR}/libwinpthread-1.dll"
            "${MINGW_BIN_DIR}/libgcc_s_seh-1.dll"
        )

        foreach(DLL ${MINGW_DLLS})
            if(EXISTS ${DLL})
                add_custom_command(TARGET test_gerror POST_BUILD
                    COMMAND ${CMAKE_COMMAND} -E copy_if_different
                        ${DLL}
                        $<TARGET_FILE_DIR:test_gerror>
                    COMMENT "Copying ${DLL}"
                )
                add_custom_command(TARGET test_wtree3 POST_BUILD
                    COMMAND ${CMAKE_COMMAND} -E copy_if_different
                        ${DLL}
                        $<TARGET_FILE_DIR:test_wtree3>
                    COMMENT "Copying ${DLL}"
                )
                add_custom_command(TARGET test_wtree3_errors POST_BUILD
                    COMMAND ${CMAKE_COMMAND} -E copy_if_different
                        ${DLL}
                        $<TARGET_FILE_DIR:test_wtree3_errors>
                    COMMENT "Copying ${DLL}"
                )
                add_custom_command(TARGET test_wtree3_advanced POST_BUILD
                    COMMAND ${CMAKE_COMMAND} -E copy_if_different
                        ${DLL}
                        $<TARGET_FILE_DIR:test_wtree3_advanced>
                    COMMENT "Copying ${DLL}"
                )
                add_custom_command(TARGET test_wtree3_upsert POST_BUILD
                    COMMAND ${CMAKE_COMMAND} -E copy_if_different
                        ${DLL}
                        $<TARGET_FILE_DIR:test_wtree3_upsert>
                    COMMENT "Copying ${DLL}"
                )
                add_custom_command(TARGET test_error_translation POST_BUILD
                    COMMAND ${CMAKE_COMMAND} -E copy_if_different
                        ${DLL}
                        $<TARGET_FILE_DIR:test_error_translation>
                    COMMENT "Copying ${DLL}"
                )
                add_custom_command(TARGET test_wtree3_stress POST_BUILD
                    COMMAND ${CMAKE_COMMAND} -E copy_if_different
                        ${DLL}
                        $<TARGET_FILE_DIR:test_wtree3_stress>
                    COMMENT "Copying ${DLL}"
                )
                add_custom_command(TARGET test_wtree3_tier1 POST_BUILD
                    COMMAND ${CMAKE_COMMAND} -E copy_if_different
                        ${DLL}
                        $<TARGET_FILE_DIR:test_wtree3_tier1>
                    COMMENT "Copying ${DLL}"
                )
                add_custom_command(TARGET test_wtree3_tier2 POST_BUILD
                    COMMAND ${CMAKE_COMMAND} -E copy_if_different
                        ${DLL}
                        $<TARGET_FILE_DIR:test_wtree3_tier2>
                    COMMENT "Copying ${DLL}"
                )
                add_custom_command(TARGET test_wtree3_coverage POST_BUILD
                    COMMAND ${CMAKE_COMMAND} -E copy_if_different
                        ${DLL}
                        $<TARGET_FILE_DIR:test_wtree3_coverage>
                    COMMENT "Copying ${DLL}"
                )
                # DISABLED: test_wtree3_persistence
                # add_custom_command(TARGET test_wtree3_persistence POST_BUILD
                #    COMMAND ${CMAKE_COMMAND} -E copy_if_different
                #        ${DLL}
                #        $<TARGET_FILE_DIR:test_wtree3_persistence>
                #    COMMENT "Copying ${DLL}"
                # )
                add_custom_command(TARGET test_wtree3_bugs POST_BUILD
                    COMMAND ${CMAKE_COMMAND} -E copy_if_different
                        ${DLL}
                        $<TARGET_FILE_DIR:test_wtree3_bugs>
                    COMMENT "Copying ${DLL}"
                )
                add_custom_command(TARGET test_wbuffer POST_BUILD
                    COMMAND ${CMAKE_COMMAND} -E copy_if_different
                        ${DLL}
                        $<TARGET_FILE_DIR:test_wbuffer>
                    COMMENT "Copying ${DLL}"
                )
                add_custom_command(TARGET test_wqueue POST_BUILD
                    COMMAND ${CMAKE_COMMAND} -E copy_if_different
                        ${DLL}
                        $<TARGET_FILE_DIR:test_wqueue>
                    COMMENT "Copying ${DLL}"
                )
                add_custom_command(TARGET test_wqueue_mpsc POST_BUILD
                    COMMAND ${CMAKE_COMMAND} -E copy_if_different
                        ${DLL}
                        $<TARGET_FILE_DIR:test_wqueue_mpsc>
                    COMMENT "Copying ${DLL}"
                )
            endif()
        endforeach()
    endif()
endif()
