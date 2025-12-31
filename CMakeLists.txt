cmake_minimum_required(VERSION 3.14)
project(wtree3 C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# Coverage support
option(ENABLE_COVERAGE "Enable coverage reporting" OFF)

if(ENABLE_COVERAGE)
    # Coverage works with GCC/Clang (including MinGW on Windows)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang")
        # Add coverage flags for GCC/Clang
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} --coverage -fprofile-arcs -ftest-coverage")
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} --coverage")

        # Find coverage tools
        find_program(GCOV_PATH gcov)
        find_program(LCOV_PATH lcov)
        find_program(GENHTML_PATH genhtml)
        find_program(GCOVR_PATH gcovr)

        if(GCOVR_PATH)
            message(STATUS "Coverage reporting enabled (gcovr found)")

            # Add gcovr target (simpler, works on Windows with MSYS2)
            add_custom_target(coverage
                COMMAND ${CMAKE_CTEST_COMMAND} --verbose
                COMMAND ${GCOVR_PATH} --root ${CMAKE_SOURCE_DIR}
                    --exclude '${CMAKE_SOURCE_DIR}/external/.*'
                    --exclude '${CMAKE_SOURCE_DIR}/tests/.*'
                    --exclude '${CMAKE_SOURCE_DIR}/build.*/.*'
                    --html --html-details -o coverage.html
                COMMAND ${CMAKE_COMMAND} -E echo "Coverage report generated in coverage.html"
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
                COMMENT "Generating coverage report with gcovr"
            )

            # Add text coverage target for quick terminal output
            add_custom_target(coverage-text
                COMMAND ${CMAKE_CTEST_COMMAND} --verbose
                COMMAND ${GCOVR_PATH} --root ${CMAKE_SOURCE_DIR}
                    --exclude '${CMAKE_SOURCE_DIR}/external/.*'
                    --exclude '${CMAKE_SOURCE_DIR}/tests/.*'
                    --exclude '${CMAKE_SOURCE_DIR}/build.*/.*'
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
                COMMENT "Generating coverage report (text)"
            )
        elseif(LCOV_PATH AND GENHTML_PATH)
            message(STATUS "Coverage reporting enabled (lcov found)")

            # Add lcov target (traditional method)
            add_custom_target(coverage
                COMMAND ${LCOV_PATH} --directory . --zerocounters
                COMMAND ${CMAKE_CTEST_COMMAND} --verbose
                COMMAND ${LCOV_PATH} --directory . --capture --output-file coverage.info
                COMMAND ${LCOV_PATH} --remove coverage.info '/usr/*' '*/external/*' '*/tests/*' --output-file coverage.info.cleaned
                COMMAND ${GENHTML_PATH} -o coverage_html coverage.info.cleaned
                COMMAND ${CMAKE_COMMAND} -E echo "Coverage report generated in coverage_html/index.html"
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
                COMMENT "Generating coverage report with lcov"
            )
        else()
            message(WARNING "Neither gcovr nor lcov/genhtml found - coverage target will not be available")
            message(WARNING "Install with: pacman -S mingw-w64-x86_64-python-gcovr (or lcov)")
        endif()
    else()
        message(WARNING "Coverage is only supported with GCC or Clang compilers")
        message(WARNING "Current compiler: ${CMAKE_C_COMPILER_ID}")
    endif()
endif()

# Build LMDB from submodule
add_library(lmdb STATIC
    external/lmdb/libraries/liblmdb/mdb.c
    external/lmdb/libraries/liblmdb/midl.c
)

target_include_directories(lmdb PUBLIC
    external/lmdb/libraries/liblmdb
)

# Platform-specific settings for LMDB
if(WIN32)
    target_compile_definitions(lmdb PRIVATE _CRT_SECURE_NO_WARNINGS)
endif()

# Build wtree3 library
add_library(wtree3 STATIC
    src/gerror.c
    src/wtree3.c
)

target_include_directories(wtree3 PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR}/external/lmdb/libraries/liblmdb
)

target_link_libraries(wtree3 PUBLIC lmdb)

# Platform-specific libraries
if(WIN32)
    target_compile_definitions(wtree3 PRIVATE _CRT_SECURE_NO_WARNINGS)
else()
    target_link_libraries(wtree3 PUBLIC pthread)
endif()

# Enable tests
enable_testing()
add_subdirectory(tests)
