add_library(catch_test STATIC test_main.cpp)


add_executable(tests test_structure.cpp ${SOURCES_BASIC} ${SOURCES_PEAR} ${SOURCES_LIB}
        test_query.cpp
        test_delete.cpp
        test_recovery.cpp)
if (${BUILD_PMDK})
    add_dependencies(tests PMDK)
    target_link_libraries(tests pmem2)
else ()
    target_link_libraries(tests ${PMEM_LIBRARIES})
endif ()
target_link_libraries(tests catch_test utils)
target_compile_definitions(tests PUBLIC -DPROJECT_ROOT="${PROJECT_SOURCE_DIR}" -DTEST_BUILD)

add_executable(playground playground.cpp ${SOURCES_LIB})
if (${BUILD_PMDK})
    add_dependencies(playground PMDK)
    target_link_libraries(playground pmem2)
else ()
    target_link_libraries(playground ${PMEM_LIBRARIES})
endif ()
target_compile_definitions(playground PUBLIC -DPROJECT_ROOT="${PROJECT_SOURCE_DIR}" -DNDEBUG)


add_executable(playground_c playground.c)

add_executable(memkind_example memkind_example.cpp)
if (${BUILD_PMDK})
    add_dependencies(memkind_example PMDK)
    target_link_libraries(memkind_example pmem2)
else ()
    target_link_libraries(memkind_example ${PMEM_LIBRARIES})
endif ()

add_executable(pmdk_example pmdk_example.cpp)
if (${BUILD_PMDK})
    add_dependencies(pmdk_example PMDK)
    target_link_libraries(pmdk_example pmem2)
else ()
    target_link_libraries(pmdk_example ${PMEM_LIBRARIES})
endif ()



