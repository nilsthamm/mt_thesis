set(
        SOURCES_BASIC
        ${PROJECT_SOURCE_DIR}/src/basic/basic_node.hpp ${PROJECT_SOURCE_DIR}/src/basic/../utils/print.hpp ${PROJECT_SOURCE_DIR}/src/basic/reinsertable.hpp ${PROJECT_SOURCE_DIR}/src/basic/basic_rtree.hpp ${PROJECT_SOURCE_DIR}/src/basic/basic_parent_node.hpp ${PROJECT_SOURCE_DIR}/src/basic/basic_rtree_persister.hpp ${PROJECT_SOURCE_DIR}/src/basic/basic_rtree_recoverer.hpp
        PARENT_SCOPE)

#add_library(basic_rtree SHARED ${SOURCES_BASIC})
#target_include_directories(basic_rtree PRIVATE ${LOCAL_INCLUDE})
##target_compile_definitions(test_basic_rtree PUBLIC -DCOMPILED_NODE_SIZE=0) ##-DUSE_REINSERT)