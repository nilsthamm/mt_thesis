set(
        SOURCES_FPR
        ${PROJECT_SOURCE_DIR}/src/fpr-tree/fpr_node.hpp ${PROJECT_SOURCE_DIR}/src/fpr-tree/fprtree.hpp ${PROJECT_SOURCE_DIR}/src/fpr-tree/fpr_parent_node.hpp ${PROJECT_SOURCE_DIR}/src/fpr-tree/fpr_branch.hpp
        PARENT_SCOPE)

#add_library(basic_rtree SHARED ${SOURCES_BASIC})
#target_include_directories(basic_rtree PRIVATE ${LOCAL_INCLUDE})
##target_compile_definitions(test_basic_rtree PUBLIC -DCOMPILED_NODE_SIZE=0) ##-DUSE_REINSERT)