set(
        SOURCES_LIB
        ${PROJECT_SOURCE_DIR}/src/lib/branch.hpp ${PROJECT_SOURCE_DIR}/src/lib/per_tree_allocator.hpp ${PROJECT_SOURCE_DIR}/src/lib/rectangle.hpp
        PARENT_SCOPE
)

#add_library(rtree_lib SHARED ${SOURCES})