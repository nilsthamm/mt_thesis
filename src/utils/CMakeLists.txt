set(
        SOURCES
        print.hpp export.hpp)

add_library(utils SHARED ${SOURCES})
set_target_properties(utils PROPERTIES LINKER_LANGUAGE CXX)