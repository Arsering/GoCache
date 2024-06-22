aux_source_directory(. LIB_SRCS)
include_directories(${PROJECT_SOURCE_DIR}/include)
add_library(bufferpool SHARED ${LIB_SRCS})

# add_library(mimalloc INTERFACE)
# message(${PROJECT_SOURCE_DIR}/deps/mimalloc/include)
# target_include_directories(mimalloc INTERFACE ${PROJECT_SOURCE_DIR}/deps/mimalloc/include)
# target_sources(mimalloc INTERFACE ${PROJECT_SOURCE_DIR}/deps/mimalloc/src/static.c)
# # Link mimalloc library
# add_subdirectory(${PROJECT_SOURCE_DIR}/deps/mimalloc)

# target_link_libraries(bufferpool mimalloc)
link_libraries(bufferpool)
target_link_libraries(bufferpool uring)

# target_link_libraries(bufferpool ${PROJECT_SOURCE_DIR}/bin/mimalloc.so)



install(TARGETS bufferpool
        RUNTIME DESTINATION bin
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib)

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/../include
        DESTINATION include/flex/graphscope_bufferpool
        FILES_MATCHING
        PATTERN "*.h")

# install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/../include/buffer_pool_manager.h
#         DESTINATION include/flex/graphscope_bufferpool/include)
