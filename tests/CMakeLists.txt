aux_source_directory(. LIB_SRCS)
include_directories(${PROJECT_SOURCE_DIR}/include)
# include_directories(${PROJECT_SOURCE_DIR}/src/cache_yz)
add_library(tests SHARED ${LIB_SRCS})

link_libraries(tests)


# install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/../include/buffer_pool_manager.h
#         DESTINATION include/flex/graphscope_bufferpool/include)
