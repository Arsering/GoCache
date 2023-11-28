aux_source_directory(. LIB_SRCS)
include_directories(${PROJECT_SOURCE_DIR}/include)
# include_directories(${PROJECT_SOURCE_DIR}/src/cache_yz)
add_library(bufferpool SHARED ${LIB_SRCS})

link_libraries(bufferpool)


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
