aux_source_directory(. LIB_SRCS)
include_directories(${PROJECT_SOURCE_DIR}/include)
add_library(bufferpool SHARED ${LIB_SRCS})

# find_package(uring)
# if(URING_FOUND)
#         message("ffffff")
# else()
#         message("ddddd")
# endif()
# target_include_directories(bufferpool SHARED ${URING_INCLUDE_DIR})
# link_directories(${URING_LIBRARY_DIRS})
# target_link_libraries(bufferpool SHARED ${URING_LIBRARIES})


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
