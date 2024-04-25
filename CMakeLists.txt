cmake_minimum_required (VERSION 3.5)
set(CMAKE_C_COMPILER "/usr/bin/gcc")
set(CMAKE_CXX_COMPILER "/usr/bin/g++")
# set(CMAKE_C_FLAGS "-fxray-instrument")
set(CMAKE_C_FLAGS "-g")
set(CMAKE_CXX_STANDARD 17)

# file(GLOB SRC_FILE *.c) # 建立变量SRC_FILE为目录下.c文件列表

# set(CMAKE_BUILD_TYPE "Debug")

project(graphscope_bufferpool)

set(DEFAULT_BUILD_TYPE "Debug")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -fPIC -O0")
set(CMAKE_CXX_FLAGS_DEBUG "-g3 -O0")
set(CMAKE_LIBRARY_PATH "/usr/local/lib;/usr/lib/x86_64-linux-gnu;/usr/lib64")


# # find glog---------------------------------------------------------------------
# include("cmake/FindGlog.cmake")
# if (NOT GLOG_FOUND)
#     message(FATAL_ERROR "glog not found, please install the glog library")
# else ()
#     include_directories(SYSTEM ${GLOG_INCLUDE_DIRS})
# endif ()

# # find gflags-------------------------------------------------------------------
# include("cmake/FindGFlags.cmake")
# if (NOT GFLAGS_FOUND)
#     message(STATUS "gflags not found, build without gflags")
# else ()
#     include_directories(SYSTEM ${GFLAGS_INCLUDE_DIRS})
# endif ()

# # 添加要搜索的路径
# list(APPEND CMAKE_LIBRARY_PATH "/usr/local/lib/x86_64-linux-gnu")
# # 查找需要的库文件
# find_library(LIBRARY_NAME NAMES mimalloc PATHS ${CMAKE_LIBRARY_PATH})
# if(NOT LIBRARY_NAME)
#     message(FATAL_ERROR "Library not found")
# endif()
# # 添加您的项目并链接库
# # add_executable(YourExecutable main.cpp)

set(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/bin)

aux_source_directory(${PROJECT_SOURCE_DIR} ALL_SRCS) # 添加当前目录下所有的源文件
include_directories(${PROJECT_SOURCE_DIR}/include)

# add_library(mimalloc INTERFACE)
# target_include_directories(mimalloc INTERFACE ${PROJECT_SOURCE_DIR}/deps/mimalloc/include)
# target_sources(mimalloc INTERFACE ${PROJECT_SOURCE_DIR}/deps/mimalloc/src/static.c)

add_subdirectory(src)
add_subdirectory(tests)

# 包含静态库路径
link_directories(${LIBRARY_OUTPUT_PATH})
# 链接静态库
link_libraries(bufferpool)
link_libraries(tests)

add_executable(${PROJECT_NAME} ${ALL_SRCS})	# 指定生成目标，注意这里要用${ALL_SRCS}!!!
find_package(Boost REQUIRED COMPONENTS system filesystem fiber
             context program_options regex thread)

# find_library(EXAMPLE_LIB mimalloc PATHS /usr/local/lib/x86_64-linux-gnu/)
# if(NOT EXAMPLE_LIB)
#     message(FATAL_ERROR "Library not found")
# endif()

target_link_libraries(${PROJECT_NAME} ${Boost_LIBRARIES} uring)
# target_link_libraries(${PROJECT_NAME} ${LIBRARY_NAME})

# add_library(mimalloc INTERFACE)
# target_include_directories(mimalloc INTERFACE ${PROJECT_SOURCE_DIR}/deps/mimalloc/include)
# target_sources(mimalloc INTERFACE ${PROJECT_SOURCE_DIR}/deps/mimalloc/src/static.c)
# # Link mimalloc library
# target_link_libraries(${PROJECT_NAME} mimalloc)