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

set(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/bin)

aux_source_directory(${PROJECT_SOURCE_DIR} ALL_SRCS) # 添加当前目录下所有的源文件
include_directories(${PROJECT_SOURCE_DIR}/include)

add_subdirectory(src)
add_subdirectory(tests)

# 包含静态库路径
link_directories(${LIBRARY_OUTPUT_PATH})
# 链接静态库
link_libraries(bufferpool)
link_libraries(tests)

add_executable(${PROJECT_NAME} ${ALL_SRCS})	# 指定生成目标，注意这里要用${ALL_SRCS}!!!
find_package(Boost REQUIRED COMPONENTS system filesystem fiber
             # required by folly
             context program_options regex thread)
target_link_libraries(${PROJECT_NAME} ${Boost_LIBRARIES} uring)

# find_package (GFlags REQUIRED)
# find_package(Glog REQUIRED) 
# target_link_libraries (${PROJECT_NAME}  ${GFLAGS_LIBRARIES} ${GLOG_LIBRARIES})
# find_package (glog REQUIRED)
# target_link_libraries (${PROJECT_NAME} glog)
# find_package (gflags REQUIRED)
# target_link_libraries (${PROJECT_NAME} gflags)

