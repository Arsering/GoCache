cmake_minimum_required(VERSION 3.5)

add_library(cgraph
    cgraph.c
    # 如果有其他.cpp文件也需要添加
)

target_include_directories(cgraph
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
)

set_target_properties(cgraph PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
) 