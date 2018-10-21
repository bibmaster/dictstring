project(global_strdict CXX C)
cmake_minimum_required(VERSION 3.5.0)

if(UNIX)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -Wall -Wextra -Werror")
else()
add_compile_options("/std:c++latest")
endif()

add_library(shared_string STATIC
  ./shared_string.cpp
  ./shared_string.hpp)
target_include_directories(shared_string PUBLIC .)

find_package(Threads REQUIRED)
add_executable(shared_string_test ./shared_string_test.cpp)
target_link_libraries(shared_string_test shared_string Threads::Threads)


