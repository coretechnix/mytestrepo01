cmake_minimum_required(VERSION 3.18)
project(evtdistr-client_module-aveco_gpi)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_VERBOSE_MAKEFILE ON)
if(NOT CMAKE_BUILD_TYPE)
#       set(CMAKE_BUILD_TYPE Release)
       set(CMAKE_BUILD_TYPE Debug)
endif()

set(CMAKE_CXX_FLAGS_DEBUG "-g -Wall -Wextra -D_C_DEBUG_")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -s -DNDEBUG")

find_package(absl REQUIRED)
find_package(Protobuf REQUIRED)
include_directories(${PROTOBUF_INCLUDE_DIRS})

if (NOT DEFINED protobuf_ABSL_USED_TARGETS)
set(protobuf_ABSL_USED_TARGETS
    absl::absl_check
    absl::absl_log
    absl::algorithm
    absl::base
    absl::bind_front
    absl::bits
    absl::btree
    absl::cleanup
    absl::cord
    absl::core_headers
    absl::debugging
    absl::die_if_null
    absl::dynamic_annotations
    absl::flags
    absl::flat_hash_map
    absl::flat_hash_set
    absl::function_ref
    absl::hash
    absl::layout
    absl::log_initialize
    absl::log_severity
    absl::memory
    absl::node_hash_map
    absl::node_hash_set
    absl::optional
    absl::span
    absl::status
    absl::statusor
    absl::strings
    absl::synchronization
    absl::time
    absl::type_traits
    absl::utility
    absl::variant
)
endif()

include(CMakePrintHelpers)
cmake_print_variables(ProtobufIncludePath)

if(DEFINED P_BUILD_OUTDIR)
        set(CMAKE_OUTPUT_DIR ${P_BUILD_OUTDIR})
else()
        set(CMAKE_OUTPUT_DIR "${CMAKE_BINARY_DIR}/lib")
        set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
        set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
        set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
endif()

if(DEFINED ProtobufIncludePath)
        include_directories(${ProtobufIncludePath})
else()
        set (CMAKE_CXX_FLAGS "-I../../../../subsys-protos/build/ -L../../../../subsys-protos/build/lib/")
endif()

if (NOT BUILD_SHARED_LIBS)
        set(CMAKE_EXE_LINKER_FLAGS "-static")
        #add_library(Protos STATIC ${ProtoSources} ${ProtoHeaders})
endif()

set(EXEC_NAME "cedm-avecogpi.bin")

add_executable( ${EXEC_NAME}
		aveco-gpiserver.cpp
                main.cpp
                ${SRC}
)

find_library(NETCOMMCLIENT netcomm-client ../../lib)
target_include_directories( ${EXEC_NAME} PUBLIC "../../include/")

if (NOT BUILD_SHARED_LIBS)
        target_link_libraries( ${EXEC_NAME} -static)
        target_link_libraries( ${EXEC_NAME} -Wl,--whole-archive -lpthread -Wl,--no-whole-archive )
endif()

target_link_libraries( ${EXEC_NAME} ${PROTOBUF_LIBRARIES})
target_link_libraries( ${EXEC_NAME} ${protobuf_ABSL_USED_TARGETS})
target_link_libraries( ${EXEC_NAME} utf8_validity)
target_link_libraries( ${EXEC_NAME} utf8_range)
target_link_libraries( ${EXEC_NAME} subsys-protos)
target_link_libraries( ${EXEC_NAME} ${PROTOBUF_LIBRARIES})
target_link_libraries( ${EXEC_NAME} ${NETCOMMCLIENT})
target_link_libraries( ${EXEC_NAME} spdlog)
target_link_libraries( ${EXEC_NAME} uuid)
