include(ExternalProject)

ExternalProject_Add(prometheus
        PREFIX "${CMAKE_CURRENT_BINARY_DIR}"
        SOURCE_DIR "${LOGDEVICE_DIR}/external/prometheus-cpp"
        DOWNLOAD_COMMAND ""
        CMAKE_ARGS
          -DCMAKE_POSITION_INDEPENDENT_CODE=ON
          -DBUILD_SHARED_LIBS=ON
          -DENABLE_PUSH=OFF
          -DENABLE_TESTING=OFF
          -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
          -DCMAKE_PREFIX_PATH=${LOGDEVICE_STAGING_DIR}/usr/local
        INSTALL_COMMAND $(MAKE) install DESTDIR=${LOGDEVICE_STAGING_DIR}
)

# Specify include dir
ExternalProject_Get_Property(prometheus SOURCE_DIR)
ExternalProject_Get_Property(prometheus BINARY_DIR)

set(PROMETHEUS_LIBRARIES
    ${BINARY_DIR}/lib/libprometheus-cpp-core.so
    ${BINARY_DIR}/lib/libprometheus-cpp-pull.so
)


set(PROMETHEUS_INCLUDE_DIR ${LOGDEVICE_STAGING_DIR}/usr/local/include)

message(STATUS "Prometheus Library: ${PROMETHEUS_LIBRARIES}")
message(STATUS "Prometheus Includes: ${PROMETHEUS_INCLUDE_DIR}")

mark_as_advanced(
    PROMETHEUS_LIBRARIES
    PROMETHEUS_INCLUDE_DIR
)