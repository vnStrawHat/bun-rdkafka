# StaticDeps.cmake — configure-time build of static OpenSSL/zlib/zstd/lz4
# (BRK_STATIC_DEPS=ON, the CI release configuration; design §9.2, ADR in plan M7).
#
# Why static: the prebuilt libbunrdkafka.so is downloaded onto arbitrary glibc
# distros. Dynamic deps are not portable across them (EL8 links OpenSSL 1.1
# while modern distros only ship libssl.so.3, sonames differ, etc.), so the
# release binary may depend on nothing beyond the glibc family
# (libc/libm/libpthread/libdl/librt). Verified by the ldd gate in the CI
# `verify-native` job.
#
# Why configure-time execute_process (not ExternalProject): librdkafka's own
# CMake runs find_package(OpenSSL/ZLIB/ZSTD/LZ4) at *configure* time, so the
# static libraries must already exist on disk before the librdkafka
# subdirectory is configured. Everything lives under ${CMAKE_BINARY_DIR}/_deps
# so the CI FetchContent cache (keyed on this file via native/CMakeLists.txt)
# carries the built deps between runs; on a cache hit this whole file reduces
# to a few EXISTS checks.
#
# Not supported on Windows (vcpkg provides static deps there via the
# x64-windows-static-md triplet) — guarded by the caller.

include(ProcessorCount)
ProcessorCount(BRK_NPROC)
if(BRK_NPROC EQUAL 0 OR BRK_NPROC GREATER 4)
  set(BRK_NPROC 4)
endif()

set(BRK_DEPS_ROOT "${CMAKE_BINARY_DIR}/_deps/brk-static")
set(BRK_DEPS_SRC "${BRK_DEPS_ROOT}/src")
set(BRK_DEPS_BUILD "${BRK_DEPS_ROOT}/build")
set(BRK_DEPS_PREFIX "${BRK_DEPS_ROOT}/install")
set(BRK_DEPS_LOG "${BRK_DEPS_ROOT}/logs")
file(MAKE_DIRECTORY "${BRK_DEPS_SRC}" "${BRK_DEPS_BUILD}" "${BRK_DEPS_PREFIX}" "${BRK_DEPS_LOG}")

# Pinned versions + SHA-256 of the release tarballs (verified downloads).
set(BRK_ZLIB_VERSION 1.3.1)
set(BRK_ZLIB_SHA256 9a93b2b7dfdac77ceba5a558a580e74667dd6fede4585b91eefb60f03b72df23)
set(BRK_ZSTD_VERSION 1.5.6)
set(BRK_ZSTD_SHA256 8c29e06cf42aacc1eafc4077ae2ec6c6fcb96a626157e0593d5e82a34fd403c1)
set(BRK_LZ4_VERSION 1.10.0)
set(BRK_LZ4_SHA256 537512904744b35e232912055ccf8ec66d768639ff3abe5788d90d792ec5f48b)
set(BRK_OPENSSL_VERSION 3.0.16) # 3.0 LTS
set(BRK_OPENSSL_SHA256 57e03c50feab5d31b152af2b764f10379aecd8ee92f16c985983ce4a99f7ef86)

# Downloads + extracts one tarball into ${BRK_DEPS_SRC}/<dirname>. No-op when
# the extracted directory already exists (cache hit).
function(brk_fetch_dep dirname url sha256)
  if(EXISTS "${BRK_DEPS_SRC}/${dirname}")
    return()
  endif()
  set(tarball "${BRK_DEPS_SRC}/${dirname}.tar.gz")
  message(STATUS "static-deps: downloading ${dirname}")
  file(DOWNLOAD "${url}" "${tarball}" EXPECTED_HASH SHA256=${sha256} STATUS dl_status)
  list(GET dl_status 0 dl_code)
  if(NOT dl_code EQUAL 0)
    message(FATAL_ERROR "static-deps: download failed for ${url}: ${dl_status}")
  endif()
  file(ARCHIVE_EXTRACT INPUT "${tarball}" DESTINATION "${BRK_DEPS_SRC}")
  file(REMOVE "${tarball}")
endfunction()

# Runs one build step, teeing output to a log file; fails the configure loudly.
function(brk_run_step logname workdir)
  execute_process(
    COMMAND ${ARGN}
    WORKING_DIRECTORY "${workdir}"
    OUTPUT_FILE "${BRK_DEPS_LOG}/${logname}.log"
    ERROR_FILE "${BRK_DEPS_LOG}/${logname}.log"
    RESULT_VARIABLE rc)
  if(NOT rc EQUAL 0)
    file(READ "${BRK_DEPS_LOG}/${logname}.log" log_tail)
    string(LENGTH "${log_tail}" log_len)
    if(log_len GREATER 4000)
      math(EXPR log_off "${log_len} - 4000")
      string(SUBSTRING "${log_tail}" ${log_off} -1 log_tail)
    endif()
    message(FATAL_ERROR "static-deps: step '${logname}' failed (rc=${rc}). Log tail:\n${log_tail}")
  endif()
endfunction()

# Configure+build+install one CMake-based dep (zstd, lz4).
function(brk_cmake_dep logname srcdir)
  set(bld "${BRK_DEPS_BUILD}/${logname}")
  set(launcher_args "")
  if(CMAKE_C_COMPILER_LAUNCHER)
    set(launcher_args "-DCMAKE_C_COMPILER_LAUNCHER=${CMAKE_C_COMPILER_LAUNCHER}")
  endif()
  brk_run_step("${logname}-configure" "${BRK_DEPS_ROOT}"
    ${CMAKE_COMMAND} -S "${srcdir}" -B "${bld}"
    -DCMAKE_BUILD_TYPE=Release
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON
    -DCMAKE_INSTALL_PREFIX=${BRK_DEPS_PREFIX}
    -DCMAKE_INSTALL_LIBDIR=lib
    ${launcher_args}
    ${ARGN})
  brk_run_step("${logname}-build" "${bld}"
    ${CMAKE_COMMAND} --build "${bld}" --target install --parallel ${BRK_NPROC})
endfunction()

# ---- zlib ------------------------------------------------------------------
if(NOT EXISTS "${BRK_DEPS_PREFIX}/lib/libz.a")
  brk_fetch_dep("zlib-${BRK_ZLIB_VERSION}"
    "https://github.com/madler/zlib/releases/download/v${BRK_ZLIB_VERSION}/zlib-${BRK_ZLIB_VERSION}.tar.gz"
    ${BRK_ZLIB_SHA256})
  message(STATUS "static-deps: building zlib ${BRK_ZLIB_VERSION}")
  set(zlib_src "${BRK_DEPS_SRC}/zlib-${BRK_ZLIB_VERSION}")
  brk_run_step("zlib-configure" "${zlib_src}"
    ${CMAKE_COMMAND} -E env CFLAGS=-fPIC
    ./configure --static --prefix=${BRK_DEPS_PREFIX})
  brk_run_step("zlib-build" "${zlib_src}" make -j${BRK_NPROC} install)
endif()

# ---- zstd ------------------------------------------------------------------
if(NOT EXISTS "${BRK_DEPS_PREFIX}/lib/libzstd.a")
  brk_fetch_dep("zstd-${BRK_ZSTD_VERSION}"
    "https://github.com/facebook/zstd/releases/download/v${BRK_ZSTD_VERSION}/zstd-${BRK_ZSTD_VERSION}.tar.gz"
    ${BRK_ZSTD_SHA256})
  message(STATUS "static-deps: building zstd ${BRK_ZSTD_VERSION}")
  brk_cmake_dep("zstd" "${BRK_DEPS_SRC}/zstd-${BRK_ZSTD_VERSION}/build/cmake"
    -DZSTD_BUILD_SHARED=OFF
    -DZSTD_BUILD_STATIC=ON
    -DZSTD_BUILD_PROGRAMS=OFF
    -DZSTD_BUILD_TESTS=OFF)
endif()

# ---- lz4 -------------------------------------------------------------------
if(NOT EXISTS "${BRK_DEPS_PREFIX}/lib/liblz4.a")
  brk_fetch_dep("lz4-${BRK_LZ4_VERSION}"
    "https://github.com/lz4/lz4/releases/download/v${BRK_LZ4_VERSION}/lz4-${BRK_LZ4_VERSION}.tar.gz"
    ${BRK_LZ4_SHA256})
  message(STATUS "static-deps: building lz4 ${BRK_LZ4_VERSION}")
  brk_cmake_dep("lz4" "${BRK_DEPS_SRC}/lz4-${BRK_LZ4_VERSION}/build/cmake"
    -DBUILD_SHARED_LIBS=OFF
    -DBUILD_STATIC_LIBS=ON
    -DLZ4_BUILD_CLI=OFF
    -DLZ4_BUILD_LEGACY_LZ4C=OFF)
endif()

# ---- OpenSSL (the slow one, ~5-10 min on a small box; fully cached after) ---
if(NOT EXISTS "${BRK_DEPS_PREFIX}/lib/libssl.a")
  brk_fetch_dep("openssl-${BRK_OPENSSL_VERSION}"
    "https://github.com/openssl/openssl/releases/download/openssl-${BRK_OPENSSL_VERSION}/openssl-${BRK_OPENSSL_VERSION}.tar.gz"
    ${BRK_OPENSSL_SHA256})
  message(STATUS "static-deps: building OpenSSL ${BRK_OPENSSL_VERSION} (this takes a few minutes)")
  set(openssl_src "${BRK_DEPS_SRC}/openssl-${BRK_OPENSSL_VERSION}")
  # ./config needs perl; -fPIC because the .a gets linked into our shared lib.
  # build_libs + install_dev skip the apps/docs — libs and headers only.
  brk_run_step("openssl-configure" "${openssl_src}"
    ./config no-shared no-tests -fPIC
    --prefix=${BRK_DEPS_PREFIX} --libdir=lib --openssldir=${BRK_DEPS_PREFIX}/ssl)
  brk_run_step("openssl-build" "${openssl_src}" make -j${BRK_NPROC} build_libs)
  brk_run_step("openssl-install" "${openssl_src}" make install_dev)
endif()

message(STATUS "static-deps: ready at ${BRK_DEPS_PREFIX}")
