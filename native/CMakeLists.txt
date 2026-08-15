# bun-rdkafka native shim — libbunrdkafka
#
# Build:  cmake -B build -S native && cmake --build build -j2
# librdkafka is pinned by ../librdkafka.version (FetchContent by git tag),
# or taken from vcpkg (find_package) on Windows / when BRK_USE_SYSTEM_RDKAFKA=ON.
cmake_minimum_required(VERSION 3.22)
project(bunrdkafka C)

option(BRK_STATIC_DEPS "Static-link OpenSSL/zlib/zstd/lz4 (CI release build)" OFF)
option(BRK_USE_SYSTEM_RDKAFKA "find_package(RdKafka) instead of FetchContent (vcpkg)" OFF)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
# Hidden by default for EVERY target (including the FetchContent static
# librdkafka) — only BRK_EXPORT is exposed; this also lets macOS skip an
# exported_symbols_list.
set(CMAKE_C_VISIBILITY_PRESET hidden)
set(CMAKE_CXX_VISIBILITY_PRESET hidden)
set(CMAKE_VISIBILITY_INLINES_HIDDEN ON)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

# ---------------------------------------------------------------------------
# librdkafka
# ---------------------------------------------------------------------------
file(READ "${CMAKE_CURRENT_SOURCE_DIR}/../librdkafka.version" RDKAFKA_TAG)
string(STRIP "${RDKAFKA_TAG}" RDKAFKA_TAG)

if(WIN32 OR BRK_USE_SYSTEM_RDKAFKA)
  # Windows CI: vcpkg manifest mode (native/vcpkg.json) provides a static librdkafka.
  find_package(RdKafka CONFIG REQUIRED)
  set(BRK_RDKAFKA_LIB RdKafka::rdkafka)
else()
  include(FetchContent)
  set(RDKAFKA_BUILD_STATIC ON CACHE BOOL "" FORCE)
  set(RDKAFKA_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
  set(RDKAFKA_BUILD_TESTS OFF CACHE BOOL "" FORCE)
  set(WITH_SSL ON CACHE BOOL "" FORCE)
  set(WITH_ZSTD ON CACHE BOOL "" FORCE)
  set(ENABLE_LZ4_EXT ON CACHE BOOL "" FORCE)
  # TODO(M4/OIDC): WITH_CURL=ON is required for sasl.oauthbearer.method=oidc;
  # the build machine needs libcurl (static for the CI release build). Currently
  # off because libcurl-dev is missing locally.
  set(WITH_CURL OFF CACHE BOOL "" FORCE)
  # SASL: PLAIN is always compiled in (rdkafka_sasl_plain.c is unconditional) and
  # SCRAM/OAUTHBEARER only need SSL. Cyrus (libsasl2 → GSSAPI/Kerberos) is
  # deliberately DISABLED for portability: prebuilt binaries must not link
  # libsasl2 dynamically — its soname differs across distros (EL8 ships .so.3,
  # Debian .so.2), which would break the downloaded prebuilt on other distros.
  # librdkafka's CMake force-enables cyrus (plain set(), not overridable) whenever
  # WITH_SASL=ON on non-Windows, so instead we turn WITH_SASL off and pre-seed the
  # SCRAM/OAUTHBEARER flags ourselves: plain parent-scope variables are inherited
  # by the FetchContent subdirectory, and the if(WITH_SASL) branch that would
  # overwrite them never runs. No librdkafka source file uses the bare WITH_SASL
  # macro (verified against v2.15.0). Net effect: PLAIN/SCRAM/OAUTHBEARER work,
  # GSSAPI is absent — build from source with cyrus-sasl to enable it.
  set(WITH_SASL OFF CACHE BOOL "" FORCE)
  set(WITH_SASL_SCRAM ON)
  set(WITH_SASL_OAUTHBEARER ON)
  if(BRK_STATIC_DEPS)
    # CI release configuration: build static OpenSSL/zlib/zstd/lz4 at configure
    # time (cmake/StaticDeps.cmake) and pin librdkafka's find_package results to
    # them, so the final libbunrdkafka.so depends on nothing beyond the glibc
    # family and is portable across glibc distros. Dev builds keep this OFF and
    # link the system -devel packages (faster).
    include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/StaticDeps.cmake")
    # OpenSSL: FindOpenSSL honors these.
    set(OPENSSL_ROOT_DIR "${BRK_DEPS_PREFIX}" CACHE PATH "" FORCE)
    set(OPENSSL_USE_STATIC_LIBS TRUE)
    # zlib: presetting ZLIB_LIBRARY/ZLIB_INCLUDE_DIR short-circuits FindZLIB.
    set(ZLIB_ROOT "${BRK_DEPS_PREFIX}" CACHE PATH "" FORCE)
    set(ZLIB_INCLUDE_DIR "${BRK_DEPS_PREFIX}/include" CACHE PATH "" FORCE)
    set(ZLIB_LIBRARY "${BRK_DEPS_PREFIX}/lib/libz.a" CACHE FILEPATH "" FORCE)
    # zstd/lz4: librdkafka's FindZSTD/FindLZ4 use these exact cache variables;
    # presetting them skips the find_path/find_library system search entirely.
    set(ZSTD_INCLUDE_DIR "${BRK_DEPS_PREFIX}/include" CACHE PATH "" FORCE)
    set(ZSTD_LIBRARY_RELEASE "${BRK_DEPS_PREFIX}/lib/libzstd.a" CACHE FILEPATH "" FORCE)
    set(ZSTD_LIBRARY "${BRK_DEPS_PREFIX}/lib/libzstd.a" CACHE FILEPATH "" FORCE)
    set(LZ4_INCLUDE_DIR "${BRK_DEPS_PREFIX}/include" CACHE PATH "" FORCE)
    set(LZ4_LIBRARY "${BRK_DEPS_PREFIX}/lib/liblz4.a" CACHE FILEPATH "" FORCE)
  endif()

  FetchContent_Declare(
    librdkafka
    GIT_REPOSITORY https://github.com/confluentinc/librdkafka.git
    GIT_TAG        ${RDKAFKA_TAG}
    GIT_SHALLOW    TRUE
  )
  FetchContent_MakeAvailable(librdkafka)
  set(BRK_RDKAFKA_LIB rdkafka)
endif()

# ---------------------------------------------------------------------------
# Shim
# ---------------------------------------------------------------------------
add_library(bunrdkafka SHARED
  src/shim_common.c
  src/shim_events.c
  src/shim_producer.c
  src/shim_consumer.c
  src/shim_admin.c
)
# Keep the "lib" prefix on EVERY platform (MSVC included) — loader.ts looks for one name only.
set_target_properties(bunrdkafka PROPERTIES PREFIX "lib" OUTPUT_NAME "bunrdkafka")
target_include_directories(bunrdkafka PRIVATE include)
target_link_libraries(bunrdkafka PRIVATE ${BRK_RDKAFKA_LIB})
if(NOT (WIN32 OR BRK_USE_SYSTEM_RDKAFKA))
  # FetchContent: the header lives inside librdkafka's source tree. SYSTEM keeps
  # upstream header warts (e.g. `const int` returns → -Wignored-qualifiers) from
  # polluting our warning output while our own code stays at full strictness.
  target_include_directories(bunrdkafka SYSTEM PRIVATE ${librdkafka_SOURCE_DIR}/src)
endif()

if(MSVC)
  target_compile_options(bunrdkafka PRIVATE /O2 /W4)
else()
  target_compile_options(bunrdkafka PRIVATE -O2 -Wall -Wextra)
  target_link_libraries(bunrdkafka PRIVATE pthread m)
  if(BRK_STATIC_DEPS)
    # Static libcrypto.a references dlopen/dlsym (providers) — resolve them at
    # the shared-lib link instead of leaving undefined symbols.
    target_link_libraries(bunrdkafka PRIVATE ${CMAKE_DL_LIBS})
  endif()
endif()
if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang" AND NOT APPLE)
  # Do not leak static librdkafka/openssl symbols into the dynamic table
  target_link_options(bunrdkafka PRIVATE "LINKER:--exclude-libs,ALL")
endif()
