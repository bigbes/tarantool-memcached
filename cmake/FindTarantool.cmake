macro(extract_definition name output input)
    string(REGEX MATCH "#define[\t ]+${name}[\t ]+\"([^\"]*)\""
        _t "${input}")
    string(REGEX REPLACE "#define[\t ]+${name}[\t ]+\"(.*)\"" "\\1"
        ${output} "${_t}")
endmacro()

find_path(_dir tarantool.h
  HINTS ENV TARANTOOL_DIR
  PATH_SUFFIXES include/tarantool
)

if (_dir)
    set(_config "-")
    file(READ "${_dir}/tarantool.h" _config0)
    string(REPLACE "\\" "\\\\" _config ${_config0})
    unset(_config0)
    extract_definition(PACKAGE_VERSION TARANTOOL_VERSION ${_config})
    extract_definition(MODULE_LUADIR TARANTOOL_LUADIR ${_config})
    extract_definition(MODULE_LIBDIR TARANTOOL_LIBDIR ${_config})
    extract_definition(MODULE_INCLUDEDIR TARANTOOL_INCLUDEDIR ${_config})
    unset(_config)
endif (_dir)
unset (_dir)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(TARANTOOL
    REQUIRED_VARS TARANTOOL_INCLUDEDIR TARANTOOL_LUADIR TARANTOOL_LIBDIR
    VERSION_VAR TARANTOOL_VERSION)
if (TARANTOOL_FOUND AND NOT TARANTOOL_FIND_QUIETLY AND NOT FIND_TARANTOOL_DETAILS)
    set(FIND_TARANTOOL_DETAILS ON CACHE INTERNAL "Details about TARANTOOL")
    message(STATUS "Tarantool LUADIR is ${TARANTOOL_LUADIR}")
    message(STATUS "Tarantool LIBDIR is ${TARANTOOL_LIBDIR}")
endif ()
mark_as_advanced(TARANTOOL_INCLUDEDIR TARANTOOL_LUADIR TARANTOOL_LIBDIR TARANTOOL_VERSION)
