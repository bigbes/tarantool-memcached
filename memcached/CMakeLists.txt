# Build
RAGEL_TARGET(number memcached.rl memcached.c COMPILE_FLAGS "-G2")
add_library(parser SHARED memcached.c)
set_target_properties(parser PROPERTIES PREFIX "" OUTPUT_NAME "libparser")

# Install
install(TARGETS parser LIBRARY DESTINATION ${TARANTOOL_LIBDIR}/memcached)
install(FILES expirationd.lua  DESTINATION ${TARANTOOL_LUADIR}/memcached)
install(FILES memcached.lua    DESTINATION ${TARANTOOL_LUADIR}/memcached)
