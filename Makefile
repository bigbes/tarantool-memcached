all:
	ragel -C memcached.rl
	gcc memcached.c -O0 -g3 -ggdb3 -dynamiclib -o libmemctnt.dylib -I/usr/local/include/

test:
	tarantool memcached.lua

rltest:
	ragel -C memcached.rl
	gcc memcached.c -O0 -g3 -ggdb3

debug:
	ragel -Vp memcached.rl -o memcached.dot
	dot memcached.dot -T png -o memcached.png
	open memcached.png
