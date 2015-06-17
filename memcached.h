enum memcache_op {
	MC_SET = 0,
	MC_ADD,
	MC_REPLACE,
	MC_APPEND,
	MC_PREPEND,
	MC_CAS,
	MC_GET,
	MC_GETS,
	MC_DELETE,
	MC_INCR,
	MC_DECR,
	MC_FLUSH,
	MC_STATS,
	MC_VERSION,
	MC_QUIT,
};

struct memcache_request {
	enum memcache_op op;
	const char *key;
	size_t      key_len;
	uint32_t    key_count;
	const char *data;
	size_t      data_len;
	uint64_t    flags;
	uint64_t    bytes;
	uint64_t    cas;
	uint64_t    exptime;
	uint64_t    inc_val;
	bool        noreply;
};

int memcache_parse(struct memcache_request *req, const char *p, const char *pe);
