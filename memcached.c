
#line 1 "memcached.rl"
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <string.h>
#include <assert.h>
#include <stdio.h>

#include "memcached.h"

#include <luajit-2.0/lua.h>


#line 16 "memcached.c"
static const char _memcached_actions[] = {
	0, 1, 0, 1, 3, 1, 4, 1, 
	5, 1, 6, 1, 7, 1, 8, 1, 
	9, 1, 10, 1, 11, 1, 12, 1, 
	13, 1, 14, 1, 16, 1, 17, 1, 
	18, 1, 19, 1, 20, 1, 21, 1, 
	22, 1, 23, 1, 24, 1, 25, 1, 
	26, 1, 27, 1, 28, 1, 29, 1, 
	30, 2, 15, 2, 3, 4, 15, 2, 
	3, 12, 15, 2, 3, 14, 15, 2, 
	3, 15, 1, 2, 3, 16, 15, 2, 
	3, 28, 15, 2, 3, 29, 15, 2, 
	3, 30, 15, 2, 4, 8, 15, 1, 
	2, 4, 10, 15, 1, 2, 4, 16, 
	15, 1, 2
};

static const short _memcached_key_offsets[] = {
	0, 0, 20, 24, 26, 27, 31, 32, 
	35, 38, 41, 44, 47, 52, 53, 58, 
	60, 62, 64, 66, 68, 70, 73, 76, 
	78, 80, 82, 84, 85, 87, 89, 90, 
	94, 95, 98, 101, 104, 107, 110, 113, 
	116, 121, 123, 127, 129, 130, 134, 135, 
	138, 143, 144, 149, 151, 153, 155, 157, 
	159, 161, 164, 167, 169, 171, 173, 174, 
	178, 181, 188, 193, 195, 197, 199, 201, 
	202, 204, 206, 208, 211, 218, 223, 225, 
	227, 230, 234, 237, 241, 242, 244, 246, 
	248, 249, 251, 253, 255, 257, 259, 261, 
	262, 264, 266, 268, 270, 272, 274, 276, 
	278, 280, 282, 283, 287, 289, 290, 292, 
	294, 296, 298
};

static const char _memcached_trans_keys[] = {
	65, 67, 68, 70, 71, 73, 80, 81, 
	82, 83, 97, 99, 100, 102, 103, 105, 
	112, 113, 114, 115, 68, 80, 100, 112, 
	68, 100, 32, 13, 32, 9, 10, 32, 
	32, 48, 57, 32, 48, 57, 32, 48, 
	57, 32, 48, 57, 32, 48, 57, 10, 
	13, 32, 48, 57, 10, 10, 13, 32, 
	78, 110, 79, 111, 82, 114, 69, 101, 
	80, 112, 76, 108, 89, 121, 10, 13, 
	32, 10, 13, 32, 80, 112, 69, 101, 
	78, 110, 68, 100, 32, 65, 97, 83, 
	115, 32, 13, 32, 9, 10, 32, 32, 
	48, 57, 32, 48, 57, 32, 48, 57, 
	32, 48, 57, 32, 48, 57, 32, 48, 
	57, 32, 48, 57, 10, 13, 32, 48, 
	57, 69, 101, 67, 76, 99, 108, 82, 
	114, 32, 13, 32, 9, 10, 32, 32, 
	48, 57, 10, 13, 32, 48, 57, 10, 
	10, 13, 32, 78, 110, 79, 111, 82, 
	114, 69, 101, 80, 112, 76, 108, 89, 
	121, 10, 13, 32, 10, 13, 32, 69, 
	101, 84, 116, 69, 101, 32, 13, 32, 
	9, 10, 10, 13, 32, 10, 13, 32, 
	78, 110, 48, 57, 10, 13, 32, 48, 
	57, 76, 108, 85, 117, 83, 115, 72, 
	104, 95, 65, 97, 76, 108, 76, 108, 
	10, 13, 32, 10, 13, 32, 78, 110, 
	48, 57, 10, 13, 32, 48, 57, 69, 
	101, 84, 116, 32, 83, 115, 13, 32, 
	9, 10, 10, 13, 32, 9, 10, 13, 
	32, 32, 78, 110, 67, 99, 82, 114, 
	32, 82, 114, 69, 101, 80, 112, 69, 
	101, 78, 110, 68, 100, 32, 85, 117, 
	73, 105, 84, 116, 10, 13, 69, 101, 
	80, 112, 76, 108, 65, 97, 67, 99, 
	69, 101, 32, 69, 84, 101, 116, 84, 
	116, 32, 65, 97, 84, 116, 83, 115, 
	10, 13, 0
};

static const char _memcached_single_lengths[] = {
	0, 20, 4, 2, 1, 2, 1, 1, 
	1, 1, 1, 1, 3, 1, 5, 2, 
	2, 2, 2, 2, 2, 3, 3, 2, 
	2, 2, 2, 1, 2, 2, 1, 2, 
	1, 1, 1, 1, 1, 1, 1, 1, 
	3, 2, 4, 2, 1, 2, 1, 1, 
	3, 1, 5, 2, 2, 2, 2, 2, 
	2, 3, 3, 2, 2, 2, 1, 2, 
	3, 5, 3, 2, 2, 2, 2, 1, 
	2, 2, 2, 3, 5, 3, 2, 2, 
	3, 2, 3, 4, 1, 2, 2, 2, 
	1, 2, 2, 2, 2, 2, 2, 1, 
	2, 2, 2, 2, 2, 2, 2, 2, 
	2, 2, 1, 4, 2, 1, 2, 2, 
	2, 2, 0
};

static const char _memcached_range_lengths[] = {
	0, 0, 0, 0, 0, 1, 0, 1, 
	1, 1, 1, 1, 1, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 1, 
	0, 1, 1, 1, 1, 1, 1, 1, 
	1, 0, 0, 0, 0, 1, 0, 1, 
	1, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 1, 
	0, 1, 1, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 1, 1, 0, 0, 
	0, 1, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0
};

static const short _memcached_index_offsets[] = {
	0, 0, 21, 26, 29, 31, 35, 37, 
	40, 43, 46, 49, 52, 57, 59, 65, 
	68, 71, 74, 77, 80, 83, 87, 91, 
	94, 97, 100, 103, 105, 108, 111, 113, 
	117, 119, 122, 125, 128, 131, 134, 137, 
	140, 145, 148, 153, 156, 158, 162, 164, 
	167, 172, 174, 180, 183, 186, 189, 192, 
	195, 198, 202, 206, 209, 212, 215, 217, 
	221, 225, 232, 237, 240, 243, 246, 249, 
	251, 254, 257, 260, 264, 271, 276, 279, 
	282, 286, 290, 294, 299, 301, 304, 307, 
	310, 312, 315, 318, 321, 324, 327, 330, 
	332, 335, 338, 341, 344, 347, 350, 353, 
	356, 359, 362, 364, 369, 372, 374, 377, 
	380, 383, 386
};

static const unsigned char _memcached_indicies[] = {
	0, 2, 3, 4, 5, 6, 7, 8, 
	9, 10, 0, 2, 3, 4, 5, 6, 
	7, 8, 9, 10, 1, 11, 12, 11, 
	12, 1, 13, 13, 1, 14, 1, 1, 
	16, 1, 15, 17, 1, 17, 18, 1, 
	19, 20, 1, 21, 22, 1, 23, 24, 
	1, 25, 26, 1, 27, 28, 29, 30, 
	1, 31, 1, 31, 32, 33, 34, 34, 
	1, 35, 35, 1, 36, 36, 1, 37, 
	37, 1, 38, 38, 1, 39, 39, 1, 
	40, 40, 1, 41, 42, 43, 1, 31, 
	32, 44, 1, 45, 45, 1, 46, 46, 
	1, 47, 47, 1, 48, 48, 1, 49, 
	1, 50, 50, 1, 51, 51, 1, 52, 
	1, 1, 54, 1, 53, 55, 1, 55, 
	56, 1, 57, 58, 1, 59, 60, 1, 
	61, 62, 1, 63, 64, 1, 65, 66, 
	1, 67, 68, 1, 69, 70, 71, 72, 
	1, 73, 73, 1, 74, 75, 74, 75, 
	1, 76, 76, 1, 77, 1, 1, 79, 
	1, 78, 80, 1, 80, 81, 1, 82, 
	83, 84, 85, 1, 86, 1, 86, 87, 
	88, 89, 89, 1, 90, 90, 1, 91, 
	91, 1, 92, 92, 1, 93, 93, 1, 
	94, 94, 1, 95, 95, 1, 96, 97, 
	98, 1, 86, 87, 99, 1, 100, 100, 
	1, 101, 101, 1, 102, 102, 1, 103, 
	1, 1, 105, 1, 104, 86, 87, 106, 
	1, 86, 87, 106, 89, 89, 107, 1, 
	108, 109, 110, 111, 1, 112, 112, 1, 
	113, 113, 1, 114, 114, 1, 115, 115, 
	1, 116, 1, 117, 117, 1, 118, 118, 
	1, 119, 119, 1, 120, 121, 122, 1, 
	86, 87, 123, 89, 89, 124, 1, 125, 
	126, 127, 128, 1, 129, 129, 1, 130, 
	130, 1, 131, 132, 132, 1, 1, 134, 
	1, 133, 86, 87, 135, 1, 1, 86, 
	87, 135, 133, 136, 1, 137, 137, 1, 
	138, 138, 1, 139, 139, 1, 140, 1, 
	141, 141, 1, 142, 142, 1, 143, 143, 
	1, 144, 144, 1, 145, 145, 1, 146, 
	146, 1, 147, 1, 148, 148, 1, 149, 
	149, 1, 150, 150, 1, 151, 152, 1, 
	153, 153, 1, 154, 154, 1, 155, 155, 
	1, 156, 156, 1, 157, 157, 1, 158, 
	158, 1, 159, 1, 160, 161, 160, 161, 
	1, 162, 162, 1, 163, 1, 164, 164, 
	1, 165, 165, 1, 166, 166, 1, 167, 
	168, 1, 1, 0
};

static const char _memcached_trans_targs[] = {
	2, 0, 28, 41, 67, 78, 85, 89, 
	96, 100, 107, 3, 23, 4, 5, 6, 
	5, 7, 8, 9, 8, 9, 10, 11, 
	10, 11, 12, 114, 13, 14, 12, 114, 
	13, 14, 15, 16, 17, 18, 19, 20, 
	21, 114, 13, 22, 22, 24, 25, 26, 
	27, 5, 29, 30, 31, 32, 31, 33, 
	34, 35, 34, 35, 36, 37, 36, 37, 
	38, 39, 38, 39, 40, 114, 13, 14, 
	40, 42, 43, 59, 44, 45, 46, 45, 
	47, 48, 114, 49, 50, 48, 114, 49, 
	50, 51, 52, 53, 54, 55, 56, 57, 
	114, 49, 58, 58, 60, 61, 62, 63, 
	64, 63, 65, 66, 114, 49, 50, 66, 
	68, 69, 70, 71, 72, 73, 74, 75, 
	114, 49, 76, 76, 77, 114, 49, 50, 
	77, 79, 80, 81, 84, 82, 81, 83, 
	81, 86, 87, 88, 45, 90, 91, 92, 
	93, 94, 95, 5, 97, 98, 99, 114, 
	49, 101, 102, 103, 104, 105, 106, 5, 
	108, 110, 109, 5, 111, 112, 113, 114, 
	49
};

static const char _memcached_trans_actions[] = {
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 31, 1, 
	0, 0, 7, 9, 0, 0, 3, 5, 
	0, 0, 11, 92, 13, 13, 0, 72, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 102, 27, 27, 0, 0, 0, 0, 
	0, 35, 0, 0, 39, 1, 0, 0, 
	7, 9, 0, 0, 3, 5, 0, 0, 
	11, 13, 0, 0, 15, 97, 17, 17, 
	0, 0, 0, 0, 0, 49, 1, 0, 
	0, 19, 64, 21, 21, 0, 57, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	76, 27, 27, 0, 0, 0, 0, 45, 
	1, 0, 0, 3, 60, 5, 5, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	84, 53, 53, 0, 23, 68, 25, 25, 
	0, 0, 0, 41, 0, 1, 0, 0, 
	43, 0, 0, 0, 47, 0, 0, 0, 
	0, 0, 0, 37, 0, 0, 0, 88, 
	55, 0, 0, 0, 0, 0, 0, 33, 
	0, 0, 0, 29, 0, 0, 0, 80, 
	51
};

static const int memcached_start = 1;
static const int memcached_first_final = 114;
static const int memcached_error = 0;

static const int memcached_en_main = 1;


#line 15 "memcached.rl"

#if defined(__APPLE__)
#  define MC_EXPORT __attribute__((visibility("default")))
#elif defined(_WIN32) || defined(__WIN32__)
#  define MC_EXPORT __declspec(dllexport)
#else
#  define MC_EXPORT
#endif

static int
mc_strtol(const char *start, const char *end, int64_t *num)
{
    *num = 0;
    char sign = 1;
    if (*start == '-') { sign *= -1; start++; }
    while (start < end) {
        uint8_t code = *start++;
        if (code < '0' || code > '9')
            return -1;
        *num = (*num) * 10 + (code - '0');
    }
    return 0;
}

static int
mc_strtoul(const char *start, const char *end, uint64_t *num)
{
    *num = 0;
    while (start < end) {
        uint8_t code = *start++;
        if (code < '0' || code > '9')
            return -1;
        *num = (*num) * 10 + (code - '0');
    }
    return 0;
}

MC_EXPORT int
mc_parse(struct mc_request *req, const char **p_ptr, const char *pe) {
    const char *p = *p_ptr;
    int cs = 0;
    const char *s = NULL;
    bool done = false;

    memset(req, 0, sizeof(struct mc_request));
    
#line 303 "memcached.c"
	{
	cs = memcached_start;
	}

#line 308 "memcached.c"
	{
	int _klen;
	unsigned int _trans;
	const char *_acts;
	unsigned int _nacts;
	const char *_keys;

	if ( p == pe )
		goto _test_eof;
	if ( cs == 0 )
		goto _out;
_resume:
	_keys = _memcached_trans_keys + _memcached_key_offsets[cs];
	_trans = _memcached_index_offsets[cs];

	_klen = _memcached_single_lengths[cs];
	if ( _klen > 0 ) {
		const char *_lower = _keys;
		const char *_mid;
		const char *_upper = _keys + _klen - 1;
		while (1) {
			if ( _upper < _lower )
				break;

			_mid = _lower + ((_upper-_lower) >> 1);
			if ( (*p) < *_mid )
				_upper = _mid - 1;
			else if ( (*p) > *_mid )
				_lower = _mid + 1;
			else {
				_trans += (unsigned int)(_mid - _keys);
				goto _match;
			}
		}
		_keys += _klen;
		_trans += _klen;
	}

	_klen = _memcached_range_lengths[cs];
	if ( _klen > 0 ) {
		const char *_lower = _keys;
		const char *_mid;
		const char *_upper = _keys + (_klen<<1) - 2;
		while (1) {
			if ( _upper < _lower )
				break;

			_mid = _lower + (((_upper-_lower) >> 1) & ~1);
			if ( (*p) < _mid[0] )
				_upper = _mid - 2;
			else if ( (*p) > _mid[1] )
				_lower = _mid + 2;
			else {
				_trans += (unsigned int)((_mid - _keys)>>1);
				goto _match;
			}
		}
		_trans += _klen;
	}

_match:
	_trans = _memcached_indicies[_trans];
	cs = _memcached_trans_targs[_trans];

	if ( _memcached_trans_actions[_trans] == 0 )
		goto _again;

	_acts = _memcached_actions + _memcached_trans_actions[_trans];
	_nacts = (unsigned int) *_acts++;
	while ( _nacts-- > 0 )
	{
		switch ( *_acts++ )
		{
	case 0:
#line 61 "memcached.rl"
	{
            s = p;
            for (; p < pe && *p != ' ' && *p != '\r' && *p != '\n'; p++);
            if (*p == ' ' || *p == '\r' || *p == '\n') {
                if (req->key == NULL)
                    req->key = s;
                req->key_len = (p-- - req->key);
                req->key_count += 1;
            } else {
                p = s;
            }
        }
	break;
	case 1:
#line 73 "memcached.rl"
	{
            req->data = p;
            req->data_len = req->bytes;

            if (req->data + req->data_len <= pe - 2) {
                if (strncmp(req->data + req->data_len, "\r\n", 2) != 0) {
                    return -7;
                }
                p += req->bytes + 2;
            } else {
                return (req->data_len + 2) - (pe - req->data);
            }
        }
	break;
	case 2:
#line 86 "memcached.rl"
	{
            *p_ptr = p;
            done = true;
        }
	break;
	case 3:
#line 94 "memcached.rl"
	{ s = p; }
	break;
	case 4:
#line 95 "memcached.rl"
	{ if (mc_strtoul(s, p, &req->exptime) == -1) return -2; }
	break;
	case 5:
#line 97 "memcached.rl"
	{ s = p; }
	break;
	case 6:
#line 98 "memcached.rl"
	{ if (mc_strtoul(s, p, &req->flags) == -1) return -3; }
	break;
	case 7:
#line 100 "memcached.rl"
	{ s = p; }
	break;
	case 8:
#line 101 "memcached.rl"
	{ if (mc_strtoul(s, p, &req->bytes) == -1) return -4; }
	break;
	case 9:
#line 103 "memcached.rl"
	{ s = p; }
	break;
	case 10:
#line 104 "memcached.rl"
	{ if (mc_strtoul(s, p, &req->cas) == -1) return -5; }
	break;
	case 11:
#line 106 "memcached.rl"
	{ s = p; }
	break;
	case 12:
#line 107 "memcached.rl"
	{ if (mc_strtoul(s, p, &req->inc_val) == -1) return -6; }
	break;
	case 13:
#line 109 "memcached.rl"
	{ s = p; }
	break;
	case 14:
#line 110 "memcached.rl"
	{ if (mc_strtoul(s, p, &req->exptime) == -1) return -7; }
	break;
	case 15:
#line 112 "memcached.rl"
	{ p++; }
	break;
	case 16:
#line 114 "memcached.rl"
	{ req->noreply = true; }
	break;
	case 17:
#line 123 "memcached.rl"
	{req->op = MC_SET;}
	break;
	case 18:
#line 124 "memcached.rl"
	{req->op = MC_ADD;}
	break;
	case 19:
#line 125 "memcached.rl"
	{req->op = MC_REPLACE;}
	break;
	case 20:
#line 126 "memcached.rl"
	{req->op = MC_APPEND;}
	break;
	case 21:
#line 127 "memcached.rl"
	{req->op = MC_PREPEND;}
	break;
	case 22:
#line 128 "memcached.rl"
	{req->op = MC_CAS;}
	break;
	case 23:
#line 130 "memcached.rl"
	{req->op = MC_GET;}
	break;
	case 24:
#line 131 "memcached.rl"
	{req->op = MC_GETS;}
	break;
	case 25:
#line 132 "memcached.rl"
	{req->op = MC_DELETE;}
	break;
	case 26:
#line 133 "memcached.rl"
	{req->op = MC_INCR;}
	break;
	case 27:
#line 134 "memcached.rl"
	{req->op = MC_DECR;}
	break;
	case 28:
#line 136 "memcached.rl"
	{req->op = MC_STATS;}
	break;
	case 29:
#line 137 "memcached.rl"
	{req->op = MC_FLUSH;}
	break;
	case 30:
#line 138 "memcached.rl"
	{req->op = MC_QUIT;}
	break;
#line 532 "memcached.c"
		}
	}

_again:
	if ( cs == 0 )
		goto _out;
	if ( ++p != pe )
		goto _resume;
	_test_eof: {}
	_out: {}
	}

#line 146 "memcached.rl"



    if (!done) {
        if (p == pe)
            return 1;
        return -1;
    }
    return 0;
}

int luaopen_memctnt(lua_State *L) {
    return 0;
}
