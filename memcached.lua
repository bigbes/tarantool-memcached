local ffi = require('ffi')
local yaml = require('yaml')

local socket = require('socket')
local fiber = require('fiber')
local log = require('log')
local boxerr = require('errno')

box.cfg{}

ffi.cdef(io.open('memcached.h', 'r'):read("*all"))

func = ffi.load('memctnt')
print(func.memcache_parse)

local function dump_request(req)
   log.info('operation: %d', tonumber(req.op))
   print(1)
   local key_count = tonumber(req.key_count)
   print(key_count)
   local key_len = req.key_len
   print(key_len)
   print(req.key)
   local key = ffi.string(req.key, req.key_len)
   log.info('key: %d, %s', key_count, key)
   print(2)
   log.info('data: %s', ffi.string(req.data, req.data_len))
end

local function memcache_handler(srv, from)
   log.info(require('yaml').encode(srv))
   log.info(require('yaml').encode(from))
   srv:nonblock(1)
   local buf = ''
   while buf == '' do
      local tmp_buf = srv:sysread(16384)
      print(tmp_buf)
      if (tmp_buf == nil and srv:errno() ~= boxerr.EAGAIN) then
         log.info(srv:error())
         log.info(boxerr.EAGAIN)
         error(srv:error())
      elseif (tmp_buf and #tmp_buf > 0) then
         buf = buf..tmp_buf
         print(buf)
         if string.sub(buf, -2) == '\r\n' then
            break
         end
      end
      require('fiber').sleep(0.1)
   end
   local req = ffi.new('struct memcache_request')

   tp = ffi.cast('const char *', buf)

   if (func.memcache_parse(req, tp, tp + #buf + 1) == -1) then
      return
   end
   dump_request(req)
end

local memcache
local memcache_methods = {
   init_loop = function(self, port)
      print (1)
      self.srv = socket.tcp_server('0.0.0.0', port,
         {handler = memcache_handler, name = 'memcached loop'}, 50)
      print(self.srv)
      print(boxerr.strerror(boxerr()))
   end
}

memcache = {
   new = function(name)
      local self = setmetatable({}, { __index = memcache_methods })
      self.name = name
      if box.space[name] == nil then
         local mcs = box.schema.space.create(name)
         mcs:create_index('primary', {type='HASH', parts={1, 'STR'}})
      end
      self.mcs = box.space[name]
      return self
   end
}

a = memcache.new('mc')
a:init_loop(1200)
