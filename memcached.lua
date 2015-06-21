local ffi = require('ffi')

local yaml = require('yaml')
local socket = require('socket')
local fiber = require('fiber')
local log = require('log')
local boxerrno = require('errno')
local fun = require('fun')

local expd = require('expirationd')

ffi.cdef(io.open('memcached.h', 'r'):read("*all"))
ffi.cdef[[
   void *memmove(void *dst, const void *src, size_t num);
]]
func = ffi.load('memctnt')

local function dump_request(req)
   log.info('operation: %d', req.op)
   log.info('key: %d, len(%d) - %s', req.key_count, req.key_len, ffi.string(req.key, req.key_len))
   log.info('data: len(%d) - %s', req.data_len, ffi.string(req.data, req.data_len))
end

local buffer_err = {
   ['EOK']    = 0,
   ['EINVAL'] = 1,
   ['ENOMEM'] = 2,
}

local buffer_strerr = {
   [0] = [['Not an error']],
   [1] = [['Bad value']],
   [2] = [['Not enough memory']]
}

local buffer
local buffer_mt = {
   extend = function (self, needed)
      self:pack()
      local had = (self.size - self.woff)
      if (had < needed) then
         had = self.size * 2
         while (had - self.woff < needed) do
            had = had * 2
         end
         self:_extend(had)
      end
      return
   end,

   _extend = function (self, new_size)
      if (new_size < self.size) then
         self.error = buffer_err.EINVAL
         return
      end
      local new_buf = ffi.new('char [?]', new_size)
      if (new_buf == nil) then
         self.error = buffer_err.ENOMEM
         return
      end
      if (self.buffer ~= nil) then
         ffi.copy(new_buf,
                  self.buffer + self.roff,
                  self.woff - self.roff)
      end
      self.woff   = self.woff - self.roff
      self.roff   = 0
      self.buffer = new_buf
      self.size   = new_size
   end,

   strerror = function(self)
      return buffer_strerr[self.error]
   end,

   write = function (self, str, str_size)
      if type(str) == 'string' then
         str_size = #str
      end
      if (self.size - self.woff) < str_size then
         self:extend(str_size)
      end
      ffi.copy(self.buffer + self.woff,
                str, str_size)
      self.woff = self.woff + str_size
   end,

   wptr_get = function (self)
      return self.buffer + self.woff
   end,

   read = function (self, size)
      if (size == nil or size > self.woff - self.roff) then
         size = self.woff - self.roff
      end
      return ffi.string(self.buffer + self.roff, size)
   end,

   rptr_get = function (self)
      return self.buffer + self.roff
   end,

   pack = function(self)
      if (self.roff == 0) then return end
      if (self.roff ~= self.woff) then
         ffi.C.memmove(self.buffer,
                       self.buffer + self.roff,
                       self.woff - self.roff)
      end
      self.woff = self.woff - self.roff
      self.roff = 0
      return
   end,

   recvfull = function(self, sckt)
      return self:recv(sckt, self.size - self.woff)
   end,

   recv = function(self, sckt, size)
      if not sckt:readable() then return -1 end
      local retval = ffi.C.recv(sckt:fd(), self.buffer + self.woff, size, 0)
      if retval == -1 then
         self._errno = boxerrno()
         return -1
      end
      self.woff = self.woff + retval
      return retval
   end,

   sendall = function(self, sckt)
      local pos      = self.roff
      local to_write = (self.woff - self.roff)
      while to_write > 0 do
         if not sckt:writable() then return -1 end
         local retval = ffi.C.send(sckt:fd(), self.buffer + pos, to_write, 0)
         if retval == -1 then
            self._errno = boxerrno()
            return -1
         end
         to_write = to_write - retval
         pos      = pos + retval
      end
      pos = pos - self.roff
      self.roff = 0
      self.woff = 0
      return pos
   end
}

local buffer = {
   new = function ()
      local self = setmetatable({}, { __index = buffer_mt })
      self.size   = 16384
      self.error  = buffer_err.EOK
      self.roff   = 0
      self.woff   = 0
      self:_extend(self.size)
      if (self.error ~= buffer_err.EOK) then
         log.error('ERROR')
         return
      end
      return self
   end
}

local memcache
local memcache_mt = {
   init_loop = function(self, port)
      local function memcache_handler(srv, from)
--         srv:nonblock(1)
         local rbuf  = buffer.new()
         local wbuf  = buffer.new()
         local p_ptr = ffi.new('const char *[1]');
         local req   = ffi.new('struct mc_request')
         while true do
            rbuf:pack()
            local size = rbuf:recvfull(srv)
            if (tonumber(size) == 0) then return end
            local tp = ffi.cast('const char *', rbuf:rptr_get())

            p_ptr[0] = tp
            while true do
               if (p_ptr[0] == rbuf:wptr_get()) then
                  break
               end
               local rval = func.mc_parse(req, p_ptr, rbuf:wptr_get())
               local resp = nil
               if (rval > 0) then break end
               if (rval == -1 or rval == -3 or rval == -4 or rval == -5) then
                  wbuf:write('CLIENT_ERROR bad command line format\r\n')
                  return
               elseif (rval == -2) then
                  wbuf:write('CLIENT_ERROR invalid exptime argument\r\n')
                  return
               elseif (rval == -6) then
                  wbuf:write('CLIENT_ERROR invalid numeric delta argument\r\n')
                  return
               elseif (rval == -7) then
                  wbuf:write('CLIENT_ERROR bad data chunk\r\n')
                  return
               end

               -- TODO:
               -- Use binary buffer instead of response in lua string
               resp = self:cmd_exec(req)
               if (resp == nil) then
                  log.info('requested exit')
                  return nil
               end
               log.info('response %s', resp)
               wbuf:write(resp)
               rbuf.roff = p_ptr[0] - rbuf.buffer
            end
            wbuf:sendall(srv)
         end
      end
      self.srv = socket.tcp_server('0.0.0.0', port, {
            handler = memcache_handler,
            name = 'memcached loop',
      }, 50)
   end,

   cmd_exec = function(self, req)
      if (req.op == ffi.C.MC_SET) then
         return self:set(ffi.string(req.key, req.key_len),
                         ffi.string(req.data, req.data_len),
                         tonumber(req.exptime), tonumber(req.flags),
                         tonumber(req.noreply))
      elseif (req.op == ffi.C.MC_ADD) then
         return self:add(ffi.string(req.key, req.key_len),
                         ffi.string(req.data, req.data_len),
                         tonumber(req.exptime), tonumber(req.flags),
                         tonumber(req.noreply))
      elseif (req.op == ffi.C.MC_REPLACE) then
         return self:replace(ffi.string(req.key, req.key_len),
                             ffi.string(req.data, req.data_len),
                             tonumber(req.exptime), tonumber(req.flags),
                             tonumber(req.noreply))
      elseif (req.op == ffi.C.MC_PREPEND) then
         return self:prepend(ffi.string(req.key, req.key_len),
                              ffi.string(req.data, req.data_len),
                              tonumber(req.exptime), tonumber(req.flags),
                              tonumber(req.noreply))
      elseif (req.op == ffi.C.MC_CAS) then
         return self:cas(ffi.string(req.key, req.key_len),
                           ffi.string(req.data, req.data_len),
                           tonumber(req.cas), tonumber(req.exptime),
                           tonumber(req.flags), tonumber(req.noreply))
      elseif (req.op == ffi.C.MC_GET) then
         local keys = ffi.string(req.key, req.key_len)
         if (req.key_count > 1) then
            local k = {};
            for i in string.gmatch(keys, '%S+') do
               table.insert(k, i)
            end
            keys = k
         end
         return self:get(keys)
      elseif (req.op == ffi.C.MC_GETS) then
         local keys = ffi.string(req.key, req.key_len)
         if (req.key_count > 1) then
            local k = {};
            for i in string.gmatch(keys, '%S+') do
               table.insert(k, i)
            end
            keys = k
         end
         return self:get(keys)
      elseif (req.op == ffi.C.MC_DELETE) then
         return self:cas(ffi.string(req.key, req.key_len),
                           tonumber(req.noreply))
      elseif (req.op == ffi.C.MC_INCR) then
         return self:incr(ffi.string(req.key, req.key_len),
                           tonumber(req.inc_val), tonumber(req.noreply))
      elseif (req.op == ffi.C.MC_DECR) then
         return self:decr(ffi.string(req.key, req.key_len),
                           tonumber(req.inc_val), tonumber(req.noreply))
      elseif (req.op == ffi.C.MC_FLUSH) then
         return self:flush()
      elseif (req.op == ffi.C.MC_STATS) then
         -- Stat cmd currently is not implemented
         return self:stats()
      elseif (req.op == ffi.C.MC_VERSION) then
         return self:version()
      elseif (req.op == ffi.C.MC_QUIT) then
         return nil
      else
         log.info('Processing undefined command')
         return nil
      end
   end,

   stat_incr = function(self, stat)
      self.stats[stat] = self.stats[stat] + 1
   end,

   get_tuple_or_expire = function(self, key)
      local tuple = self.mcs:get{key}
      if tuple == nil then return 'none' end
      local time, etime, ptime = math.floor(fiber.time()), tuple[3], tuple[6]
      -- check for invalidation
      if ((ptime <= self.flush and self.flush <= time) or
          (etime <= time and etime ~= 0)) then
         -- invalidate and free
         self.mcs:delete{key}
         self:stat_incr('expired_runtime')
         return 'expired'
      end
      return tuple
   end,

   normalize_exptime = function(self, exptime)
      exptime = exptime or 0
      if type(exptime) ~= 'number' then exptime = 0 end
      if (exptime < (30*24*60*60) and exptime > 0) then
         exptime = math.floor(fiber.time()) + exptime
      end
      return exptime
   end,

   ----
   -- store this data
   ----
   set = function(self, key, value, exptime, flags, noreply)
      -- Check arguments
      assert(key ~= nil and value ~= nil)
      key = tostring(key)
      value = tostring(value)
      exptime = self:normalize_exptime(exptime)
      flags = flags or 0
      if type(flags) ~= 'number' or flags < 0 or flags > 4294967296 then
         return 'CLIENT_ERROR Bad flags value\r\n'
      end
      if noreply == nil or type(noreply) ~= 'boolean' then noreply = false end
      -- Execute requests
      local cas = self.cas; self.cas = self.cas + 1
      local ptime = math.floor(fiber.time())
      self:stat_incr('cmd_set')
      self.mcs:replace{key, value, exptime, flags, cas, ptime}
      if noreply == true then return '' end
      return 'STORED\r\n'
   end,

   ----
   -- store this data, but only if the server *doesn't* already
   -- hold data for this key
   ----
   add = function(self, key, value, exptime, flags, noreply)
      -- Check arguments
      assert(key ~= nil and value ~= nil)
      key = tostring(key)
      value = tostring(value)
      exptime = self:normalize_exptime(exptime)
      flags = flags or 0
      if type(flags) ~= 'number' or flags < 0 or flags > 4294967296 then
         return 'CLIENT_ERROR Bad flags value\r\n'
      end
      if noreply == nil or type(noreply) ~= 'boolean' then noreply = false end
      -- Execute requests
      local cas = self.cas; self.cas = self.cas + 1
      local ptime = math.floor(fiber.time())
      self:stat_incr('cmd_set')
      box.begin()
      local tuple = self:get_tuple_or_expire(key)
      if type(tuple) == 'string' then
         self.mcs:replace{key, value, exptime, flags, cas, ptime}
      end
      box.commit()
      if noreply == true then return '' end
      if tuple ~= nil then return 'NOT_STORED\r\n' end
      return 'STORED\r\n'
   end,

   ----
   -- store this data, but only if the server *does*
   -- already hold data for this key
   ----
   replace = function(self, key, value, exptime, flags, noreply)
      -- Check arguments
      -- if key == nil then return 'CLIENT_ERROR Bad key\r\n' end
      -- if value == nil then return 'CLIENT_ERROR Bad value\r\n' end
      -- exptime = exptime or 0
      -- if type(exptime) ~= 'number' or (exptime < (24*60*60) and exptime > 0) then
         -- exptime = math.floor(fiber.time()) + exptime
      -- end
      -- flags = flags or 0
      -- if type(flags) ~= 'number' or flags < 0 or flags > 4294967296 then
         -- return 'CLIENT_ERROR Bad flags value\r\n'
      -- end
      -- if noreply == nil then noreply = false end
      -- if type(noreply) ~= 'boolean' then
         -- return 'CLIENT_ERROR Bad type of noreply flag\r\n'
      -- end
      -- Execute requests
      local cas = self.cas; self.cas = self.cas + 1
      local ptime = math.floor(fiber.time())
      self:stat_incr('cmd_set')
      box.begin()
      local tuple = self:get_tuple_or_expire(key)
      if type(tuple) ~= 'string' then
         self.mcs:replace{key, value, exptime, flags, cas, ptime}
      end
      box.commit()
      if noreply == true then return '' end
      if tuple == nil then return 'NOT_STORED\r\n' end
      return 'STORED\r\n'
   end,

   append = function(self, key, value, exptime, flags, noreply)
      -- Check arguments
      assert(key ~= nil and value ~= nil)
      key = tostring(key)
      value = tostring(value)
      exptime = self:normalize_exptime(exptime)
      flags = flags or 0
      if type(flags) ~= 'number' or flags < 0 or flags > 4294967296 then
         return 'CLIENT_ERROR Bad flags value\r\n'
      end
      if noreply == nil or type(noreply) ~= 'boolean' then noreply = false end
      -- Execute requests
      local cas = self.cas; self.cas = self.cas + 1
      self:stat_incr('cmd_set')
      box.begin()
      local tuple = self:get_tuple_or_expire(key)
      local t = self.mcs:update({key}, {
         {':', 2, -1, 0, value}, {'=', 5, cas}
      })
      box.commit()
      if noreply == true then return '' end
      if t == nil then return 'NOT_STORED\r\n' end
      return 'STORED\r\n'
   end,

   prepend = function(self, key, value, exptime, flags, noreply)
      -- Check arguments
      assert(key ~= nil and value ~= nil)
      key = tostring(key)
      value = tostring(value)
      exptime = self:normalize_exptime(exptime)
      flags = flags or 0
      if type(flags) ~= 'number' or flags < 0 or flags > 4294967296 then
         return 'CLIENT_ERROR Bad flags value\r\n'
      end
      if noreply == nil or type(noreply) ~= 'boolean' then noreply = false end
      -- Execute requests
      local cas = self.cas; self.cas = self.cas + 1
      self:stat_incr('cmd_set')
      box.begin()
      local tuple = self:get_tuple_or_expire(key)
      local t = self.mcs:update({key}, {
         {':', 2, 1, 0, value}, {'=', 5, cas}
      })
      box.commit()
      if noreply == true then return '' end
      if t == nil then return 'NOT_STORED\r\n' end
      return 'STORED\r\n'
   end,

   ----
   -- check and set operation which means "store this data but
   -- only if no one else has updated since I last fetched it."
   ----
   cas = function(self, key, value, cas, exptime, flags, noreply)
      -- Check arguments
      assert(key ~= nil and value ~= nil)
      key = tostring(key)
      value = tostring(value)
      exptime = self:normalize_exptime(exptime)
      flags = flags or 0
      if type(flags) ~= 'number' or flags < 0 or flags > 4294967296 then
         return 'CLIENT_ERROR Bad flags value\r\n'
      end
      if noreply == nil or type(noreply) ~= 'boolean' then noreply = false end
      -- Execute requests
      local ncas = self.cas; self.cas = self.cas + 1
      local ptime = math.floor(fiber.time())
      box.begin()
      local tuple = self:get_tuple_or_expire(key)
      if type(tuple) ~= 'string' then
         if tuple[5] ~= cas then
            self:stat_incr('cas_badval')
         else
            self:stat_incr('cas_hits')
            self.mcs:replace{key, value, exptime, flags, cas, ptime}
         end
      else
         self:stat_incr('cas_misses')
      end
      box.commit()

      if type(tuple) == 'string' then
         return 'NOT_FOUND\r\n'
      elseif tuple[5] ~= cas then
         return 'EXISTS\r\n'
      end
      return 'STORED\r\n'
   end,

   get = function(self, keys)
      -- Check arguments
      -- if keys == nil then return 'CLIENT_ERROR Bad key\r\n' end
      -- Execute requests
      local resp = ''
      if type(keys) ~= 'table' then keys = {keys} end
      box.begin()
      for _, key in ipairs(keys) do
         local t = self:get_tuple_or_expire(key)
         if (type(t) ~= 'string') then
            resp = resp .. string.format('VALUE %s %d %d \r\n%s\r\n',
               t[1], t[4], #t[2], t[2])
            self:stat_incr('get_hits')
         else
            self:stat_incr('get_misses')
         end
      end
      box.commit()
      return resp .. 'END\r\n'
   end,

   gets = function(self, keys)
      -- Check arguments
      -- if keys == nil then return 'CLIENT_ERROR Bad key\r\n' end
      -- Execute requests
      local resp = ''
      if type(keys) ~= 'table' then keys = {keys} end
      box.begin()
      for k, v in ipairs(keys) do
         local t = self:get_tuple_or_expire(key)
         if (type(t) ~= 'string') then
            resp = resp .. string.format('VALUE %s %d %d %d\r\n%s\r\n',
               t[1], t[4], #t[2], t[5], t[2])
            self:stat_incr('get_hits')
         else
            self:stat_incr('get_misses')
         end
      end
      box.commit()
      return resp .. 'END\r\n'
   end,

   delete = function(self, key, noreply)
      -- Check arguments
      -- if key == nil then return 'CLIENT_ERROR Bad key\r\n' end
      -- if noreply == nil then noreply = false end
      -- if type(noreply) ~= 'boolean' then
         -- return 'CLIENT_ERROR Bad type of noreply flag\r\n'
      -- end
      -- Execute requests
      box.begin()
      local tuple = self:get_tuple_or_expire(key)
      if type(tuple) == 'string' then
         self:stat_incr('delete_misses')
      else
         self.mcs:delete{key}
         self:stat_incr('delete_hits')
      end
      box.commit()
      if noreply == true then return '' end
      if err == nil then return 'NOT_FOUND\r\n' end
      return 'DELETED\r\n'
   end,

   incr = function(self, key, value, noreply)
      -- Check arguments
      -- if key == nil then return 'CLIENT_ERROR Bad key\r\n' end
      -- if value == nil then return 'CLIENT_ERROR Bad value\r\n' end
      -- if noreply == nil then noreply = false end
      -- if type(noreply) ~= 'boolean' then
         -- return 'CLIENT_ERROR Bad type of noreply flag\r\n'
      -- end
      -- Execute requests
      local cas = self.cas; self.cas = self.cas + 1
      box.begin()
      local t, tuple = self:get_tuple_or_expire(key)
      if type(tuple) == 'string' then
         self:stat_incr('incr_misses')
      else
         self:stat_incr('incr_hits')
         num = tonumber(t[2])
         if num == nil then
            return 'CLIENT_ERROR cannot increment or decrement non-numeric value\r\n'
         end
         num = tostring(num + value)
         t = self.mcs:update({key}, {{'=', 2, num}, {'=', 5, cas}})
      end
      box.commit()
      if noreply == true then return '' end
      if type(tuple) == 'string' then return 'NOT_FOUND\r\n' end
      return string.format('%d\r\n', t[2])
   end,

   decr = function(self, key, value, noreply)
      -- Check arguments
      -- if key == nil then return 'CLIENT_ERROR Bad key\r\n' end
      -- if value == nil then return 'CLIENT_ERROR Bad value\r\n' end
      -- if noreply == nil then noreply = false end
      -- if type(noreply) ~= 'boolean' then
         -- return 'CLIENT_ERROR Bad type of noreply flag\r\n'
      -- end
      -- Execute requests
      local cas = self.cas; self.cas = self.cas + 1
      box.begin()
      local t, tuple = self:get_tuple_or_expire(key)
      if type(tuple) == 'string' then
         self:stat_incr('decr_misses')
      else
         self:stat_incr('decr_hits')
         num = tonumber(t[2])
         if num == nil then
            return 'CLIENT_ERROR cannot increment or decrement non-numeric value\r\n'
         end
         num = tostring(num - value)
         t = self.mcs:update({key}, {{'=', 2, value}, {'=', 5, cas}})
      end
      box.commit()
      if noreply == true then return '' end
      if type(tuple) == 'string' then return 'NOT_FOUND\r\n' end
      return string.format('%d\r\n', t[2])
   end,

   touch = function(self, key, exptime, noreply)
      -- Check arguments
      -- if key == nil then return 'CLIENT_ERROR Bad key\r\n' end
      -- exptime = exptime or 0
      -- if type(exptime) ~= 'number' or (exptime < (24*60*60) and exptime > 0) then
         -- exptime = math.floor(fiber.time()) + exptime
      -- end
      -- if noreply == nil then noreply = false end
      -- if type(noreply) ~= 'boolean' then
         -- return 'CLIENT_ERROR Bad type of noreply flag\r\n'
      -- end
      -- Execute requests
      self:stat_incr('cmd_touch')
      box.begin()
      local t, tuple = self:get_tuple_or_expire(key)
      if type(tuple) == 'string' then
         self:stat_incr('touch_misses')
      else
         self:stat_incr('touch_hits')
         t = self.mcs:update({key}, {{'=', 3, exptime}})
      end
      box.commit()
      if noreply == true then return '' end
      if type(tuple) == 'string' then return 'NOT_FOUND\r\n' end
      return 'TOUCHED\r\n'
   end,
   flush_all = function(self, time)
      self:stat_incr('cmd_flush')
      self.flush = math.max(self.flush, self:normalize_exptime(time))
      return 'OK\r\n'
   end,
   stats = function(self, key)
      return ''
   end,
   version = function(self)
      return string.format('VERSION Tarantool Memcached %s', box.info().version)
   end,
   destroy = function(self)
   end
}

local function mc_is_expired(args, tuple)
   local time, etime, ptime = math.floor(fiber.time()), tuple[3], tuple[6]
   -- check for invalidation
   if ((ptime <= args.flush and args.flush <= time) or
         (etime <= time and etime ~= 0)) then
      return true
   end
   return false
end

local function mc_pr_expired(space_id, args, tuple)
   local key = fun.map(
      function(x) return tuple[x.fieldno] end,
      box.space[space_id].index[0].parts
   ):totable()

   args:stat_incr('expired_daemon')
   box.space[space_id]:delete(key)
end

memcache = {
   new = function(name)
      local self = setmetatable({}, { __index = memcache_mt })
      self.name = name
      if box.space[name] == nil then
         local mcs = box.schema.space.create(name)
         mcs:create_index('primary', {type='HASH', parts={1, 'STR'}})
      elseif box.space._schema:get{name} == nil then
         error('memcached: space is already used')
      end
      self.mcs = box.space[name]
      self.cas = 0
      self.flush = -1
      self.expd = 'memcached_' .. name
      expd.run_task(self.expd, name, mc_is_expired,
                    mc_pr_expired, self, 1024, 60*60)
      self.stats = {
         ['cmd_get'] = 0,
         ['cmd_set'] = 0,
         ['cmd_flush'] = 0,
         ['cmd_touch'] = 0,
         ['get_hits'] = 0,
         ['get_misses'] = 0,
         ['delete_hits'] = 0,
         ['delete_misses'] = 0,
         ['incr_hits'] = 0,
         ['incr_misses'] = 0,
         ['decr_hits'] = 0,
         ['decr_misses'] = 0,
         ['cas_hits'] = 0,
         ['cas_misses'] = 0,
         ['cas_badval'] = 0,
         ['touch_hits'] = 0,
         ['touch_misses'] = 0,
         ['expired_daemon'] = 0,
         ['expired_runtime'] = 0,
      }
      box.space._schema:put{name}
      return self
   end
}

box.cfg{}

a = memcache.new('mc')
a:init_loop(1200)
--log.info(a:set('test1', 'value1', 2))
--log.info(a:set('test3', '12', 4))
--log.info(a:append('test1', 'end'))
--log.info(a:prepend('test1', 'begin'))
--log.info(a:get({'test1', 'test2'}))
--log.info(a:incr('test3', 1))
--log.info(a:decr('test3', 2))
--log.info(a:flush_all(10))
--log.info(require('yaml').encode(a.stats))
