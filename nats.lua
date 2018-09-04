local ngx_re = require "ngx.re"


local _M = {}


local mt = { __index = _M }


-- registration callback table
local r = {}


-- proto line re-use tables
local line_t = {}


local function random_string()
  return "foo"
end


local function parse_message(line)
  local res = ngx_re.split(line, [[\s+]], "oj", nil, nil, line_t)
  local msg = {}

  msg.type = string.upper(res[1])

  if msg.type == "MSG" then
    msg.subject = res[2]
    msg.sid = res[3]

    -- TODO check this logic
    if res[5] == nil then
      msg.len = tonumber(res[4])
    else
      msg.reply_to = res[4]
      msg.len = tonumber(res[5])
    end
  end

  return msg
end


local function validate_send(self)
  if self.verbose then
    local res, err = self.sock:receive()
    if err then
      return false, err
    end

    if res ~= "+OK" then
      local msg, err = self.sock:receive()
      if err then
        return false, err
      end

      return false, msg
    end
  end

  return true
end


function _M:subscribe(subject, callback, sid)
  if not self.connected then
    return false, "not connected"
  end

  if type(subject) ~= "string" then
    return false, "subject is not a string"
  end

  if type(callback) ~= "function" then
    return false, "callback is not a function"
  end

  if id and type(sid) ~= "string" then
    return false, "sid is not a string"
  end

  if not sid then
    sid = random_string()
  end

  if r[sid] then
    return false, "already subscribed with sid " .. sid
  end

  local _, err = self.sock:send(string.format("SUB %s %s\r\n", subject, sid))
  if err then
    return false, err
  end

  local ok, err = validate_send(self)
  if not ok then
    return false, err
  end

  r[sid] = callback
  return true
end


function _M:unsubscribe(sid)
  if not self.connected then
    return false, "not connected"
  end

  if type(sid) ~= "string" then
    return false, "sid is not a string"
  end

  local _, err = self.sock:send(string.format("UNSUB %s\r\n", sid))
  if err then
    return false, err
  end

  local ok, err = validate_send(self)
  if not ok then
    return false, err
  end

  r[sid] = nil

  return true
end


function _M:publish(subject, reply_to, payload)
  if not self.connected then
    return false, "not connected"
  end

  local msg
  if reply_to then
    msg = string.format("PUB %s %s %d\r\n%s\r\n", subject, reply_to, #payload, payload)
  else
    msg = string.format("PUB %s %d\r\n%s\r\n", subject, #payload, payload)
  end

  local _, err = self.socket:send(msg)
  if err then
    return false, err
  end

  return validate_send(self)
end


function _M:connect()
  self.sock:settimeout(self.timeout)

  local ok, err = self.sock:connect(self.host, self.port)
  if not ok then
    return false, err
  end

  -- initial INFO packet
  local data, err = self.sock:receive()
  if not data then
    return false, err
  end
  ngx.log(ngx.DEBUG, data)

  -- TODO authorize, CONNECT

  self.connected = true

  return true
end


local function watch(premature, self)
  if premature then
    return
  end

  while true do
    local data, err = self.sock:receive()
    if not data then
      ngx.log(ngx.WARN, err)
      break
    end

    local msg = parse_message(data)

    if msg.type == "PING" then
      local _, err = self.sock:send("PONG\r\n")
      if err then
        ngx.log(ngx.WARN, err)
        break
      end

      ngx.log(ngx.DEBUG, "ping-pong")
    elseif msg.type == "MSG" then
      local payload, err = self.sock:receive(msg.len)
      if not payload then
        ngx.log(ngx.WARN, err)
      end

      msg.payload = payload

      -- discard trailing newline
      local _, err = self.sock:receive(2)
      if err then
        ngx.log(ngx.WARN, err)
      end

      if r[msg.sid] then
        r[msg.sid](msg)
      end
    else
      ngx.log(ngx.WARN, "unknown message type " .. msg.type)
    end
  end
end


function _M:start_sub()
  if not self.connected then
    return false, "not connected"
  end

  --ngx.timer.at(0, watch, self)
watch(false, self)

  return true
end


function _M.new(opts)
  local m = {
    sock    = ngx.socket.tcp(),
    timeout = opts.timeout,
    host    = opts.host,
    port    = opts.port,
    verbose = opts.verbose,
  }

  return setmetatable(m, mt)
end

return _M
