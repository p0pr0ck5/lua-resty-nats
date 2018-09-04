local nats = require "nats"
local n = nats.new({
  host = "127.0.0.1",
  port = 4222,
  timeout = 300000,
  verbose = true,
})

local ok, err = n:connect()
if not ok then
  error(err)
end

ok, err = n:subscribe("foo.*", function(msg)
  print("i got " .. msg.payload)
end)
if not ok then
  error(err)
end

n:start_sub()
