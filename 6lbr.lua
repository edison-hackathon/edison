#!/usr/bin/env tarantool

local mqtt = require('mqtt')
local fiber = require('fiber')

local replication = require('replication')

local function say(msg)
  local yaml = require('yaml')
  require('log').error("%s", yaml.encode(msg))
end

conn = mqtt.new()

ok, msg = conn:on_message(function(mid, topic, payload)
  say({">>>>>>>>>>>>> ON_MESSAGE", mid, topic, payload})

  mac, measurement = string.match(topic, '(aaaa::%w+:%w+:%w+:%w+)/(.*)')

  if measurement == 'sht21/temperature/measure' then
    replication.devices:replace{mac, 'temperature', payload}
  elseif measurement == 'sht21/humidity/measure' then
    replication.devices:replace{mac, 'humidity', payload}
  elseif measurement == 'rs485/rx' then
    replication.devices:replace{mac, 'latlon',string.sub(payload, 0, -2)}
  end

end)

ok, msg = conn:connect({host="0.0.0.0", port=1883})
say({'CONNETED', ok, msg})

ok, emsg = conn:subscribe('devices/#')
say({'SUBSCRIBE', ok, emsg})

fiber.create(function()
  while true do

    --[[
    ok, emsg = conn:publish("devices/Edison/get", "1")
    --]]

    
    ok, emsg = conn:publish("devices/Edison/aaaa::212:4b00:939:3b90/sht21/temperature/get", "1")
    say({'publish -> get temperature(1)', ok, emsg})

    ok, emsg = conn:publish("devices/Edison/aaaa::212:4b00:939:3b90/sht21/humidity/get", "1")
    say({'publish -> get humidity(1)', ok, emsg})

    ok, emsg = conn:publish("devices/Edison/aaaa::212:4b00:939:3b90/rs485/tx", "1")
    say({'publish -> get humidity(1)', ok, emsg})

    fiber.sleep(2) 

    ok, emsg = conn:publish("devices/Edison/aaaa::212:4b00:939:3356/sht21/temperature/get", "1")
    say({'publish -> get temperature(1)', ok, emsg})                            
                                                                                
    ok, emsg = conn:publish("devices/Edison/aaaa::212:4b00:939:3356/sht21/humidity/get", "1")
    say({'publish -> get humidity(1)', ok, emsg})                               
                                                                                
    ok, emsg = conn:publish("devices/Edison/aaaa::212:4b00:939:3356/rs485/tx", "1")
    say({'publish -> get humidity(1)', ok, emsg}) 

    fiber.sleep(2) 

    ok, emsg = conn:publish("devices/Edison/aaaa::212:4b00:939:360a/sht21/temperature/get", "1")
    say({'publish -> get temperature(1)', ok, emsg})                            
                                                                                
    ok, emsg = conn:publish("devices/Edison/aaaa::212:4b00:939:360a/sht21/humidity", "1")
    say({'publish -> get humidity(1)', ok, emsg})                               
                                                                                
    ok, emsg = conn:publish("devices/Edison/aaaa::212:4b00:939:360a/rs485/tx", "1")
    say({'publish -> get humidity(1)', ok, emsg}) 

    fiber.sleep(2) 

    ok, emsg = conn:publish("devices/Edison/aaaa::212:4b00:939:336b/sht21/temperature/get", "1")
    say({'publish -> get temperature(1)', ok, emsg})                            
                                                                                
    ok, emsg = conn:publish("devices/Edison/aaaa::212:4b00:939:336b/sht21/humidity/get", "1")
    say({'publish -> get humidity(1)', ok, emsg})                               
                                                                                
    ok, emsg = conn:publish("devices/Edison/aaaa::212:4b00:939:336b/rs485/tx", "1")
    say({'publish -> get humidity(1)', ok, emsg}) 

    fiber.sleep(2)
  end
end)

