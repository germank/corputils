kt = __kyototycoon__
db = kt.db

-- log the start-up message
if kt.thid == 0 then
   kt.log("system", "the Lua script has been loaded")
end

-- atomically increment counts
function incrementbulk(inmap, outmap)
   --db:begin_transaction()
   for key, value in pairs(inmap) do
      if not db:increment(key, tonumber(value), 0) then
        return kt.RVEINTERNAL
      end
   end
   --db:end_transaction()
   return kt.RVSUCCESS
end

function incrementbulk2(inmap, outmap)
   local function visit(key, value)
      return key, value + inmap[key]
   end
   local keys = {}
   for key, value in pairs(inmap) do
      table.insert(keys, key)
   end
   if not db:accept_bulk(keys, visit) then
      return kt.RVEINTERNAL
   end
   return kt.RVSUCCESS
end

-- remove records at once
function incrementbulk3(inmap, outmap)
   db:begin_transaction()
   local keys = {}
   for key, value in pairs(inmap) do
      print (key)
      table.insert(keys, key)
   end
   local values = db:get_bulk(keys)
   for key,value in pairs(values) do
      if value == "" then
        value = 0
      end
      print ("*" .. tonumber(value) .. "*")
      print ("*" .. string.len(value) .. "*")
      print("*")
      for i=1,string.len(value) do
        print (value:byte(i))
      end
      print ("*")
      values[key] = tonumber(value) + inmap[key]
   end
   --[[
   num = db:set_bulk(values)
   if num < 0 then
       db:end_transaction()
      return kt.RVEINTERNAL
   end
   outmap["num"] = num
   db:end_transaction()]]--
   return kt.RVSUCCESS
end

-- list all records
function list(inmap, outmap)
   local cur = db:cursor()
   cur:jump()
   while true do
      local key, value, xt = cur:get(true)
      if not key then break end
      outmap[key] = value
   end
   return kt.RVSUCCESS
end
