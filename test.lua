#!/usr/local/bin/lua
local mysql = require 'mysql'
print(mysql.version());

local db, e = mysql.connect('127.0.0.1:3306', 'root', '123456')
if not db then
	print(e)
	os.exit(-1)
end
print(db:server_version())
print(db:server_info())
assert(db:set_charset("utf-8"))
assert(db:select_db('TestAudioDB'))

local rs, e = db:query("select zoneid, groupid, appmodel from `T_GROUP_INFO_TAB` where appmodel <= 4 and appmodel >=3 ")
if not rs then
	print(e)
	os.exit(-1)
end

print('rs:size',   rs:size())
rs:next()

print(rs:row(0))
print(rs:row(1))
print(rs:row(2))
print(rs:row('zoneid'))
print(rs:row('groupid'))
print(rs:row('appmodel'))

rs:seek(0)
print(table.unpack(rs:field()))
while rs:next() do
	print(table.unpack(rs:row()))
end
