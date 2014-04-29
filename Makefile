# luamysql

CC= gcc
RM= rm -f
MAKE= make
LUAMYSQL= mysql.so
DRIVER_LIBS= /usr/lib64/mysql
DRIVER_INCS= /usr/include/mysql
INSTALL_PATH= /usr/local/lib/lua/5.2

$(LUAMYSQL): luamysql.c
	$(CC) -o $(LUAMYSQL) -shared luamysql.c -I/usr/include/lua/5.2 -I$(DRIVER_INCS) -O3 -Wall -fPIC -llua -lmysqlclient_r -lz -L$(DRIVER_LIBS)

install: $(LUAMYSQL)
	$(MAKE) test
	install -s $(LUAMYSQL) $(INSTALL_PATH)

clean:
	$(RM) $(LUAMYSQL)

test: $(LUAMYSQL) test.lua
	lua test.lua

all: $(LUAMYSQL)
