#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#if defined WIN32
#include <winsock2.h>
#define NO_CLIENT_LONG_LONG
#endif

#define LUA_LIB

#include <mysql.h>

#if defined _MSC_VER
#	include <lua.hpp>
#	include <malloc.h>
#	define dynarray(type, name, size) type* name = (type*)_alloca((size) * sizeof(type))
#else
#	include <lua.h>
#	include <lauxlib.h>
#	define dynarray(type, name, size) type name[size]
#endif

#define LUA_MYSQL_USE_RESULT    0
#define LUA_MYSQL_STORE_RESULT  1

#define LUA_MYSQL_CONN "mysql connection"
#define LUA_MYSQL_RES "mysql result"
#define LUA_MYSQL_TABLENAME "mysql"

typedef struct {
    short   closed;
    MYSQL  *conn;
} lua_mysql_conn;

typedef struct {
    short          closed;
    int            field;
    MYSQL_ROW      row;
    unsigned long *lengths;
	unsigned int   numcols;
    MYSQL_RES     *res;
} lua_mysql_res;

#if MYSQL_VERSION_ID < 32200
static int
mysql_real_query(MYSQL *mysql, const char *query, unsigned long length) {
    return mysql_query(mysql, query);
}

static int
mysql_real_connect(MYSQL *mysql,const char *host, const char *user, const char *passwd, const char *db, unsigned int port, const char *unix_socket,unsigned long client_flag) {
    mysql_port = port;
    return mysql_connect(mysql, host, user, passwd);
}
#endif

static int
lm_error(lua_State *L, const char *m) {
    lua_pushnil(L);
    lua_pushstring(L, m);
    return 2;
}

static void
lm_pushvalue(lua_State *L, void *row, long int len) {
    if (row == NULL)
        lua_pushnil(L);
    else
        lua_pushlstring(L, (const char*)row, len);
}

static int
lm_fetch_field(lua_State *L, lua_mysql_res *my_res) {
    MYSQL_RES *res = my_res->res;
    if (my_res->field == LUA_NOREF) {
        MYSQL_FIELD *field;
        size_t i;
        lua_newtable(L);
        mysql_field_seek(res, 0);
        for (field = mysql_fetch_field(res), i = 1; field; field = mysql_fetch_field(res), i++) {
            lm_pushvalue(L, field->name, field->name_length);
            lua_pushunsigned(L, i);
            lua_rawset(L, -3);
        }
        my_res->field = luaL_ref(L, LUA_REGISTRYINDEX);
    }
    lua_rawgeti(L, LUA_REGISTRYINDEX, my_res->field);
    return 1;
}

static lua_mysql_conn *
lm_get_conn(lua_State *L) {
    lua_mysql_conn *my_conn = (lua_mysql_conn *)luaL_checkudata(L, 1, LUA_MYSQL_CONN);
    luaL_argcheck(L, my_conn != NULL, 1, "connection expected");
    luaL_argcheck(L, !my_conn->closed, 1, "connection is closed");
    return my_conn;
}

static lua_mysql_res *
lm_get_res(lua_State *L) {
    lua_mysql_res *my_res = (lua_mysql_res *)luaL_checkudata(L, 1, LUA_MYSQL_RES);
    luaL_argcheck(L, my_res != NULL, 1, "result expected");
    luaL_argcheck(L, !my_res->closed, 1, "result is closed");
    return my_res;
}

static int
Lmysql_version(lua_State *L) {
	int n = lua_gettop(L);
#ifdef LIBMYSQL_VERSION
    lua_pushfstring(L, "LIBMYSQL_VERSION     = %s\n", LIBMYSQL_VERSION);
#endif
    lua_pushfstring(L, "MYSQL_SERVER_VERSION = %s\n", MYSQL_SERVER_VERSION);
#ifdef MYSQL_BASE_VERSION
    lua_pushfstring(L, "MYSQL_BASE_VERSION   = %s\n", MYSQL_BASE_VERSION);
#endif
    lua_pushfstring(L, "PROTOCOL_VERSION     = %d\n", PROTOCOL_VERSION);
    lua_concat(L, lua_gettop(L) - n);
    return 1;
}

static int
Lmysql_connect(lua_State *L) {
    lua_mysql_conn *my_conn = (lua_mysql_conn *)lua_newuserdata(L, sizeof(lua_mysql_conn));
    luaL_setmetatable(L, LUA_MYSQL_CONN);

    const char *host_and_port_tmp = luaL_optstring(L, 1, NULL);
    const char *user = luaL_optstring(L, 2, NULL);
    const char *passwd = luaL_optstring(L, 3, NULL);

    MYSQL *conn = mysql_init(NULL);
    if (!conn) {
        return lm_error(L, "Error: mysql_init failed.");
    }

    char *host_and_port = strdup(host_and_port_tmp);
    char *host = NULL, *socket=NULL;
    int port = MYSQL_PORT;
    // parser : hostname:port:/path/to/socket or hostname:port or hostname:/path/to/socket or :/path/to/socket
    if (host_and_port && (strchr(host_and_port, ':'))) {
        char *tmp = strtok(host_and_port, ":");

        if (host_and_port[0] != ':') {
            host = tmp;
            tmp = strtok(NULL, ":");
        }

        if (tmp[0] != '/') {
            port = atoi(tmp);
            if ((tmp=strtok(NULL, ":"))) {
                socket = tmp;
            }
        } else {
            socket = tmp;
        }
    }
    else {
        host = host_and_port;
    }

    if (!mysql_real_connect(conn, host, user, passwd, NULL, port, socket, 0)) {
        mysql_close(conn);
        return lm_error(L, mysql_error(conn));
    }

    my_conn->closed = 0;
    my_conn->conn = conn;

    free(host_and_port);
    return 1;
}

static int
Lmysql_select_db(lua_State *L) {
    lua_mysql_conn *my_conn = lm_get_conn(L);
    const char *db = luaL_checkstring(L, 2);

    if (mysql_select_db(my_conn->conn, db) != 0) {
        return lm_error(L, mysql_error(my_conn->conn));
    }
    else {
        lua_pushboolean(L, 1);
        return 1;
    }
}

static int
Lmysql_set_charset(lua_State *L) {
    lua_mysql_conn *my_conn = lm_get_conn(L);
    const char *ncharset = luaL_checkstring(L, 2);
    char charset[1024];

    unsigned long version = mysql_get_server_version(my_conn->conn);

    size_t i = 0;
    const char *p;
    for (p = ncharset; *p && i < sizeof(charset) / sizeof(charset[0]); ++p) {
        if (*p != '-') {
            charset[i] = *p ;
            i++;
        }
    }
    charset[i] = 0;

    if (version > 41000) {
        const char *statement = lua_pushfstring(L, "SET character_set_connection=%s, character_set_results=%s, character_set_client=binary", charset, charset);
        if (mysql_real_query(my_conn->conn, statement, strlen(statement))) {
            return lm_error(L, mysql_error(my_conn->conn));
        }
    }
    else {
        if (mysql_set_character_set(my_conn->conn, charset)) {
            return lm_error(L, mysql_error(my_conn->conn));
        }
    }

    if (version > 50001) {
        if (mysql_real_query(my_conn->conn, "SET sql_mode=''", sizeof("SET sql_mode=''")-1)) {
            return lm_error(L, mysql_error(my_conn->conn));
        }
    }

    lua_pushboolean(L, 1);
    return 1;
}

static int
Lmysql_error(lua_State *L) {
    lua_pushstring(L, mysql_error(lm_get_conn(L)->conn));
    return 1;
}

static int
Lmysql_errno(lua_State *L) {
    lua_pushnumber(L, mysql_errno(lm_get_conn(L)->conn));
    return 1;
}

static int
Lmysql_get_server_version(lua_State *L) {
    lua_pushnumber(L, mysql_get_server_version(lm_get_conn(L)->conn));
    return 1;
}

static int
Lmysql_get_server_info(lua_State *L) {
    lua_pushstring(L, mysql_get_server_info(lm_get_conn(L)->conn));
    return 1;
}

static int
Lmysql_affected_rows(lua_State *L) {
    lua_pushnumber(L, mysql_affected_rows(lm_get_conn(L)->conn));
    return 1;
}

static int
Lmysql_do_query(lua_State *L, int use_store) {
    lua_mysql_conn *my_conn = lm_get_conn(L);
    size_t st_len = 0;
    const char *statement = luaL_checklstring(L, 2, &st_len);
    MYSQL_RES *res;

    if (mysql_real_query(my_conn->conn, statement, st_len)) {
        return lm_error(L, mysql_error(my_conn->conn));
    }
    else {
        if(use_store == LUA_MYSQL_USE_RESULT) {
            res = mysql_use_result(my_conn->conn);
        } else {
            res = mysql_store_result(my_conn->conn);
        }

        unsigned int num_cols = mysql_field_count(my_conn->conn);
        if (res) {
            lua_mysql_res *my_res = (lua_mysql_res *)lua_newuserdata(L, sizeof(lua_mysql_res));
            luaL_setmetatable(L, LUA_MYSQL_RES);
            my_res->closed = 0;
            my_res->numcols = num_cols;
            my_res->res = res;
            my_res->row = 0;
            my_res->lengths = 0;
            my_res->field = LUA_NOREF;
            return 1;
        }
        else {
            if (num_cols == 0) {
                lua_pushnumber(L, mysql_affected_rows(my_conn->conn));
                return 1;
            }
            else {
                return lm_error(L, mysql_error(my_conn->conn));
            }
        }
    }
}

static int
Lmysql_query(lua_State *L) {
    return Lmysql_do_query(L, LUA_MYSQL_STORE_RESULT);
}

static int
Lmysql_unbuffered_query(lua_State *L) {
    return Lmysql_do_query(L, LUA_MYSQL_USE_RESULT);
}

static int
Lmysql_insert_id(lua_State *L) {
    lua_pushnumber(L, mysql_insert_id(lm_get_conn(L)->conn));
    return 1;
}

static int
Lmysql_escape_string(lua_State *L) {
    size_t st_len = 0;
    const char *unescaped_string = luaL_checklstring(L, 1, &st_len);
	dynarray(char, to, st_len * 2 + 1);
    mysql_escape_string(to, unescaped_string, st_len);
    lua_pushstring(L, to);
    return 1;
}

static int
Lmysql_real_escape_string(lua_State *L) {
    lua_mysql_conn *my_conn = lm_get_conn(L);
    size_t st_len = 0;
    const char *unescaped_string = luaL_checklstring(L, 2, &st_len);
	dynarray(char, to, st_len * 2 + 1);
    mysql_real_escape_string(my_conn->conn, to, unescaped_string, st_len);
    lua_pushstring(L, to);
    return 1;
}

static int
Lmysql_rollback(lua_State *L) {
#if MYSQL_VERSION_ID < 40100
    return lm_error(L, "Error: Unsupported rollback.");
#else
    lua_mysql_conn *my_conn = lm_get_conn(L);
    if (mysql_rollback(my_conn->conn)) {
        return lm_error(L, mysql_error(my_conn->conn));
    }
    return 0;
#endif
}

static int
Lmysql_close(lua_State *L) {
    lua_mysql_conn *my_conn = lm_get_conn(L);
    luaL_argcheck(L, my_conn != NULL, 1, "connection expected");
    if (my_conn->closed) {
        lua_pushboolean(L, 0);
        return 1;
    }

    my_conn->closed = 1;
    mysql_close(my_conn->conn);
    lua_pushboolean(L, 1);
    return 1;
}

static int
Lmysql_tostring(lua_State *L) {
    lua_mysql_conn* my_conn = lm_get_conn(L);
    if (my_conn->closed)
        lua_pushstring(L, LUA_MYSQL_CONN " (closed)");
    else
        lua_pushfstring(L, LUA_MYSQL_CONN " (%p)", (void *)my_conn);
    return 1;
}

static int
Lmysql_result_next(lua_State *L) {
    lua_mysql_res *my_res = lm_get_res(L);
    my_res->row = mysql_fetch_row(my_res->res);
    if (my_res->row == NULL) {
        lua_pushboolean(L, 0);
    }
    else {
        my_res->lengths = mysql_fetch_lengths(my_res->res);
        lua_pushboolean(L, 1);
    }
    return 1;
}

static int
Lmysql_result_row(lua_State *L) {
    lua_mysql_res *my_res = lm_get_res(L);
    if (my_res->row == NULL) {
        return 0;
    }
    if (lua_gettop(L) == 1) {
        size_t i;
        lua_newtable(L);
        for (i = 0; i < my_res->numcols; i++) {
            lm_pushvalue(L, my_res->row[i], my_res->lengths[i]);
            lua_rawseti(L, -2, i+1);
        }
        return 1;
    }
    switch (lua_type(L, 2)) {
        case LUA_TNUMBER: {
            size_t n = lua_tounsigned(L, 2);
            if (n >= my_res->numcols) {
                return 0;
            }
            lm_pushvalue(L, my_res->row[n], my_res->lengths[n]);
            return 1;
        }
        case LUA_TSTRING: {
            lm_fetch_field(L, my_res);
            lua_pushvalue(L, 2);
            lua_rawget(L, -2);
            if (!lua_isnumber(L, -1)) {
                lua_pop(L, 2);
                return 0;
            }
            size_t n = lua_tounsigned(L, -1);
            lua_pop(L, 2);
            if (n < 1 || n > my_res->numcols) {
                return 0;
            }
            lm_pushvalue(L, my_res->row[n-1], my_res->lengths[n-1]);
            return 1;
        }
    }
    return 0;
}

static int
Lmysql_result_field(lua_State *L) {
    lua_mysql_res *my_res = lm_get_res(L);
    lm_fetch_field(L, my_res);
    lua_newtable(L);
    lua_pushnil(L);
    while (lua_next(L, -3)) {
        // -2 - key   - name
        // -1 - value - index
        lua_pushvalue(L, -2);
        lua_rawseti(L, -4, lua_tounsigned(L, -2));
        lua_pop(L, 1);
    }
    lua_remove(L, -2);
    return 1;
}

static int
Lmysql_result_seek(lua_State *L) {
    lua_Number offset = luaL_optnumber(L, 2, 0);
    mysql_data_seek(lm_get_res(L)->res, offset);
    return 0;
}

static int
Lmysql_result_size(lua_State *L) {
    lua_pushnumber(L, (lua_Number)mysql_num_rows(lm_get_res(L)->res));
    return 1;
}

static int
Lmysql_result_close(lua_State *L) {
    lua_mysql_res *my_res = (lua_mysql_res *)luaL_checkudata(L, 1, LUA_MYSQL_RES);
    luaL_argcheck(L, my_res != NULL, 1, "result expected");
    if (my_res->closed) {
        lua_pushboolean(L, 0);
        return 1;
    }

    my_res->closed = 1;
    mysql_free_result(my_res->res);
    luaL_unref(L, LUA_REGISTRYINDEX, my_res->field);

    lua_pushboolean(L, 1);
    return 1;
}

static int
Lmysql_result_tostring(lua_State *L) {
    lua_mysql_res* my_res = lm_get_res(L);
    if (my_res->closed)
        lua_pushstring(L, LUA_MYSQL_RES " (closed)");
    else
        lua_pushfstring(L, LUA_MYSQL_RES " (%p)", (void *)my_res);
    return 1;
}

LUAMOD_API int
luaopen_mysql(lua_State *L) {
    luaL_Reg driver[] = {
        { "connect", Lmysql_connect },
        { "escape_string", Lmysql_escape_string },
        { "version", Lmysql_version },
        { NULL, NULL },
    };

    luaL_Reg result_methods[] = {
        { "field", Lmysql_result_field },
        { "next", Lmysql_result_next },
        { "row", Lmysql_result_row },
        { "seek", Lmysql_result_seek },
        { "size", Lmysql_result_size },
        { "close", Lmysql_result_close },
        { "__gc", Lmysql_result_close },
        { "__tostring", Lmysql_result_tostring },
        { NULL, NULL }
    };

    luaL_Reg connection_methods[] = {
        { "error", Lmysql_error },
        { "errno", Lmysql_errno },
        { "select_db", Lmysql_select_db },
        { "insert_id", Lmysql_insert_id },
        { "set_charset", Lmysql_set_charset },
        { "affected_rows", Lmysql_affected_rows },
        { "server_info", Lmysql_get_server_info },
        { "server_version", Lmysql_get_server_version },
        { "real_escape_string", Lmysql_real_escape_string },
        { "query", Lmysql_query },
        { "unbuffered_query", Lmysql_unbuffered_query },
        { "rollback", Lmysql_rollback },
        { "close", Lmysql_close },
        { "__gc", Lmysql_close },
        { "__tostring", Lmysql_tostring },
        { NULL, NULL }
    };

    luaL_newmetatable(L, LUA_MYSQL_CONN);
    luaL_setfuncs(L, connection_methods, 0);
    lua_pushliteral(L, "__index");
    lua_pushvalue(L, -2);
    lua_rawset(L, -3);
    lua_pop(L, 1);

    luaL_newmetatable(L, LUA_MYSQL_RES);
    luaL_setfuncs(L, result_methods, 0);
    lua_pushliteral(L, "__index");
    lua_pushvalue(L, -2);
    lua_rawset(L, -3);
    lua_pop(L, 1);

    luaL_newmetatable(L, LUA_MYSQL_TABLENAME);
    luaL_setfuncs(L, driver, 0);
    return 1;
}
