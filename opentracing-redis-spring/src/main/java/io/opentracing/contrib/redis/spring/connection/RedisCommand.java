/*
 * Copyright 2017-2018 The OpenTracing Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.opentracing.contrib.redis.spring.connection;

/**
 * A list of Redis comands.
 *
 * Redis command list (some commands which are not in this page but they exists in the Java API were added):
 * https://redis.io/commands
 *
 * @author Daniel del Castillo
 */
final class RedisCommand {

  static final String APPEND = "APPEND";
  static final String AUTH = "AUTH";
  static final String BGREWRITEAOF = "BGREWRITEAOF";
  static final String BGSAVE = "BGSAVE";
  static final String BGWRITEAOF = "BGWRITEAOF";
  static final String BITCOUNT = "BITCOUNT";
  static final String BITFIELD = "BITFIELD";
  static final String BITOP = "BITOP";
  static final String BITPOS = "BITPOS";
  static final String BLPOP = "BLPOP";
  static final String BRPOP = "BRPOP";
  static final String BRPOPLPUSH = "BRPOPLPUSH";
  static final String BZPOPMIN = "BZPOPMIN";
  static final String BZPOPMAX = "BZPOPMAX";
  static final String CLIENT_KILL = "CLIENT KILL";
  static final String CLIENT_LIST = "CLIENT LIST";
  static final String CLIENT_GETNAME = "CLIENT GETNAME";
  static final String CLIENT_PAUSE = "CLIENT PAUSE";
  static final String CLIENT_REPLY = "CLIENT REPLY";
  static final String CLIENT_SETNAME = "CLIENT SETNAME";
  static final String CLUSTER_ADDSLOTS = "CLUSTER ADDSLOTS";
  static final String CLUSTER_COUNT_FAILURE_REPORTS = "CLUSTER COUNT-FAILURE-REPORTS";
  static final String CLUSTER_COUNTKEYSINSLOT = "CLUSTER COUNTKEYSINSLOT";
  static final String CLUSTER_DELSLOTS = "CLUSTER DELSLOTS";
  static final String CLUSTER_FAILOVER = "CLUSTER FAILOVER";
  static final String CLUSTER_FORGET = "CLUSTER FORGET";
  static final String CLUSTER_GETKEYSINSLOT = "CLUSTER GETKEYSINSLOT";
  static final String CLUSTER_INFO = "CLUSTER INFO";
  static final String CLUSTER_KEYSLOT = "CLUSTER KEYSLOT";
  static final String CLUSTER_MASTER_SLAVE_MAP = "CLUSTER MASTER-SLAVE-MAP";
  static final String CLUSTER_MEET = "CLUSTER MEET";
  static final String CLUSTER_NODES = "CLUSTER NODES";
  static final String CLUSTER_NODE_FOR_KEY = "CLUSTER_NODE_FOR_KEY";
  static final String CLUSTER_NODE_FOR_SLOT = "CLUSTER_NODE_FOR_SLOT";
  static final String CLUSTER_REPLICATE = "CLUSTER REPLICATE";
  static final String CLUSTER_RESET = "CLUSTER RESET";
  static final String CLUSTER_SAVECONFIG = "CLUSTER SAVECONFIG";
  static final String CLUSTER_SET_CONFIG_EPOCH = "CLUSTER SET-CONFIG-EPOCH";
  static final String CLUSTER_SETSLOT = "CLUSTER SETSLOT";
  static final String CLUSTER_SLAVES = "CLUSTER SLAVES";
  static final String CLUSTER_REPLICAS = "CLUSTER REPLICAS";
  static final String CLUSTER_SLOTS = "CLUSTER SLOTS";
  static final String COMMAND = "COMMAND";
  static final String COMMAND_COUNT = "COMMAND COUNT";
  static final String COMMAND_GETKEYS = "COMMAND GETKEYS";
  static final String COMMAND_INFO = "COMMAND INFO";
  static final String CONFIG_GET = "CONFIG GET";
  static final String CONFIG_REWRITE = "CONFIG REWRITE";
  static final String CONFIG_SET = "CONFIG SET";
  static final String CONFIG_RESETSTAT = "CONFIG RESETSTAT";
  static final String DBSIZE = "DBSIZE";
  static final String DEBUG_OBJECT = "DEBUG OBJECT";
  static final String DEBUG_SEGFAULT = "DEBUG SEGFAULT";
  static final String DECR = "DECR";
  static final String DECRBY = "DECRBY";
  static final String DEL = "DEL";
  static final String DISCARD = "DISCARD";
  static final String DUMP = "DUMP";
  static final String ECHO = "ECHO";
  static final String EVAL = "EVAL";
  static final String EVALSHA = "EVALSHA";
  static final String EXEC = "EXEC";
  static final String EXISTS = "EXISTS";
  static final String EXPIRE = "EXPIRE";
  static final String EXPIREAT = "EXPIREAT";
  static final String FLUSHALL = "FLUSHALL";
  static final String FLUSHDB = "FLUSHDB";
  static final String GEOADD = "GEOADD";
  static final String GEOHASH = "GEOHASH";
  static final String GEOPOS = "GEOPOS";
  static final String GEODIST = "GEODIST";
  static final String GEORADIUS = "GEORADIUS";
  static final String GEORADIUSBYMEMBER = "GEORADIUSBYMEMBER";
  static final String GEOREMOVE = "GEOREMOVE";
  static final String GET = "GET";
  static final String GETBIT = "GETBIT";
  static final String GETRANGE = "GETRANGE";
  static final String GETSET = "GETSET";
  static final String HDEL = "HDEL";
  static final String HEXISTS = "HEXISTS";
  static final String HGET = "HGET";
  static final String HGETALL = "HGETALL";
  static final String HINCRBY = "HINCRBY";
  static final String HINCRBYFLOAT = "HINCRBYFLOAT";
  static final String HKEYS = "HKEYS";
  static final String HLEN = "HLEN";
  static final String HMGET = "HMGET";
  static final String HMSET = "HMSET";
  static final String HSET = "HSET";
  static final String HSETNX = "HSETNX";
  static final String HSTRLEN = "HSTRLEN";
  static final String HVALS = "HVALS";
  static final String INCR = "INCR";
  static final String INCRBY = "INCRBY";
  static final String INCRBYFLOAT = "INCRBYFLOAT";
  static final String INFO = "INFO";
  static final String KEYS = "KEYS";
  static final String LASTSAVE = "LASTSAVE";
  static final String LINDEX = "LINDEX";
  static final String LINSERT = "LINSERT";
  static final String LLEN = "LLEN";
  static final String LPOP = "LPOP";
  static final String LPUSH = "LPUSH";
  static final String LPUSHX = "LPUSHX";
  static final String LRANGE = "LRANGE";
  static final String LREM = "LREM";
  static final String LSET = "LSET";
  static final String LTRIM = "LTRIM";
  static final String MEMORY_DOCTOR = "MEMORY DOCTOR";
  static final String MEMORY_HELP = "MEMORY HELP";
  static final String MEMORY_MALLOC_STATS = "MEMORY MALLOC-STATS";
  static final String MEMORY_PURGE = "MEMORY PURGE";
  static final String MEMORY_STATS = "MEMORY STATS";
  static final String MEMORY_USAGE = "MEMORY USAGE";
  static final String MGET = "MGET";
  static final String MIGRATE = "MIGRATE";
  static final String MONITOR = "MONITOR";
  static final String MOVE = "MOVE";
  static final String MSET = "MSET";
  static final String MSETNX = "MSETNX";
  static final String MULTI = "MULTI";
  static final String OBJECT = "OBJECT";
  static final String PERSIST = "PERSIST";
  static final String PEXPIRE = "PEXPIRE";
  static final String PEXPIREAT = "PEXPIREAT";
  static final String PFADD = "PFADD";
  static final String PFCOUNT = "PFCOUNT";
  static final String PFMERGE = "PFMERGE";
  static final String PING = "PING";
  static final String PSETEX = "PSETEX";
  static final String PSUBSCRIBE = "PSUBSCRIBE";
  static final String PUBSUB = "PUBSUB";
  static final String PTTL = "PTTL";
  static final String PUBLISH = "PUBLISH";
  static final String PUNSUBSCRIBE = "PUNSUBSCRIBE";
  static final String QUIT = "QUIT";
  static final String RANDOMKEY = "RANDOMKEY";
  static final String READONLY = "READONLY";
  static final String READWRITE = "READWRITE";
  static final String RENAME = "RENAME";
  static final String RENAMENX = "RENAMENX";
  static final String RESTORE = "RESTORE";
  static final String ROLE = "ROLE";
  static final String RPOP = "RPOP";
  static final String RPOPLPUSH = "RPOPLPUSH";
  static final String RPUSH = "RPUSH";
  static final String RPUSHX = "RPUSHX";
  static final String SADD = "SADD";
  static final String SAVE = "SAVE";
  static final String SCARD = "SCARD";
  static final String SCRIPT_DEBUG = "SCRIPT DEBUG";
  static final String SCRIPT_EXISTS = "SCRIPT EXISTS";
  static final String SCRIPT_FLUSH = "SCRIPT FLUSH";
  static final String SCRIPT_KILL = "SCRIPT KILL";
  static final String SCRIPT_LOAD = "SCRIPT LOAD";
  static final String SDIFF = "SDIFF";
  static final String SDIFFSTORE = "SDIFFSTORE";
  static final String SELECT = "SELECT";
  static final String SET = "SET";
  static final String SETBIT = "SETBIT";
  static final String SETEX = "SETEX";
  static final String SETNX = "SETNX";
  static final String SETRANGE = "SETRANGE";
  static final String SHUTDOWN = "SHUTDOWN";
  static final String SINTER = "SINTER";
  static final String SINTERSTORE = "SINTERSTORE";
  static final String SISMEMBER = "SISMEMBER";
  static final String SLAVEOF = "SLAVEOF";
  static final String SLAVEOFNOONE = "SLAVEOFNOONE";
  static final String REPLICAOF = "REPLICAOF";
  static final String SLOWLOG = "SLOWLOG";
  static final String SMEMBERS = "SMEMBERS";
  static final String SMOVE = "SMOVE";
  static final String SORT = "SORT";
  static final String SPOP = "SPOP";
  static final String SRANDMEMBER = "SRANDMEMBER";
  static final String SREM = "SREM";
  static final String STRLEN = "STRLEN";
  static final String SUBSCRIBE = "SUBSCRIBE";
  static final String SUNION = "SUNION";
  static final String SUNIONSTORE = "SUNIONSTORE";
  static final String SWAPDB = "SWAPDB";
  static final String SYNC = "SYNC";
  static final String TIME = "TIME";
  static final String TOUCH = "TOUCH";
  static final String TTL = "TTL";
  static final String TYPE = "TYPE";
  static final String UNSUBSCRIBE = "UNSUBSCRIBE";
  static final String UNLINK = "UNLINK";
  static final String UNWATCH = "UNWATCH";
  static final String WAIT = "WAIT";
  static final String WATCH = "WATCH";
  static final String ZADD = "ZADD";
  static final String ZCARD = "ZCARD";
  static final String ZCOUNT = "ZCOUNT";
  static final String ZINCRBY = "ZINCRBY";
  static final String ZINTERSTORE = "ZINTERSTORE";
  static final String ZLEXCOUNT = "ZLEXCOUNT";
  static final String ZPOPMAX = "ZPOPMAX";
  static final String ZPOPMIN = "ZPOPMIN";
  static final String ZRANGE = "ZRANGE";
  static final String ZRANGE_WITHSCORES = "ZRANGE WITHSCORES";
  static final String ZRANGEBYLEX = "ZRANGEBYLEX";
  static final String ZREVRANGEBYLEX = "ZREVRANGEBYLEX";
  static final String ZRANGEBYSCORE = "ZRANGEBYSCORE";
  static final String ZRANGEBYSCORE_WITHSCORES = "ZRANGEBYSCORE WITHSCORES";
  static final String ZRANK = "ZRANK";
  static final String ZREM = "ZREM";
  static final String ZREMRANGE = "ZREMRANGE";
  static final String ZREMRANGEBYLEX = "ZREMRANGEBYLEX";
  static final String ZREMRANGEBYRANK = "ZREMRANGEBYRANK";
  static final String ZREMRANGEBYSCORE = "ZREMRANGEBYSCORE";
  static final String ZREVRANGE = "ZREVRANGE";
  static final String ZREVRANGE_WITHSCORES = "ZREVRANGE WITHSCORES";
  static final String ZREVRANGEBYSCORE = "ZREVRANGEBYSCORE";
  static final String ZREVRANGEBYSCORE_WITHSCORES = "ZREVRANGEBYSCORE WITHSCORES";
  static final String ZREVRANK = "ZREVRANK";
  static final String ZSCORE = "ZSCORE";
  static final String ZUNIONSTORE = "ZUNIONSTORE";
  static final String SCAN = "SCAN";
  static final String SSCAN = "SSCAN";
  static final String HSCAN = "HSCAN";
  static final String ZSCAN = "ZSCAN";
  static final String XADD = "XADD";
  static final String XRANGE = "XRANGE";
  static final String XREVRANGE = "XREVRANGE";
  static final String XLEN = "XLEN";
  static final String XREAD = "XREAD";
  static final String XREADGROUP = "XREADGROUP";
  static final String XPENDING = "XPENDING";

  private RedisCommand() {
  }

}
