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

import io.opentracing.Tracer;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.springframework.dao.DataAccessException;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metric;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.connection.RedisHashCommands;
import org.springframework.data.redis.connection.RedisHyperLogLogCommands;
import org.springframework.data.redis.connection.RedisKeyCommands;
import org.springframework.data.redis.connection.RedisListCommands;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.RedisPipelineException;
import org.springframework.data.redis.connection.RedisScriptingCommands;
import org.springframework.data.redis.connection.RedisSentinelConnection;
import org.springframework.data.redis.connection.RedisServerCommands;
import org.springframework.data.redis.connection.RedisSetCommands;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.connection.RedisZSetCommands;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.connection.SortParameters;
import org.springframework.data.redis.connection.Subscription;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.core.types.RedisClientInfo;

/**
 * OpenTracing instrumentation of a {@link RedisConnection}.
 *
 * @author Daniel del Castillo
 */
public class TracingRedisConnection implements RedisConnection {


  private final RedisConnection connection;
  private final boolean withActiveSpanOnly;
  private final Tracer tracer;

  public TracingRedisConnection(RedisConnection connection, boolean withActiveSpanOnly,
      Tracer tracer) {
    this.connection = connection;
    this.withActiveSpanOnly = withActiveSpanOnly;
    this.tracer = tracer;
  }

  <T> T doInScope(String command, Supplier<T> supplier) {
    return RedisTracingUtils.doInScope(command, supplier, withActiveSpanOnly, tracer);
  }

  void doInScope(String command, Runnable action) {
    doInScope(command, () -> {
      action.run();
      return null;
    });
  }

  @Override
  public RedisGeoCommands geoCommands() {
    return connection.geoCommands();
  }

  @Override
  public RedisHashCommands hashCommands() {
    return connection.hashCommands();
  }

  @Override
  public RedisHyperLogLogCommands hyperLogLogCommands() {
    return connection.hyperLogLogCommands();
  }

  @Override
  public RedisKeyCommands keyCommands() {
    return connection.keyCommands();
  }

  @Override
  public RedisListCommands listCommands() {
    return connection.listCommands();
  }

  @Override
  public RedisSetCommands setCommands() {
    return connection.setCommands();
  }

  @Override
  public RedisScriptingCommands scriptingCommands() {
    return connection.scriptingCommands();
  }

  @Override
  public RedisServerCommands serverCommands() {
    return connection.serverCommands();
  }

  @Override
  public RedisStringCommands stringCommands() {
    return connection.stringCommands();
  }

  @Override
  public RedisZSetCommands zSetCommands() {
    return connection.zSetCommands();
  }

  @Override
  public void close() throws DataAccessException {
    connection.close();
  }

  @Override
  public boolean isClosed() {
    return connection.isClosed();
  }

  @Override
  public Object getNativeConnection() {
    return connection.getNativeConnection();
  }

  @Override
  public boolean isQueueing() {
    return connection.isQueueing();
  }

  @Override
  public boolean isPipelined() {
    return connection.isPipelined();
  }

  @Override
  public void openPipeline() {
    connection.openPipeline();
  }

  @Override
  public List<Object> closePipeline() throws RedisPipelineException {
    return connection.closePipeline();
  }

  @Override
  public boolean isSubscribed() {
    return connection.isSubscribed();
  }

  @Override
  public Subscription getSubscription() {
    return connection.getSubscription();
  }

  @Override
  public RedisSentinelConnection getSentinelConnection() {
    return new TracingRedisSentinelConnection(connection.getSentinelConnection(),
        withActiveSpanOnly, tracer);
  }

  @Override
  public Object execute(String command, byte[]... args) {
    return doInScope(command, () -> connection.execute(command, args));
  }

  @Override
  public Boolean exists(byte[] key) {
    return doInScope(RedisCommand.EXISTS, () -> connection.exists(key));
  }

  @Override
  public Long del(byte[]... keys) {
    return doInScope(RedisCommand.DEL, () -> connection.del(keys));
  }

  @Override
  public DataType type(byte[] key) {
    return doInScope(RedisCommand.TYPE, () -> connection.type(key));
  }

  @Override
  public Set<byte[]> keys(byte[] pattern) {
    return doInScope(RedisCommand.KEYS, () -> connection.keys(pattern));
  }

  @Override
  public Cursor<byte[]> scan(ScanOptions options) {
    return doInScope(RedisCommand.SCAN, () -> connection.scan(options));
  }

  @Override
  public byte[] randomKey() {
    return doInScope(RedisCommand.RANDOMKEY, () -> connection.randomKey());
  }

  @Override
  public void rename(byte[] oldName, byte[] newName) {
    doInScope(RedisCommand.RENAME, () -> connection.rename(oldName, newName));
  }

  @Override
  public Boolean renameNX(byte[] oldName, byte[] newName) {
    return doInScope(RedisCommand.RENAMENX, () -> connection.renameNX(oldName, newName));
  }

  @Override
  public Boolean expire(byte[] key, long seconds) {
    return doInScope(RedisCommand.EXPIRE, () -> connection.expire(key, seconds));
  }

  @Override
  public Boolean pExpire(byte[] key, long millis) {
    return doInScope(RedisCommand.PEXPIRE, () -> connection.pExpire(key, millis));
  }

  @Override
  public Boolean expireAt(byte[] key, long unixTime) {
    return doInScope(RedisCommand.EXPIREAT, () -> connection.expireAt(key, unixTime));
  }

  @Override
  public Boolean pExpireAt(byte[] key, long unixTimeInMillis) {
    return doInScope(RedisCommand.PEXPIREAT, () -> connection.pExpireAt(key, unixTimeInMillis));
  }

  @Override
  public Boolean persist(byte[] key) {
    return doInScope(RedisCommand.PERSIST, () -> connection.persist(key));
  }

  @Override
  public Boolean move(byte[] key, int dbIndex) {
    return doInScope(RedisCommand.MOVE, () -> connection.move(key, dbIndex));
  }

  @Override
  public Long ttl(byte[] key) {
    return doInScope(RedisCommand.TTL, () -> connection.ttl(key));
  }

  @Override
  public Long ttl(byte[] key, TimeUnit timeUnit) {
    return doInScope(RedisCommand.TTL, () -> connection.ttl(key, timeUnit));
  }

  @Override
  public Long pTtl(byte[] key) {
    return doInScope(RedisCommand.PTTL, () -> connection.pTtl(key));
  }

  @Override
  public Long pTtl(byte[] key, TimeUnit timeUnit) {
    return doInScope(RedisCommand.PTTL, () -> connection.pTtl(key, timeUnit));
  }

  @Override
  public List<byte[]> sort(byte[] key, SortParameters params) {
    return doInScope(RedisCommand.SORT, () -> connection.sort(key, params));
  }

  @Override
  public Long sort(byte[] key, SortParameters params, byte[] storeKey) {
    return doInScope(RedisCommand.SORT, () -> connection.sort(key, params, storeKey));
  }

  @Override
  public byte[] dump(byte[] key) {
    return doInScope(RedisCommand.DUMP, () -> connection.dump(key));
  }

  @Override
  public void restore(byte[] key, long ttlInMillis, byte[] serializedValue) {
    doInScope(RedisCommand.RESTORE, () -> connection.restore(key, ttlInMillis, serializedValue));
  }

  @Override
  public byte[] get(byte[] key) {
    return doInScope(RedisCommand.GET, () -> connection.get(key));
  }

  @Override
  public byte[] getSet(byte[] key, byte[] value) {
    return doInScope(RedisCommand.GETSET, () -> connection.getSet(key, value));
  }

  @Override
  public List<byte[]> mGet(byte[]... keys) {
    return doInScope(RedisCommand.MGET, () -> connection.mGet(keys));
  }

  @Override
  public Boolean set(byte[] key, byte[] value) {
    return doInScope(RedisCommand.SET, () -> connection.set(key, value));
  }

  @Override
  public Boolean set(byte[] key, byte[] value, Expiration expiration, SetOption option) {
    return doInScope(RedisCommand.SET, () -> connection.set(key, value, expiration, option));
  }

  @Override
  public Boolean setNX(byte[] key, byte[] value) {
    return doInScope(RedisCommand.SETNX, () -> connection.setNX(key, value));
  }

  @Override
  public Boolean setEx(byte[] key, long seconds, byte[] value) {
    return doInScope(RedisCommand.SETEX, () -> connection.setEx(key, seconds, value));
  }

  @Override
  public Boolean pSetEx(byte[] key, long milliseconds, byte[] value) {
    return doInScope(RedisCommand.PSETEX, () -> connection.pSetEx(key, milliseconds, value));
  }

  @Override
  public Boolean mSet(Map<byte[], byte[]> tuple) {
    return doInScope(RedisCommand.MSET, () -> connection.mSet(tuple));
  }

  @Override
  public Boolean mSetNX(Map<byte[], byte[]> tuple) {
    return doInScope(RedisCommand.MSETNX, () -> connection.mSetNX(tuple));
  }

  @Override
  public Long incr(byte[] key) {
    return doInScope(RedisCommand.INCR, () -> connection.incr(key));
  }

  @Override
  public Long incrBy(byte[] key, long value) {
    return doInScope(RedisCommand.INCRBY, () -> connection.incrBy(key, value));
  }

  @Override
  public Double incrBy(byte[] key, double value) {
    return doInScope(RedisCommand.INCRBY, () -> connection.incrBy(key, value));
  }

  @Override
  public Long decr(byte[] key) {
    return doInScope(RedisCommand.DECR, () -> connection.decr(key));
  }

  @Override
  public Long decrBy(byte[] key, long value) {
    return doInScope(RedisCommand.DECRBY, () -> connection.decrBy(key, value));
  }

  @Override
  public Long append(byte[] key, byte[] value) {
    return doInScope(RedisCommand.APPEND, () -> connection.append(key, value));
  }

  @Override
  public byte[] getRange(byte[] key, long begin, long end) {
    return doInScope(RedisCommand.GETRANGE, () -> connection.getRange(key, begin, end));
  }

  @Override
  public void setRange(byte[] key, byte[] value, long offset) {
    doInScope(RedisCommand.SETRANGE, () -> connection.setRange(key, value, offset));
  }

  @Override
  public Boolean getBit(byte[] key, long offset) {
    return doInScope(RedisCommand.GETBIT, () -> connection.getBit(key, offset));
  }

  @Override
  public Boolean setBit(byte[] key, long offset, boolean value) {
    return doInScope(RedisCommand.SETBIT, () -> connection.setBit(key, offset, value));
  }

  @Override
  public Long bitCount(byte[] key) {
    return doInScope(RedisCommand.BITCOUNT, () -> connection.bitCount(key));
  }

  @Override
  public Long bitCount(byte[] key, long begin, long end) {
    return doInScope(RedisCommand.BITCOUNT, () -> connection.bitCount(key, begin, end));
  }

  @Override
  public Long bitOp(BitOperation op, byte[] destination, byte[]... keys) {
    return doInScope(RedisCommand.BITOP, () -> connection.bitOp(op, destination, keys));
  }

  @Override
  public Long strLen(byte[] key) {
    return doInScope(RedisCommand.STRLEN, () -> connection.strLen(key));
  }

  @Override
  public Long rPush(byte[] key, byte[]... values) {
    return doInScope(RedisCommand.RPUSH, () -> connection.rPush(key, values));
  }

  @Override
  public Long lPush(byte[] key, byte[]... values) {
    return doInScope(RedisCommand.LPUSH, () -> connection.lPush(key, values));
  }

  @Override
  public Long rPushX(byte[] key, byte[] value) {
    return doInScope(RedisCommand.RPUSHX, () -> connection.rPushX(key, value));
  }

  @Override
  public Long lPushX(byte[] key, byte[] value) {
    return doInScope(RedisCommand.LPUSHX, () -> connection.lPushX(key, value));
  }

  @Override
  public Long lLen(byte[] key) {
    return doInScope(RedisCommand.LLEN, () -> connection.lLen(key));
  }

  @Override
  public List<byte[]> lRange(byte[] key, long start, long end) {
    return doInScope(RedisCommand.LRANGE, () -> connection.lRange(key, start, end));
  }

  @Override
  public void lTrim(byte[] key, long start, long end) {
    doInScope(RedisCommand.LTRIM, () -> connection.lTrim(key, start, end));
  }

  @Override
  public byte[] lIndex(byte[] key, long index) {
    return doInScope(RedisCommand.LINDEX, () -> connection.lIndex(key, index));
  }

  @Override
  public Long lInsert(byte[] key, Position where, byte[] pivot, byte[] value) {
    return doInScope(RedisCommand.LINSERT, () -> connection.lInsert(key, where, pivot, value));
  }

  @Override
  public void lSet(byte[] key, long index, byte[] value) {
    doInScope(RedisCommand.LSET, () -> connection.lSet(key, index, value));
  }

  @Override
  public Long lRem(byte[] key, long count, byte[] value) {
    return doInScope(RedisCommand.LREM, () -> connection.lRem(key, count, value));
  }

  @Override
  public byte[] lPop(byte[] key) {
    return doInScope(RedisCommand.LPOP, () -> connection.lPop(key));
  }

  @Override
  public byte[] rPop(byte[] key) {
    return doInScope(RedisCommand.RPOP, () -> connection.rPop(key));
  }

  @Override
  public List<byte[]> bLPop(int timeout, byte[]... keys) {
    return doInScope(RedisCommand.BLPOP, () -> connection.bLPop(timeout, keys));
  }

  @Override
  public List<byte[]> bRPop(int timeout, byte[]... keys) {
    return doInScope(RedisCommand.BRPOP, () -> connection.bRPop(timeout, keys));
  }

  @Override
  public byte[] rPopLPush(byte[] srcKey, byte[] dstKey) {
    return doInScope(RedisCommand.RPOPLPUSH, () -> connection.rPopLPush(srcKey, dstKey));
  }

  @Override
  public byte[] bRPopLPush(int timeout, byte[] srcKey, byte[] dstKey) {
    return doInScope(RedisCommand.BRPOPLPUSH, () -> connection.bRPopLPush(timeout, srcKey, dstKey));
  }

  @Override
  public Long sAdd(byte[] key, byte[]... values) {
    return doInScope(RedisCommand.SADD, () -> connection.sAdd(key, values));
  }

  @Override
  public Long sRem(byte[] key, byte[]... values) {
    return doInScope(RedisCommand.SREM, () -> connection.sRem(key, values));
  }

  @Override
  public byte[] sPop(byte[] key) {
    return doInScope(RedisCommand.SPOP, () -> connection.sPop(key));
  }

  @Override
  public List<byte[]> sPop(byte[] key, long count) {
    return doInScope(RedisCommand.SPOP, () -> connection.sPop(key, count));
  }

  @Override
  public Boolean sMove(byte[] srcKey, byte[] destKey, byte[] value) {
    return doInScope(RedisCommand.SMOVE, () -> connection.sMove(srcKey, destKey, value));
  }

  @Override
  public Long sCard(byte[] key) {
    return doInScope(RedisCommand.SCARD, () -> connection.sCard(key));
  }

  @Override
  public Boolean sIsMember(byte[] key, byte[] value) {
    return doInScope(RedisCommand.SISMEMBER, () -> connection.sIsMember(key, value));
  }

  @Override
  public Set<byte[]> sInter(byte[]... keys) {
    return doInScope(RedisCommand.SINTER, () -> connection.sInter(keys));
  }

  @Override
  public Long sInterStore(byte[] destKey, byte[]... keys) {
    return doInScope(RedisCommand.SINTERSTORE, () -> connection.sInterStore(destKey, keys));
  }

  @Override
  public Set<byte[]> sUnion(byte[]... keys) {
    return doInScope(RedisCommand.SUNION, () -> connection.sUnion(keys));
  }

  @Override
  public Long sUnionStore(byte[] destKey, byte[]... keys) {
    return doInScope(RedisCommand.SUNIONSTORE, () -> connection.sUnionStore(destKey, keys));
  }

  @Override
  public Set<byte[]> sDiff(byte[]... keys) {
    return doInScope(RedisCommand.SDIFF, () -> connection.sDiff(keys));
  }

  @Override
  public Long sDiffStore(byte[] destKey, byte[]... keys) {
    return doInScope(RedisCommand.SDIFFSTORE, () -> connection.sDiffStore(destKey, keys));
  }

  @Override
  public Set<byte[]> sMembers(byte[] key) {
    return doInScope(RedisCommand.SMEMBERS, () -> connection.sMembers(key));
  }

  @Override
  public byte[] sRandMember(byte[] key) {
    return doInScope(RedisCommand.SRANDMEMBER, () -> connection.sRandMember(key));
  }

  @Override
  public List<byte[]> sRandMember(byte[] key, long count) {
    return doInScope(RedisCommand.SRANDMEMBER, () -> connection.sRandMember(key, count));
  }

  @Override
  public Cursor<byte[]> sScan(byte[] key, ScanOptions options) {
    return doInScope(RedisCommand.SSCAN, () -> connection.sScan(key, options));
  }

  @Override
  public Boolean zAdd(byte[] key, double score, byte[] value) {
    return doInScope(RedisCommand.ZADD, () -> connection.zAdd(key, score, value));
  }

  @Override
  public Long zAdd(byte[] key, Set<Tuple> tuples) {
    return doInScope(RedisCommand.ZADD, () -> connection.zAdd(key, tuples));
  }

  @Override
  public Long zRem(byte[] key, byte[]... values) {
    return doInScope(RedisCommand.ZREM, () -> connection.zRem(key, values));
  }

  @Override
  public Double zIncrBy(byte[] key, double increment, byte[] value) {
    return doInScope(RedisCommand.ZINCRBY, () -> connection.zIncrBy(key, increment, value));
  }

  @Override
  public Long zRank(byte[] key, byte[] value) {
    return doInScope(RedisCommand.ZRANK, () -> connection.zRank(key, value));
  }

  @Override
  public Long zRevRank(byte[] key, byte[] value) {
    return doInScope(RedisCommand.ZREVRANK, () -> connection.zRevRank(key, value));
  }

  @Override
  public Set<byte[]> zRange(byte[] key, long start, long end) {
    return doInScope(RedisCommand.ZRANGE, () -> connection.zRange(key, start, end));
  }

  @Override
  public Set<byte[]> zRevRange(byte[] key, long start, long end) {
    return doInScope(RedisCommand.ZREVRANGE, () -> connection.zRevRange(key, start, end));
  }

  @Override
  public Set<Tuple> zRevRangeWithScores(byte[] key, long start, long end) {
    return doInScope(RedisCommand.ZREVRANGE_WITHSCORES,
        () -> connection.zRevRangeWithScores(key, start, end));
  }

  @Override
  public Set<Tuple> zRangeWithScores(byte[] key, long start, long end) {
    return doInScope(RedisCommand.ZRANGE_WITHSCORES,
        () -> connection.zRangeWithScores(key, start, end));
  }

  @Override
  public Set<byte[]> zRangeByScore(byte[] key, double min, double max) {
    return doInScope(RedisCommand.ZRANGEBYSCORE, () -> connection.zRangeByScore(key, min, max));
  }

  @Override
  public Set<byte[]> zRangeByScore(byte[] key, double min, double max, long offset, long count) {
    return doInScope(RedisCommand.ZRANGEBYSCORE,
        () -> connection.zRangeByScore(key, min, max, offset, count));
  }

  @Override
  public Set<byte[]> zRangeByScore(byte[] key, String min, String max) {
    return doInScope(RedisCommand.ZRANGEBYSCORE, () -> connection.zRangeByScore(key, min, max));
  }

  @Override
  public Set<byte[]> zRangeByScore(byte[] key, Range range) {
    return doInScope(RedisCommand.ZRANGEBYSCORE, () -> connection.zRangeByScore(key, range));
  }

  @Override
  public Set<byte[]> zRangeByScore(byte[] key, String min, String max, long offset, long count) {
    return doInScope(RedisCommand.ZRANGEBYSCORE,
        () -> connection.zRangeByScore(key, min, max, offset, count));
  }

  @Override
  public Set<byte[]> zRangeByScore(byte[] key, Range range, Limit limit) {
    return doInScope(RedisCommand.ZRANGEBYSCORE, () -> connection.zRangeByScore(key, range, limit));
  }

  @Override
  public Set<Tuple> zRangeByScoreWithScores(byte[] key, Range range) {
    return doInScope(RedisCommand.ZRANGEBYSCORE_WITHSCORES,
        () -> connection.zRangeByScoreWithScores(key, range));
  }

  @Override
  public Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max) {
    return doInScope(RedisCommand.ZRANGEBYSCORE_WITHSCORES,
        () -> connection.zRangeByScoreWithScores(key, min, max));
  }

  @Override
  public Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max, long offset,
      long count) {
    return doInScope(RedisCommand.ZRANGEBYSCORE_WITHSCORES,
        () -> connection.zRangeByScoreWithScores(key, min, max, offset, count));
  }

  @Override
  public Set<Tuple> zRangeByScoreWithScores(byte[] key, Range range, Limit limit) {
    return doInScope(RedisCommand.ZRANGEBYSCORE_WITHSCORES,
        () -> connection.zRangeByScoreWithScores(key, range, limit));
  }

  @Override
  public Set<byte[]> zRevRangeByScore(byte[] key, double min, double max) {
    return doInScope(RedisCommand.ZREVRANGEBYSCORE,
        () -> connection.zRevRangeByScore(key, min, max));
  }

  @Override
  public Set<byte[]> zRevRangeByScore(byte[] key, Range range) {
    return doInScope(RedisCommand.ZREVRANGEBYSCORE, () -> connection.zRevRangeByScore(key, range));
  }

  @Override
  public Set<byte[]> zRevRangeByScore(byte[] key, double min, double max, long offset, long count) {
    return doInScope(RedisCommand.ZREVRANGEBYSCORE,
        () -> connection.zRevRangeByScore(key, min, max, offset, count));
  }

  @Override
  public Set<byte[]> zRevRangeByScore(byte[] key, Range range, Limit limit) {
    return doInScope(RedisCommand.ZREVRANGEBYSCORE,
        () -> connection.zRevRangeByScore(key, range, limit));
  }

  @Override
  public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max) {
    return doInScope(RedisCommand.ZREVRANGEBYSCORE_WITHSCORES,
        () -> connection.zRevRangeByScoreWithScores(key, min, max));
  }

  @Override
  public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max, long offset,
      long count) {
    return doInScope(RedisCommand.ZREVRANGEBYSCORE_WITHSCORES,
        () -> connection.zRevRangeByScoreWithScores(key, min, max, offset, count));
  }

  @Override
  public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, Range range) {
    return doInScope(RedisCommand.ZREVRANGEBYSCORE_WITHSCORES,
        () -> connection.zRevRangeByScoreWithScores(key, range));
  }

  @Override
  public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, Range range, Limit limit) {
    return doInScope(RedisCommand.ZREVRANGEBYSCORE_WITHSCORES,
        () -> connection.zRevRangeByScoreWithScores(key, range, limit));
  }

  @Override
  public Long zCount(byte[] key, double min, double max) {
    return doInScope(RedisCommand.ZCOUNT, () -> connection.zCount(key, min, max));
  }

  @Override
  public Long zCount(byte[] key, Range range) {
    return doInScope(RedisCommand.ZCOUNT, () -> connection.zCount(key, range));
  }

  @Override
  public Long zCard(byte[] key) {
    return doInScope(RedisCommand.ZCARD, () -> connection.zCard(key));
  }

  @Override
  public Double zScore(byte[] key, byte[] value) {
    return doInScope(RedisCommand.ZSCORE, () -> connection.zScore(key, value));
  }

  @Override
  public Long zRemRange(byte[] key, long start, long end) {
    return doInScope(RedisCommand.ZREMRANGE, () -> connection.zRemRange(key, start, end));
  }

  @Override
  public Long zRemRangeByScore(byte[] key, double min, double max) {
    return doInScope(RedisCommand.ZREMRANGEBYSCORE,
        () -> connection.zRemRangeByScore(key, min, max));
  }

  @Override
  public Long zRemRangeByScore(byte[] key, Range range) {
    return doInScope(RedisCommand.ZREMRANGEBYSCORE, () -> connection.zRemRangeByScore(key, range));
  }

  @Override
  public Long zUnionStore(byte[] destKey, byte[]... sets) {
    return doInScope(RedisCommand.ZUNIONSTORE, () -> connection.zUnionStore(destKey, sets));
  }

  @Override
  public Long zUnionStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
    return doInScope(RedisCommand.ZUNIONSTORE,
        () -> connection.zUnionStore(destKey, aggregate, weights, sets));
  }

  @Override
  public Long zInterStore(byte[] destKey, byte[]... sets) {
    return doInScope(RedisCommand.ZINTERSTORE, () -> connection.zInterStore(destKey, sets));
  }

  @Override
  public Long zInterStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
    return doInScope(RedisCommand.ZINTERSTORE,
        () -> connection.zInterStore(destKey, aggregate, weights, sets));
  }

  @Override
  public Cursor<Tuple> zScan(byte[] key, ScanOptions options) {
    return doInScope(RedisCommand.ZSCAN, () -> connection.zScan(key, options));
  }

  @Override
  public Set<byte[]> zRangeByLex(byte[] key) {
    return doInScope(RedisCommand.ZRANGEBYLEX, () -> connection.zRangeByLex(key));
  }

  @Override
  public Set<byte[]> zRangeByLex(byte[] key, Range range) {
    return doInScope(RedisCommand.ZRANGEBYLEX, () -> connection.zRangeByLex(key, range));
  }

  @Override
  public Set<byte[]> zRangeByLex(byte[] key, Range range, Limit limit) {
    return doInScope(RedisCommand.ZRANGEBYLEX, () -> connection.zRangeByLex(key, range, limit));
  }

  @Override
  public Boolean hSet(byte[] key, byte[] field, byte[] value) {
    return doInScope(RedisCommand.HSET, () -> connection.hSet(key, field, value));
  }

  @Override
  public Boolean hSetNX(byte[] key, byte[] field, byte[] value) {
    return doInScope(RedisCommand.HSETNX, () -> connection.hSetNX(key, field, value));
  }

  @Override
  public byte[] hGet(byte[] key, byte[] field) {
    return doInScope(RedisCommand.HGET, () -> connection.hGet(key, field));
  }

  @Override
  public List<byte[]> hMGet(byte[] key, byte[]... fields) {
    return doInScope(RedisCommand.HMGET, () -> connection.hMGet(key, fields));
  }

  @Override
  public void hMSet(byte[] key, Map<byte[], byte[]> hashes) {
    doInScope(RedisCommand.HMSET, () -> connection.hMSet(key, hashes));
  }

  @Override
  public Long hIncrBy(byte[] key, byte[] field, long delta) {
    return doInScope(RedisCommand.HINCRBY, () -> connection.hIncrBy(key, field, delta));
  }

  @Override
  public Double hIncrBy(byte[] key, byte[] field, double delta) {
    return doInScope(RedisCommand.HINCRBY, () -> connection.hIncrBy(key, field, delta));
  }

  @Override
  public Boolean hExists(byte[] key, byte[] field) {
    return doInScope(RedisCommand.HEXISTS, () -> connection.hExists(key, field));
  }

  @Override
  public Long hDel(byte[] key, byte[]... fields) {
    return doInScope(RedisCommand.HDEL, () -> connection.hDel(key, fields));
  }

  @Override
  public Long hLen(byte[] key) {
    return doInScope(RedisCommand.HLEN, () -> connection.hLen(key));
  }

  @Override
  public Set<byte[]> hKeys(byte[] key) {
    return doInScope(RedisCommand.HKEYS, () -> connection.hKeys(key));
  }

  @Override
  public List<byte[]> hVals(byte[] key) {
    return doInScope(RedisCommand.HVALS, () -> connection.hVals(key));
  }

  @Override
  public Map<byte[], byte[]> hGetAll(byte[] key) {
    return doInScope(RedisCommand.HGETALL, () -> connection.hGetAll(key));
  }

  @Override
  public Cursor<Entry<byte[], byte[]>> hScan(byte[] key, ScanOptions options) {
    return doInScope(RedisCommand.HSCAN, () -> connection.hScan(key, options));
  }

  @Override
  public void multi() {
    doInScope(RedisCommand.MULTI, () -> connection.multi());
  }

  @Override
  public List<Object> exec() {
    return doInScope(RedisCommand.EXEC, () -> connection.exec());
  }

  @Override
  public void discard() {
    doInScope(RedisCommand.DISCARD, () -> connection.discard());
  }

  @Override
  public void watch(byte[]... keys) {
    doInScope(RedisCommand.WATCH, () -> connection.watch(keys));
  }

  @Override
  public void unwatch() {
    doInScope(RedisCommand.UNWATCH, () -> connection.unwatch());
  }

  @Override
  public Long publish(byte[] channel, byte[] message) {
    return doInScope(RedisCommand.PUBLISH, () -> connection.publish(channel, message));
  }

  @Override
  public void subscribe(MessageListener listener, byte[]... channels) {
    doInScope(RedisCommand.SUBSCRIBE, () -> connection.subscribe(listener, channels));
  }

  @Override
  public void pSubscribe(MessageListener listener, byte[]... patterns) {
    doInScope(RedisCommand.PSUBSCRIBE, () -> connection.pSubscribe(listener, patterns));
  }

  @Override
  public void select(int dbIndex) {
    doInScope(RedisCommand.SELECT, () -> connection.select(dbIndex));
  }

  @Override
  public byte[] echo(byte[] message) {
    return doInScope(RedisCommand.ECHO, () -> connection.echo(message));
  }

  @Override
  public String ping() {
    return doInScope(RedisCommand.PING, () -> connection.ping());
  }

  @Override
  public void bgWriteAof() {
    doInScope(RedisCommand.BGWRITEAOF, () -> connection.bgWriteAof());
  }

  @Override
  public void bgReWriteAof() {
    doInScope(RedisCommand.BGREWRITEAOF, () -> connection.bgReWriteAof());
  }

  @Override
  public void bgSave() {
    doInScope(RedisCommand.BGSAVE, () -> connection.bgSave());
  }

  @Override
  public Long lastSave() {
    return doInScope(RedisCommand.LASTSAVE, () -> connection.lastSave());
  }

  @Override
  public void save() {
    doInScope(RedisCommand.SAVE, () -> connection.save());
  }

  @Override
  public Long dbSize() {
    return doInScope(RedisCommand.DBSIZE, () -> connection.dbSize());
  }

  @Override
  public void flushDb() {
    doInScope(RedisCommand.FLUSHDB, () -> connection.flushDb());
  }

  @Override
  public void flushAll() {
    doInScope(RedisCommand.FLUSHALL, () -> connection.flushAll());
  }

  @Override
  public Properties info() {
    return doInScope(RedisCommand.INFO, () -> connection.info());
  }

  @Override
  public Properties info(String section) {
    return doInScope(RedisCommand.INFO, () -> connection.info(section));
  }

  @Override
  public void shutdown() {
    doInScope(RedisCommand.SHUTDOWN, () -> connection.shutdown());
  }

  @Override
  public void shutdown(ShutdownOption option) {
    doInScope(RedisCommand.SHUTDOWN, () -> connection.shutdown(option));
  }

  @Override
  public Properties getConfig(String pattern) {
    return doInScope(RedisCommand.CONFIG_GET, () -> connection.getConfig(pattern));
  }

  @Override
  public void setConfig(String param, String value) {
    doInScope(RedisCommand.CONFIG_SET, () -> connection.setConfig(param, value));
  }

  @Override
  public void resetConfigStats() {
    doInScope(RedisCommand.CONFIG_RESETSTAT, () -> connection.resetConfigStats());
  }

  @Override
  public Long time() {
    return doInScope(RedisCommand.TIME, () -> connection.time());
  }

  @Override
  public void killClient(String host, int port) {
    doInScope(RedisCommand.CLIENT_KILL, () -> connection.killClient(host, port));
  }

  @Override
  public void setClientName(byte[] name) {
    doInScope(RedisCommand.CLIENT_SETNAME, () -> connection.setClientName(name));
  }

  @Override
  public String getClientName() {
    return doInScope(RedisCommand.CLIENT_GETNAME, () -> connection.getClientName());
  }

  @Override
  public List<RedisClientInfo> getClientList() {
    return doInScope(RedisCommand.CLIENT_LIST, () -> connection.getClientList());
  }

  @Override
  public void slaveOf(String host, int port) {
    doInScope(RedisCommand.SLAVEOF, () -> connection.slaveOf(host, port));
  }

  @Override
  public void slaveOfNoOne() {
    doInScope(RedisCommand.SLAVEOFNOONE, () -> connection.slaveOfNoOne());
  }

  @Override
  public void migrate(byte[] key, RedisNode target, int dbIndex, MigrateOption option) {
    doInScope(RedisCommand.MIGRATE, () -> connection.migrate(key, target, dbIndex, option));
  }

  @Override
  public void migrate(byte[] key, RedisNode target, int dbIndex, MigrateOption option,
      long timeout) {
    doInScope(RedisCommand.MIGRATE,
        () -> connection.migrate(key, target, dbIndex, option, timeout));
  }

  @Override
  public void scriptFlush() {
    doInScope(RedisCommand.SCRIPT_FLUSH, () -> connection.scriptFlush());
  }

  @Override
  public void scriptKill() {
    doInScope(RedisCommand.SCRIPT_KILL, () -> connection.scriptKill());
  }

  @Override
  public String scriptLoad(byte[] script) {
    return doInScope(RedisCommand.SCRIPT_LOAD, () -> connection.scriptLoad(script));
  }

  @Override
  public List<Boolean> scriptExists(String... scriptShas) {
    return doInScope(RedisCommand.SCRIPT_EXISTS, () -> connection.scriptExists(scriptShas));
  }

  @Override
  public <T> T eval(byte[] script, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
    return doInScope(RedisCommand.EVAL,
        () -> connection.eval(script, returnType, numKeys, keysAndArgs));
  }

  @Override
  public <T> T evalSha(String scriptSha, ReturnType returnType, int numKeys,
      byte[]... keysAndArgs) {
    return doInScope(RedisCommand.EVALSHA,
        () -> connection.evalSha(scriptSha, returnType, numKeys, keysAndArgs));
  }

  @Override
  public <T> T evalSha(byte[] scriptSha, ReturnType returnType, int numKeys,
      byte[]... keysAndArgs) {
    return doInScope(RedisCommand.EVALSHA,
        () -> connection.evalSha(scriptSha, returnType, numKeys, keysAndArgs));
  }

  @Override
  public Long geoAdd(byte[] key, Point point, byte[] member) {
    return doInScope(RedisCommand.GEOADD, () -> connection.geoAdd(key, point, member));
  }

  @Override
  public Long geoAdd(byte[] key, GeoLocation<byte[]> location) {
    return doInScope(RedisCommand.GEOADD, () -> connection.geoAdd(key, location));
  }

  @Override
  public Long geoAdd(byte[] key, Map<byte[], Point> memberCoordinateMap) {
    return doInScope(RedisCommand.GEOADD, () -> connection.geoAdd(key, memberCoordinateMap));
  }

  @Override
  public Long geoAdd(byte[] key, Iterable<GeoLocation<byte[]>> locations) {
    return doInScope(RedisCommand.GEOADD, () -> connection.geoAdd(key, locations));
  }

  @Override
  public Distance geoDist(byte[] key, byte[] member1, byte[] member2) {
    return doInScope(RedisCommand.GEODIST, () -> connection.geoDist(key, member1, member2));
  }

  @Override
  public Distance geoDist(byte[] key, byte[] member1, byte[] member2, Metric metric) {
    return doInScope(RedisCommand.GEODIST, () -> connection.geoDist(key, member1, member2, metric));
  }

  @Override
  public List<String> geoHash(byte[] key, byte[]... members) {
    return doInScope(RedisCommand.GEOHASH, () -> connection.geoHash(key, members));
  }

  @Override
  public List<Point> geoPos(byte[] key, byte[]... members) {
    return doInScope(RedisCommand.GEOPOS, () -> connection.geoPos(key, members));
  }

  @Override
  public GeoResults<GeoLocation<byte[]>> geoRadius(byte[] key, Circle within) {
    return doInScope(RedisCommand.GEORADIUS, () -> connection.geoRadius(key, within));
  }

  @Override
  public GeoResults<GeoLocation<byte[]>> geoRadius(byte[] key, Circle within,
      GeoRadiusCommandArgs args) {
    return doInScope(RedisCommand.GEORADIUS, () -> connection.geoRadius(key, within, args));
  }

  @Override
  public GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte[] key, byte[] member,
      double radius) {
    return doInScope(RedisCommand.GEORADIUSBYMEMBER,
        () -> connection.geoRadiusByMember(key, member, radius));
  }

  @Override
  public GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte[] key, byte[] member,
      Distance radius) {
    return doInScope(RedisCommand.GEORADIUSBYMEMBER,
        () -> connection.geoRadiusByMember(key, member, radius));
  }

  @Override
  public GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte[] key, byte[] member,
      Distance radius, GeoRadiusCommandArgs args) {
    return doInScope(RedisCommand.GEORADIUSBYMEMBER,
        () -> connection.geoRadiusByMember(key, member, radius, args));
  }

  @Override
  public Long geoRemove(byte[] key, byte[]... members) {
    return doInScope(RedisCommand.GEOREMOVE, () -> connection.geoRemove(key, members));
  }

  @Override
  public Long pfAdd(byte[] key, byte[]... values) {
    return doInScope(RedisCommand.PFADD, () -> connection.pfAdd(key, values));
  }

  @Override
  public Long pfCount(byte[]... keys) {
    return doInScope(RedisCommand.PFCOUNT, () -> connection.pfCount(keys));
  }

  @Override
  public void pfMerge(byte[] destinationKey, byte[]... sourceKeys) {
    doInScope(RedisCommand.PFMERGE, () -> connection.pfMerge(destinationKey, sourceKeys));
  }

}
