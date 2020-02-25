/*
 * Copyright 2017-2020 The OpenTracing Authors
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
package io.opentracing.contrib.redis.spring.data.connection;

import io.opentracing.contrib.redis.common.RedisCommand;
import io.opentracing.contrib.redis.common.TracingConfiguration;
import io.opentracing.contrib.redis.common.TracingHelper;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.springframework.dao.DataAccessException;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metric;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.RedisPipelineException;
import org.springframework.data.redis.connection.RedisSentinelConnection;
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
  private final TracingConfiguration tracingConfiguration;
  private final TracingHelper helper;

  public TracingRedisConnection(RedisConnection connection,
      TracingConfiguration tracingConfiguration) {
    this.connection = connection;
    this.tracingConfiguration = tracingConfiguration;
    this.helper = new TracingHelper(tracingConfiguration);
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
        tracingConfiguration);
  }

  @Override
  public Object execute(String command, byte[]... args) {
    return helper.doInScope(command, () -> connection.execute(command, args));
  }

  @Override
  public Boolean exists(byte[] key) {
    return helper.doInScope(RedisCommand.EXISTS, key, () -> connection.exists(key));
  }

  @Override
  public Long del(byte[]... keys) {
    return helper.doInScope(RedisCommand.DEL, keys, () -> connection.del(keys));
  }

  @Override
  public DataType type(byte[] key) {
    return helper.doInScope(RedisCommand.TYPE, key, () -> connection.type(key));
  }

  @Override
  public Set<byte[]> keys(byte[] pattern) {
    return helper.doInScope(RedisCommand.KEYS, () -> connection.keys(pattern));
  }

  @Override
  public Cursor<byte[]> scan(ScanOptions options) {
    return helper.doInScope(RedisCommand.SCAN, () -> connection.scan(options));
  }

  @Override
  public byte[] randomKey() {
    return helper.doInScope(RedisCommand.RANDOMKEY, connection::randomKey);
  }

  @Override
  public void rename(byte[] oldName, byte[] newName) {
    helper.doInScope(RedisCommand.RENAME, oldName, () -> connection.rename(oldName, newName));
  }

  @Override
  public Boolean renameNX(byte[] oldName, byte[] newName) {
    return helper
        .doInScope(RedisCommand.RENAMENX, oldName, () -> connection.renameNX(oldName, newName));
  }

  @Override
  public Boolean expire(byte[] key, long seconds) {
    return helper.doInScope(RedisCommand.EXPIRE, key, () -> connection.expire(key, seconds));
  }

  @Override
  public Boolean pExpire(byte[] key, long millis) {
    return helper.doInScope(RedisCommand.PEXPIRE, key, () -> connection.pExpire(key, millis));
  }

  @Override
  public Boolean expireAt(byte[] key, long unixTime) {
    return helper.doInScope(RedisCommand.EXPIREAT, key, () -> connection.expireAt(key, unixTime));
  }

  @Override
  public Boolean pExpireAt(byte[] key, long unixTimeInMillis) {
    return helper
        .doInScope(RedisCommand.PEXPIREAT, key, () -> connection.pExpireAt(key, unixTimeInMillis));
  }

  @Override
  public Boolean persist(byte[] key) {
    return helper.doInScope(RedisCommand.PERSIST, key, () -> connection.persist(key));
  }

  @Override
  public Boolean move(byte[] key, int dbIndex) {
    return helper.doInScope(RedisCommand.MOVE, key, () -> connection.move(key, dbIndex));
  }

  @Override
  public Long ttl(byte[] key) {
    return helper.doInScope(RedisCommand.TTL, key, () -> connection.ttl(key));
  }

  @Override
  public Long ttl(byte[] key, TimeUnit timeUnit) {
    return helper.doInScope(RedisCommand.TTL, key, () -> connection.ttl(key, timeUnit));
  }

  @Override
  public Long pTtl(byte[] key) {
    return helper.doInScope(RedisCommand.PTTL, key, () -> connection.pTtl(key));
  }

  @Override
  public Long pTtl(byte[] key, TimeUnit timeUnit) {
    return helper.doInScope(RedisCommand.PTTL, key, () -> connection.pTtl(key, timeUnit));
  }

  @Override
  public List<byte[]> sort(byte[] key, SortParameters params) {
    return helper.doInScope(RedisCommand.SORT, key, () -> connection.sort(key, params));
  }

  @Override
  public Long sort(byte[] key, SortParameters params, byte[] storeKey) {
    return helper.doInScope(RedisCommand.SORT, key, () -> connection.sort(key, params, storeKey));
  }

  @Override
  public byte[] dump(byte[] key) {
    return helper.doInScope(RedisCommand.DUMP, key, () -> connection.dump(key));
  }

  @Override
  public void restore(byte[] key, long ttlInMillis, byte[] serializedValue) {
    helper.doInScope(RedisCommand.RESTORE, key,
        () -> connection.restore(key, ttlInMillis, serializedValue));
  }

  @Override
  public byte[] get(byte[] key) {
    return helper.doInScope(RedisCommand.GET, key, () -> connection.get(key));
  }

  @Override
  public byte[] getSet(byte[] key, byte[] value) {
    return helper.doInScope(RedisCommand.GETSET, key, () -> connection.getSet(key, value));
  }

  @Override
  public List<byte[]> mGet(byte[]... keys) {
    return helper.doInScope(RedisCommand.MGET, keys, () -> connection.mGet(keys));
  }

  @Override
  public void set(byte[] key, byte[] value) {
    helper.doInScope(RedisCommand.SET, key, () -> connection.set(key, value));
  }

  @Override
  public void set(byte[] key, byte[] value, Expiration expiration, SetOption option) {
    helper.doInScope(RedisCommand.SET, key, () -> connection.set(key, value, expiration, option));
  }


  @Override
  public Boolean setNX(byte[] key, byte[] value) {
    return helper.doInScope(RedisCommand.SETNX, key, () -> connection.setNX(key, value));
  }

  @Override
  public void setEx(byte[] key, long seconds, byte[] value) {
    helper.doInScope(RedisCommand.SETEX, key, () -> connection.setEx(key, seconds, value));
  }

  @Override
  public void pSetEx(byte[] key, long milliseconds, byte[] value) {
    helper.doInScope(RedisCommand.PSETEX, key, () -> connection.pSetEx(key, milliseconds, value));
  }

  @Override
  public void mSet(Map<byte[], byte[]> tuple) {
    helper.doInScope(RedisCommand.MSET, () -> connection.mSet(tuple));
  }

  @Override
  public Boolean mSetNX(Map<byte[], byte[]> tuple) {
    return helper.doInScope(RedisCommand.MSETNX, () -> connection.mSetNX(tuple));
  }

  @Override
  public Long incr(byte[] key) {
    return helper.doInScope(RedisCommand.INCR, key, () -> connection.incr(key));
  }

  @Override
  public Long incrBy(byte[] key, long value) {
    return helper.doInScope(RedisCommand.INCRBY, key, () -> connection.incrBy(key, value));
  }

  @Override
  public Double incrBy(byte[] key, double value) {
    return helper.doInScope(RedisCommand.INCRBY, key, () -> connection.incrBy(key, value));
  }

  @Override
  public Long decr(byte[] key) {
    return helper.doInScope(RedisCommand.DECR, key, () -> connection.decr(key));
  }

  @Override
  public Long decrBy(byte[] key, long value) {
    return helper.doInScope(RedisCommand.DECRBY, key, () -> connection.decrBy(key, value));
  }

  @Override
  public Long append(byte[] key, byte[] value) {
    return helper.doInScope(RedisCommand.APPEND, key, () -> connection.append(key, value));
  }

  @Override
  public byte[] getRange(byte[] key, long begin, long end) {
    return helper.doInScope(RedisCommand.GETRANGE, key, () -> connection.getRange(key, begin, end));
  }

  @Override
  public void setRange(byte[] key, byte[] value, long offset) {
    helper.doInScope(RedisCommand.SETRANGE, key, () -> connection.setRange(key, value, offset));
  }

  @Override
  public Boolean getBit(byte[] key, long offset) {
    return helper.doInScope(RedisCommand.GETBIT, key, () -> connection.getBit(key, offset));
  }

  @Override
  public Boolean setBit(byte[] key, long offset, boolean value) {
    return helper.doInScope(RedisCommand.SETBIT, key, () -> connection.setBit(key, offset, value));
  }

  @Override
  public Long bitCount(byte[] key) {
    return helper.doInScope(RedisCommand.BITCOUNT, key, () -> connection.bitCount(key));
  }

  @Override
  public Long bitCount(byte[] key, long begin, long end) {
    return helper.doInScope(RedisCommand.BITCOUNT, key, () -> connection.bitCount(key, begin, end));
  }

  @Override
  public Long bitOp(BitOperation op, byte[] destination, byte[]... keys) {
    return helper
        .doInScope(RedisCommand.BITOP, keys, () -> connection.bitOp(op, destination, keys));
  }

  @Override
  public Long strLen(byte[] key) {
    return helper.doInScope(RedisCommand.STRLEN, key, () -> connection.strLen(key));
  }

  @Override
  public Long rPush(byte[] key, byte[]... values) {
    return helper.doInScope(RedisCommand.RPUSH, key, () -> connection.rPush(key, values));
  }

  @Override
  public Long lPush(byte[] key, byte[]... values) {
    return helper.doInScope(RedisCommand.LPUSH, key, () -> connection.lPush(key, values));
  }

  @Override
  public Long rPushX(byte[] key, byte[] value) {
    return helper.doInScope(RedisCommand.RPUSHX, key, () -> connection.rPushX(key, value));
  }

  @Override
  public Long lPushX(byte[] key, byte[] value) {
    return helper.doInScope(RedisCommand.LPUSHX, key, () -> connection.lPushX(key, value));
  }

  @Override
  public Long lLen(byte[] key) {
    return helper.doInScope(RedisCommand.LLEN, key, () -> connection.lLen(key));
  }

  @Override
  public List<byte[]> lRange(byte[] key, long start, long end) {
    return helper.doInScope(RedisCommand.LRANGE, key, () -> connection.lRange(key, start, end));
  }

  @Override
  public void lTrim(byte[] key, long start, long end) {
    helper.doInScope(RedisCommand.LTRIM, key, () -> connection.lTrim(key, start, end));
  }

  @Override
  public byte[] lIndex(byte[] key, long index) {
    return helper.doInScope(RedisCommand.LINDEX, key, () -> connection.lIndex(key, index));
  }

  @Override
  public Long lInsert(byte[] key, Position where, byte[] pivot, byte[] value) {
    return helper
        .doInScope(RedisCommand.LINSERT, key, () -> connection.lInsert(key, where, pivot, value));
  }

  @Override
  public void lSet(byte[] key, long index, byte[] value) {
    helper.doInScope(RedisCommand.LSET, key, () -> connection.lSet(key, index, value));
  }

  @Override
  public Long lRem(byte[] key, long count, byte[] value) {
    return helper.doInScope(RedisCommand.LREM, key, () -> connection.lRem(key, count, value));
  }

  @Override
  public byte[] lPop(byte[] key) {
    return helper.doInScope(RedisCommand.LPOP, key, () -> connection.lPop(key));
  }

  @Override
  public byte[] rPop(byte[] key) {
    return helper.doInScope(RedisCommand.RPOP, key, () -> connection.rPop(key));
  }

  @Override
  public List<byte[]> bLPop(int timeout, byte[]... keys) {
    return helper.doInScope(RedisCommand.BLPOP, keys, () -> connection.bLPop(timeout, keys));
  }

  @Override
  public List<byte[]> bRPop(int timeout, byte[]... keys) {
    return helper.doInScope(RedisCommand.BRPOP, keys, () -> connection.bRPop(timeout, keys));
  }

  @Override
  public byte[] rPopLPush(byte[] srcKey, byte[] dstKey) {
    return helper
        .doInScope(RedisCommand.RPOPLPUSH, srcKey, () -> connection.rPopLPush(srcKey, dstKey));
  }

  @Override
  public byte[] bRPopLPush(int timeout, byte[] srcKey, byte[] dstKey) {
    return helper.doInScope(RedisCommand.BRPOPLPUSH, srcKey,
        () -> connection.bRPopLPush(timeout, srcKey, dstKey));
  }

  @Override
  public Long sAdd(byte[] key, byte[]... values) {
    return helper.doInScope(RedisCommand.SADD, key, () -> connection.sAdd(key, values));
  }

  @Override
  public Long sRem(byte[] key, byte[]... values) {
    return helper.doInScope(RedisCommand.SREM, key, () -> connection.sRem(key, values));
  }

  @Override
  public byte[] sPop(byte[] key) {
    return helper.doInScope(RedisCommand.SPOP, key, () -> connection.sPop(key));
  }


  @Override
  public Boolean sMove(byte[] srcKey, byte[] destKey, byte[] value) {
    return helper
        .doInScope(RedisCommand.SMOVE, srcKey, () -> connection.sMove(srcKey, destKey, value));
  }

  @Override
  public Long sCard(byte[] key) {
    return helper.doInScope(RedisCommand.SCARD, key, () -> connection.sCard(key));
  }

  @Override
  public Boolean sIsMember(byte[] key, byte[] value) {
    return helper.doInScope(RedisCommand.SISMEMBER, key, () -> connection.sIsMember(key, value));
  }

  @Override
  public Set<byte[]> sInter(byte[]... keys) {
    return helper.doInScope(RedisCommand.SINTER, keys, () -> connection.sInter(keys));
  }

  @Override
  public Long sInterStore(byte[] destKey, byte[]... keys) {
    return helper
        .doInScope(RedisCommand.SINTERSTORE, keys, () -> connection.sInterStore(destKey, keys));
  }

  @Override
  public Set<byte[]> sUnion(byte[]... keys) {
    return helper.doInScope(RedisCommand.SUNION, keys, () -> connection.sUnion(keys));
  }

  @Override
  public Long sUnionStore(byte[] destKey, byte[]... keys) {
    return helper
        .doInScope(RedisCommand.SUNIONSTORE, keys, () -> connection.sUnionStore(destKey, keys));
  }

  @Override
  public Set<byte[]> sDiff(byte[]... keys) {
    return helper.doInScope(RedisCommand.SDIFF, keys, () -> connection.sDiff(keys));
  }

  @Override
  public Long sDiffStore(byte[] destKey, byte[]... keys) {
    return helper
        .doInScope(RedisCommand.SDIFFSTORE, keys, () -> connection.sDiffStore(destKey, keys));
  }

  @Override
  public Set<byte[]> sMembers(byte[] key) {
    return helper.doInScope(RedisCommand.SMEMBERS, key, () -> connection.sMembers(key));
  }

  @Override
  public byte[] sRandMember(byte[] key) {
    return helper.doInScope(RedisCommand.SRANDMEMBER, key, () -> connection.sRandMember(key));
  }

  @Override
  public List<byte[]> sRandMember(byte[] key, long count) {
    return helper
        .doInScope(RedisCommand.SRANDMEMBER, key, () -> connection.sRandMember(key, count));
  }

  @Override
  public Cursor<byte[]> sScan(byte[] key, ScanOptions options) {
    return helper.doInScope(RedisCommand.SSCAN, key, () -> connection.sScan(key, options));
  }

  @Override
  public Boolean zAdd(byte[] key, double score, byte[] value) {
    return helper.doInScope(RedisCommand.ZADD, key, () -> connection.zAdd(key, score, value));
  }

  @Override
  public Long zAdd(byte[] key, Set<Tuple> tuples) {
    return helper.doInScope(RedisCommand.ZADD, key, () -> connection.zAdd(key, tuples));
  }

  @Override
  public Long zRem(byte[] key, byte[]... values) {
    return helper.doInScope(RedisCommand.ZREM, key, () -> connection.zRem(key, values));
  }

  @Override
  public Double zIncrBy(byte[] key, double increment, byte[] value) {
    return helper
        .doInScope(RedisCommand.ZINCRBY, key, () -> connection.zIncrBy(key, increment, value));
  }

  @Override
  public Long zRank(byte[] key, byte[] value) {
    return helper.doInScope(RedisCommand.ZRANK, key, () -> connection.zRank(key, value));
  }

  @Override
  public Long zRevRank(byte[] key, byte[] value) {
    return helper.doInScope(RedisCommand.ZREVRANK, key, () -> connection.zRevRank(key, value));
  }

  @Override
  public Set<byte[]> zRange(byte[] key, long start, long end) {
    return helper.doInScope(RedisCommand.ZRANGE, key, () -> connection.zRange(key, start, end));
  }

  @Override
  public Set<byte[]> zRevRange(byte[] key, long start, long end) {
    return helper
        .doInScope(RedisCommand.ZREVRANGE, key, () -> connection.zRevRange(key, start, end));
  }

  @Override
  public Set<Tuple> zRevRangeWithScores(byte[] key, long start, long end) {
    return helper.doInScope(RedisCommand.ZREVRANGE_WITHSCORES,
        key, () -> connection.zRevRangeWithScores(key, start, end));
  }

  @Override
  public Set<Tuple> zRangeWithScores(byte[] key, long start, long end) {
    return helper.doInScope(RedisCommand.ZRANGE_WITHSCORES,
        key, () -> connection.zRangeWithScores(key, start, end));
  }

  @Override
  public Set<byte[]> zRangeByScore(byte[] key, double min, double max) {
    return helper
        .doInScope(RedisCommand.ZRANGEBYSCORE, key, () -> connection.zRangeByScore(key, min, max));
  }

  @Override
  public Set<byte[]> zRangeByScore(byte[] key, double min, double max, long offset, long count) {
    return helper.doInScope(RedisCommand.ZRANGEBYSCORE,
        key, () -> connection.zRangeByScore(key, min, max, offset, count));
  }

  @Override
  public Set<byte[]> zRangeByScore(byte[] key, String min, String max) {
    return helper
        .doInScope(RedisCommand.ZRANGEBYSCORE, key, () -> connection.zRangeByScore(key, min, max));
  }

  @Override
  public Set<byte[]> zRangeByScore(byte[] key, Range range) {
    return helper
        .doInScope(RedisCommand.ZRANGEBYSCORE, key, () -> connection.zRangeByScore(key, range));
  }

  @Override
  public Set<byte[]> zRangeByScore(byte[] key, String min, String max, long offset, long count) {
    return helper.doInScope(RedisCommand.ZRANGEBYSCORE,
        key, () -> connection.zRangeByScore(key, min, max, offset, count));
  }

  @Override
  public Set<byte[]> zRangeByScore(byte[] key, Range range, Limit limit) {
    return helper.doInScope(RedisCommand.ZRANGEBYSCORE, key,
        () -> connection.zRangeByScore(key, range, limit));
  }

  @Override
  public Set<Tuple> zRangeByScoreWithScores(byte[] key, Range range) {
    return helper.doInScope(RedisCommand.ZRANGEBYSCORE_WITHSCORES,
        key, () -> connection.zRangeByScoreWithScores(key, range));
  }

  @Override
  public Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max) {
    return helper.doInScope(RedisCommand.ZRANGEBYSCORE_WITHSCORES,
        key, () -> connection.zRangeByScoreWithScores(key, min, max));
  }

  @Override
  public Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max, long offset,
      long count) {
    return helper.doInScope(RedisCommand.ZRANGEBYSCORE_WITHSCORES,
        key, () -> connection.zRangeByScoreWithScores(key, min, max, offset, count));
  }

  @Override
  public Set<Tuple> zRangeByScoreWithScores(byte[] key, Range range, Limit limit) {
    return helper.doInScope(RedisCommand.ZRANGEBYSCORE_WITHSCORES,
        key, () -> connection.zRangeByScoreWithScores(key, range, limit));
  }

  @Override
  public Set<byte[]> zRevRangeByScore(byte[] key, double min, double max) {
    return helper.doInScope(RedisCommand.ZREVRANGEBYSCORE,
        key, () -> connection.zRevRangeByScore(key, min, max));
  }

  @Override
  public Set<byte[]> zRevRangeByScore(byte[] key, Range range) {
    return helper.doInScope(RedisCommand.ZREVRANGEBYSCORE, key,
        () -> connection.zRevRangeByScore(key, range));
  }

  @Override
  public Set<byte[]> zRevRangeByScore(byte[] key, double min, double max, long offset, long count) {
    return helper.doInScope(RedisCommand.ZREVRANGEBYSCORE,
        key, () -> connection.zRevRangeByScore(key, min, max, offset, count));
  }

  @Override
  public Set<byte[]> zRevRangeByScore(byte[] key, Range range, Limit limit) {
    return helper.doInScope(RedisCommand.ZREVRANGEBYSCORE,
        key, () -> connection.zRevRangeByScore(key, range, limit));
  }

  @Override
  public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max) {
    return helper.doInScope(RedisCommand.ZREVRANGEBYSCORE_WITHSCORES,
        key, () -> connection.zRevRangeByScoreWithScores(key, min, max));
  }

  @Override
  public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max, long offset,
      long count) {
    return helper.doInScope(RedisCommand.ZREVRANGEBYSCORE_WITHSCORES,
        key, () -> connection.zRevRangeByScoreWithScores(key, min, max, offset, count));
  }

  @Override
  public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, Range range) {
    return helper.doInScope(RedisCommand.ZREVRANGEBYSCORE_WITHSCORES,
        key, () -> connection.zRevRangeByScoreWithScores(key, range));
  }

  @Override
  public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, Range range, Limit limit) {
    return helper.doInScope(RedisCommand.ZREVRANGEBYSCORE_WITHSCORES,
        key, () -> connection.zRevRangeByScoreWithScores(key, range, limit));
  }

  @Override
  public Long zCount(byte[] key, double min, double max) {
    return helper.doInScope(RedisCommand.ZCOUNT, key, () -> connection.zCount(key, min, max));
  }

  @Override
  public Long zCount(byte[] key, Range range) {
    return helper.doInScope(RedisCommand.ZCOUNT, key, () -> connection.zCount(key, range));
  }

  @Override
  public Long zCard(byte[] key) {
    return helper.doInScope(RedisCommand.ZCARD, key, () -> connection.zCard(key));
  }

  @Override
  public Double zScore(byte[] key, byte[] value) {
    return helper.doInScope(RedisCommand.ZSCORE, key, () -> connection.zScore(key, value));
  }

  @Override
  public Long zRemRange(byte[] key, long start, long end) {
    return helper
        .doInScope(RedisCommand.ZREMRANGE, key, () -> connection.zRemRange(key, start, end));
  }

  @Override
  public Long zRemRangeByScore(byte[] key, double min, double max) {
    return helper.doInScope(RedisCommand.ZREMRANGEBYSCORE,
        key, () -> connection.zRemRangeByScore(key, min, max));
  }

  @Override
  public Long zRemRangeByScore(byte[] key, Range range) {
    return helper.doInScope(RedisCommand.ZREMRANGEBYSCORE, key,
        () -> connection.zRemRangeByScore(key, range));
  }

  @Override
  public Long zUnionStore(byte[] destKey, byte[]... sets) {
    return helper
        .doInScope(RedisCommand.ZUNIONSTORE, destKey, () -> connection.zUnionStore(destKey, sets));
  }

  @Override
  public Long zUnionStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
    return helper.doInScope(RedisCommand.ZUNIONSTORE,
        destKey, () -> connection.zUnionStore(destKey, aggregate, weights, sets));
  }

  @Override
  public Long zInterStore(byte[] destKey, byte[]... sets) {
    return helper
        .doInScope(RedisCommand.ZINTERSTORE, destKey, () -> connection.zInterStore(destKey, sets));
  }

  @Override
  public Long zInterStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
    return helper.doInScope(RedisCommand.ZINTERSTORE,
        destKey, () -> connection.zInterStore(destKey, aggregate, weights, sets));
  }

  @Override
  public Cursor<Tuple> zScan(byte[] key, ScanOptions options) {
    return helper.doInScope(RedisCommand.ZSCAN, key, () -> connection.zScan(key, options));
  }

  @Override
  public Set<byte[]> zRangeByLex(byte[] key) {
    return helper.doInScope(RedisCommand.ZRANGEBYLEX, key, () -> connection.zRangeByLex(key));
  }

  @Override
  public Set<byte[]> zRangeByLex(byte[] key, Range range) {
    return helper
        .doInScope(RedisCommand.ZRANGEBYLEX, key, () -> connection.zRangeByLex(key, range));
  }

  @Override
  public Set<byte[]> zRangeByLex(byte[] key, Range range, Limit limit) {
    return helper
        .doInScope(RedisCommand.ZRANGEBYLEX, key, () -> connection.zRangeByLex(key, range, limit));
  }

  @Override
  public Boolean hSet(byte[] key, byte[] field, byte[] value) {
    return helper.doInScope(RedisCommand.HSET, key, () -> connection.hSet(key, field, value));
  }

  @Override
  public Boolean hSetNX(byte[] key, byte[] field, byte[] value) {
    return helper.doInScope(RedisCommand.HSETNX, key, () -> connection.hSetNX(key, field, value));
  }

  @Override
  public byte[] hGet(byte[] key, byte[] field) {
    return helper.doInScope(RedisCommand.HGET, key, () -> connection.hGet(key, field));
  }

  @Override
  public List<byte[]> hMGet(byte[] key, byte[]... fields) {
    return helper.doInScope(RedisCommand.HMGET, key, () -> connection.hMGet(key, fields));
  }

  @Override
  public void hMSet(byte[] key, Map<byte[], byte[]> hashes) {
    helper.doInScope(RedisCommand.HMSET, key, () -> connection.hMSet(key, hashes));
  }

  @Override
  public Long hIncrBy(byte[] key, byte[] field, long delta) {
    return helper.doInScope(RedisCommand.HINCRBY, key, () -> connection.hIncrBy(key, field, delta));
  }

  @Override
  public Double hIncrBy(byte[] key, byte[] field, double delta) {
    return helper.doInScope(RedisCommand.HINCRBY, key, () -> connection.hIncrBy(key, field, delta));
  }

  @Override
  public Boolean hExists(byte[] key, byte[] field) {
    return helper.doInScope(RedisCommand.HEXISTS, key, () -> connection.hExists(key, field));
  }

  @Override
  public Long hDel(byte[] key, byte[]... fields) {
    return helper.doInScope(RedisCommand.HDEL, key, () -> connection.hDel(key, fields));
  }

  @Override
  public Long hLen(byte[] key) {
    return helper.doInScope(RedisCommand.HLEN, key, () -> connection.hLen(key));
  }

  @Override
  public Set<byte[]> hKeys(byte[] key) {
    return helper.doInScope(RedisCommand.HKEYS, key, () -> connection.hKeys(key));
  }

  @Override
  public List<byte[]> hVals(byte[] key) {
    return helper.doInScope(RedisCommand.HVALS, key, () -> connection.hVals(key));
  }

  @Override
  public Map<byte[], byte[]> hGetAll(byte[] key) {
    return helper.doInScope(RedisCommand.HGETALL, key, () -> connection.hGetAll(key));
  }

  @Override
  public Cursor<Entry<byte[], byte[]>> hScan(byte[] key, ScanOptions options) {
    return helper.doInScope(RedisCommand.HSCAN, key, () -> connection.hScan(key, options));
  }

  @Override
  public void multi() {
    helper.doInScope(RedisCommand.MULTI, connection::multi);
  }

  @Override
  public List<Object> exec() {
    return helper.doInScope(RedisCommand.EXEC, connection::exec);
  }

  @Override
  public void discard() {
    helper.doInScope(RedisCommand.DISCARD, connection::discard);
  }

  @Override
  public void watch(byte[]... keys) {
    helper.doInScope(RedisCommand.WATCH, () -> connection.watch(keys));
  }

  @Override
  public void unwatch() {
    helper.doInScope(RedisCommand.UNWATCH, connection::unwatch);
  }

  @Override
  public Long publish(byte[] channel, byte[] message) {
    return helper.doInScope(RedisCommand.PUBLISH, () -> connection.publish(channel, message));
  }

  @Override
  public void subscribe(MessageListener listener, byte[]... channels) {
    helper.doInScope(RedisCommand.SUBSCRIBE, () -> connection.subscribe(listener, channels));
  }

  @Override
  public void pSubscribe(MessageListener listener, byte[]... patterns) {
    helper.doInScope(RedisCommand.PSUBSCRIBE, () -> connection.pSubscribe(listener, patterns));
  }

  @Override
  public void select(int dbIndex) {
    helper.doInScope(RedisCommand.SELECT, () -> connection.select(dbIndex));
  }

  @Override
  public byte[] echo(byte[] message) {
    return helper.doInScope(RedisCommand.ECHO, () -> connection.echo(message));
  }

  @Override
  public String ping() {
    return helper.doInScope(RedisCommand.PING, connection::ping);
  }

  @Override
  public void bgWriteAof() {
    helper.doInScope(RedisCommand.BGWRITEAOF, connection::bgWriteAof);
  }

  @Override
  public void bgReWriteAof() {
    helper.doInScope(RedisCommand.BGREWRITEAOF, connection::bgReWriteAof);
  }

  @Override
  public void bgSave() {
    helper.doInScope(RedisCommand.BGSAVE, connection::bgSave);
  }

  @Override
  public Long lastSave() {
    return helper.doInScope(RedisCommand.LASTSAVE, connection::lastSave);
  }

  @Override
  public void save() {
    helper.doInScope(RedisCommand.SAVE, connection::save);
  }

  @Override
  public Long dbSize() {
    return helper.doInScope(RedisCommand.DBSIZE, connection::dbSize);
  }

  @Override
  public void flushDb() {
    helper.doInScope(RedisCommand.FLUSHDB, connection::flushDb);
  }

  @Override
  public void flushAll() {
    helper.doInScope(RedisCommand.FLUSHALL, connection::flushAll);
  }

  @Override
  public Properties info() {
    return helper.doInScope(RedisCommand.INFO, () -> connection.info());
  }

  @Override
  public Properties info(String section) {
    return helper.doInScope(RedisCommand.INFO, () -> connection.info(section));
  }

  @Override
  public void shutdown() {
    helper.doInScope(RedisCommand.SHUTDOWN, () -> connection.shutdown());
  }

  @Override
  public void shutdown(ShutdownOption option) {
    helper.doInScope(RedisCommand.SHUTDOWN, () -> connection.shutdown(option));
  }

  @Override
  public List<String> getConfig(String pattern) {
    return helper.doInScope(RedisCommand.CONFIG_GET, () -> connection.getConfig(pattern));
  }


  @Override
  public void setConfig(String param, String value) {
    helper.doInScope(RedisCommand.CONFIG_SET, () -> connection.setConfig(param, value));
  }

  @Override
  public void resetConfigStats() {
    helper.doInScope(RedisCommand.CONFIG_RESETSTAT, () -> connection.resetConfigStats());
  }

  @Override
  public Long time() {
    return helper.doInScope(RedisCommand.TIME, () -> connection.time());
  }

  @Override
  public void killClient(String host, int port) {
    helper.doInScope(RedisCommand.CLIENT_KILL, () -> connection.killClient(host, port));
  }

  @Override
  public void setClientName(byte[] name) {
    helper.doInScope(RedisCommand.CLIENT_SETNAME, () -> connection.setClientName(name));
  }

  @Override
  public String getClientName() {
    return helper.doInScope(RedisCommand.CLIENT_GETNAME, () -> connection.getClientName());
  }

  @Override
  public List<RedisClientInfo> getClientList() {
    return helper.doInScope(RedisCommand.CLIENT_LIST, () -> connection.getClientList());
  }

  @Override
  public void slaveOf(String host, int port) {
    helper.doInScope(RedisCommand.SLAVEOF, () -> connection.slaveOf(host, port));
  }

  @Override
  public void slaveOfNoOne() {
    helper.doInScope(RedisCommand.SLAVEOFNOONE, () -> connection.slaveOfNoOne());
  }

  @Override
  public void migrate(byte[] key, RedisNode target, int dbIndex, MigrateOption option) {
    helper.doInScope(RedisCommand.MIGRATE, key,
        () -> connection.migrate(key, target, dbIndex, option));
  }

  @Override
  public void migrate(byte[] key, RedisNode target, int dbIndex, MigrateOption option,
      long timeout) {
    helper.doInScope(RedisCommand.MIGRATE,
        key, () -> connection.migrate(key, target, dbIndex, option, timeout));
  }

  @Override
  public void scriptFlush() {
    helper.doInScope(RedisCommand.SCRIPT_FLUSH, () -> connection.scriptFlush());
  }

  @Override
  public void scriptKill() {
    helper.doInScope(RedisCommand.SCRIPT_KILL, () -> connection.scriptKill());
  }

  @Override
  public String scriptLoad(byte[] script) {
    return helper.doInScope(RedisCommand.SCRIPT_LOAD, () -> connection.scriptLoad(script));
  }

  @Override
  public List<Boolean> scriptExists(String... scriptShas) {
    return helper.doInScope(RedisCommand.SCRIPT_EXISTS, () -> connection.scriptExists(scriptShas));
  }

  @Override
  public <T> T eval(byte[] script, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
    return helper.doInScope(RedisCommand.EVAL,
        () -> connection.eval(script, returnType, numKeys, keysAndArgs));
  }

  @Override
  public <T> T evalSha(String scriptSha, ReturnType returnType, int numKeys,
      byte[]... keysAndArgs) {
    return helper.doInScope(RedisCommand.EVALSHA,
        () -> connection.evalSha(scriptSha, returnType, numKeys, keysAndArgs));
  }

  @Override
  public <T> T evalSha(byte[] scriptSha, ReturnType returnType, int numKeys,
      byte[]... keysAndArgs) {
    return helper.doInScope(RedisCommand.EVALSHA,
        () -> connection.evalSha(scriptSha, returnType, numKeys, keysAndArgs));
  }

  @Override
  public Long geoAdd(byte[] key, Point point, byte[] member) {
    return helper.doInScope(RedisCommand.GEOADD, key, () -> connection.geoAdd(key, point, member));
  }

  @Override
  public Long geoAdd(byte[] key, GeoLocation<byte[]> location) {
    return helper.doInScope(RedisCommand.GEOADD, key, () -> connection.geoAdd(key, location));
  }

  @Override
  public Long geoAdd(byte[] key, Map<byte[], Point> memberCoordinateMap) {
    return helper
        .doInScope(RedisCommand.GEOADD, key, () -> connection.geoAdd(key, memberCoordinateMap));
  }

  @Override
  public Long geoAdd(byte[] key, Iterable<GeoLocation<byte[]>> locations) {
    return helper.doInScope(RedisCommand.GEOADD, key, () -> connection.geoAdd(key, locations));
  }

  @Override
  public Distance geoDist(byte[] key, byte[] member1, byte[] member2) {
    return helper
        .doInScope(RedisCommand.GEODIST, key, () -> connection.geoDist(key, member1, member2));
  }

  @Override
  public Distance geoDist(byte[] key, byte[] member1, byte[] member2, Metric metric) {
    return helper.doInScope(RedisCommand.GEODIST, key,
        () -> connection.geoDist(key, member1, member2, metric));
  }

  @Override
  public List<String> geoHash(byte[] key, byte[]... members) {
    return helper.doInScope(RedisCommand.GEOHASH, key, () -> connection.geoHash(key, members));
  }

  @Override
  public List<Point> geoPos(byte[] key, byte[]... members) {
    return helper.doInScope(RedisCommand.GEOPOS, key, () -> connection.geoPos(key, members));
  }

  @Override
  public GeoResults<GeoLocation<byte[]>> geoRadius(byte[] key, Circle within) {
    return helper.doInScope(RedisCommand.GEORADIUS, key, () -> connection.geoRadius(key, within));
  }

  @Override
  public GeoResults<GeoLocation<byte[]>> geoRadius(byte[] key, Circle within,
      GeoRadiusCommandArgs args) {
    return helper
        .doInScope(RedisCommand.GEORADIUS, key, () -> connection.geoRadius(key, within, args));
  }

  @Override
  public GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte[] key, byte[] member,
      double radius) {
    return helper.doInScope(RedisCommand.GEORADIUSBYMEMBER,
        key, () -> connection.geoRadiusByMember(key, member, radius));
  }

  @Override
  public GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte[] key, byte[] member,
      Distance radius) {
    return helper.doInScope(RedisCommand.GEORADIUSBYMEMBER,
        key, () -> connection.geoRadiusByMember(key, member, radius));
  }

  @Override
  public GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte[] key, byte[] member,
      Distance radius, GeoRadiusCommandArgs args) {
    return helper.doInScope(RedisCommand.GEORADIUSBYMEMBER,
        key, () -> connection.geoRadiusByMember(key, member, radius, args));
  }

  @Override
  public Long geoRemove(byte[] key, byte[]... members) {
    return helper.doInScope(RedisCommand.GEOREMOVE, key, () -> connection.geoRemove(key, members));
  }

  @Override
  public Long pfAdd(byte[] key, byte[]... values) {
    return helper.doInScope(RedisCommand.PFADD, key, () -> connection.pfAdd(key, values));
  }

  @Override
  public Long pfCount(byte[]... keys) {
    return helper.doInScope(RedisCommand.PFCOUNT, keys, () -> connection.pfCount(keys));
  }

  @Override
  public void pfMerge(byte[] destinationKey, byte[]... sourceKeys) {
    helper.doInScope(RedisCommand.PFMERGE,
        () -> connection.pfMerge(destinationKey, sourceKeys));
  }

}
