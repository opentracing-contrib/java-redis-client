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
package io.opentracing.contrib.redis.jedis;

import static io.opentracing.contrib.redis.common.TracingHelper.nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import io.opentracing.Span;
import io.opentracing.contrib.redis.common.TracingConfiguration;
import io.opentracing.contrib.redis.common.TracingHelper;
import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.BitOP;
import redis.clients.jedis.BitPosParams;
import redis.clients.jedis.DebugParams;
import redis.clients.jedis.GeoCoordinate;
import redis.clients.jedis.GeoRadiusResponse;
import redis.clients.jedis.GeoUnit;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.ListPosition;
import redis.clients.jedis.Protocol.Command;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.ZParams;
import redis.clients.jedis.params.geo.GeoRadiusParam;
import redis.clients.jedis.params.sortedset.ZAddParams;
import redis.clients.jedis.params.sortedset.ZIncrByParams;

public class TracingJedisCluster extends JedisCluster {

  private final TracingHelper helper;

  public TracingJedisCluster(HostAndPort node, TracingConfiguration tracingConfiguration) {
    super(node);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedisCluster(HostAndPort node, int timeout,
      TracingConfiguration tracingConfiguration) {
    super(node, timeout);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedisCluster(HostAndPort node, int timeout, int maxAttempts,
      TracingConfiguration tracingConfiguration) {
    super(node, timeout, maxAttempts);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedisCluster(HostAndPort node, GenericObjectPoolConfig poolConfig,
      TracingConfiguration tracingConfiguration) {
    super(node, poolConfig);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedisCluster(HostAndPort node, int timeout, GenericObjectPoolConfig poolConfig,
      TracingConfiguration tracingConfiguration) {
    super(node, timeout, poolConfig);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedisCluster(HostAndPort node, int timeout, int maxAttempts,
      GenericObjectPoolConfig poolConfig, TracingConfiguration tracingConfiguration) {
    super(node, timeout, maxAttempts, poolConfig);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedisCluster(HostAndPort node, int connectionTimeout, int soTimeout,
      int maxAttempts,
      GenericObjectPoolConfig poolConfig, TracingConfiguration tracingConfiguration) {
    super(node, connectionTimeout, soTimeout, maxAttempts, poolConfig);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedisCluster(HostAndPort node, int connectionTimeout, int soTimeout,
      int maxAttempts,
      String password, GenericObjectPoolConfig poolConfig,
      TracingConfiguration tracingConfiguration) {
    super(node, connectionTimeout, soTimeout, maxAttempts, password, poolConfig);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedisCluster(Set<HostAndPort> nodes, TracingConfiguration tracingConfiguration) {
    super(nodes);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedisCluster(Set<HostAndPort> nodes, int timeout,
      TracingConfiguration tracingConfiguration) {
    super(nodes, timeout);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedisCluster(Set<HostAndPort> nodes, int timeout, int maxAttempts,
      TracingConfiguration tracingConfiguration) {
    super(nodes, timeout, maxAttempts);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedisCluster(Set<HostAndPort> nodes, GenericObjectPoolConfig poolConfig,
      TracingConfiguration tracingConfiguration) {
    super(nodes, poolConfig);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedisCluster(Set<HostAndPort> nodes, int timeout,
      GenericObjectPoolConfig poolConfig, TracingConfiguration tracingConfiguration) {
    super(nodes, timeout, poolConfig);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedisCluster(Set<HostAndPort> jedisClusterNode, int timeout, int maxAttempts,
      GenericObjectPoolConfig poolConfig, TracingConfiguration tracingConfiguration) {
    super(jedisClusterNode, timeout, maxAttempts, poolConfig);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedisCluster(Set<HostAndPort> jedisClusterNode, int connectionTimeout,
      int soTimeout, int maxAttempts,
      GenericObjectPoolConfig poolConfig, TracingConfiguration tracingConfiguration,
      Function<String, String> customSpanNameProvider) {
    super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, poolConfig);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedisCluster(Set<HostAndPort> jedisClusterNode, int connectionTimeout,
      int soTimeout,
      int maxAttempts, GenericObjectPoolConfig poolConfig,
      TracingConfiguration tracingConfiguration) {
    super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, poolConfig);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedisCluster(Set<HostAndPort> jedisClusterNode, int connectionTimeout,
      int soTimeout,
      int maxAttempts, String password, GenericObjectPoolConfig poolConfig,
      TracingConfiguration tracingConfiguration) {
    super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, password, poolConfig);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  @Override
  public String set(String key, String value) {
    Span span = helper.buildSpan("set", key);
    span.setTag("value", value);
    return helper.decorate(span, () -> super.set(key, value));
  }

  @Override
  public String set(String key, String value, String nxxx, String expx, long time) {
    Span span = helper.buildSpan("set", key);
    span.setTag("value", value);
    span.setTag("nxxx", nxxx);
    span.setTag("expx", expx);
    span.setTag("time", time);
    return helper.decorate(span, () -> super.set(key, value, nxxx, expx, time));
  }

  @Override
  public String set(String key, String value, String expx, long time) {
    Span span = helper.buildSpan("set", key);
    span.setTag("value", value);
    span.setTag("expx", expx);
    span.setTag("time", time);
    return helper.decorate(span, () -> super.set(key, value, expx, time));
  }

  @Override
  public String get(String key) {
    Span span = helper.buildSpan("get", key);
    return helper.decorate(span, () -> super.get(key));
  }

  @Override
  public Long exists(String... keys) {
    Span span = helper.buildSpan("exists", keys);
    return helper.decorate(span, () -> super.exists(keys));
  }

  @Override
  public Boolean exists(String key) {
    Span span = helper.buildSpan("exists", key);
    return helper.decorate(span, () -> super.exists(key));
  }

  @Override
  public Long del(String... keys) {
    Span span = helper.buildSpan("del", keys);
    return helper.decorate(span, () -> super.del(keys));
  }

  @Override
  public Long del(String key) {
    Span span = helper.buildSpan("del", key);
    return helper.decorate(span, () -> super.del(key));
  }

  @Override
  public Long unlink(String key) {
    Span span = helper.buildSpan("unlink", key);
    return helper.decorate(span, () -> super.unlink(key));
  }

  @Override
  public Long unlink(String... keys) {
    Span span = helper.buildSpan("unlink", keys);
    return helper.decorate(span, () -> super.unlink(keys));
  }

  @Override
  public String type(String key) {
    Span span = helper.buildSpan("type", key);
    return helper.decorate(span, () -> super.type(key));
  }

  @Override
  public byte[] dump(String key) {
    Span span = helper.buildSpan("dump", key);
    return helper.decorate(span, () -> super.dump(key));
  }

  @Override
  public String restore(String key, int ttl, byte[] serializedValue) {
    Span span = helper.buildSpan("restore", key);
    span.setTag("ttl", ttl);
    span.setTag("serializedValue", Arrays.toString(serializedValue));
    return helper.decorate(span, () -> super.restore(key, ttl, serializedValue));
  }

  @Override
  public String rename(String oldkey, String newkey) {
    Span span = helper.buildSpan("rename");
    span.setTag("oldKey", nullable(oldkey));
    span.setTag("newKey", nullable(newkey));
    return helper.decorate(span, () -> super.rename(oldkey, newkey));
  }

  @Override
  public Long renamenx(String oldkey, String newkey) {
    Span span = helper.buildSpan("renamenx");
    span.setTag("oldKey", nullable(oldkey));
    span.setTag("newKey", nullable(newkey));
    return helper.decorate(span, () -> super.renamenx(oldkey, newkey));
  }

  @Override
  public Long expire(String key, int seconds) {
    Span span = helper.buildSpan("expire", key);
    span.setTag("seconds", seconds);
    return helper.decorate(span, () -> super.expire(key, seconds));
  }

  @Override
  public Long expireAt(String key, long unixTime) {
    Span span = helper.buildSpan("expireAt", key);
    span.setTag("unixTime", unixTime);
    return helper.decorate(span, () -> super.expireAt(key, unixTime));
  }

  @Override
  public Long ttl(String key) {
    Span span = helper.buildSpan("ttl", key);
    return helper.decorate(span, () -> super.ttl(key));
  }

  @Override
  public Long touch(String key) {
    Span span = helper.buildSpan("touch", key);
    return helper.decorate(span, () -> super.touch(key));
  }

  @Override
  public Long touch(String... keys) {
    Span span = helper.buildSpan("touch", keys);
    return helper.decorate(span, () -> super.touch(keys));
  }

  @Override
  public Long move(String key, int dbIndex) {
    Span span = helper.buildSpan("move", key);
    span.setTag("dbIndex", dbIndex);
    return helper.decorate(span, () -> super.move(key, dbIndex));
  }

  @Override
  public String getSet(String key, String value) {
    Span span = helper.buildSpan("getSet", key);
    span.setTag("value", value);
    return helper.decorate(span, () -> super.getSet(key, value));
  }

  @Override
  public List<String> mget(String... keys) {
    Span span = helper.buildSpan("mget", keys);
    return helper.decorate(span, () -> super.mget(keys));
  }

  @Override
  public Long setnx(String key, String value) {
    Span span = helper.buildSpan("setnx", key);
    span.setTag("value", value);
    return helper.decorate(span, () -> super.setnx(key, value));
  }

  @Override
  public String setex(String key, int seconds, String value) {
    Span span = helper.buildSpan("setex", key);
    span.setTag("seconds", seconds);
    span.setTag("value", value);
    return helper.decorate(span, () -> super.setex(key, seconds, value));
  }

  @Override
  public String mset(String... keysvalues) {
    Span span = helper.buildSpan("mset");
    span.setTag("keysvalues", Arrays.toString(keysvalues));
    return helper.decorate(span, () -> super.mset(keysvalues));
  }

  @Override
  public Long msetnx(String... keysvalues) {
    Span span = helper.buildSpan("msetnx");
    span.setTag("keysvalues", Arrays.toString(keysvalues));
    return helper.decorate(span, () -> super.msetnx(keysvalues));
  }

  @Override
  public Long decrBy(String key, long integer) {
    Span span = helper.buildSpan("decrBy", key);
    span.setTag("integer", integer);
    return helper.decorate(span, () -> super.decrBy(key, integer));
  }

  @Override
  public Long decr(String key) {
    Span span = helper.buildSpan("decr", key);
    return helper.decorate(span, () -> super.decr(key));
  }

  @Override
  public Long incrBy(String key, long integer) {
    Span span = helper.buildSpan("incrBy", key);
    span.setTag("integer", integer);
    return helper.decorate(span, () -> super.incrBy(key, integer));
  }

  @Override
  public Double incrByFloat(String key, double value) {
    Span span = helper.buildSpan("incrByFloat", key);
    span.setTag("value", value);
    return helper.decorate(span, () -> super.incrByFloat(key, value));
  }

  @Override
  public Long incr(String key) {
    Span span = helper.buildSpan("incr", key);
    return helper.decorate(span, () -> super.incr(key));
  }

  @Override
  public Long append(String key, String value) {
    Span span = helper.buildSpan("append", key);
    span.setTag("value", value);
    return helper.decorate(span, () -> super.append(key, value));
  }

  @Override
  public String substr(String key, int start, int end) {
    Span span = helper.buildSpan("substr", key);
    span.setTag("start", start);
    span.setTag("end", end);
    return helper.decorate(span, () -> super.substr(key, start, end));
  }

  @Override
  public Long hset(String key, String field, String value) {
    Span span = helper.buildSpan("hset", key);
    span.setTag("field", field);
    span.setTag("value", value);
    return helper.decorate(span, () -> super.hset(key, field, value));
  }

  @Override
  public Long hset(String key, Map<String, String> hash) {
    Span span = helper.buildSpan("hset", key);
    span.setTag("hash", TracingHelper.toString(hash));
    return helper.decorate(span, () -> super.hset(key, hash));
  }

  @Override
  public String hget(String key, String field) {
    Span span = helper.buildSpan("hget", key);
    span.setTag("field", field);
    return helper.decorate(span, () -> super.hget(key, field));
  }

  @Override
  public Long hsetnx(String key, String field, String value) {
    Span span = helper.buildSpan("hsetnx", key);
    span.setTag("field", field);
    span.setTag("value", value);
    return helper.decorate(span, () -> super.hsetnx(key, field, value));
  }

  @Override
  public String hmset(String key, Map<String, String> hash) {
    Span span = helper.buildSpan("hmset", key);
    span.setTag("hash", TracingHelper.toString(hash));
    return helper.decorate(span, () -> super.hmset(key, hash));
  }

  @Override
  public List<String> hmget(String key, String... fields) {
    Span span = helper.buildSpan("hmget", key);
    span.setTag("fields", Arrays.toString(fields));
    return helper.decorate(span, () -> super.hmget(key, fields));
  }

  @Override
  public Long hincrBy(String key, String field, long value) {
    Span span = helper.buildSpan("hincrBy", key);
    span.setTag("field", field);
    span.setTag("value", value);
    return helper.decorate(span, () -> super.hincrBy(key, field, value));
  }

  @Override
  public Double hincrByFloat(String key, String field, double value) {
    Span span = helper.buildSpan("hincrByFloat", key);
    span.setTag("field", field);
    span.setTag("value", value);
    return helper.decorate(span, () -> super.hincrByFloat(key, field, value));
  }

  @Override
  public Boolean hexists(String key, String field) {
    Span span = helper.buildSpan("hexists", key);
    span.setTag("field", field);
    return helper.decorate(span, () -> super.hexists(key, field));
  }

  @Override
  public Long hdel(String key, String... fields) {
    Span span = helper.buildSpan("hdel", key);
    span.setTag("fields", Arrays.toString(fields));
    return helper.decorate(span, () -> super.hdel(key, fields));
  }

  @Override
  public Long hlen(String key) {
    Span span = helper.buildSpan("hlen", key);
    return helper.decorate(span, () -> super.hlen(key));
  }

  @Override
  public Set<String> hkeys(String key) {
    Span span = helper.buildSpan("hkeys", key);
    return helper.decorate(span, () -> super.hkeys(key));
  }

  @Override
  public List<String> hvals(String key) {
    Span span = helper.buildSpan("hvals", key);
    return helper.decorate(span, () -> super.hvals(key));
  }

  @Override
  public Map<String, String> hgetAll(String key) {
    Span span = helper.buildSpan("hgetAll", key);
    return helper.decorate(span, () -> super.hgetAll(key));
  }

  @Override
  public Long rpush(String key, String... strings) {
    Span span = helper.buildSpan("rpush", key);
    span.setTag("strings", Arrays.toString(strings));
    return helper.decorate(span, () -> super.rpush(key, strings));
  }

  @Override
  public Long lpush(String key, String... strings) {
    Span span = helper.buildSpan("lpush", key);
    span.setTag("strings", Arrays.toString(strings));
    return helper.decorate(span, () -> super.lpush(key, strings));
  }

  @Override
  public Long llen(String key) {
    Span span = helper.buildSpan("llen", key);
    return helper.decorate(span, () -> super.llen(key));
  }

  @Override
  public List<String> lrange(String key, long start, long end) {
    Span span = helper.buildSpan("lrange", key);
    span.setTag("start", start);
    span.setTag("end", end);
    return helper.decorate(span, () -> super.lrange(key, start, end));
  }

  @Override
  public String ltrim(String key, long start, long end) {
    Span span = helper.buildSpan("ltrim", key);
    span.setTag("start", start);
    span.setTag("end", end);
    return helper.decorate(span, () -> super.ltrim(key, start, end));
  }

  @Override
  public String lindex(String key, long index) {
    Span span = helper.buildSpan("lindex", key);
    span.setTag("index", index);
    return helper.decorate(span, () -> super.lindex(key, index));
  }

  @Override
  public String lset(String key, long index, String value) {
    Span span = helper.buildSpan("lset", key);
    span.setTag("index", index);
    span.setTag("value", value);
    return helper.decorate(span, () -> super.lset(key, index, value));
  }

  @Override
  public Long lrem(String key, long count, String value) {
    Span span = helper.buildSpan("lrem", key);
    span.setTag("count", count);
    span.setTag("value", value);
    return helper.decorate(span, () -> super.lrem(key, count, value));
  }

  @Override
  public String lpop(String key) {
    Span span = helper.buildSpan("lpop", key);
    return helper.decorate(span, () -> super.lpop(key));
  }

  @Override
  public String rpop(String key) {
    Span span = helper.buildSpan("rpop", key);
    return helper.decorate(span, () -> super.rpop(key));
  }

  @Override
  public String rpoplpush(String srckey, String dstkey) {
    Span span = helper.buildSpan("rpoplpush");
    span.setTag("srckey", srckey);
    span.setTag("dstkey", dstkey);
    return helper.decorate(span, () -> super.rpoplpush(srckey, dstkey));
  }

  @Override
  public Long sadd(String key, String... members) {
    Span span = helper.buildSpan("sadd", key);
    span.setTag("members", Arrays.toString(members));
    return helper.decorate(span, () -> super.sadd(key, members));
  }

  @Override
  public Set<String> smembers(String key) {
    Span span = helper.buildSpan("smembers", key);
    return helper.decorate(span, () -> super.smembers(key));
  }

  @Override
  public Long srem(String key, String... members) {
    Span span = helper.buildSpan("srem", key);
    span.setTag("members", Arrays.toString(members));
    return helper.decorate(span, () -> super.srem(key, members));
  }

  @Override
  public String spop(String key) {
    Span span = helper.buildSpan("spop", key);
    return helper.decorate(span, () -> super.spop(key));
  }

  @Override
  public Set<String> spop(String key, long count) {
    Span span = helper.buildSpan("spop", key);
    span.setTag("count", count);
    return helper.decorate(span, () -> super.spop(key, count));
  }

  @Override
  public Long smove(String srckey, String dstkey, String member) {
    Span span = helper.buildSpan("smove");
    span.setTag("srckey", srckey);
    span.setTag("dstkey", dstkey);
    span.setTag("member", member);
    return helper.decorate(span, () -> super.smove(srckey, dstkey, member));
  }

  @Override
  public Long scard(String key) {
    Span span = helper.buildSpan("scard", key);
    return helper.decorate(span, () -> super.scard(key));
  }

  @Override
  public Boolean sismember(String key, String member) {
    Span span = helper.buildSpan("sismember", key);
    span.setTag("member", member);
    return helper.decorate(span, () -> super.sismember(key, member));
  }

  @Override
  public Set<String> sinter(String... keys) {
    Span span = helper.buildSpan("sinter", keys);
    return helper.decorate(span, () -> super.sinter(keys));
  }

  @Override
  public Long sinterstore(String dstkey, String... keys) {
    Span span = helper.buildSpan("sinterstore", keys);
    span.setTag("dstkey", dstkey);
    return helper.decorate(span, () -> super.sinterstore(dstkey, keys));
  }

  @Override
  public Set<String> sunion(String... keys) {
    Span span = helper.buildSpan("sunion", keys);
    return helper.decorate(span, () -> super.sunion(keys));
  }

  @Override
  public Long sunionstore(String dstkey, String... keys) {
    Span span = helper.buildSpan("sunionstore", keys);
    span.setTag("dstkey", dstkey);
    return helper.decorate(span, () -> super.sunionstore(dstkey, keys));
  }

  @Override
  public Set<String> sdiff(String... keys) {
    Span span = helper.buildSpan("sdiff", keys);
    return helper.decorate(span, () -> super.sdiff(keys));
  }

  @Override
  public Long sdiffstore(String dstkey, String... keys) {
    Span span = helper.buildSpan("sdiffstore", keys);
    span.setTag("dstkey", dstkey);
    return helper.decorate(span, () -> super.sdiffstore(dstkey, keys));
  }

  @Override
  public String srandmember(String key) {
    Span span = helper.buildSpan("srandmember", key);
    return helper.decorate(span, () -> super.srandmember(key));
  }

  @Override
  public List<String> srandmember(String key, int count) {
    Span span = helper.buildSpan("srandmember", key);
    span.setTag("count", count);
    return helper.decorate(span, () -> super.srandmember(key, count));
  }

  @Override
  public Long zadd(String key, double score, String member) {
    Span span = helper.buildSpan("zadd", key);
    span.setTag("score", score);
    span.setTag("member", member);
    return helper.decorate(span, () -> super.zadd(key, score, member));
  }

  @Override
  public Long zadd(String key, double score, String member, ZAddParams params) {
    Span span = helper.buildSpan("zadd", key);
    span.setTag("score", score);
    span.setTag("member", member);
    span.setTag("params", TracingHelper.toString(params.getByteParams()));
    return helper.decorate(span, () -> super.zadd(key, score, member, params));
  }

  @Override
  public Long zadd(String key, Map<String, Double> scoreMembers) {
    Span span = helper.buildSpan("zadd", key);
    span.setTag("scoreMembers", TracingHelper.toString(scoreMembers));
    return helper.decorate(span, () -> super.zadd(key, scoreMembers));
  }

  @Override
  public Long zadd(String key, Map<String, Double> scoreMembers, ZAddParams params) {
    Span span = helper.buildSpan("zadd", key);
    span.setTag("scoreMembers", TracingHelper.toString(scoreMembers));
    span.setTag("params", TracingHelper.toString(params.getByteParams()));
    return helper.decorate(span, () -> super.zadd(key, scoreMembers, params));
  }

  @Override
  public Set<String> zrange(String key, long start, long end) {
    Span span = helper.buildSpan("zrange", key);
    span.setTag("start", start);
    span.setTag("end", end);
    return helper.decorate(span, () -> super.zrange(key, start, end));
  }

  @Override
  public Long zrem(String key, String... members) {
    Span span = helper.buildSpan("zrem", key);
    span.setTag("members", Arrays.toString(members));
    return helper.decorate(span, () -> super.zrem(key, members));
  }

  @Override
  public Double zincrby(String key, double score, String member) {
    Span span = helper.buildSpan("zincrby", key);
    span.setTag("score", score);
    span.setTag("member", member);
    return helper.decorate(span, () -> super.zincrby(key, score, member));
  }

  @Override
  public Double zincrby(String key, double score, String member, ZIncrByParams params) {
    Span span = helper.buildSpan("zincrby", key);
    span.setTag("score", score);
    span.setTag("member", member);
    span.setTag("params", TracingHelper.toString(params.getByteParams()));
    return helper.decorate(span, () -> super.zincrby(key, score, member, params));
  }

  @Override
  public Long zrank(String key, String member) {
    Span span = helper.buildSpan("zrank", key);
    span.setTag("member", member);
    return helper.decorate(span, () -> super.zrank(key, member));
  }

  @Override
  public Long zrevrank(String key, String member) {
    Span span = helper.buildSpan("zrevrank", key);
    span.setTag("member", member);
    return helper.decorate(span, () -> super.zrevrank(key, member));
  }

  @Override
  public Set<String> zrevrange(String key, long start, long end) {
    Span span = helper.buildSpan("zrevrange", key);
    span.setTag("start", start);
    span.setTag("end", end);
    return helper.decorate(span, () -> super.zrevrange(key, start, end));
  }

  @Override
  public Set<Tuple> zrangeWithScores(String key, long start, long end) {
    Span span = helper.buildSpan("zrangeWithScores", key);
    span.setTag("start", start);
    span.setTag("end", end);
    return helper.decorate(span, () -> super.zrangeWithScores(key, start, end));
  }

  @Override
  public Set<Tuple> zrevrangeWithScores(String key, long start, long end) {
    Span span = helper.buildSpan("zrevrangeWithScores", key);
    span.setTag("start", start);
    span.setTag("end", end);
    return helper.decorate(span, () -> super.zrevrangeWithScores(key, start, end));
  }

  @Override
  public Long zcard(String key) {
    Span span = helper.buildSpan("zcard", key);
    return helper.decorate(span, () -> super.zcard(key));
  }

  @Override
  public Double zscore(String key, String member) {
    Span span = helper.buildSpan("zscore", key);
    span.setTag("member", member);
    return helper.decorate(span, () -> super.zscore(key, member));
  }

  @Override
  public List<String> sort(String key) {
    Span span = helper.buildSpan("sort", key);
    return helper.decorate(span, () -> super.sort(key));
  }

  @Override
  public List<String> sort(String key, SortingParams sortingParameters) {
    Span span = helper.buildSpan("sort", key);
    span.setTag("sortingParameters", TracingHelper.toString(sortingParameters.getParams()));
    return helper.decorate(span, () -> super.sort(key, sortingParameters));
  }

  @Override
  public List<String> blpop(int timeout, String... keys) {
    Span span = helper.buildSpan("blpop", keys);
    span.setTag("timeout", timeout);
    return helper.decorate(span, () -> super.blpop(timeout, keys));
  }

  @Override
  public List<String> blpop(String arg) {
    Span span = helper.buildSpan("blpop");
    span.setTag("arg", arg);
    return helper.decorate(span, () -> super.blpop(arg));
  }

  @Override
  public List<String> brpop(String arg) {
    Span span = helper.buildSpan("brpop");
    span.setTag("arg", arg);
    return helper.decorate(span, () -> super.brpop(arg));
  }

  @Override
  public Long sort(String key, SortingParams sortingParameters, String dstkey) {
    Span span = helper.buildSpan("sort", key);
    span.setTag("sortingParameters", TracingHelper.toString(sortingParameters.getParams()));
    span.setTag("dstkey", dstkey);
    return helper.decorate(span, () -> super.sort(key, sortingParameters, dstkey));
  }

  @Override
  public Long sort(String key, String dstkey) {
    Span span = helper.buildSpan("sort", key);
    span.setTag("dstkey", dstkey);
    return helper.decorate(span, () -> super.sort(key, dstkey));
  }

  @Override
  public List<String> brpop(int timeout, String... keys) {
    Span span = helper.buildSpan("brpop", keys);
    span.setTag("timeout", timeout);
    return helper.decorate(span, () -> super.brpop(timeout, keys));
  }

  @Override
  public Long zcount(String key, double min, double max) {
    Span span = helper.buildSpan("zcount", key);
    span.setTag("min", min);
    span.setTag("max", max);
    return helper.decorate(span, () -> super.zcount(key, min, max));
  }

  @Override
  public Long zcount(String key, String min, String max) {
    Span span = helper.buildSpan("zcount", key);
    span.setTag("min", min);
    span.setTag("max", max);
    return helper.decorate(span, () -> super.zcount(key, min, max));
  }

  @Override
  public Set<String> zrangeByScore(String key, double min, double max) {
    Span span = helper.buildSpan("zrangeByScore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    return helper.decorate(span, () -> super.zrangeByScore(key, min, max));
  }

  @Override
  public Set<String> zrangeByScore(String key, String min, String max) {
    Span span = helper.buildSpan("zrangeByScore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    return helper.decorate(span, () -> super.zrangeByScore(key, min, max));
  }

  @Override
  public Set<String> zrangeByScore(String key, double min, double max, int offset, int count) {
    Span span = helper.buildSpan("zrangeByScore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return helper.decorate(span, () -> super.zrangeByScore(key, min, max, offset, count));
  }

  @Override
  public Set<String> zrangeByScore(String key, String min, String max, int offset, int count) {
    Span span = helper.buildSpan("zrangeByScore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return helper.decorate(span, () -> super.zrangeByScore(key, min, max, offset, count));
  }

  @Override
  public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max) {
    Span span = helper.buildSpan("zrangeByScoreWithScores", key);
    span.setTag("min", min);
    span.setTag("max", max);
    return helper.decorate(span, () -> super.zrangeByScoreWithScores(key, min, max));
  }

  @Override
  public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max) {
    Span span = helper.buildSpan("zrangeByScoreWithScores", key);
    span.setTag("min", min);
    span.setTag("max", max);
    return helper.decorate(span, () -> super.zrangeByScoreWithScores(key, min, max));
  }

  @Override
  public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max, int offset,
      int count) {
    Span span = helper.buildSpan("zrangeByScoreWithScores", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return helper.decorate(span, () -> super.zrangeByScoreWithScores(key, min, max, offset, count));
  }

  @Override
  public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max, int offset,
      int count) {
    Span span = helper.buildSpan("zrangeByScoreWithScores", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return helper.decorate(span, () -> super.zrangeByScoreWithScores(key, min, max, offset, count));
  }

  @Override
  public Set<String> zrevrangeByScore(String key, double max, double min) {
    Span span = helper.buildSpan("zrevrangeByScore", key);
    span.setTag("max", max);
    span.setTag("min", min);
    return helper.decorate(span, () -> super.zrevrangeByScore(key, max, min));
  }

  @Override
  public Set<String> zrevrangeByScore(String key, String max, String min) {
    Span span = helper.buildSpan("zrevrangeByScore", key);
    span.setTag("max", max);
    span.setTag("min", min);
    return helper.decorate(span, () -> super.zrevrangeByScore(key, max, min));
  }

  @Override
  public Set<String> zrevrangeByScore(String key, double max, double min, int offset, int count) {
    Span span = helper.buildSpan("zrevrangeByScore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return helper.decorate(span, () -> super.zrevrangeByScore(key, max, min, offset, count));
  }

  @Override
  public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min) {
    Span span = helper.buildSpan("zrevrangeByScoreWithScores", key);
    span.setTag("max", max);
    span.setTag("min", min);
    return helper.decorate(span, () -> super.zrevrangeByScoreWithScores(key, max, min));
  }

  @Override
  public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min, int offset,
      int count) {
    Span span = helper.buildSpan("zrevrangeByScoreWithScores", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return helper.decorate(span, () -> super.zrevrangeByScoreWithScores(key, max, min, offset, count));
  }

  @Override
  public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min, int offset,
      int count) {
    Span span = helper.buildSpan("zrevrangeByScoreWithScores", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return helper.decorate(span, () -> super.zrevrangeByScoreWithScores(key, max, min, offset, count));
  }

  @Override
  public Set<String> zrevrangeByScore(String key, String max, String min, int offset, int count) {
    Span span = helper.buildSpan("zrevrangeByScore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return helper.decorate(span, () -> super.zrevrangeByScore(key, max, min, offset, count));
  }

  @Override
  public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min) {
    Span span = helper.buildSpan("zrevrangeByScoreWithScores", key);
    span.setTag("max", max);
    span.setTag("min", min);
    return helper.decorate(span, () -> super.zrevrangeByScoreWithScores(key, max, min));
  }

  @Override
  public Long zremrangeByRank(String key, long start, long end) {
    Span span = helper.buildSpan("zremrangeByRank", key);
    span.setTag("start", start);
    span.setTag("end", end);
    return helper.decorate(span, () -> super.zremrangeByRank(key, start, end));
  }

  @Override
  public Long zremrangeByScore(String key, double start, double end) {
    Span span = helper.buildSpan("zremrangeByScore", key);
    span.setTag("start", start);
    span.setTag("end", end);
    return helper.decorate(span, () -> super.zremrangeByScore(key, start, end));
  }

  @Override
  public Long zremrangeByScore(String key, String start, String end) {
    Span span = helper.buildSpan("zremrangeByScore", key);
    span.setTag("start", start);
    span.setTag("end", end);
    return helper.decorate(span, () -> super.zremrangeByScore(key, start, end));
  }

  @Override
  public Long zunionstore(String dstkey, String... sets) {
    Span span = helper.buildSpan("zunionstore");
    span.setTag("dstkey", dstkey);
    span.setTag("sets", Arrays.toString(sets));
    return helper.decorate(span, () -> super.zunionstore(dstkey, sets));
  }

  @Override
  public Long zunionstore(String dstkey, ZParams params, String... sets) {
    Span span = helper.buildSpan("zunionstore");
    span.setTag("params", TracingHelper.toString(params.getParams()));
    span.setTag("sets", Arrays.toString(sets));
    return helper.decorate(span, () -> super.zunionstore(dstkey, params, sets));
  }

  @Override
  public Long zinterstore(String dstkey, String... sets) {
    Span span = helper.buildSpan("zinterstore");
    span.setTag("dstkey", dstkey);
    span.setTag("sets", Arrays.toString(sets));
    return helper.decorate(span, () -> super.zinterstore(dstkey, sets));
  }

  @Override
  public Long zinterstore(String dstkey, ZParams params, String... sets) {
    Span span = helper.buildSpan("zinterstore");
    span.setTag("dstkey", dstkey);
    span.setTag("params", TracingHelper.toString(params.getParams()));
    span.setTag("sets", Arrays.toString(sets));
    return helper.decorate(span, () -> super.zinterstore(dstkey, params, sets));
  }

  @Override
  public Long zlexcount(String key, String min, String max) {
    Span span = helper.buildSpan("zlexcount", key);
    span.setTag("min", min);
    span.setTag("max", max);
    return helper.decorate(span, () -> super.zlexcount(key, min, max));
  }

  @Override
  public Set<String> zrangeByLex(String key, String min, String max) {
    Span span = helper.buildSpan("zrangeByLex", key);
    span.setTag("min", min);
    span.setTag("max", max);
    return helper.decorate(span, () -> super.zrangeByLex(key, min, max));
  }

  @Override
  public Set<String> zrangeByLex(String key, String min, String max, int offset, int count) {
    Span span = helper.buildSpan("zrangeByLex", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return helper.decorate(span, () -> super.zrangeByLex(key, min, max, offset, count));
  }

  @Override
  public Set<String> zrevrangeByLex(String key, String max, String min) {
    Span span = helper.buildSpan("zrevrangeByLex", key);
    span.setTag("max", max);
    span.setTag("min", min);
    return helper.decorate(span, () -> super.zrevrangeByLex(key, max, min));
  }

  @Override
  public Set<String> zrevrangeByLex(String key, String max, String min, int offset, int count) {
    Span span = helper.buildSpan("zrevrangeByLex", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return helper.decorate(span, () -> super.zrevrangeByLex(key, max, min, offset, count));
  }

  @Override
  public Long zremrangeByLex(String key, String min, String max) {
    Span span = helper.buildSpan("zremrangeByLex", key);
    span.setTag("min", min);
    span.setTag("max", max);
    return helper.decorate(span, () -> super.zremrangeByLex(key, min, max));
  }

  @Override
  public Long strlen(String key) {
    Span span = helper.buildSpan("strlen", key);
    return helper.decorate(span, () -> super.strlen(key));
  }

  @Override
  public Long lpushx(String key, String... string) {
    Span span = helper.buildSpan("lpushx", key);
    span.setTag("string", Arrays.toString(string));
    return helper.decorate(span, () -> super.lpushx(key, string));
  }

  @Override
  public Long persist(String key) {
    Span span = helper.buildSpan("persist", key);
    return helper.decorate(span, () -> super.persist(key));
  }

  @Override
  public Long rpushx(String key, String... string) {
    Span span = helper.buildSpan("rpushx", key);
    span.setTag("string", Arrays.toString(string));
    return helper.decorate(span, () -> super.rpushx(key, string));
  }

  @Override
  public String echo(String string) {
    Span span = helper.buildSpan("echo");
    span.setTag("string", string);
    return helper.decorate(span, () -> super.echo(string));
  }

  @Override
  public Long linsert(String key, LIST_POSITION where, String pivot, String value) {
    Span span = helper.buildSpan("linsert", key);
    span.setTag("where", where.name());
    span.setTag("pivot", pivot);
    span.setTag("value", value);
    return helper.decorate(span, () -> super.linsert(key, where, pivot, value));
  }

  @Override
  public Long linsert(String key, ListPosition where, String pivot, String value) {
    Span span = helper.buildSpan("linsert", key);
    span.setTag("where", where.name());
    span.setTag("pivot", pivot);
    span.setTag("value", value);
    return helper.decorate(span, () -> super.linsert(key, where, pivot, value));
  }

  @Override
  public String brpoplpush(String source, String destination, int timeout) {
    Span span = helper.buildSpan("brpoplpush");
    span.setTag("source", source);
    span.setTag("destination", destination);
    span.setTag("timeout", timeout);
    return helper.decorate(span, () -> super.brpoplpush(source, destination, timeout));
  }

  @Override
  public Boolean setbit(String key, long offset, boolean value) {
    Span span = helper.buildSpan("setbit", key);
    span.setTag("offset", offset);
    span.setTag("value", value);
    return helper.decorate(span, () -> super.setbit(key, offset, value));
  }

  @Override
  public Boolean setbit(String key, long offset, String value) {
    Span span = helper.buildSpan("setbit", key);
    span.setTag("offset", offset);
    span.setTag("value", value);
    return helper.decorate(span, () -> super.setbit(key, offset, value));
  }

  @Override
  public Boolean getbit(String key, long offset) {
    Span span = helper.buildSpan("getbit", key);
    span.setTag("offset", offset);
    return helper.decorate(span, () -> super.getbit(key, offset));
  }

  @Override
  public Long setrange(String key, long offset, String value) {
    Span span = helper.buildSpan("setrange", key);
    span.setTag("offset", offset);
    span.setTag("value", value);
    return helper.decorate(span, () -> super.setrange(key, offset, value));
  }

  @Override
  public String getrange(String key, long startOffset, long endOffset) {
    Span span = helper.buildSpan("getrange", key);
    span.setTag("startOffset", startOffset);
    span.setTag("endOffset", endOffset);
    return helper.decorate(span, () -> super.getrange(key, startOffset, endOffset));
  }

  @Override
  public Long bitpos(String key, boolean value) {
    Span span = helper.buildSpan("bitpos", key);
    span.setTag("value", value);
    return helper.decorate(span, () -> super.bitpos(key, value));
  }

  @Override
  public Long bitpos(String key, boolean value, BitPosParams params) {
    Span span = helper.buildSpan("bitpos", key);
    span.setTag("value", value);
    span.setTag("params", TracingHelper.toString(params.getParams()));
    return helper.decorate(span, () -> super.bitpos(key, value, params));
  }

  @Override
  public Object eval(String script, int keyCount, String... params) {
    Span span = helper.buildSpan("eval");
    span.setTag("keyCount", keyCount);
    span.setTag("params", Arrays.toString(params));
    return helper.decorate(span, () -> super.eval(script, keyCount, params));
  }

  @Override
  public Object eval(String script, String key) {
    Span span = helper.buildSpan(Command.EVAL.name(), key);
    span.setTag("script", script);
    return helper.decorate(span, () -> super.eval(script, key));
  }

  @Override
  public void subscribe(JedisPubSub jedisPubSub, String... channels) {
    Span span = helper.buildSpan("subscribe");
    span.setTag("channels", Arrays.toString(channels));
    helper.decorate(span, () -> super.subscribe(jedisPubSub, channels));
  }

  @Override
  public Long publish(String channel, String message) {
    Span span = helper.buildSpan("publish");
    span.setTag("channel", channel);
    span.setTag("message", message);
    return helper.decorate(span, () -> super.publish(channel, message));
  }

  @Override
  public void psubscribe(JedisPubSub jedisPubSub, String... patterns) {
    Span span = helper.buildSpan("psubscribe");
    span.setTag("patterns", Arrays.toString(patterns));
    helper.decorate(span, () -> super.psubscribe(jedisPubSub, patterns));
  }

  @Override
  public Object eval(String script, List<String> keys, List<String> args) {
    Span span = helper.buildSpan("eval");
    span.setTag("keys", TracingHelper.toString(keys));
    span.setTag("args", TracingHelper.toString(args));
    return helper.decorate(span, () -> super.eval(script, keys, args));
  }

  @Override
  public Object evalsha(String sha1, List<String> keys, List<String> args) {
    Span span = helper.buildSpan("evalsha");
    span.setTag("keys", TracingHelper.toString(keys));
    span.setTag("args", TracingHelper.toString(args));
    span.setTag("sha1", sha1);
    return helper.decorate(span, () -> super.evalsha(sha1, keys, args));
  }

  @Override
  public Object evalsha(String script, String key) {
    Span span = helper.buildSpan(Command.EVALSHA.name(), key);
    span.setTag("script", script);
    return helper.decorate(span, () -> super.evalsha(script, key));
  }

  @Override
  public Boolean scriptExists(String sha1, String key) {
    Span span = helper.buildSpan("scriptExists", key);
    span.setTag("sha1", sha1);
    return helper.decorate(span, () -> super.scriptExists(sha1, key));
  }

  @Override
  public List<Boolean> scriptExists(String key, String... sha1) {
    Span span = helper.buildSpan("scriptExists", key);
    span.setTag("sha1", Arrays.toString(sha1));
    return helper.decorate(span, () -> super.scriptExists(key, sha1));
  }

  @Override
  public String scriptLoad(String script, String key) {
    Span span = helper.buildSpan("scriptLoad", key);
    span.setTag("script", script);
    return helper.decorate(span, () -> super.scriptLoad(script, key));
  }

  @Override
  public String scriptFlush(String sampleKey) {
    Span span = helper.buildSpan("scriptFlush");
    span.setTag("sampleKey", sampleKey);
    return helper.decorate(span, () -> super.scriptFlush(sampleKey));
  }

  @Override
  public String scriptKill(String sampleKey) {
    Span span = helper.buildSpan("scriptKill");
    span.setTag("sampleKey", sampleKey);
    return helper.decorate(span, () -> super.scriptKill(sampleKey));
  }

  @Override
  public Object evalsha(String sha1, int keyCount, String... params) {
    Span span = helper.buildSpan("evalsha");
    span.setTag("keyCount", keyCount);
    span.setTag("params", Arrays.toString(params));
    span.setTag("sha1", sha1);
    return helper.decorate(span, () -> super.evalsha(sha1, keyCount, params));
  }

  @Override
  public Long bitcount(String key) {
    Span span = helper.buildSpan("bitcount", key);
    return helper.decorate(span, () -> super.bitcount(key));
  }

  @Override
  public Long bitcount(String key, long start, long end) {
    Span span = helper.buildSpan("bitcount", key);
    span.setTag("start", start);
    span.setTag("end", end);
    return helper.decorate(span, () -> super.bitcount(key, start, end));
  }

  @Override
  public Set<String> keys(String pattern) {
    Span span = helper.buildSpan("keys");
    span.setTag("pattern", nullable(pattern));
    return helper.decorate(span, () -> super.keys(pattern));
  }

  @Override
  public Long bitop(BitOP op, String destKey, String... srcKeys) {
    Span span = helper.buildSpan("bitop");
    span.setTag("destKey", destKey);
    span.setTag("srcKeys", Arrays.toString(srcKeys));
    return helper.decorate(span, () -> super.bitop(op, destKey, srcKeys));
  }

  @Override
  public String ping() {
    Span span = helper.buildSpan("ping");
    return helper.decorate(span, () -> super.ping());
  }

  @Override
  public String set(byte[] key, byte[] value) {
    Span span = helper.buildSpan("set", key);
    span.setTag("value", Arrays.toString(value));
    return helper.decorate(span, () -> super.set(key, value));
  }

  @Override
  public String set(byte[] key, byte[] value, byte[] nxxx, byte[] expx, long time) {
    Span span = helper.buildSpan("set", key);
    span.setTag("nxxx", Arrays.toString(nxxx));
    span.setTag("expx", Arrays.toString(expx));
    span.setTag("time", time);
    span.setTag("value", Arrays.toString(value));
    return helper.decorate(span, () -> super.set(key, value, nxxx, expx, time));
  }

  @Override
  public String set(byte[] key, byte[] value, byte[] expx, long time) {
    Span span = helper.buildSpan("set", key);
    span.setTag("value", Arrays.toString(value));
    span.setTag("expx", Arrays.toString(expx));
    span.setTag("time", time);
    return helper.decorate(span, () -> super.set(key, value, expx, time));
  }

  @Override
  public byte[] get(byte[] key) {
    Span span = helper.buildSpan("get", key);
    return helper.decorate(span, () -> super.get(key));
  }

  @Override
  public String quit() {
    Span span = helper.buildSpan("quit");
    return helper.decorate(span, () -> super.quit());
  }

  @Override
  public Long exists(byte[]... keys) {
    Span span = helper.buildSpan("exists");
    span.setTag("keys", TracingHelper.toString(keys));
    return helper.decorate(span, () -> super.exists(keys));
  }

  @Override
  public Boolean exists(byte[] key) {
    Span span = helper.buildSpan("exists", key);
    return helper.decorate(span, () -> super.exists(key));
  }

  @Override
  public Long del(byte[]... keys) {
    Span span = helper.buildSpan("del");
    span.setTag("keys", TracingHelper.toString(keys));
    return helper.decorate(span, () -> super.del(keys));
  }

  @Override
  public Long del(byte[] key) {
    Span span = helper.buildSpan("del", key);
    return helper.decorate(span, () -> super.del(key));
  }

  @Override
  public Long unlink(byte[] key) {
    Span span = helper.buildSpan("unlink", key);
    return helper.decorate(span, () -> super.unlink(key));
  }

  @Override
  public Long unlink(byte[]... keys) {
    Span span = helper.buildSpan("unlink", keys);
    return helper.decorate(span, () -> super.unlink(keys));
  }

  @Override
  public String type(byte[] key) {
    Span span = helper.buildSpan("type", key);
    return helper.decorate(span, () -> super.type(key));
  }

  @Override
  public byte[] dump(byte[] key) {
    Span span = helper.buildSpan("dump", key);
    return helper.decorate(span, () -> super.dump(key));
  }

  @Override
  public String restore(byte[] key, int ttl, byte[] serializedValue) {
    Span span = helper.buildSpan("restore", key);
    span.setTag("ttl", ttl);
    span.setTag("serializedValue", Arrays.toString(serializedValue));
    return helper.decorate(span, () -> super.restore(key, ttl, serializedValue));
  }

  @Override
  public String flushDB() {
    Span span = helper.buildSpan("flushDB");
    return helper.decorate(span, () -> super.flushDB());
  }

  @Override
  public String rename(byte[] oldkey, byte[] newkey) {
    Span span = helper.buildSpan("rename");
    span.setTag("oldkey", Arrays.toString(oldkey));
    span.setTag("newkey", Arrays.toString(newkey));
    return helper.decorate(span, () -> super.rename(oldkey, newkey));
  }

  @Override
  public Long renamenx(byte[] oldkey, byte[] newkey) {
    Span span = helper.buildSpan("renamenx");
    span.setTag("oldkey", Arrays.toString(oldkey));
    span.setTag("newkey", Arrays.toString(newkey));
    return helper.decorate(span, () -> super.renamenx(oldkey, newkey));
  }

  @Override
  public Long dbSize() {
    Span span = helper.buildSpan("dbSize");
    return helper.decorate(span, () -> super.dbSize());
  }

  @Override
  public Long expire(byte[] key, int seconds) {
    Span span = helper.buildSpan("expire", key);
    span.setTag("seconds", seconds);
    return helper.decorate(span, () -> super.expire(key, seconds));
  }

  @Override
  public Long pexpire(String key, long milliseconds) {
    Span span = helper.buildSpan("pexpire", key);
    span.setTag("milliseconds", milliseconds);
    return helper.decorate(span, () -> super.pexpire(key, milliseconds));
  }

  @Override
  public Long expireAt(byte[] key, long unixTime) {
    Span span = helper.buildSpan("expireAt", key);
    span.setTag("unixTime", unixTime);
    return helper.decorate(span, () -> super.expireAt(key, unixTime));
  }

  @Override
  public Long ttl(byte[] key) {
    Span span = helper.buildSpan("ttl", key);
    return helper.decorate(span, () -> super.ttl(key));
  }

  @Override
  public Long pttl(byte[] key) {
    Span span = helper.buildSpan("pttl", key);
    return helper.decorate(span, () -> super.pttl(key));
  }

  @Override
  public Long touch(byte[] key) {
    Span span = helper.buildSpan("touch", key);
    return helper.decorate(span, () -> super.touch(key));
  }

  @Override
  public Long touch(byte[]... keys) {
    Span span = helper.buildSpan("touch", keys);
    return helper.decorate(span, () -> super.touch(keys));
  }

  @Override
  public String select(int index) {
    Span span = helper.buildSpan("select");
    span.setTag("index", index);
    return helper.decorate(span, () -> super.select(index));
  }

  @Override
  public String swapDB(int index1, int index2) {
    Span span = helper.buildSpan("swapDB");
    span.setTag("index1", index1);
    span.setTag("index2", index2);
    return helper.decorate(span, () -> super.swapDB(index1, index2));
  }

  @Override
  public String flushAll() {
    Span span = helper.buildSpan("flushAll");
    return helper.decorate(span, () -> super.flushAll());
  }

  @Override
  public byte[] getSet(byte[] key, byte[] value) {
    Span span = helper.buildSpan("getSet", key);
    span.setTag("value", Arrays.toString(value));
    return helper.decorate(span, () -> super.getSet(key, value));
  }

  @Override
  public List<byte[]> mget(byte[]... keys) {
    Span span = helper.buildSpan("mget");
    span.setTag("keys", TracingHelper.toString(keys));
    return helper.decorate(span, () -> super.mget(keys));
  }

  @Override
  public Long setnx(byte[] key, byte[] value) {
    Span span = helper.buildSpan("setnx", key);
    span.setTag("value", Arrays.toString(value));
    return helper.decorate(span, () -> super.setnx(key, value));
  }

  @Override
  public String psetex(byte[] key, long milliseconds, byte[] value) {
    Span span = helper.buildSpan("psetex", key);
    span.setTag("milliseconds", milliseconds);
    span.setTag("value", Arrays.toString(value));
    return helper.decorate(span, () -> super.psetex(key, milliseconds, value));
  }

  @Override
  public String setex(byte[] key, int seconds, byte[] value) {
    Span span = helper.buildSpan("setex", key);
    span.setTag("value", Arrays.toString(value));
    span.setTag("seconds", seconds);
    return helper.decorate(span, () -> super.setex(key, seconds, value));
  }

  @Override
  public String mset(byte[]... keysvalues) {
    Span span = helper.buildSpan("mset");
    span.setTag("keysvalues", TracingHelper.toString(keysvalues));
    return helper.decorate(span, () -> super.mset(keysvalues));
  }

  @Override
  public Long msetnx(byte[]... keysvalues) {
    Span span = helper.buildSpan("msetnx");
    span.setTag("keysvalues", TracingHelper.toString(keysvalues));
    return helper.decorate(span, () -> super.msetnx(keysvalues));
  }

  @Override
  public Long decrBy(byte[] key, long integer) {
    Span span = helper.buildSpan("decrBy", key);
    span.setTag("integer", integer);
    return helper.decorate(span, () -> super.decrBy(key, integer));
  }

  @Override
  public Long decr(byte[] key) {
    Span span = helper.buildSpan("decr", key);
    return helper.decorate(span, () -> super.decr(key));
  }

  @Override
  public Long incrBy(byte[] key, long integer) {
    Span span = helper.buildSpan("incrBy", key);
    span.setTag("integer", integer);
    return helper.decorate(span, () -> super.incrBy(key, integer));
  }

  @Override
  public Double incrByFloat(byte[] key, double integer) {
    Span span = helper.buildSpan("incrByFloat", key);
    span.setTag("integer", integer);
    return helper.decorate(span, () -> super.incrByFloat(key, integer));
  }

  @Override
  public Long incr(byte[] key) {
    Span span = helper.buildSpan("incr", key);
    return helper.decorate(span, () -> super.incr(key));
  }

  @Override
  public Long append(byte[] key, byte[] value) {
    Span span = helper.buildSpan("append", key);
    span.setTag("value", Arrays.toString(value));
    return helper.decorate(span, () -> super.append(key, value));
  }

  @Override
  public byte[] substr(byte[] key, int start, int end) {
    Span span = helper.buildSpan("substr", key);
    span.setTag("start", start);
    span.setTag("end", end);
    return helper.decorate(span, () -> super.substr(key, start, end));
  }

  @Override
  public Long hset(byte[] key, byte[] field, byte[] value) {
    Span span = helper.buildSpan("hset", key);
    span.setTag("field", Arrays.toString(field));
    span.setTag("value", Arrays.toString(value));
    return helper.decorate(span, () -> super.hset(key, field, value));
  }

  @Override
  public Long hset(byte[] key, Map<byte[], byte[]> hash) {
    Span span = helper.buildSpan("hset", key);
    span.setTag("hash", TracingHelper.toStringMap(hash));
    return helper.decorate(span, () -> super.hset(key, hash));
  }

  @Override
  public byte[] hget(byte[] key, byte[] field) {
    Span span = helper.buildSpan("hget", key);
    span.setTag("field", Arrays.toString(field));
    return helper.decorate(span, () -> super.hget(key, field));
  }

  @Override
  public Long hsetnx(byte[] key, byte[] field, byte[] value) {
    Span span = helper.buildSpan("hsetnx", key);
    span.setTag("field", Arrays.toString(field));
    span.setTag("value", Arrays.toString(value));
    return helper.decorate(span, () -> super.hsetnx(key, field, value));
  }

  @Override
  public String hmset(byte[] key, Map<byte[], byte[]> hash) {
    Span span = helper.buildSpan("hmset", key);
    span.setTag("hash", TracingHelper.toStringMap(hash));
    return helper.decorate(span, () -> super.hmset(key, hash));
  }

  @Override
  public List<byte[]> hmget(byte[] key, byte[]... fields) {
    Span span = helper.buildSpan("hmget", key);
    span.setTag("fields", TracingHelper.toString(fields));
    return helper.decorate(span, () -> super.hmget(key, fields));
  }

  @Override
  public Long hincrBy(byte[] key, byte[] field, long value) {
    Span span = helper.buildSpan("hincrBy", key);
    span.setTag("field", Arrays.toString(field));
    span.setTag("value", value);
    return helper.decorate(span, () -> super.hincrBy(key, field, value));
  }

  @Override
  public Double hincrByFloat(byte[] key, byte[] field, double value) {
    Span span = helper.buildSpan("hincrByFloat", key);
    span.setTag("field", Arrays.toString(field));
    span.setTag("value", value);
    return helper.decorate(span, () -> super.hincrByFloat(key, field, value));
  }

  @Override
  public Boolean hexists(byte[] key, byte[] field) {
    Span span = helper.buildSpan("hexists", key);
    span.setTag("field", Arrays.toString(field));
    return helper.decorate(span, () -> super.hexists(key, field));
  }

  @Override
  public Long hdel(byte[] key, byte[]... fields) {
    Span span = helper.buildSpan("hdel", key);
    span.setTag("fields", TracingHelper.toString(fields));
    return helper.decorate(span, () -> super.hdel(key, fields));
  }

  @Override
  public Long hlen(byte[] key) {
    Span span = helper.buildSpan("hlen", key);
    return helper.decorate(span, () -> super.hlen(key));
  }

  @Override
  public Set<byte[]> hkeys(byte[] key) {
    Span span = helper.buildSpan("hkeys", key);
    return helper.decorate(span, () -> super.hkeys(key));
  }

  @Override
  public Collection<byte[]> hvals(byte[] key) {
    Span span = helper.buildSpan("hvals", key);
    return helper.decorate(span, () -> super.hvals(key));
  }

  @Override
  public Map<byte[], byte[]> hgetAll(byte[] key) {
    Span span = helper.buildSpan("hgetAll", key);
    return helper.decorate(span, () -> super.hgetAll(key));
  }

  @Override
  public Long rpush(byte[] key, byte[]... strings) {
    Span span = helper.buildSpan("rpush", key);
    span.setTag("strings", TracingHelper.toString(strings));
    return helper.decorate(span, () -> super.rpush(key, strings));
  }

  @Override
  public Long lpush(byte[] key, byte[]... strings) {
    Span span = helper.buildSpan("lpush", key);
    span.setTag("strings", TracingHelper.toString(strings));
    return helper.decorate(span, () -> super.lpush(key, strings));
  }

  @Override
  public Long llen(byte[] key) {
    Span span = helper.buildSpan("llen", key);
    return helper.decorate(span, () -> super.llen(key));
  }

  @Override
  public List<byte[]> lrange(byte[] key, long start, long end) {
    Span span = helper.buildSpan("lrange", key);
    span.setTag("start", start);
    span.setTag("end", end);
    return helper.decorate(span, () -> super.lrange(key, start, end));
  }

  @Override
  public String ltrim(byte[] key, long start, long end) {
    Span span = helper.buildSpan("ltrim", key);
    span.setTag("start", start);
    span.setTag("end", end);
    return helper.decorate(span, () -> super.ltrim(key, start, end));
  }

  @Override
  public byte[] lindex(byte[] key, long index) {
    Span span = helper.buildSpan("lindex", key);
    span.setTag("index", index);
    return helper.decorate(span, () -> super.lindex(key, index));
  }

  @Override
  public String lset(byte[] key, long index, byte[] value) {
    Span span = helper.buildSpan("lset", key);
    span.setTag("index", index);
    span.setTag("value", Arrays.toString(value));
    return helper.decorate(span, () -> super.lset(key, index, value));
  }

  @Override
  public Long lrem(byte[] key, long count, byte[] value) {
    Span span = helper.buildSpan("lrem", key);
    span.setTag("count", count);
    span.setTag("value", Arrays.toString(value));
    return helper.decorate(span, () -> super.lrem(key, count, value));
  }

  @Override
  public byte[] lpop(byte[] key) {
    Span span = helper.buildSpan("lpop", key);
    return helper.decorate(span, () -> super.lpop(key));
  }

  @Override
  public byte[] rpop(byte[] key) {
    Span span = helper.buildSpan("rpop", key);
    return helper.decorate(span, () -> super.rpop(key));
  }

  @Override
  public byte[] rpoplpush(byte[] srckey, byte[] dstkey) {
    Span span = helper.buildSpan("rpoplpush");
    span.setTag("srckey", Arrays.toString(srckey));
    span.setTag("dstkey", Arrays.toString(dstkey));
    return helper.decorate(span, () -> super.rpoplpush(srckey, dstkey));
  }

  @Override
  public Long sadd(byte[] key, byte[]... members) {
    Span span = helper.buildSpan("sadd", key);
    span.setTag("members", Arrays.toString(members));
    return helper.decorate(span, () -> super.sadd(key, members));
  }

  @Override
  public Set<byte[]> smembers(byte[] key) {
    Span span = helper.buildSpan("smembers", key);
    return helper.decorate(span, () -> super.smembers(key));
  }

  @Override
  public Long srem(byte[] key, byte[]... member) {
    Span span = helper.buildSpan("srem", key);
    span.setTag("member", Arrays.toString(member));
    return helper.decorate(span, () -> super.srem(key, member));
  }

  @Override
  public byte[] spop(byte[] key) {
    Span span = helper.buildSpan("spop", key);
    return helper.decorate(span, () -> super.spop(key));
  }

  @Override
  public Set<byte[]> spop(byte[] key, long count) {
    Span span = helper.buildSpan("spop", key);
    span.setTag("count", count);
    return helper.decorate(span, () -> super.spop(key, count));
  }

  @Override
  public Long smove(byte[] srckey, byte[] dstkey, byte[] member) {
    Span span = helper.buildSpan("smove");
    span.setTag("srckey", Arrays.toString(srckey));
    span.setTag("dstkey", Arrays.toString(dstkey));
    span.setTag("member", Arrays.toString(member));
    return helper.decorate(span, () -> super.smove(srckey, dstkey, member));
  }

  @Override
  public Long scard(byte[] key) {
    Span span = helper.buildSpan("scard", key);
    return helper.decorate(span, () -> super.scard(key));
  }

  @Override
  public Boolean sismember(byte[] key, byte[] member) {
    Span span = helper.buildSpan("sismember", key);
    span.setTag("member", Arrays.toString(member));
    return helper.decorate(span, () -> super.sismember(key, member));
  }

  @Override
  public Set<byte[]> sinter(byte[]... keys) {
    Span span = helper.buildSpan("sinter");
    span.setTag("keys", TracingHelper.toString(keys));
    return helper.decorate(span, () -> super.sinter(keys));
  }

  @Override
  public Long sinterstore(byte[] dstkey, byte[]... keys) {
    Span span = helper.buildSpan("sinterstore");
    span.setTag("dstkey", Arrays.toString(dstkey));
    span.setTag("keys", TracingHelper.toString(keys));
    return helper.decorate(span, () -> super.sinterstore(dstkey, keys));
  }

  @Override
  public Set<byte[]> sunion(byte[]... keys) {
    Span span = helper.buildSpan("sunion");
    span.setTag("keys", TracingHelper.toString(keys));
    return helper.decorate(span, () -> super.sunion(keys));
  }

  @Override
  public Long sunionstore(byte[] dstkey, byte[]... keys) {
    Span span = helper.buildSpan("sunionstore");
    span.setTag("dstkey", Arrays.toString(dstkey));
    span.setTag("keys", TracingHelper.toString(keys));
    return helper.decorate(span, () -> super.sunionstore(dstkey, keys));
  }

  @Override
  public Set<byte[]> sdiff(byte[]... keys) {
    Span span = helper.buildSpan("sdiff");
    span.setTag("keys", TracingHelper.toString(keys));
    return helper.decorate(span, () -> super.sdiff(keys));
  }

  @Override
  public Long sdiffstore(byte[] dstkey, byte[]... keys) {
    Span span = helper.buildSpan("sdiffstore");
    span.setTag("dstkey", Arrays.toString(dstkey));
    span.setTag("keys", TracingHelper.toString(keys));
    return helper.decorate(span, () -> super.sdiffstore(dstkey, keys));
  }

  @Override
  public byte[] srandmember(byte[] key) {
    Span span = helper.buildSpan("srandmember", key);
    return helper.decorate(span, () -> super.srandmember(key));
  }

  @Override
  public List<byte[]> srandmember(byte[] key, int count) {
    Span span = helper.buildSpan("srandmember", key);
    span.setTag("count", count);
    return helper.decorate(span, () -> super.srandmember(key, count));
  }

  @Override
  public Long zadd(byte[] key, double score, byte[] member) {
    Span span = helper.buildSpan("zadd", key);
    span.setTag("member", Arrays.toString(member));
    span.setTag("score", score);
    return helper.decorate(span, () -> super.zadd(key, score, member));
  }

  @Override
  public Long zadd(byte[] key, double score, byte[] member, ZAddParams params) {
    Span span = helper.buildSpan("zadd", key);
    span.setTag("member", Arrays.toString(member));
    span.setTag("score", score);
    span.setTag("params", TracingHelper.toString(params.getByteParams()));
    return helper.decorate(span, () -> super.zadd(key, score, member, params));
  }

  @Override
  public Long zadd(byte[] key, Map<byte[], Double> scoreMembers) {
    Span span = helper.buildSpan("zadd", key);
    span.setTag("scoreMembers", TracingHelper.toStringMap2(scoreMembers));
    return helper.decorate(span, () -> super.zadd(key, scoreMembers));
  }

  @Override
  public Long zadd(byte[] key, Map<byte[], Double> scoreMembers, ZAddParams params) {
    Span span = helper.buildSpan("zadd", key);
    span.setTag("scoreMembers", TracingHelper.toStringMap2(scoreMembers));
    span.setTag("params", TracingHelper.toString(params.getByteParams()));
    return helper.decorate(span, () -> super.zadd(key, scoreMembers, params));
  }

  @Override
  public Set<byte[]> zrange(byte[] key, long start, long end) {
    Span span = helper.buildSpan("zrange", key);
    span.setTag("start", start);
    span.setTag("end", end);
    return helper.decorate(span, () -> super.zrange(key, start, end));
  }

  @Override
  public Long zrem(byte[] key, byte[]... members) {
    Span span = helper.buildSpan("zrem", key);
    span.setTag("members", Arrays.toString(members));
    return helper.decorate(span, () -> super.zrem(key, members));
  }

  @Override
  public Double zincrby(byte[] key, double score, byte[] member) {
    Span span = helper.buildSpan("zincrby", key);
    span.setTag("member", Arrays.toString(member));
    span.setTag("score", score);
    return helper.decorate(span, () -> super.zincrby(key, score, member));
  }

  @Override
  public Double zincrby(byte[] key, double score, byte[] member, ZIncrByParams params) {
    Span span = helper.buildSpan("zincrby", key);
    span.setTag("member", Arrays.toString(member));
    span.setTag("score", score);
    span.setTag("params", TracingHelper.toString(params.getByteParams()));
    return helper.decorate(span, () -> super.zincrby(key, score, member, params));
  }

  @Override
  public Long zrank(byte[] key, byte[] member) {
    Span span = helper.buildSpan("zrank", key);
    span.setTag("member", Arrays.toString(member));
    return helper.decorate(span, () -> super.zrank(key, member));
  }

  @Override
  public Long zrevrank(byte[] key, byte[] member) {
    Span span = helper.buildSpan("zrevrank", key);
    span.setTag("member", Arrays.toString(member));
    return helper.decorate(span, () -> super.zrevrank(key, member));
  }

  @Override
  public Set<byte[]> zrevrange(byte[] key, long start, long end) {
    Span span = helper.buildSpan("zrevrange", key);
    span.setTag("start", start);
    span.setTag("end", end);
    return helper.decorate(span, () -> super.zrevrange(key, start, end));
  }

  @Override
  public Set<Tuple> zrangeWithScores(byte[] key, long start, long end) {
    Span span = helper.buildSpan("zrangeWithScores", key);
    span.setTag("start", start);
    span.setTag("end", end);
    return helper.decorate(span, () -> super.zrangeWithScores(key, start, end));
  }

  @Override
  public Set<Tuple> zrevrangeWithScores(byte[] key, long start, long end) {
    Span span = helper.buildSpan("zrevrangeWithScores", key);
    span.setTag("start", start);
    span.setTag("end", end);
    return helper.decorate(span, () -> super.zrevrangeWithScores(key, start, end));
  }

  @Override
  public Long zcard(byte[] key) {
    Span span = helper.buildSpan("zcard", key);
    return helper.decorate(span, () -> super.zcard(key));
  }

  @Override
  public Double zscore(byte[] key, byte[] member) {
    Span span = helper.buildSpan("zscore", key);
    span.setTag("member", Arrays.toString(member));
    return helper.decorate(span, () -> super.zscore(key, member));
  }

  @Override
  public Long pexpireAt(String key, long millisecondsTimestamp) {
    Span span = helper.buildSpan("pexpireAt", key);
    span.setTag("millisecondsTimestamp", millisecondsTimestamp);
    return helper.decorate(span, () -> super.pexpireAt(key, millisecondsTimestamp));
  }

  @Override
  public Long pttl(String key) {
    Span span = helper.buildSpan("pttl", key);
    return helper.decorate(span, () -> super.pttl(key));
  }

  @Override
  public String psetex(String key, long milliseconds, String value) {
    Span span = helper.buildSpan("psetex", key);
    span.setTag("value", value);
    span.setTag("milliseconds", milliseconds);
    return helper.decorate(span, () -> super.psetex(key, milliseconds, value));
  }

  @Override
  public String set(String key, String value, String nxxx) {
    Span span = helper.buildSpan("set", key);
    span.setTag("value", value);
    span.setTag("nxxx", nxxx);
    return helper.decorate(span, () -> super.set(key, value, nxxx));
  }

  @Override
  public ScanResult<Entry<String, String>> hscan(String key, int cursor) {
    Span span = helper.buildSpan("hscan", key);
    span.setTag("cursor", cursor);
    return helper.decorate(span, () -> super.hscan(key, cursor));
  }

  @Override
  public ScanResult<String> sscan(String key, int cursor) {
    Span span = helper.buildSpan("sscan", key);
    span.setTag("cursor", cursor);
    return helper.decorate(span, () -> super.sscan(key, cursor));
  }

  @Override
  public ScanResult<Tuple> zscan(String key, int cursor) {
    Span span = helper.buildSpan("zscan", key);
    span.setTag("cursor", cursor);
    return helper.decorate(span, () -> super.zscan(key, cursor));
  }

  @Override
  public ScanResult<String> scan(String cursor, ScanParams params) {
    Span span = helper.buildSpan("scan");
    span.setTag("cursor", cursor);
    span.setTag("params", TracingHelper.toString(params.getParams()));
    return helper.decorate(span, () -> super.scan(cursor, params));
  }

  @Override
  public ScanResult<Entry<String, String>> hscan(String key, String cursor) {
    Span span = helper.buildSpan("hscan", key);
    span.setTag("cursor", cursor);
    return helper.decorate(span, () -> super.hscan(key, cursor));
  }

  @Override
  public ScanResult<Entry<String, String>> hscan(String key, String cursor, ScanParams params) {
    Span span = helper.buildSpan("hscan", key);
    span.setTag("cursor", cursor);
    span.setTag("params", TracingHelper.toString(params.getParams()));
    return helper.decorate(span, () -> super.hscan(key, cursor, params));
  }

  @Override
  public ScanResult<String> sscan(String key, String cursor) {
    Span span = helper.buildSpan("sscan", key);
    span.setTag("cursor", cursor);
    return helper.decorate(span, () -> super.sscan(key, cursor));
  }

  @Override
  public ScanResult<String> sscan(String key, String cursor, ScanParams params) {
    Span span = helper.buildSpan("sscan", key);
    span.setTag("cursor", cursor);
    span.setTag("params", TracingHelper.toString(params.getParams()));
    return helper.decorate(span, () -> super.sscan(key, cursor, params));
  }

  @Override
  public ScanResult<Tuple> zscan(String key, String cursor) {
    Span span = helper.buildSpan("zscan", key);
    span.setTag("cursor", cursor);
    return helper.decorate(span, () -> super.zscan(key, cursor));
  }

  @Override
  public ScanResult<Tuple> zscan(String key, String cursor, ScanParams params) {
    Span span = helper.buildSpan("zscan", key);
    span.setTag("cursor", cursor);
    span.setTag("params", TracingHelper.toString(params.getParams()));
    return helper.decorate(span, () -> super.zscan(key, cursor, params));
  }

  @Override
  public void close() throws IOException {
    super.close();
  }

  @Override
  public Map<String, JedisPool> getClusterNodes() {
    Span span = helper.buildSpan("getClusterNodes");
    return helper.decorate(span, () -> super.getClusterNodes());
  }

  @Override
  public Jedis getConnectionFromSlot(int slot) {
    // No way to wrap Jedis into TracingJedis
    return super.getConnectionFromSlot(slot);
  }

  @Override
  public List<byte[]> sort(byte[] key) {
    Span span = helper.buildSpan("sort", key);
    return helper.decorate(span, () -> super.sort(key));
  }

  @Override
  public List<byte[]> sort(byte[] key, SortingParams sortingParameters) {
    Span span = helper.buildSpan("sort", key);
    span.setTag("sortingParameters", TracingHelper.toString(sortingParameters.getParams()));
    return helper.decorate(span, () -> super.sort(key, sortingParameters));
  }

  @Override
  public List<byte[]> blpop(int timeout, byte[]... keys) {
    Span span = helper.buildSpan("blpop");
    span.setTag("timeout", timeout);
    span.setTag("keys", TracingHelper.toString(keys));
    return helper.decorate(span, () -> super.blpop(timeout, keys));
  }

  @Override
  public Long sort(byte[] key, SortingParams sortingParameters, byte[] dstkey) {
    Span span = helper.buildSpan("sort", key);
    span.setTag("sortingParameters", TracingHelper.toString(sortingParameters.getParams()));
    span.setTag("dstkey", Arrays.toString(dstkey));
    return helper.decorate(span, () -> super.sort(key, sortingParameters, dstkey));
  }

  @Override
  public Long sort(byte[] key, byte[] dstkey) {
    Span span = helper.buildSpan("sort", key);
    span.setTag("dstkey", Arrays.toString(dstkey));
    return helper.decorate(span, () -> super.sort(key, dstkey));
  }

  @Override
  public List<byte[]> brpop(int timeout, byte[]... keys) {
    Span span = helper.buildSpan("brpop");
    span.setTag("timeout", timeout);
    span.setTag("keys", TracingHelper.toString(keys));
    return helper.decorate(span, () -> super.brpop(timeout, keys));
  }

  @Override
  public String auth(String password) {
    Span span = helper.buildSpan("auth");
    return helper.decorate(span, () -> super.auth(password));
  }

  @Override
  public Long zcount(byte[] key, double min, double max) {
    Span span = helper.buildSpan("zcount", key);
    span.setTag("min", min);
    span.setTag("max", max);
    return helper.decorate(span, () -> super.zcount(key, min, max));
  }

  @Override
  public Long zcount(byte[] key, byte[] min, byte[] max) {
    Span span = helper.buildSpan("zcount", key);
    span.setTag("min", Arrays.toString(min));
    span.setTag("max", Arrays.toString(max));
    return helper.decorate(span, () -> super.zcount(key, min, max));
  }

  @Override
  public Set<byte[]> zrangeByScore(byte[] key, double min, double max) {
    Span span = helper.buildSpan("zrangeByScore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    return helper.decorate(span, () -> super.zrangeByScore(key, min, max));
  }

  @Override
  public Set<byte[]> zrangeByScore(byte[] key, byte[] min, byte[] max) {
    Span span = helper.buildSpan("zrangeByScore", key);
    span.setTag("min", Arrays.toString(min));
    span.setTag("max", Arrays.toString(max));
    return helper.decorate(span, () -> super.zrangeByScore(key, min, max));
  }

  @Override
  public Set<byte[]> zrangeByScore(byte[] key, double min, double max, int offset, int count) {
    Span span = helper.buildSpan("zrangeByScore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return helper.decorate(span, () -> super.zrangeByScore(key, min, max, offset, count));
  }

  @Override
  public Set<byte[]> zrangeByScore(byte[] key, byte[] min, byte[] max, int offset, int count) {
    Span span = helper.buildSpan("zrangeByScore", key);
    span.setTag("min", Arrays.toString(min));
    span.setTag("max", Arrays.toString(max));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return helper.decorate(span, () -> super.zrangeByScore(key, min, max, offset, count));
  }

  @Override
  public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max) {
    Span span = helper.buildSpan("zrangeByScoreWithScores", key);
    span.setTag("min", min);
    span.setTag("max", max);
    return helper.decorate(span, () -> super.zrangeByScoreWithScores(key, min, max));
  }

  @Override
  public Set<Tuple> zrangeByScoreWithScores(byte[] key, byte[] min, byte[] max) {
    Span span = helper.buildSpan("zrangeByScoreWithScores", key);
    span.setTag("min", Arrays.toString(min));
    span.setTag("max", Arrays.toString(max));
    return helper.decorate(span, () -> super.zrangeByScoreWithScores(key, min, max));
  }

  @Override
  public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max, int offset,
      int count) {
    Span span = helper.buildSpan("zrangeByScoreWithScores", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return helper.decorate(span, () -> super.zrangeByScoreWithScores(key, min, max, offset, count));
  }

  @Override
  public Set<Tuple> zrangeByScoreWithScores(byte[] key, byte[] min, byte[] max, int offset,
      int count) {
    Span span = helper.buildSpan("zrangeByScoreWithScores", key);
    span.setTag("min", Arrays.toString(min));
    span.setTag("max", Arrays.toString(max));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return helper.decorate(span, () -> super.zrangeByScoreWithScores(key, min, max, offset, count));
  }

  @Override
  public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min) {
    Span span = helper.buildSpan("zrevrangeByScore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    return helper.decorate(span, () -> super.zrevrangeByScore(key, max, min));
  }

  @Override
  public Set<byte[]> zrevrangeByScore(byte[] key, byte[] max, byte[] min) {
    Span span = helper.buildSpan("zrevrangeByScore", key);
    span.setTag("max", Arrays.toString(max));
    span.setTag("min", Arrays.toString(min));
    return helper.decorate(span, () -> super.zrevrangeByScore(key, max, min));
  }

  @Override
  public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min, int offset, int count) {
    Span span = helper.buildSpan("zrevrangeByScore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return helper.decorate(span, () -> super.zrevrangeByScore(key, max, min, offset, count));
  }

  @Override
  public Set<byte[]> zrevrangeByScore(byte[] key, byte[] max, byte[] min, int offset, int count) {
    Span span = helper.buildSpan("zrevrangeByScore", key);
    span.setTag("min", Arrays.toString(min));
    span.setTag("max", Arrays.toString(max));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return helper.decorate(span, () -> super.zrevrangeByScore(key, max, min, offset, count));
  }

  @Override
  public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min) {
    Span span = helper.buildSpan("zrevrangeByScoreWithScores", key);
    span.setTag("min", min);
    span.setTag("max", max);
    return helper.decorate(span, () -> super.zrevrangeByScoreWithScores(key, max, min));
  }

  @Override
  public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min, int offset,
      int count) {
    Span span = helper.buildSpan("zrevrangeByScoreWithScores", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return helper.decorate(span, () -> super.zrevrangeByScoreWithScores(key, max, min, offset, count));
  }

  @Override
  public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, byte[] max, byte[] min) {
    Span span = helper.buildSpan("zrevrangeByScoreWithScores", key);
    span.setTag("max", Arrays.toString(max));
    span.setTag("min", Arrays.toString(min));
    return helper.decorate(span, () -> super.zrevrangeByScoreWithScores(key, max, min));
  }

  @Override
  public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, byte[] max, byte[] min, int offset,
      int count) {
    Span span = helper.buildSpan("zrevrangeByScoreWithScores", key);
    span.setTag("min", Arrays.toString(min));
    span.setTag("max", Arrays.toString(max));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return helper.decorate(span, () -> super.zrevrangeByScoreWithScores(key, max, min, offset, count));
  }

  @Override
  public Long zremrangeByRank(byte[] key, long start, long end) {
    Span span = helper.buildSpan("zremrangeByRank", key);
    span.setTag("start", start);
    span.setTag("end", end);
    return helper.decorate(span, () -> super.zremrangeByRank(key, start, end));
  }

  @Override
  public Long zremrangeByScore(byte[] key, double start, double end) {
    Span span = helper.buildSpan("zremrangeByScore", key);
    span.setTag("start", start);
    span.setTag("end", end);
    return helper.decorate(span, () -> super.zremrangeByScore(key, start, end));
  }

  @Override
  public Long zremrangeByScore(byte[] key, byte[] start, byte[] end) {
    Span span = helper.buildSpan("zremrangeByScore", key);
    span.setTag("start", Arrays.toString(start));
    span.setTag("end", Arrays.toString(end));
    return helper.decorate(span, () -> super.zremrangeByScore(key, start, end));
  }

  @Override
  public Long zunionstore(byte[] dstkey, byte[]... sets) {
    Span span = helper.buildSpan("zunionstore");
    span.setTag("dstkey", Arrays.toString(dstkey));
    span.setTag("sets", TracingHelper.toString(sets));
    return helper.decorate(span, () -> super.zunionstore(dstkey, sets));
  }

  @Override
  public Long zunionstore(byte[] dstkey, ZParams params, byte[]... sets) {
    Span span = helper.buildSpan("zunionstore");
    span.setTag("dstkey", Arrays.toString(dstkey));
    span.setTag("params", TracingHelper.toString(params.getParams()));
    span.setTag("sets", TracingHelper.toString(sets));
    return helper.decorate(span, () -> super.zunionstore(dstkey, params, sets));
  }

  @Override
  public Long zinterstore(byte[] dstkey, byte[]... sets) {
    Span span = helper.buildSpan("zinterstore");
    span.setTag("dstkey", Arrays.toString(dstkey));
    span.setTag("sets", TracingHelper.toString(sets));
    return helper.decorate(span, () -> super.zinterstore(dstkey, sets));
  }

  @Override
  public Long zinterstore(byte[] dstkey, ZParams params, byte[]... sets) {
    Span span = helper.buildSpan("zinterstore");
    span.setTag("dstkey", Arrays.toString(dstkey));
    span.setTag("params", TracingHelper.toString(params.getParams()));
    span.setTag("sets", TracingHelper.toString(sets));
    return helper.decorate(span, () -> super.zinterstore(dstkey, params, sets));
  }

  @Override
  public Long zlexcount(byte[] key, byte[] min, byte[] max) {
    Span span = helper.buildSpan("zlexcount");
    span.setTag("min", Arrays.toString(min));
    span.setTag("max", Arrays.toString(max));
    return helper.decorate(span, () -> super.zlexcount(key, min, max));
  }

  @Override
  public Set<byte[]> zrangeByLex(byte[] key, byte[] min, byte[] max) {
    Span span = helper.buildSpan("zrangeByLex", key);
    span.setTag("min", Arrays.toString(min));
    span.setTag("max", Arrays.toString(max));
    return helper.decorate(span, () -> super.zrangeByLex(key, min, max));
  }

  @Override
  public Set<byte[]> zrangeByLex(byte[] key, byte[] min, byte[] max, int offset, int count) {
    Span span = helper.buildSpan("zrangeByLex", key);
    span.setTag("min", Arrays.toString(min));
    span.setTag("max", Arrays.toString(max));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return helper.decorate(span, () -> super.zrangeByLex(key, min, max, offset, count));
  }

  @Override
  public Set<byte[]> zrevrangeByLex(byte[] key, byte[] max, byte[] min) {
    Span span = helper.buildSpan("zrevrangeByLex", key);
    span.setTag("max", Arrays.toString(max));
    span.setTag("min", Arrays.toString(min));
    return helper.decorate(span, () -> super.zrevrangeByLex(key, max, min));
  }

  @Override
  public Set<byte[]> zrevrangeByLex(byte[] key, byte[] max, byte[] min, int offset, int count) {
    Span span = helper.buildSpan("zrevrangeByLex", key);
    span.setTag("min", Arrays.toString(min));
    span.setTag("max", Arrays.toString(max));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return helper.decorate(span, () -> super.zrevrangeByLex(key, max, min, offset, count));
  }

  @Override
  public Long zremrangeByLex(byte[] key, byte[] min, byte[] max) {
    Span span = helper.buildSpan("zremrangeByLex", key);
    span.setTag("min", Arrays.toString(min));
    span.setTag("max", Arrays.toString(max));
    return helper.decorate(span, () -> super.zremrangeByLex(key, min, max));
  }

  @Override
  public String save() {
    Span span = helper.buildSpan("save");
    return helper.decorate(span, () -> super.save());
  }

  @Override
  public String bgsave() {
    Span span = helper.buildSpan("bgsave");
    return helper.decorate(span, () -> super.bgsave());
  }

  @Override
  public String bgrewriteaof() {
    Span span = helper.buildSpan("bgrewriteaof");
    return helper.decorate(span, () -> super.bgrewriteaof());
  }

  @Override
  public Long lastsave() {
    Span span = helper.buildSpan("lastsave");
    return helper.decorate(span, () -> super.lastsave());
  }

  @Override
  public String shutdown() {
    Span span = helper.buildSpan("shutdown");
    return helper.decorate(span, () -> super.shutdown());
  }

  @Override
  public String info() {
    Span span = helper.buildSpan("info");
    return helper.decorate(span, () -> super.info());
  }

  @Override
  public String info(String section) {
    Span span = helper.buildSpan("info");
    span.setTag("section", section);
    return helper.decorate(span, () -> super.info(section));
  }

  @Override
  public String slaveof(String host, int port) {
    Span span = helper.buildSpan("slaveof");
    span.setTag("host", host);
    span.setTag("port", port);
    return helper.decorate(span, () -> super.slaveof(host, port));
  }

  @Override
  public String slaveofNoOne() {
    Span span = helper.buildSpan("slaveofNoOne");
    return helper.decorate(span, () -> super.slaveofNoOne());
  }

  @Override
  public String configResetStat() {
    Span span = helper.buildSpan("configResetStat");
    return helper.decorate(span, () -> super.configResetStat());
  }

  @Override
  public String configRewrite() {
    Span span = helper.buildSpan("configRewrite");
    return helper.decorate(span, super::configRewrite);
  }

  @Override
  public Long strlen(byte[] key) {
    Span span = helper.buildSpan("strlen", key);
    return helper.decorate(span, () -> super.strlen(key));
  }

  @Override
  public Long lpushx(byte[] key, byte[]... string) {
    Span span = helper.buildSpan("lpushx", key);
    span.setTag("string", TracingHelper.toString(string));
    return helper.decorate(span, () -> super.lpushx(key, string));
  }

  @Override
  public Long persist(byte[] key) {
    Span span = helper.buildSpan("persist", key);
    return helper.decorate(span, () -> super.persist(key));
  }

  @Override
  public Long rpushx(byte[] key, byte[]... string) {
    Span span = helper.buildSpan("rpushx", key);
    span.setTag("string", TracingHelper.toString(string));
    return helper.decorate(span, () -> super.rpushx(key, string));
  }

  @Override
  public byte[] echo(byte[] string) {
    Span span = helper.buildSpan("echo");
    span.setTag("string", Arrays.toString(string));
    return helper.decorate(span, () -> super.echo(string));
  }

  @Override
  public Long linsert(byte[] key, LIST_POSITION where, byte[] pivot, byte[] value) {
    Span span = helper.buildSpan("linsert", key);
    span.setTag("where", where.name());
    span.setTag("pivot", Arrays.toString(pivot));
    span.setTag("value", Arrays.toString(value));
    return helper.decorate(span, () -> super.linsert(key, where, pivot, value));
  }

  @Override
  public Long linsert(byte[] key, ListPosition where, byte[] pivot, byte[] value) {
    Span span = helper.buildSpan("linsert", key);
    span.setTag("where", where.name());
    span.setTag("pivot", Arrays.toString(pivot));
    span.setTag("value", Arrays.toString(value));
    return helper.decorate(span, () -> super.linsert(key, where, pivot, value));
  }

  @Override
  public String debug(DebugParams params) {
    Span span = helper.buildSpan("debug");
    span.setTag("params", Arrays.toString(params.getCommand()));
    return helper.decorate(span, () -> super.debug(params));
  }

  @Override
  public byte[] brpoplpush(byte[] source, byte[] destination, int timeout) {
    Span span = helper.buildSpan("brpoplpush");
    span.setTag("timeout", timeout);
    span.setTag("source", Arrays.toString(source));
    span.setTag("destination", Arrays.toString(destination));
    return helper.decorate(span, () -> super.brpoplpush(source, destination, timeout));
  }

  @Override
  public Boolean setbit(byte[] key, long offset, boolean value) {
    Span span = helper.buildSpan("setbit", key);
    span.setTag("offset", offset);
    span.setTag("value", value);
    return helper.decorate(span, () -> super.setbit(key, offset, value));
  }

  @Override
  public Boolean setbit(byte[] key, long offset, byte[] value) {
    Span span = helper.buildSpan("setbit", key);
    span.setTag("offset", offset);
    span.setTag("value", Arrays.toString(value));
    return helper.decorate(span, () -> super.setbit(key, offset, value));
  }

  @Override
  public Boolean getbit(byte[] key, long offset) {
    Span span = helper.buildSpan("getbit", key);
    span.setTag("offset", offset);
    return helper.decorate(span, () -> super.getbit(key, offset));
  }

  @Override
  public Long setrange(byte[] key, long offset, byte[] value) {
    Span span = helper.buildSpan("setrange", key);
    span.setTag("offset", offset);
    span.setTag("value", Arrays.toString(value));
    return helper.decorate(span, () -> super.setrange(key, offset, value));
  }

  @Override
  public byte[] getrange(byte[] key, long startOffset, long endOffset) {
    Span span = helper.buildSpan("getrange", key);
    span.setTag("startOffset", startOffset);
    span.setTag("endOffset", endOffset);
    return helper.decorate(span, () -> super.getrange(key, startOffset, endOffset));
  }

  @Override
  public Long publish(byte[] channel, byte[] message) {
    Span span = helper.buildSpan("publish");
    span.setTag("channel", Arrays.toString(channel));
    span.setTag("message", Arrays.toString(message));
    return helper.decorate(span, () -> super.publish(channel, message));
  }

  @Override
  public void subscribe(BinaryJedisPubSub jedisPubSub, byte[]... channels) {
    Span span = helper.buildSpan("subscribe");
    span.setTag("channels", Arrays.toString(channels));
    helper.decorate(span, () -> super.subscribe(jedisPubSub, channels));
  }

  @Override
  public void psubscribe(BinaryJedisPubSub jedisPubSub, byte[]... patterns) {
    Span span = helper.buildSpan("psubscribe");
    span.setTag("patterns", Arrays.toString(patterns));
    helper.decorate(span, () -> super.psubscribe(jedisPubSub, patterns));
  }

  @Override
  public Long getDB() {
    Span span = helper.buildSpan("getDB");
    return helper.decorate(span, () -> super.getDB());
  }

  @Override
  public Object eval(byte[] script, List<byte[]> keys, List<byte[]> args) {
    Span span = helper.buildSpan("eval");
    span.setTag("script", Arrays.toString(script));
    span.setTag("keys", TracingHelper.toString(keys));
    span.setTag("args", TracingHelper.toString(args));
    return helper.decorate(span, () -> super.eval(script, keys, args));
  }

  @Override
  public Object eval(byte[] script, byte[] key) {
    Span span = helper.buildSpan("eval", key);
    span.setTag("script", Arrays.toString(script));
    return helper.decorate(span, () -> super.eval(script, key));
  }

  @Override
  public Object evalsha(byte[] script, byte[] key) {
    Span span = helper.buildSpan("evalsha", key);
    span.setTag("script", Arrays.toString(script));
    return helper.decorate(span, () -> super.evalsha(script, key));
  }

  @Override
  public Object eval(byte[] script, byte[] keyCount, byte[]... params) {
    Span span = helper.buildSpan("eval");
    span.setTag("script", Arrays.toString(script));
    span.setTag("keyCount", Arrays.toString(keyCount));
    span.setTag("params", TracingHelper.toString(params));
    return helper.decorate(span, () -> super.eval(script, keyCount, params));
  }

  @Override
  public Object eval(byte[] script, int keyCount, byte[]... params) {
    Span span = helper.buildSpan("eval");
    span.setTag("script", Arrays.toString(script));
    span.setTag("keyCount", keyCount);
    span.setTag("params", TracingHelper.toString(params));
    return helper.decorate(span, () -> super.eval(script, keyCount, params));
  }

  @Override
  public Object evalsha(byte[] sha1, List<byte[]> keys, List<byte[]> args) {
    Span span = helper.buildSpan("evalsha");
    span.setTag("sha1", Arrays.toString(sha1));
    span.setTag("keys", TracingHelper.toString(keys));
    span.setTag("args", TracingHelper.toString(args));
    return helper.decorate(span, () -> super.evalsha(sha1, keys, args));
  }

  @Override
  public Object evalsha(byte[] sha1, int keyCount, byte[]... params) {
    Span span = helper.buildSpan("evalsha");
    span.setTag("params", TracingHelper.toString(params));
    span.setTag("sha1", Arrays.toString(sha1));
    span.setTag("keyCount", keyCount);
    return helper.decorate(span, () -> super.evalsha(sha1, keyCount, params));
  }

  @Override
  public List<Long> scriptExists(byte[] key, byte[][] sha1) {
    Span span = helper.buildSpan("scriptExists", key);
    span.setTag("sha1", TracingHelper.toString(sha1));
    return helper.decorate(span, () -> super.scriptExists(key, sha1));
  }

  @Override
  public byte[] scriptLoad(byte[] script, byte[] key) {
    Span span = helper.buildSpan("scriptLoad", key);
    span.setTag("script", Arrays.toString(script));
    return helper.decorate(span, () -> super.scriptLoad(script, key));
  }

  @Override
  public String scriptFlush(byte[] key) {
    Span span = helper.buildSpan("scriptFlush", key);
    return helper.decorate(span, () -> super.scriptFlush(key));
  }

  @Override
  public String scriptKill(byte[] key) {
    Span span = helper.buildSpan("scriptKill", key);
    return helper.decorate(span, () -> super.scriptKill(key));
  }

  @Override
  public Long bitcount(byte[] key) {
    Span span = helper.buildSpan("bitcount", key);
    return helper.decorate(span, () -> super.bitcount(key));
  }

  @Override
  public Long bitcount(byte[] key, long start, long end) {
    Span span = helper.buildSpan("bitcount", key);
    span.setTag("start", start);
    span.setTag("end", end);
    return helper.decorate(span, () -> super.bitcount(key, start, end));
  }

  @Override
  public Long bitop(BitOP op, byte[] destKey, byte[]... srcKeys) {
    Span span = helper.buildSpan("bitop");
    span.setTag("destKey", Arrays.toString(destKey));
    span.setTag("srcKeys", TracingHelper.toString(srcKeys));
    return helper.decorate(span, () -> super.bitop(op, destKey, srcKeys));
  }

  @Override
  public Long pexpire(byte[] key, long milliseconds) {
    Span span = helper.buildSpan("pexpire", key);
    span.setTag("milliseconds", milliseconds);
    return helper.decorate(span, () -> super.pexpire(key, milliseconds));
  }

  @Override
  public Long pexpireAt(byte[] key, long millisecondsTimestamp) {
    Span span = helper.buildSpan("pexpireAt", key);
    span.setTag("millisecondsTimestamp", millisecondsTimestamp);
    return helper.decorate(span, () -> super.pexpireAt(key, millisecondsTimestamp));
  }

  @Override
  public Long waitReplicas(int replicas, long timeout) {
    Span span = helper.buildSpan("waitReplicas");
    span.setTag("replicas", replicas);
    span.setTag("timeout", timeout);
    return helper.decorate(span, () -> super.waitReplicas(replicas, timeout));
  }

  @Override
  public Long pfadd(byte[] key, byte[]... elements) {
    Span span = helper.buildSpan("pfadd", key);
    span.setTag("elements", TracingHelper.toString(elements));
    return helper.decorate(span, () -> super.pfadd(key, elements));
  }

  @Override
  public long pfcount(byte[] key) {
    Span span = helper.buildSpan("pfcount", key);
    return helper.decorate(span, () -> super.pfcount(key));
  }

  @Override
  public String pfmerge(byte[] destkey, byte[]... sourcekeys) {
    Span span = helper.buildSpan("pfmerge");
    span.setTag("destkey", Arrays.toString(destkey));
    span.setTag("sourcekeys", TracingHelper.toString(sourcekeys));
    return helper.decorate(span, () -> super.pfmerge(destkey, sourcekeys));
  }

  @Override
  public Long pfcount(byte[]... keys) {
    Span span = helper.buildSpan("pfcount");
    span.setTag("keys", TracingHelper.toString(keys));
    return helper.decorate(span, () -> super.pfcount(keys));
  }

  @Override
  public ScanResult<byte[]> scan(byte[] cursor, ScanParams params) {
    Span span = helper.buildSpan("scan");
    span.setTag("cursor", Arrays.toString(cursor));
    span.setTag("params", TracingHelper.toString(params.getParams()));
    return helper.decorate(span, () -> super.scan(cursor, params));
  }

  @Override
  public ScanResult<Entry<byte[], byte[]>> hscan(byte[] key, byte[] cursor) {
    Span span = helper.buildSpan("hscan", key);
    span.setTag("cursor", Arrays.toString(cursor));
    return helper.decorate(span, () -> super.hscan(key, cursor));
  }

  @Override
  public ScanResult<Entry<byte[], byte[]>> hscan(byte[] key, byte[] cursor, ScanParams params) {
    Span span = helper.buildSpan("hscan", key);
    span.setTag("cursor", Arrays.toString(cursor));
    span.setTag("params", TracingHelper.toString(params.getParams()));
    return helper.decorate(span, () -> super.hscan(key, cursor, params));
  }

  @Override
  public ScanResult<byte[]> sscan(byte[] key, byte[] cursor) {
    Span span = helper.buildSpan("sscan", key);
    span.setTag("cursor", Arrays.toString(cursor));
    return helper.decorate(span, () -> super.sscan(key, cursor));
  }

  @Override
  public ScanResult<byte[]> sscan(byte[] key, byte[] cursor, ScanParams params) {
    Span span = helper.buildSpan("sscan", key);
    span.setTag("cursor", Arrays.toString(cursor));
    span.setTag("params", TracingHelper.toString(params.getParams()));
    return helper.decorate(span, () -> super.sscan(key, cursor, params));
  }

  @Override
  public ScanResult<Tuple> zscan(byte[] key, byte[] cursor) {
    Span span = helper.buildSpan("zscan", key);
    span.setTag("cursor", Arrays.toString(cursor));
    return helper.decorate(span, () -> super.zscan(key, cursor));
  }

  @Override
  public ScanResult<Tuple> zscan(byte[] key, byte[] cursor, ScanParams params) {
    Span span = helper.buildSpan("zscan", key);
    span.setTag("cursor", Arrays.toString(cursor));
    span.setTag("params", TracingHelper.toString(params.getParams()));
    return helper.decorate(span, () -> super.zscan(key, cursor, params));
  }

  @Override
  public List<Long> bitfield(byte[] key, byte[]... arguments) {
    Span span = helper.buildSpan("bitfield", key);
    span.setTag("arguments", Arrays.toString(arguments));
    return helper.decorate(span, () -> super.bitfield(key, arguments));
  }

  @Override
  public Long hstrlen(byte[] key, byte[] field) {
    Span span = helper.buildSpan("hstrlen", key);
    span.setTag("field", Arrays.toString(field));
    return helper.decorate(span, () -> super.hstrlen(key, field));
  }

  @Override
  public Long waitReplicas(byte[] key, int replicas, long timeout) {
    Span span = helper.buildSpan("waitReplicas", key);
    span.setTag("replicas", replicas);
    span.setTag("timeout", timeout);
    return helper.decorate(span, () -> super.waitReplicas(key, replicas, timeout));
  }

  @Override
  public Long geoadd(byte[] key, double longitude, double latitude, byte[] member) {
    Span span = helper.buildSpan("geoadd", key);
    span.setTag("longitude", longitude);
    span.setTag("latitude", latitude);
    span.setTag("member", Arrays.toString(member));
    return helper.decorate(span, () -> super.geoadd(key, longitude, latitude, member));
  }

  @Override
  public Long geoadd(byte[] key, Map<byte[], GeoCoordinate> memberCoordinateMap) {
    Span span = helper.buildSpan("geoadd", key);
    span.setTag("memberCoordinateMap", TracingHelper.toStringMap2(memberCoordinateMap));
    return helper.decorate(span, () -> super.geoadd(key, memberCoordinateMap));
  }

  @Override
  public Double geodist(byte[] key, byte[] member1, byte[] member2) {
    Span span = helper.buildSpan("geodist", key);
    span.setTag("member1", Arrays.toString(member1));
    span.setTag("member2", Arrays.toString(member2));
    return helper.decorate(span, () -> super.geodist(key, member1, member2));
  }

  @Override
  public Double geodist(byte[] key, byte[] member1, byte[] member2, GeoUnit unit) {
    Span span = helper.buildSpan("geodist", key);
    span.setTag("member1", Arrays.toString(member1));
    span.setTag("member2", Arrays.toString(member2));
    span.setTag("unit", unit.name());
    return helper.decorate(span, () -> super.geodist(key, member1, member2, unit));
  }

  @Override
  public List<byte[]> geohash(byte[] key, byte[]... members) {
    Span span = helper.buildSpan("geohash", key);
    span.setTag("members", Arrays.toString(members));
    return helper.decorate(span, () -> super.geohash(key, members));
  }

  @Override
  public List<GeoCoordinate> geopos(byte[] key, byte[]... members) {
    Span span = helper.buildSpan("geopos", key);
    span.setTag("members", Arrays.toString(members));
    return helper.decorate(span, () -> super.geopos(key, members));
  }

  @Override
  public List<GeoRadiusResponse> georadius(byte[] key, double longitude, double latitude,
      double radius, GeoUnit unit) {
    Span span = helper.buildSpan("georadius", key);
    span.setTag("longitude", longitude);
    span.setTag("latitude", latitude);
    span.setTag("radius", radius);
    span.setTag("unit", unit.name());
    return helper.decorate(span, () -> super.georadius(key, longitude, latitude, radius, unit));
  }

  @Override
  public List<GeoRadiusResponse> georadiusReadonly(byte[] key, double longitude, double latitude,
      double radius, GeoUnit unit) {
    Span span = helper.buildSpan("georadiusReadonly", key);
    span.setTag("longitude", longitude);
    span.setTag("latitude", latitude);
    span.setTag("radius", radius);
    span.setTag("unit", unit.name());
    return helper
        .decorate(span, () -> super.georadiusReadonly(key, longitude, latitude, radius, unit));
  }

  @Override
  public List<GeoRadiusResponse> georadius(byte[] key, double longitude, double latitude,
      double radius, GeoUnit unit, GeoRadiusParam param) {
    Span span = helper.buildSpan("georadius", key);
    span.setTag("longitude", longitude);
    span.setTag("latitude", latitude);
    span.setTag("radius", radius);
    span.setTag("unit", unit.name());
    span.setTag("param", TracingHelper.toString(param.getByteParams()));
    return helper.decorate(span, () -> super.georadius(key, longitude, latitude, radius, unit, param));
  }

  @Override
  public List<GeoRadiusResponse> georadiusReadonly(byte[] key, double longitude, double latitude,
      double radius, GeoUnit unit, GeoRadiusParam param) {
    Span span = helper.buildSpan("georadiusReadonly", key);
    span.setTag("longitude", longitude);
    span.setTag("latitude", latitude);
    span.setTag("radius", radius);
    span.setTag("unit", unit.name());
    span.setTag("param", TracingHelper.toString(param.getByteParams()));
    return helper
        .decorate(span,
            () -> super.georadiusReadonly(key, longitude, latitude, radius, unit, param));
  }

  @Override
  public List<GeoRadiusResponse> georadiusByMember(byte[] key, byte[] member, double radius,
      GeoUnit unit) {
    Span span = helper.buildSpan("georadiusByMember", key);
    span.setTag("member", Arrays.toString(member));
    span.setTag("radius", radius);
    span.setTag("unit", unit.name());
    return helper.decorate(span, () -> super.georadiusByMember(key, member, radius, unit));
  }

  @Override
  public List<GeoRadiusResponse> georadiusByMemberReadonly(byte[] key, byte[] member, double radius,
      GeoUnit unit) {
    Span span = helper.buildSpan("georadiusByMemberReadonly", key);
    span.setTag("member", Arrays.toString(member));
    span.setTag("radius", radius);
    span.setTag("unit", unit.name());
    return helper
        .decorate(span, () -> super.georadiusByMemberReadonly(key, member, radius, unit));
  }

  @Override
  public List<GeoRadiusResponse> georadiusByMember(byte[] key, byte[] member, double radius,
      GeoUnit unit, GeoRadiusParam param) {
    Span span = helper.buildSpan("georadiusByMember", key);
    span.setTag("member", Arrays.toString(member));
    span.setTag("radius", radius);
    span.setTag("unit", unit.name());
    span.setTag("param", TracingHelper.toString(param.getByteParams()));
    return helper.decorate(span, () -> super.georadiusByMember(key, member, radius, unit, param));
  }

  @Override
  public List<GeoRadiusResponse> georadiusByMemberReadonly(byte[] key, byte[] member, double radius,
      GeoUnit unit, GeoRadiusParam param) {
    Span span = helper.buildSpan("georadiusByMemberReadonly", key);
    span.setTag("member", Arrays.toString(member));
    span.setTag("radius", radius);
    span.setTag("unit", unit.name());
    span.setTag("param", TracingHelper.toString(param.getByteParams()));
    return helper
        .decorate(span, () -> super.georadiusByMemberReadonly(key, member, radius, unit, param));
  }

  @Override
  public Set<byte[]> keys(byte[] pattern) {
    Span span = helper.buildSpan("keys");
    span.setTag("pattern", nullable(pattern));
    return helper.decorate(span, () -> super.keys(pattern));
  }

  @Override
  public Long pfadd(String key, String... elements) {
    Span span = helper.buildSpan("pfadd");
    span.setTag("elements", Arrays.toString(elements));
    return helper.decorate(span, () -> super.pfadd(key, elements));
  }

  @Override
  public long pfcount(String key) {
    Span span = helper.buildSpan("pfcount", key);
    return helper.decorate(span, () -> super.pfcount(key));
  }

  @Override
  public long pfcount(String... keys) {
    Span span = helper.buildSpan("pfcount", keys);
    return helper.decorate(span, () -> super.pfcount(keys));
  }

  @Override
  public String pfmerge(String destkey, String... sourcekeys) {
    Span span = helper.buildSpan("pfmerge");
    span.setTag("destkey", destkey);
    span.setTag("sourcekeys", Arrays.toString(sourcekeys));
    return helper.decorate(span, () -> super.pfmerge(destkey, sourcekeys));
  }

  @Override
  public List<String> blpop(int timeout, String key) {
    Span span = helper.buildSpan("blpop");
    span.setTag("timeout", timeout);
    return helper.decorate(span, () -> super.blpop(timeout, key));
  }

  @Override
  public List<String> brpop(int timeout, String key) {
    Span span = helper.buildSpan("brpop");
    span.setTag("timeout", timeout);
    return helper.decorate(span, () -> super.brpop(timeout, key));
  }

  @Override
  public Long geoadd(String key, double longitude, double latitude, String member) {
    Span span = helper.buildSpan("geoadd");
    span.setTag("longitude", longitude);
    span.setTag("latitude", latitude);
    span.setTag("member", member);
    return helper.decorate(span, () -> super.geoadd(key, longitude, latitude, member));
  }

  @Override
  public Long geoadd(String key, Map<String, GeoCoordinate> memberCoordinateMap) {
    Span span = helper.buildSpan("geoadd");
    span.setTag("memberCoordinateMap", TracingHelper.toString(memberCoordinateMap));
    return helper.decorate(span, () -> super.geoadd(key, memberCoordinateMap));
  }

  @Override
  public Double geodist(String key, String member1, String member2) {
    Span span = helper.buildSpan("geodist", key);
    span.setTag("member1", member1);
    span.setTag("member2", member2);
    return helper.decorate(span, () -> super.geodist(key, member1, member2));
  }

  @Override
  public Double geodist(String key, String member1, String member2, GeoUnit unit) {
    Span span = helper.buildSpan("geodist", key);
    span.setTag("member1", member1);
    span.setTag("member2", member2);
    span.setTag("unit", unit.name());
    return helper.decorate(span, () -> super.geodist(key, member1, member2, unit));
  }

  @Override
  public List<String> geohash(String key, String... members) {
    Span span = helper.buildSpan("geohash", key);
    span.setTag("members", Arrays.toString(members));
    return helper.decorate(span, () -> super.geohash(key, members));
  }

  @Override
  public List<GeoCoordinate> geopos(String key, String... members) {
    Span span = helper.buildSpan("geopos", key);
    span.setTag("members", Arrays.toString(members));
    return helper.decorate(span, () -> super.geopos(key, members));
  }

  @Override
  public List<GeoRadiusResponse> georadius(String key, double longitude, double latitude,
      double radius, GeoUnit unit) {
    Span span = helper.buildSpan("georadius", key);
    span.setTag("longitude", longitude);
    span.setTag("latitude", latitude);
    span.setTag("radius", radius);
    span.setTag("unit", unit.name());
    return helper.decorate(span, () -> super.georadius(key, longitude, latitude, radius, unit));
  }

  @Override
  public List<GeoRadiusResponse> georadiusReadonly(String key, double longitude, double latitude,
      double radius, GeoUnit unit) {
    Span span = helper.buildSpan("georadiusReadonly", key);
    span.setTag("longitude", longitude);
    span.setTag("latitude", latitude);
    span.setTag("radius", radius);
    span.setTag("unit", unit.name());
    return helper
        .decorate(span, () -> super.georadiusReadonly(key, longitude, latitude, radius, unit));
  }

  @Override
  public List<GeoRadiusResponse> georadius(String key, double longitude, double latitude,
      double radius, GeoUnit unit, GeoRadiusParam param) {
    Span span = helper.buildSpan("georadius", key);
    span.setTag("longitude", longitude);
    span.setTag("latitude", latitude);
    span.setTag("radius", radius);
    span.setTag("unit", unit.name());
    span.setTag("param", TracingHelper.toString(param.getByteParams()));
    return helper.decorate(span, () -> super.georadius(key, longitude, latitude, radius, unit, param));
  }

  @Override
  public List<GeoRadiusResponse> georadiusReadonly(String key, double longitude, double latitude,
      double radius, GeoUnit unit, GeoRadiusParam param) {
    Span span = helper.buildSpan("georadiusReadonly", key);
    span.setTag("longitude", longitude);
    span.setTag("latitude", latitude);
    span.setTag("radius", radius);
    span.setTag("unit", unit.name());
    span.setTag("param", TracingHelper.toString(param.getByteParams()));
    return helper
        .decorate(span,
            () -> super.georadiusReadonly(key, longitude, latitude, radius, unit, param));
  }

  @Override
  public List<GeoRadiusResponse> georadiusByMember(String key, String member, double radius,
      GeoUnit unit) {
    Span span = helper.buildSpan("georadiusByMember", key);
    span.setTag("member", member);
    span.setTag("radius", radius);
    span.setTag("unit", unit.name());
    return helper.decorate(span, () -> super.georadiusByMember(key, member, radius, unit));
  }

  @Override
  public List<GeoRadiusResponse> georadiusByMemberReadonly(String key, String member, double radius,
      GeoUnit unit) {
    Span span = helper.buildSpan("georadiusByMemberReadonly", key);
    span.setTag("member", member);
    span.setTag("radius", radius);
    span.setTag("unit", unit.name());
    return helper
        .decorate(span, () -> super.georadiusByMemberReadonly(key, member, radius, unit));
  }

  @Override
  public List<GeoRadiusResponse> georadiusByMember(String key, String member, double radius,
      GeoUnit unit, GeoRadiusParam param) {
    Span span = helper.buildSpan("georadiusByMember", key);
    span.setTag("member", member);
    span.setTag("radius", radius);
    span.setTag("unit", unit.name());
    span.setTag("param", TracingHelper.toString(param.getByteParams()));
    return helper.decorate(span, () -> super.georadiusByMember(key, member, radius, unit, param));
  }

  @Override
  public List<GeoRadiusResponse> georadiusByMemberReadonly(String key, String member, double radius,
      GeoUnit unit, GeoRadiusParam param) {
    Span span = helper.buildSpan("georadiusByMemberReadonly", key);
    span.setTag("member", member);
    span.setTag("radius", radius);
    span.setTag("unit", unit.name());
    span.setTag("param", TracingHelper.toString(param.getByteParams()));
    return helper
        .decorate(span, () -> super.georadiusByMemberReadonly(key, member, radius, unit, param));
  }

  @Override
  public List<Long> bitfield(String key, String... arguments) {
    Span span = helper.buildSpan("bitfield", key);
    span.setTag("arguments", Arrays.toString(arguments));
    return helper.decorate(span, () -> super.bitfield(key, arguments));
  }

  @Override
  public Long hstrlen(String key, String field) {
    Span span = helper.buildSpan("hstrlen", key);
    span.setTag("field", field);
    return helper.decorate(span, () -> super.hstrlen(key, field));
  }

  @Override
  public Long waitReplicas(String key, int replicas, long timeout) {
    Span span = helper.buildSpan("waitReplicas", key);
    span.setTag("replicas", replicas);
    span.setTag("timeout", timeout);
    return helper.decorate(span, () -> super.waitReplicas(key, replicas, timeout));
  }

}
