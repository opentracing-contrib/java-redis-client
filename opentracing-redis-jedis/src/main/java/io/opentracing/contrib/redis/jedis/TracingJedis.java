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
package io.opentracing.contrib.redis.jedis;

import static io.opentracing.contrib.redis.common.TracingHelper.nullable;
import static io.opentracing.contrib.redis.common.TracingHelper.onError;

import io.opentracing.Span;
import io.opentracing.contrib.redis.common.TracingConfiguration;
import io.opentracing.contrib.redis.common.TracingHelper;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;
import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.BitOP;
import redis.clients.jedis.BitPosParams;
import redis.clients.jedis.Client;
import redis.clients.jedis.DebugParams;
import redis.clients.jedis.GeoCoordinate;
import redis.clients.jedis.GeoRadiusResponse;
import redis.clients.jedis.GeoUnit;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster.Reset;
import redis.clients.jedis.JedisMonitor;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.PipelineBlock;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.TransactionBlock;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.ZParams;
import redis.clients.jedis.params.geo.GeoRadiusParam;
import redis.clients.jedis.params.sortedset.ZAddParams;
import redis.clients.jedis.params.sortedset.ZIncrByParams;
import redis.clients.util.Pool;
import redis.clients.util.Slowlog;


public class TracingJedis extends Jedis {

  private final TracingHelper helper;

  public TracingJedis(TracingConfiguration tracingConfiguration) {
    super();
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedis(final String host, TracingConfiguration tracingConfiguration) {
    super(host);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedis(final String host, final int port,
      TracingConfiguration tracingConfiguration) {
    super(host, port);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedis(final String host, final int port, final boolean ssl,
      TracingConfiguration tracingConfiguration) {
    super(host, port, ssl);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedis(final String host, final int port, final boolean ssl,
      final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier, TracingConfiguration tracingConfiguration) {
    super(host, port, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedis(final String host, final int port, final int timeout,
      TracingConfiguration tracingConfiguration) {
    super(host, port, timeout);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedis(final String host, final int port, final int timeout, final boolean ssl,
      TracingConfiguration tracingConfiguration) {
    super(host, port, timeout, ssl);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedis(final String host, final int port, final int timeout, final boolean ssl,
      final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier, TracingConfiguration tracingConfiguration) {
    super(host, port, timeout, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedis(final String host, final int port, final int connectionTimeout,
      final int soTimeout, TracingConfiguration tracingConfiguration) {
    super(host, port, connectionTimeout, soTimeout);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedis(final String host, final int port, final int connectionTimeout,
      final int soTimeout,
      final boolean ssl, TracingConfiguration tracingConfiguration) {
    super(host, port, connectionTimeout, soTimeout, ssl);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedis(final String host, final int port, final int connectionTimeout,
      final int soTimeout,
      final boolean ssl, final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier, TracingConfiguration tracingConfiguration) {
    super(host, port, connectionTimeout, soTimeout, ssl, sslSocketFactory, sslParameters,
        hostnameVerifier);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedis(JedisShardInfo shardInfo, TracingConfiguration tracingConfiguration) {
    super(shardInfo);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedis(URI uri, TracingConfiguration tracingConfiguration) {
    super(uri);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedis(URI uri, final SSLSocketFactory sslSocketFactory,
      final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier, TracingConfiguration tracingConfiguration) {
    super(uri, sslSocketFactory, sslParameters, hostnameVerifier);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedis(final URI uri, final int timeout, TracingConfiguration tracingConfiguration) {
    super(uri, timeout);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedis(final URI uri, final int timeout, final SSLSocketFactory sslSocketFactory,
      final SSLParameters sslParameters, final HostnameVerifier hostnameVerifier,
      TracingConfiguration tracingConfiguration) {
    super(uri, timeout, sslSocketFactory, sslParameters, hostnameVerifier);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedis(final URI uri, final int connectionTimeout, final int soTimeout,
      TracingConfiguration tracingConfiguration) {
    super(uri, connectionTimeout, soTimeout);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  public TracingJedis(final URI uri, final int connectionTimeout, final int soTimeout,
      final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier, TracingConfiguration tracingConfiguration) {
    super(uri, connectionTimeout, soTimeout, sslSocketFactory, sslParameters, hostnameVerifier);
    this.helper = new TracingHelper(tracingConfiguration);
  }

  @Override
  public String set(String key, String value) {
    Span span = helper.buildSpan("set", key);
    span.setTag("value", value);
    return TracingHelper.doInScope(span, () -> super.set(key, value));
  }

  @Override
  public String set(String key, String value, String nxxx, String expx, long time) {
    Span span = helper.buildSpan("set", key);
    span.setTag("value", value);
    span.setTag("nxxx", nxxx);
    span.setTag("expx", expx);
    span.setTag("time", time);
    try {
      return super.set(key, value, nxxx, expx, time);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String get(String key) {
    Span span = helper.buildSpan("get", key);
    return TracingHelper.doInScope(span, () -> super.get(key));
  }

  @Override
  public Long exists(String... keys) {
    Span span = helper.buildSpan("exists", keys);
    try {
      return super.exists(keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Boolean exists(String key) {
    Span span = helper.buildSpan("exists", key);
    try {
      return super.exists(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long del(String... keys) {
    Span span = helper.buildSpan("del", keys);
    try {
      return super.del(keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long del(String key) {
    Span span = helper.buildSpan("del", key);
    try {
      return super.del(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String type(String key) {
    Span span = helper.buildSpan("type", key);
    try {
      return super.type(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<String> keys(String pattern) {
    Span span = helper.buildSpan("keys");
    span.setTag("pattern", nullable(pattern));
    try {
      return super.keys(pattern);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String randomKey() {
    Span span = helper.buildSpan("randomKey");
    try {
      return super.randomKey();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String rename(String oldkey, String newkey) {
    Span span = helper.buildSpan("rename");
    span.setTag("oldKey", nullable(oldkey));
    span.setTag("newKey", nullable(newkey));
    try {
      return super.rename(oldkey, newkey);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long renamenx(String oldkey, String newkey) {
    Span span = helper.buildSpan("renamenx");
    span.setTag("oldKey", nullable(oldkey));
    span.setTag("newKey", nullable(newkey));
    try {
      return super.renamenx(oldkey, newkey);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long expire(String key, int seconds) {
    Span span = helper.buildSpan("expire", key);
    span.setTag("seconds", seconds);
    try {
      return super.expire(key, seconds);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long expireAt(String key, long unixTime) {
    Span span = helper.buildSpan("expireAt", key);
    span.setTag("unixTime", unixTime);
    try {
      return super.expireAt(key, unixTime);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long ttl(String key) {
    Span span = helper.buildSpan("ttl", key);
    try {
      return super.ttl(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long move(String key, int dbIndex) {
    Span span = helper.buildSpan("move", key);
    span.setTag("dbIndex", dbIndex);
    try {
      return super.move(key, dbIndex);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String getSet(String key, String value) {
    Span span = helper.buildSpan("getSet", key);
    span.setTag("value", value);
    return TracingHelper.doInScope(span, () -> super.getSet(key, value));
  }

  @Override
  public List<String> mget(String... keys) {
    Span span = helper.buildSpan("mget", keys);
    return TracingHelper.doInScope(span, () -> super.mget(keys));
  }

  @Override
  public Long setnx(String key, String value) {
    Span span = helper.buildSpan("setnx", key);
    span.setTag("value", value);
    try {
      return super.setnx(key, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String setex(String key, int seconds, String value) {
    Span span = helper.buildSpan("setex", key);
    span.setTag("seconds", seconds);
    span.setTag("value", value);
    try {
      return super.setex(key, seconds, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String mset(String... keysvalues) {
    Span span = helper.buildSpan("mset");
    span.setTag("keysvalues", Arrays.toString(keysvalues));
    try {
      return super.mset(keysvalues);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long msetnx(String... keysvalues) {
    Span span = helper.buildSpan("msetnx");
    span.setTag("keysvalues", Arrays.toString(keysvalues));
    try {
      return super.msetnx(keysvalues);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long decrBy(String key, long integer) {
    Span span = helper.buildSpan("decrBy", key);
    span.setTag("integer", integer);
    try {
      return super.decrBy(key, integer);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long decr(String key) {
    Span span = helper.buildSpan("decr", key);
    try {
      return super.decr(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long incrBy(String key, long integer) {
    Span span = helper.buildSpan("incrBy", key);
    span.setTag("integer", integer);
    try {
      return super.incrBy(key, integer);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Double incrByFloat(String key, double value) {
    Span span = helper.buildSpan("incrByFloat", key);
    span.setTag("value", value);
    try {
      return super.incrByFloat(key, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long incr(String key) {
    Span span = helper.buildSpan("incr", key);
    try {
      return super.incr(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long append(String key, String value) {
    Span span = helper.buildSpan("append", key);
    span.setTag("value", value);
    try {
      return super.append(key, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String substr(String key, int start, int end) {
    Span span = helper.buildSpan("substr", key);
    span.setTag("start", start);
    span.setTag("end", end);
    try {
      return super.substr(key, start, end);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long hset(String key, String field, String value) {
    Span span = helper.buildSpan("hset", key);
    span.setTag("field", field);
    span.setTag("value", value);
    try {
      return super.hset(key, field, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String hget(String key, String field) {
    Span span = helper.buildSpan("hget", key);
    span.setTag("field", field);
    try {
      return super.hget(key, field);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long hsetnx(String key, String field, String value) {
    Span span = helper.buildSpan("hsetnx", key);
    span.setTag("field", field);
    span.setTag("value", value);
    try {
      return super.hsetnx(key, field, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String hmset(String key, Map<String, String> hash) {
    Span span = helper.buildSpan("hmset", key);
    span.setTag("hash", TracingHelper.toString(hash));
    try {
      return super.hmset(key, hash);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<String> hmget(String key, String... fields) {
    Span span = helper.buildSpan("hmget", key);
    span.setTag("fields", Arrays.toString(fields));
    try {
      return super.hmget(key, fields);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long hincrBy(String key, String field, long value) {
    Span span = helper.buildSpan("hincrBy", key);
    span.setTag("field", field);
    span.setTag("value", value);
    try {
      return super.hincrBy(key, field, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Double hincrByFloat(String key, String field, double value) {
    Span span = helper.buildSpan("hincrByFloat", key);
    span.setTag("field", field);
    span.setTag("value", value);
    try {
      return super.hincrByFloat(key, field, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Boolean hexists(String key, String field) {
    Span span = helper.buildSpan("hexists", key);
    span.setTag("field", field);
    try {
      return super.hexists(key, field);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long hdel(String key, String... fields) {
    Span span = helper.buildSpan("hdel", key);
    span.setTag("fields", Arrays.toString(fields));
    try {
      return super.hdel(key, fields);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long hlen(String key) {
    Span span = helper.buildSpan("hlen", key);
    try {
      return super.hlen(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<String> hkeys(String key) {
    Span span = helper.buildSpan("hkeys", key);
    try {
      return super.hkeys(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<String> hvals(String key) {
    Span span = helper.buildSpan("hvals", key);
    try {
      return super.hvals(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Map<String, String> hgetAll(String key) {
    Span span = helper.buildSpan("hgetAll", key);
    try {
      return super.hgetAll(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long rpush(String key, String... strings) {
    Span span = helper.buildSpan("rpush", key);
    span.setTag("strings", Arrays.toString(strings));
    try {
      return super.rpush(key, strings);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long lpush(String key, String... strings) {
    Span span = helper.buildSpan("lpush", key);
    span.setTag("strings", Arrays.toString(strings));
    try {
      return super.lpush(key, strings);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long llen(String key) {
    Span span = helper.buildSpan("llen", key);
    try {
      return super.llen(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<String> lrange(String key, long start, long end) {
    Span span = helper.buildSpan("lrange", key);
    span.setTag("start", start);
    span.setTag("end", end);
    try {
      return super.lrange(key, start, end);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String ltrim(String key, long start, long end) {
    Span span = helper.buildSpan("ltrim", key);
    span.setTag("start", start);
    span.setTag("end", end);
    try {
      return super.ltrim(key, start, end);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String lindex(String key, long index) {
    Span span = helper.buildSpan("lindex", key);
    span.setTag("index", index);
    try {
      return super.lindex(key, index);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String lset(String key, long index, String value) {
    Span span = helper.buildSpan("lset", key);
    span.setTag("index", index);
    span.setTag("value", value);
    try {
      return super.lset(key, index, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long lrem(String key, long count, String value) {
    Span span = helper.buildSpan("lrem", key);
    span.setTag("count", count);
    span.setTag("value", value);
    try {
      return super.lrem(key, count, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String lpop(String key) {
    Span span = helper.buildSpan("lpop", key);
    try {
      return super.lpop(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String rpop(String key) {
    Span span = helper.buildSpan("rpop", key);
    try {
      return super.rpop(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String rpoplpush(String srckey, String dstkey) {
    Span span = helper.buildSpan("rpoplpush");
    span.setTag("srckey", srckey);
    span.setTag("dstkey", dstkey);
    try {
      return super.rpoplpush(srckey, dstkey);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long sadd(String key, String... members) {
    Span span = helper.buildSpan("sadd", key);
    span.setTag("members", Arrays.toString(members));
    try {
      return super.sadd(key, members);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<String> smembers(String key) {
    Span span = helper.buildSpan("smembers", key);
    try {
      return super.smembers(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long srem(String key, String... members) {
    Span span = helper.buildSpan("srem", key);
    span.setTag("members", Arrays.toString(members));
    try {
      return super.srem(key, members);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String spop(String key) {
    Span span = helper.buildSpan("spop", key);
    try {
      return super.spop(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<String> spop(String key, long count) {
    Span span = helper.buildSpan("spop", key);
    span.setTag("count", count);
    try {
      return super.spop(key, count);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long smove(String srckey, String dstkey, String member) {
    Span span = helper.buildSpan("smove");
    span.setTag("srckey", srckey);
    span.setTag("dstkey", dstkey);
    span.setTag("member", member);
    try {
      return super.smove(srckey, dstkey, member);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long scard(String key) {
    Span span = helper.buildSpan("scard", key);
    try {
      return super.scard(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Boolean sismember(String key, String member) {
    Span span = helper.buildSpan("sismember", key);
    span.setTag("member", member);
    try {
      return super.sismember(key, member);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<String> sinter(String... keys) {
    Span span = helper.buildSpan("sinter", keys);
    try {
      return super.sinter(keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long sinterstore(String dstkey, String... keys) {
    Span span = helper.buildSpan("sinterstore", keys);
    span.setTag("dstkey", dstkey);
    try {
      return super.sinterstore(dstkey, keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<String> sunion(String... keys) {
    Span span = helper.buildSpan("sunion", keys);
    try {
      return super.sunion(keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long sunionstore(String dstkey, String... keys) {
    Span span = helper.buildSpan("sunionstore", keys);
    span.setTag("dstkey", dstkey);
    try {
      return super.sunionstore(dstkey, keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<String> sdiff(String... keys) {
    Span span = helper.buildSpan("sdiff", keys);
    try {
      return super.sdiff(keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long sdiffstore(String dstkey, String... keys) {
    Span span = helper.buildSpan("sdiffstore", keys);
    span.setTag("dstkey", dstkey);
    try {
      return super.sdiffstore(dstkey, keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String srandmember(String key) {
    Span span = helper.buildSpan("srandmember", key);
    try {
      return super.srandmember(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<String> srandmember(String key, int count) {
    Span span = helper.buildSpan("srandmember", key);
    span.setTag("count", count);
    try {
      return super.srandmember(key, count);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zadd(String key, double score, String member) {
    Span span = helper.buildSpan("zadd", key);
    span.setTag("score", score);
    span.setTag("member", member);
    try {
      return super.zadd(key, score, member);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zadd(String key, double score, String member, ZAddParams params) {
    Span span = helper.buildSpan("zadd", key);
    span.setTag("score", score);
    span.setTag("member", member);
    span.setTag("params", TracingHelper.toString(params.getByteParams()));
    try {
      return super.zadd(key, score, member, params);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zadd(String key, Map<String, Double> scoreMembers) {
    Span span = helper.buildSpan("zadd", key);
    span.setTag("scoreMembers", TracingHelper.toString(scoreMembers));
    try {
      return super.zadd(key, scoreMembers);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zadd(String key, Map<String, Double> scoreMembers, ZAddParams params) {
    Span span = helper.buildSpan("zadd", key);
    span.setTag("scoreMembers", TracingHelper.toString(scoreMembers));
    span.setTag("params", TracingHelper.toString(params.getByteParams()));
    try {
      return super.zadd(key, scoreMembers, params);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<String> zrange(String key, long start, long end) {
    Span span = helper.buildSpan("zrange", key);
    span.setTag("start", start);
    span.setTag("end", end);
    try {
      return super.zrange(key, start, end);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zrem(String key, String... members) {
    Span span = helper.buildSpan("zrem", key);
    span.setTag("members", Arrays.toString(members));
    try {
      return super.zrem(key, members);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Double zincrby(String key, double score, String member) {
    Span span = helper.buildSpan("zincrby", key);
    span.setTag("score", score);
    span.setTag("member", member);
    try {
      return super.zincrby(key, score, member);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Double zincrby(String key, double score, String member, ZIncrByParams params) {
    Span span = helper.buildSpan("zincrby", key);
    span.setTag("score", score);
    span.setTag("member", member);
    span.setTag("params", TracingHelper.toString(params.getByteParams()));
    try {
      return super.zincrby(key, score, member, params);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zrank(String key, String member) {
    Span span = helper.buildSpan("zrank", key);
    span.setTag("member", member);
    try {
      return super.zrank(key, member);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zrevrank(String key, String member) {
    Span span = helper.buildSpan("zrevrank", key);
    span.setTag("member", member);
    try {
      return super.zrevrank(key, member);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<String> zrevrange(String key, long start, long end) {
    Span span = helper.buildSpan("zrevrange", key);
    span.setTag("start", start);
    span.setTag("end", end);
    try {
      return super.zrevrange(key, start, end);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<Tuple> zrangeWithScores(String key, long start, long end) {
    Span span = helper.buildSpan("zrangeWithScores", key);
    span.setTag("start", start);
    span.setTag("end", end);
    try {
      return super.zrangeWithScores(key, start, end);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<Tuple> zrevrangeWithScores(String key, long start, long end) {
    Span span = helper.buildSpan("zrevrangeWithScores", key);
    span.setTag("start", start);
    span.setTag("end", end);
    try {
      return super.zrevrangeWithScores(key, start, end);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zcard(String key) {
    Span span = helper.buildSpan("zcard", key);
    try {
      return super.zcard(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Double zscore(String key, String member) {
    Span span = helper.buildSpan("zscore", key);
    span.setTag("member", member);
    try {
      return super.zscore(key, member);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String watch(String... keys) {
    Span span = helper.buildSpan("watch", keys);
    try {
      return super.watch(keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<String> sort(String key) {
    Span span = helper.buildSpan("sort", key);
    try {
      return super.sort(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<String> sort(String key, SortingParams sortingParameters) {
    Span span = helper.buildSpan("sort", key);
    span.setTag("sortingParameters", TracingHelper.toString(sortingParameters.getParams()));
    try {
      return super.sort(key, sortingParameters);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<String> blpop(int timeout, String... keys) {
    Span span = helper.buildSpan("blpop", keys);
    span.setTag("timeout", timeout);
    try {
      return super.blpop(timeout, keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<String> blpop(String... args) {
    Span span = helper.buildSpan("blpop");
    span.setTag("args", Arrays.toString(args));
    try {
      return super.blpop(args);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<String> brpop(String... args) {
    Span span = helper.buildSpan("brpop");
    span.setTag("args", Arrays.toString(args));
    try {
      return super.brpop(args);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<String> blpop(String arg) {
    Span span = helper.buildSpan("blpop");
    span.setTag("arg", arg);
    try {
      return super.blpop(arg);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<String> brpop(String arg) {
    Span span = helper.buildSpan("brpop");
    span.setTag("arg", arg);
    try {
      return super.brpop(arg);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long sort(String key, SortingParams sortingParameters, String dstkey) {
    Span span = helper.buildSpan("sort", key);
    span.setTag("sortingParameters", TracingHelper.toString(sortingParameters.getParams()));
    span.setTag("dstkey", dstkey);
    try {
      return super.sort(key, sortingParameters, dstkey);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long sort(String key, String dstkey) {
    Span span = helper.buildSpan("sort", key);
    span.setTag("dstkey", dstkey);
    try {
      return super.sort(key, dstkey);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<String> brpop(int timeout, String... keys) {
    Span span = helper.buildSpan("brpop", keys);
    span.setTag("timeout", timeout);
    try {
      return super.brpop(timeout, keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zcount(String key, double min, double max) {
    Span span = helper.buildSpan("zcount", key);
    span.setTag("min", min);
    span.setTag("max", max);
    try {
      return super.zcount(key, min, max);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zcount(String key, String min, String max) {
    Span span = helper.buildSpan("zcount", key);
    span.setTag("min", min);
    span.setTag("max", max);
    try {
      return super.zcount(key, min, max);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<String> zrangeByScore(String key, double min, double max) {
    Span span = helper.buildSpan("zrangeByScore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    try {
      return super.zrangeByScore(key, min, max);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<String> zrangeByScore(String key, String min, String max) {
    Span span = helper.buildSpan("zrangeByScore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    try {
      return super.zrangeByScore(key, min, max);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<String> zrangeByScore(String key, double min, double max, int offset, int count) {
    Span span = helper.buildSpan("zrangeByScore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    try {
      return super.zrangeByScore(key, min, max, offset, count);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<String> zrangeByScore(String key, String min, String max, int offset, int count) {
    Span span = helper.buildSpan("zrangeByScore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    try {
      return super.zrangeByScore(key, min, max, offset, count);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max) {
    Span span = helper.buildSpan("zrangeByScoreWithScores", key);
    span.setTag("min", min);
    span.setTag("max", max);
    try {
      return super.zrangeByScoreWithScores(key, min, max);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max) {
    Span span = helper.buildSpan("zrangeByScoreWithScores", key);
    span.setTag("min", min);
    span.setTag("max", max);
    try {
      return super.zrangeByScoreWithScores(key, min, max);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max, int offset,
      int count) {
    Span span = helper.buildSpan("zrangeByScoreWithScores", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    try {
      return super.zrangeByScoreWithScores(key, min, max, offset, count);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max, int offset,
      int count) {
    Span span = helper.buildSpan("zrangeByScoreWithScores", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    try {
      return super.zrangeByScoreWithScores(key, min, max, offset, count);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<String> zrevrangeByScore(String key, double max, double min) {
    Span span = helper.buildSpan("zrevrangeByScore", key);
    span.setTag("max", max);
    span.setTag("min", min);
    try {
      return super.zrevrangeByScore(key, max, min);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<String> zrevrangeByScore(String key, String max, String min) {
    Span span = helper.buildSpan("zrevrangeByScore", key);
    span.setTag("max", max);
    span.setTag("min", min);
    try {
      return super.zrevrangeByScore(key, max, min);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<String> zrevrangeByScore(String key, double max, double min, int offset, int count) {
    Span span = helper.buildSpan("zrevrangeByScore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    try {
      return super.zrevrangeByScore(key, max, min, offset, count);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min) {
    Span span = helper.buildSpan("zrevrangeByScoreWithScores", key);
    span.setTag("max", max);
    span.setTag("min", min);
    try {
      return super.zrevrangeByScoreWithScores(key, max, min);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min, int offset,
      int count) {
    Span span = helper.buildSpan("zrevrangeByScoreWithScores", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    try {
      return super.zrevrangeByScoreWithScores(key, max, min, offset, count);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min, int offset,
      int count) {
    Span span = helper.buildSpan("zrevrangeByScoreWithScores", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    try {
      return super.zrevrangeByScoreWithScores(key, max, min, offset, count);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<String> zrevrangeByScore(String key, String max, String min, int offset, int count) {
    Span span = helper.buildSpan("zrevrangeByScore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    try {
      return super.zrevrangeByScore(key, max, min, offset, count);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min) {
    Span span = helper.buildSpan("zrevrangeByScoreWithScores", key);
    span.setTag("max", max);
    span.setTag("min", min);
    try {
      return super.zrevrangeByScoreWithScores(key, max, min);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zremrangeByRank(String key, long start, long end) {
    Span span = helper.buildSpan("zremrangeByRank", key);
    span.setTag("start", start);
    span.setTag("end", end);
    try {
      return super.zremrangeByRank(key, start, end);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zremrangeByScore(String key, double start, double end) {
    Span span = helper.buildSpan("zremrangeByScore", key);
    span.setTag("start", start);
    span.setTag("end", end);
    try {
      return super.zremrangeByScore(key, start, end);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zremrangeByScore(String key, String start, String end) {
    Span span = helper.buildSpan("zremrangeByScore", key);
    span.setTag("start", start);
    span.setTag("end", end);
    try {
      return super.zremrangeByScore(key, start, end);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zunionstore(String dstkey, String... sets) {
    Span span = helper.buildSpan("zunionstore");
    span.setTag("dstkey", dstkey);
    span.setTag("sets", Arrays.toString(sets));
    try {
      return super.zunionstore(dstkey, sets);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zunionstore(String dstkey, ZParams params, String... sets) {
    Span span = helper.buildSpan("zunionstore");
    span.setTag("params", TracingHelper.toString(params.getParams()));
    span.setTag("sets", Arrays.toString(sets));
    try {
      return super.zunionstore(dstkey, params, sets);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zinterstore(String dstkey, String... sets) {
    Span span = helper.buildSpan("zinterstore");
    span.setTag("dstkey", dstkey);
    span.setTag("sets", Arrays.toString(sets));
    try {
      return super.zinterstore(dstkey, sets);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zinterstore(String dstkey, ZParams params, String... sets) {
    Span span = helper.buildSpan("zinterstore");
    span.setTag("dstkey", dstkey);
    span.setTag("params", TracingHelper.toString(params.getParams()));
    span.setTag("sets", Arrays.toString(sets));
    try {
      return super.zinterstore(dstkey, params, sets);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zlexcount(String key, String min, String max) {
    Span span = helper.buildSpan("zlexcount", key);
    span.setTag("min", min);
    span.setTag("max", max);
    try {
      return super.zlexcount(key, min, max);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<String> zrangeByLex(String key, String min, String max) {
    Span span = helper.buildSpan("zrangeByLex", key);
    span.setTag("min", min);
    span.setTag("max", max);
    try {
      return super.zrangeByLex(key, min, max);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<String> zrangeByLex(String key, String min, String max, int offset, int count) {
    Span span = helper.buildSpan("zrangeByLex", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    try {
      return super.zrangeByLex(key, min, max, offset, count);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<String> zrevrangeByLex(String key, String max, String min) {
    Span span = helper.buildSpan("zrevrangeByLex", key);
    span.setTag("max", max);
    span.setTag("min", min);
    try {
      return super.zrevrangeByLex(key, max, min);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<String> zrevrangeByLex(String key, String max, String min, int offset, int count) {
    Span span = helper.buildSpan("zrevrangeByLex", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    try {
      return super.zrevrangeByLex(key, max, min, offset, count);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zremrangeByLex(String key, String min, String max) {
    Span span = helper.buildSpan("zremrangeByLex", key);
    span.setTag("min", min);
    span.setTag("max", max);
    try {
      return super.zremrangeByLex(key, min, max);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long strlen(String key) {
    Span span = helper.buildSpan("strlen", key);
    try {
      return super.strlen(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long lpushx(String key, String... string) {
    Span span = helper.buildSpan("lpushx", key);
    span.setTag("string", Arrays.toString(string));
    try {
      return super.lpushx(key, string);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long persist(String key) {
    Span span = helper.buildSpan("persist", key);
    try {
      return super.persist(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long rpushx(String key, String... string) {
    Span span = helper.buildSpan("rpushx", key);
    span.setTag("string", Arrays.toString(string));
    try {
      return super.rpushx(key, string);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String echo(String string) {
    Span span = helper.buildSpan("echo");
    span.setTag("string", string);
    try {
      return super.echo(string);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long linsert(String key, LIST_POSITION where, String pivot, String value) {
    Span span = helper.buildSpan("linsert", key);
    span.setTag("where", where.name());
    span.setTag("pivot", pivot);
    span.setTag("value", value);
    try {
      return super.linsert(key, where, pivot, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String brpoplpush(String source, String destination, int timeout) {
    Span span = helper.buildSpan("brpoplpush");
    span.setTag("source", source);
    span.setTag("destination", destination);
    span.setTag("timeout", timeout);
    try {
      return super.brpoplpush(source, destination, timeout);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Boolean setbit(String key, long offset, boolean value) {
    Span span = helper.buildSpan("setbit", key);
    span.setTag("offset", offset);
    span.setTag("value", value);
    try {
      return super.setbit(key, offset, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Boolean setbit(String key, long offset, String value) {
    Span span = helper.buildSpan("setbit", key);
    span.setTag("offset", offset);
    span.setTag("value", value);
    try {
      return super.setbit(key, offset, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Boolean getbit(String key, long offset) {
    Span span = helper.buildSpan("getbit", key);
    span.setTag("offset", offset);
    try {
      return super.getbit(key, offset);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long setrange(String key, long offset, String value) {
    Span span = helper.buildSpan("setrange", key);
    span.setTag("offset", offset);
    span.setTag("value", value);
    try {
      return super.setrange(key, offset, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String getrange(String key, long startOffset, long endOffset) {
    Span span = helper.buildSpan("getrange", key);
    span.setTag("startOffset", startOffset);
    span.setTag("endOffset", endOffset);
    try {
      return super.getrange(key, startOffset, endOffset);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long bitpos(String key, boolean value) {
    Span span = helper.buildSpan("bitpos", key);
    span.setTag("value", value);
    try {
      return super.bitpos(key, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long bitpos(String key, boolean value, BitPosParams params) {
    Span span = helper.buildSpan("bitpos", key);
    span.setTag("value", value);
    span.setTag("params", TracingHelper.toString(params.getParams()));
    try {
      return super.bitpos(key, value, params);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<String> configGet(String pattern) {
    Span span = helper.buildSpan("configGet");
    span.setTag("pattern", pattern);
    try {
      return super.configGet(pattern);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String configSet(String parameter, String value) {
    Span span = helper.buildSpan("configSet");
    span.setTag("value", value);
    try {
      return super.configSet(parameter, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Object eval(String script, int keyCount, String... params) {
    Span span = helper.buildSpan("eval");
    span.setTag("keyCount", keyCount);
    span.setTag("params", Arrays.toString(params));
    try {
      return super.eval(script, keyCount, params);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public void subscribe(JedisPubSub jedisPubSub, String... channels) {
    Span span = helper.buildSpan("subscribe");
    span.setTag("channels", Arrays.toString(channels));
    try {
      super.subscribe(jedisPubSub, channels);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long publish(String channel, String message) {
    Span span = helper.buildSpan("publish");
    span.setTag("channel", channel);
    span.setTag("message", message);
    try {
      return super.publish(channel, message);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public void psubscribe(JedisPubSub jedisPubSub, String... patterns) {
    Span span = helper.buildSpan("psubscribe");
    span.setTag("patterns", Arrays.toString(patterns));
    try {
      super.psubscribe(jedisPubSub, patterns);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Object eval(String script, List<String> keys, List<String> args) {
    Span span = helper.buildSpan("eval");
    span.setTag("keys", TracingHelper.toString(keys));
    span.setTag("args", TracingHelper.toString(args));
    try {
      return super.eval(script, keys, args);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Object eval(String script) {
    Span span = helper.buildSpan("eval");
    span.setTag("script", script);
    try {
      return super.eval(script);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Object evalsha(String script) {
    Span span = helper.buildSpan("evalsha");
    span.setTag("script", script);
    try {
      return super.evalsha(script);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Object evalsha(String sha1, List<String> keys, List<String> args) {
    Span span = helper.buildSpan("evalsha");
    span.setTag("keys", TracingHelper.toString(keys));
    span.setTag("args", TracingHelper.toString(args));
    span.setTag("sha1", sha1);
    try {
      return super.evalsha(sha1, keys, args);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Object evalsha(String sha1, int keyCount, String... params) {
    Span span = helper.buildSpan("evalsha");
    span.setTag("keyCount", keyCount);
    span.setTag("params", Arrays.toString(params));
    span.setTag("sha1", sha1);
    try {
      return super.evalsha(sha1, keyCount, params);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Boolean scriptExists(String sha1) {
    Span span = helper.buildSpan("scriptExists");
    span.setTag("sha1", sha1);
    try {
      return super.scriptExists(sha1);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<Boolean> scriptExists(String... sha1) {
    Span span = helper.buildSpan("scriptExists");
    span.setTag("sha1", Arrays.toString(sha1));
    try {
      return super.scriptExists(sha1);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String scriptLoad(String script) {
    Span span = helper.buildSpan("scriptLoad");
    span.setTag("script", script);
    try {
      return super.scriptLoad(script);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<Slowlog> slowlogGet() {
    Span span = helper.buildSpan("slowlogGet");
    try {
      return super.slowlogGet();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<Slowlog> slowlogGet(long entries) {
    Span span = helper.buildSpan("slowlogGet");
    span.setTag("entries", entries);
    try {
      return super.slowlogGet(entries);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long objectRefcount(String string) {
    Span span = helper.buildSpan("objectRefcount");
    span.setTag("string", string);
    try {
      return super.objectRefcount(string);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String objectEncoding(String string) {
    Span span = helper.buildSpan("objectEncoding");
    span.setTag("string", string);
    try {
      return super.objectEncoding(string);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long objectIdletime(String string) {
    Span span = helper.buildSpan("objectIdletime");
    span.setTag("string", string);
    try {
      return super.objectIdletime(string);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long bitcount(String key) {
    Span span = helper.buildSpan("bitcount", key);
    try {
      return super.bitcount(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long bitcount(String key, long start, long end) {
    Span span = helper.buildSpan("bitcount", key);
    span.setTag("start", start);
    span.setTag("end", end);
    try {
      return super.bitcount(key, start, end);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long bitop(BitOP op, String destKey, String... srcKeys) {
    Span span = helper.buildSpan("bitop");
    span.setTag("destKey", destKey);
    span.setTag("srcKeys", Arrays.toString(srcKeys));
    try {
      return super.bitop(op, destKey, srcKeys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<Map<String, String>> sentinelMasters() {
    Span span = helper.buildSpan("sentinelMasters");
    try {
      return super.sentinelMasters();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<String> sentinelGetMasterAddrByName(String masterName) {
    Span span = helper.buildSpan("sentinelGetMasterAddrByName");
    span.setTag("masterName", masterName);
    try {
      return super.sentinelGetMasterAddrByName(masterName);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long sentinelReset(String pattern) {
    Span span = helper.buildSpan("sentinelReset");
    span.setTag("pattern", pattern);
    try {
      return super.sentinelReset(pattern);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<Map<String, String>> sentinelSlaves(String masterName) {
    Span span = helper.buildSpan("sentinelSlaves");
    span.setTag("masterName", masterName);
    try {
      return super.sentinelSlaves(masterName);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String sentinelFailover(String masterName) {
    Span span = helper.buildSpan("sentinelFailover");
    try {
      return super.sentinelFailover(masterName);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String sentinelMonitor(String masterName, String ip, int port, int quorum) {
    Span span = helper.buildSpan("sentinelMonitor");
    span.setTag("masterName", masterName);
    span.setTag("ip", ip);
    span.setTag("port", port);
    span.setTag("quorum", quorum);
    try {
      return super.sentinelMonitor(masterName, ip, port, quorum);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String sentinelRemove(String masterName) {
    Span span = helper.buildSpan("sentinelRemove");
    span.setTag("masterName", masterName);
    try {
      return super.sentinelRemove(masterName);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String sentinelSet(String masterName, Map<String, String> parameterMap) {
    Span span = helper.buildSpan("sentinelSet");
    span.setTag("masterName", masterName);
    span.setTag("parameterMap", TracingHelper.toString(parameterMap));
    try {
      return super.sentinelSet(masterName, parameterMap);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public byte[] dump(String key) {
    Span span = helper.buildSpan("dump", key);
    try {
      return super.dump(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String restore(String key, int ttl, byte[] serializedValue) {
    Span span = helper.buildSpan("restore", key);
    span.setTag("ttl", ttl);
    span.setTag("serializedValue", Arrays.toString(serializedValue));
    try {
      return super.restore(key, ttl, serializedValue);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long pexpire(String key, int milliseconds) {
    Span span = helper.buildSpan("pexpire", key);
    span.setTag("milliseconds", milliseconds);
    try {
      return super.pexpire(key, milliseconds);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String ping() {
    Span span = helper.buildSpan("ping");
    try {
      return super.ping();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String set(byte[] key, byte[] value) {
    Span span = helper.buildSpan("set", key);
    span.setTag("value", Arrays.toString(value));
    try {
      return super.set(key, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String set(byte[] key, byte[] value, byte[] nxxx, byte[] expx, long time) {
    Span span = helper.buildSpan("set", key);
    span.setTag("nxxx", Arrays.toString(nxxx));
    span.setTag("expx", Arrays.toString(expx));
    span.setTag("time", time);
    span.setTag("value", Arrays.toString(value));
    try {
      return super.set(key, value, nxxx, expx, time);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public byte[] get(byte[] key) {
    Span span = helper.buildSpan("get", key);
    try {
      return super.get(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String quit() {
    Span span = helper.buildSpan("quit");
    try {
      return super.quit();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long exists(byte[]... keys) {
    Span span = helper.buildSpan("exists");
    span.setTag("keys", TracingHelper.toString(keys));
    try {
      return super.exists(keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Boolean exists(byte[] key) {
    Span span = helper.buildSpan("exists", key);
    try {
      return super.exists(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long del(byte[]... keys) {
    Span span = helper.buildSpan("del");
    span.setTag("keys", TracingHelper.toString(keys));
    try {
      return super.del(keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long del(byte[] key) {
    Span span = helper.buildSpan("del", key);
    try {
      return super.del(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String type(byte[] key) {
    Span span = helper.buildSpan("type", key);
    try {
      return super.type(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String flushDB() {
    Span span = helper.buildSpan("flushDB");
    try {
      return super.flushDB();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<byte[]> keys(byte[] pattern) {
    Span span = helper.buildSpan("keys");
    span.setTag("pattern", Arrays.toString(pattern));
    try {
      return super.keys(pattern);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public byte[] randomBinaryKey() {
    Span span = helper.buildSpan("randomBinaryKey");
    try {
      return super.randomBinaryKey();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String rename(byte[] oldkey, byte[] newkey) {
    Span span = helper.buildSpan("rename");
    span.setTag("oldkey", Arrays.toString(oldkey));
    span.setTag("newkey", Arrays.toString(newkey));
    try {
      return super.rename(oldkey, newkey);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long renamenx(byte[] oldkey, byte[] newkey) {
    Span span = helper.buildSpan("renamenx");
    span.setTag("oldkey", Arrays.toString(oldkey));
    span.setTag("newkey", Arrays.toString(newkey));
    try {
      return super.renamenx(oldkey, newkey);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long dbSize() {
    Span span = helper.buildSpan("dbSize");
    try {
      return super.dbSize();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long expire(byte[] key, int seconds) {
    Span span = helper.buildSpan("expire", key);
    span.setTag("seconds", seconds);
    try {
      return super.expire(key, seconds);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long pexpire(String key, long milliseconds) {
    Span span = helper.buildSpan("pexpire", key);
    span.setTag("milliseconds", milliseconds);
    try {
      return super.pexpire(key, milliseconds);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long expireAt(byte[] key, long unixTime) {
    Span span = helper.buildSpan("expireAt", key);
    span.setTag("unixTime", unixTime);
    try {
      return super.expireAt(key, unixTime);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long ttl(byte[] key) {
    Span span = helper.buildSpan("ttl", key);
    try {
      return super.ttl(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String select(int index) {
    Span span = helper.buildSpan("select");
    span.setTag("index", index);
    try {
      return super.select(index);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long move(byte[] key, int dbIndex) {
    Span span = helper.buildSpan("move", key);
    span.setTag("dbIndex", dbIndex);
    try {
      return super.move(key, dbIndex);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String flushAll() {
    Span span = helper.buildSpan("flushAll");
    try {
      return super.flushAll();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public byte[] getSet(byte[] key, byte[] value) {
    Span span = helper.buildSpan("getSet", key);
    span.setTag("value", Arrays.toString(value));
    try {
      return super.getSet(key, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<byte[]> mget(byte[]... keys) {
    Span span = helper.buildSpan("mget");
    span.setTag("keys", TracingHelper.toString(keys));
    try {
      return super.mget(keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long setnx(byte[] key, byte[] value) {
    Span span = helper.buildSpan("setnx", key);
    span.setTag("value", Arrays.toString(value));
    try {
      return super.setnx(key, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String setex(byte[] key, int seconds, byte[] value) {
    Span span = helper.buildSpan("setex", key);
    span.setTag("value", Arrays.toString(value));
    span.setTag("seconds", seconds);
    try {
      return super.setex(key, seconds, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String mset(byte[]... keysvalues) {
    Span span = helper.buildSpan("mset");
    span.setTag("keysvalues", TracingHelper.toString(keysvalues));
    try {
      return super.mset(keysvalues);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long msetnx(byte[]... keysvalues) {
    Span span = helper.buildSpan("msetnx");
    span.setTag("keysvalues", TracingHelper.toString(keysvalues));
    try {
      return super.msetnx(keysvalues);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long decrBy(byte[] key, long integer) {
    Span span = helper.buildSpan("decrBy", key);
    span.setTag("integer", integer);
    try {
      return super.decrBy(key, integer);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long decr(byte[] key) {
    Span span = helper.buildSpan("decr", key);
    try {
      return super.decr(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long incrBy(byte[] key, long integer) {
    Span span = helper.buildSpan("incrBy", key);
    span.setTag("integer", integer);
    try {
      return super.incrBy(key, integer);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Double incrByFloat(byte[] key, double integer) {
    Span span = helper.buildSpan("incrByFloat", key);
    span.setTag("integer", integer);
    try {
      return super.incrByFloat(key, integer);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long incr(byte[] key) {
    Span span = helper.buildSpan("incr", key);
    try {
      return super.incr(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long append(byte[] key, byte[] value) {
    Span span = helper.buildSpan("append", key);
    span.setTag("value", Arrays.toString(value));
    try {
      return super.append(key, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public byte[] substr(byte[] key, int start, int end) {
    Span span = helper.buildSpan("substr", key);
    span.setTag("start", start);
    span.setTag("end", end);
    try {
      return super.substr(key, start, end);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long hset(byte[] key, byte[] field, byte[] value) {
    Span span = helper.buildSpan("hset", key);
    span.setTag("field", Arrays.toString(field));
    span.setTag("value", Arrays.toString(value));
    try {
      return super.hset(key, field, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public byte[] hget(byte[] key, byte[] field) {
    Span span = helper.buildSpan("hget", key);
    span.setTag("field", Arrays.toString(field));
    try {
      return super.hget(key, field);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long hsetnx(byte[] key, byte[] field, byte[] value) {
    Span span = helper.buildSpan("hsetnx", key);
    span.setTag("field", Arrays.toString(field));
    span.setTag("value", Arrays.toString(value));
    try {
      return super.hsetnx(key, field, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String hmset(byte[] key, Map<byte[], byte[]> hash) {
    Span span = helper.buildSpan("hmset", key);
    span.setTag("hash", TracingHelper.toStringMap(hash));
    try {
      return super.hmset(key, hash);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<byte[]> hmget(byte[] key, byte[]... fields) {
    Span span = helper.buildSpan("hmget", key);
    span.setTag("fields", TracingHelper.toString(fields));
    try {
      return super.hmget(key, fields);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long hincrBy(byte[] key, byte[] field, long value) {
    Span span = helper.buildSpan("hincrBy", key);
    span.setTag("field", Arrays.toString(field));
    span.setTag("value", value);
    try {
      return super.hincrBy(key, field, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Double hincrByFloat(byte[] key, byte[] field, double value) {
    Span span = helper.buildSpan("hincrByFloat", key);
    span.setTag("field", Arrays.toString(field));
    span.setTag("value", value);
    try {
      return super.hincrByFloat(key, field, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Boolean hexists(byte[] key, byte[] field) {
    Span span = helper.buildSpan("hexists", key);
    span.setTag("field", Arrays.toString(field));
    try {
      return super.hexists(key, field);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long hdel(byte[] key, byte[]... fields) {
    Span span = helper.buildSpan("hdel", key);
    span.setTag("fields", TracingHelper.toString(fields));
    try {
      return super.hdel(key, fields);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long hlen(byte[] key) {
    Span span = helper.buildSpan("hlen", key);
    try {
      return super.hlen(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<byte[]> hkeys(byte[] key) {
    Span span = helper.buildSpan("hkeys", key);
    try {
      return super.hkeys(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<byte[]> hvals(byte[] key) {
    Span span = helper.buildSpan("hvals", key);
    try {
      return super.hvals(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Map<byte[], byte[]> hgetAll(byte[] key) {
    Span span = helper.buildSpan("hgetAll", key);
    try {
      return super.hgetAll(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long rpush(byte[] key, byte[]... strings) {
    Span span = helper.buildSpan("rpush", key);
    span.setTag("strings", TracingHelper.toString(strings));
    try {
      return super.rpush(key, strings);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long lpush(byte[] key, byte[]... strings) {
    Span span = helper.buildSpan("lpush", key);
    span.setTag("strings", TracingHelper.toString(strings));
    try {
      return super.lpush(key, strings);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long llen(byte[] key) {
    Span span = helper.buildSpan("llen", key);
    try {
      return super.llen(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<byte[]> lrange(byte[] key, long start, long end) {
    Span span = helper.buildSpan("lrange", key);
    span.setTag("start", start);
    span.setTag("end", end);
    try {
      return super.lrange(key, start, end);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String ltrim(byte[] key, long start, long end) {
    Span span = helper.buildSpan("ltrim", key);
    span.setTag("start", start);
    span.setTag("end", end);
    try {
      return super.ltrim(key, start, end);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public byte[] lindex(byte[] key, long index) {
    Span span = helper.buildSpan("lindex", key);
    span.setTag("index", index);
    try {
      return super.lindex(key, index);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String lset(byte[] key, long index, byte[] value) {
    Span span = helper.buildSpan("lset", key);
    span.setTag("index", index);
    span.setTag("value", Arrays.toString(value));
    try {
      return super.lset(key, index, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long lrem(byte[] key, long count, byte[] value) {
    Span span = helper.buildSpan("lrem", key);
    span.setTag("count", count);
    span.setTag("value", Arrays.toString(value));
    try {
      return super.lrem(key, count, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public byte[] lpop(byte[] key) {
    Span span = helper.buildSpan("lpop", key);
    try {
      return super.lpop(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public byte[] rpop(byte[] key) {
    Span span = helper.buildSpan("rpop", key);
    try {
      return super.rpop(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public byte[] rpoplpush(byte[] srckey, byte[] dstkey) {
    Span span = helper.buildSpan("rpoplpush");
    span.setTag("srckey", Arrays.toString(srckey));
    span.setTag("dstkey", Arrays.toString(dstkey));
    try {
      return super.rpoplpush(srckey, dstkey);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long sadd(byte[] key, byte[]... members) {
    Span span = helper.buildSpan("sadd", key);
    span.setTag("members", Arrays.toString(members));
    try {
      return super.sadd(key, members);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<byte[]> smembers(byte[] key) {
    Span span = helper.buildSpan("smembers", key);
    try {
      return super.smembers(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long srem(byte[] key, byte[]... member) {
    Span span = helper.buildSpan("srem", key);
    span.setTag("member", Arrays.toString(member));
    try {
      return super.srem(key, member);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public byte[] spop(byte[] key) {
    Span span = helper.buildSpan("spop", key);
    try {
      return super.spop(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<byte[]> spop(byte[] key, long count) {
    Span span = helper.buildSpan("spop", key);
    span.setTag("count", count);
    try {
      return super.spop(key, count);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long smove(byte[] srckey, byte[] dstkey, byte[] member) {
    Span span = helper.buildSpan("smove");
    span.setTag("srckey", Arrays.toString(srckey));
    span.setTag("dstkey", Arrays.toString(dstkey));
    span.setTag("member", Arrays.toString(member));
    try {
      return super.smove(srckey, dstkey, member);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long scard(byte[] key) {
    Span span = helper.buildSpan("scard", key);
    try {
      return super.scard(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Boolean sismember(byte[] key, byte[] member) {
    Span span = helper.buildSpan("sismember", key);
    span.setTag("member", Arrays.toString(member));
    try {
      return super.sismember(key, member);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<byte[]> sinter(byte[]... keys) {
    Span span = helper.buildSpan("sinter");
    span.setTag("keys", TracingHelper.toString(keys));
    try {
      return super.sinter(keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long sinterstore(byte[] dstkey, byte[]... keys) {
    Span span = helper.buildSpan("sinterstore");
    span.setTag("dstkey", Arrays.toString(dstkey));
    span.setTag("keys", TracingHelper.toString(keys));
    try {
      return super.sinterstore(dstkey, keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<byte[]> sunion(byte[]... keys) {
    Span span = helper.buildSpan("sunion");
    span.setTag("keys", TracingHelper.toString(keys));
    try {
      return super.sunion(keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long sunionstore(byte[] dstkey, byte[]... keys) {
    Span span = helper.buildSpan("sunionstore");
    span.setTag("dstkey", Arrays.toString(dstkey));
    span.setTag("keys", TracingHelper.toString(keys));
    try {
      return super.sunionstore(dstkey, keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<byte[]> sdiff(byte[]... keys) {
    Span span = helper.buildSpan("sdiff");
    span.setTag("keys", TracingHelper.toString(keys));
    try {
      return super.sdiff(keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long sdiffstore(byte[] dstkey, byte[]... keys) {
    Span span = helper.buildSpan("sdiffstore");
    span.setTag("dstkey", Arrays.toString(dstkey));
    span.setTag("keys", TracingHelper.toString(keys));
    try {
      return super.sdiffstore(dstkey, keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public byte[] srandmember(byte[] key) {
    Span span = helper.buildSpan("srandmember", key);
    try {
      return super.srandmember(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<byte[]> srandmember(byte[] key, int count) {
    Span span = helper.buildSpan("srandmember", key);
    span.setTag("count", count);
    try {
      return super.srandmember(key, count);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zadd(byte[] key, double score, byte[] member) {
    Span span = helper.buildSpan("zadd", key);
    span.setTag("member", Arrays.toString(member));
    span.setTag("score", score);
    try {
      return super.zadd(key, score, member);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zadd(byte[] key, double score, byte[] member, ZAddParams params) {
    Span span = helper.buildSpan("zadd", key);
    span.setTag("member", Arrays.toString(member));
    span.setTag("score", score);
    span.setTag("params", TracingHelper.toString(params.getByteParams()));
    try {
      return super.zadd(key, score, member, params);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zadd(byte[] key, Map<byte[], Double> scoreMembers) {
    Span span = helper.buildSpan("zadd", key);
    span.setTag("scoreMembers", TracingHelper.toStringMap2(scoreMembers));
    try {
      return super.zadd(key, scoreMembers);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zadd(byte[] key, Map<byte[], Double> scoreMembers, ZAddParams params) {
    Span span = helper.buildSpan("zadd", key);
    span.setTag("scoreMembers", TracingHelper.toStringMap2(scoreMembers));
    span.setTag("params", TracingHelper.toString(params.getByteParams()));
    try {
      return super.zadd(key, scoreMembers, params);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<byte[]> zrange(byte[] key, long start, long end) {
    Span span = helper.buildSpan("zrange", key);
    span.setTag("start", start);
    span.setTag("end", end);
    try {
      return super.zrange(key, start, end);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zrem(byte[] key, byte[]... members) {
    Span span = helper.buildSpan("zrem", key);
    span.setTag("members", Arrays.toString(members));
    try {
      return super.zrem(key, members);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Double zincrby(byte[] key, double score, byte[] member) {
    Span span = helper.buildSpan("zincrby", key);
    span.setTag("member", Arrays.toString(member));
    span.setTag("score", score);
    try {
      return super.zincrby(key, score, member);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Double zincrby(byte[] key, double score, byte[] member, ZIncrByParams params) {
    Span span = helper.buildSpan("zincrby", key);
    span.setTag("member", Arrays.toString(member));
    span.setTag("score", score);
    span.setTag("params", TracingHelper.toString(params.getByteParams()));
    try {
      return super.zincrby(key, score, member, params);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zrank(byte[] key, byte[] member) {
    Span span = helper.buildSpan("zrank", key);
    span.setTag("member", Arrays.toString(member));
    try {
      return super.zrank(key, member);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zrevrank(byte[] key, byte[] member) {
    Span span = helper.buildSpan("zrevrank", key);
    span.setTag("member", Arrays.toString(member));
    try {
      return super.zrevrank(key, member);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<byte[]> zrevrange(byte[] key, long start, long end) {
    Span span = helper.buildSpan("zrevrange", key);
    span.setTag("start", start);
    span.setTag("end", end);
    try {
      return super.zrevrange(key, start, end);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<Tuple> zrangeWithScores(byte[] key, long start, long end) {
    Span span = helper.buildSpan("zrangeWithScores", key);
    span.setTag("start", start);
    span.setTag("end", end);
    try {
      return super.zrangeWithScores(key, start, end);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<Tuple> zrevrangeWithScores(byte[] key, long start, long end) {
    Span span = helper.buildSpan("zrevrangeWithScores", key);
    span.setTag("start", start);
    span.setTag("end", end);
    try {
      return super.zrevrangeWithScores(key, start, end);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zcard(byte[] key) {
    Span span = helper.buildSpan("zcard", key);
    try {
      return super.zcard(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Double zscore(byte[] key, byte[] member) {
    Span span = helper.buildSpan("zscore", key);
    span.setTag("member", Arrays.toString(member));
    try {
      return super.zscore(key, member);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Transaction multi() {
    Span span = helper.buildSpan("multi");
    try {
      return super.multi();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<Object> multi(TransactionBlock jedisTransaction) {
    Span span = helper.buildSpan("multi");
    try {
      return super.multi(jedisTransaction);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public void connect() {
    Span span = helper.buildSpan("connect");
    try {
      super.connect();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public void disconnect() {
    Span span = helper.buildSpan("disconnect");
    try {
      super.disconnect();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public void resetState() {
    Span span = helper.buildSpan("resetState");
    try {
      super.resetState();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String watch(byte[]... keys) {
    Span span = helper.buildSpan("watch");
    span.setTag("keys", TracingHelper.toString(keys));
    try {
      return super.watch(keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String unwatch() {
    Span span = helper.buildSpan("unwatch");
    try {
      return super.unwatch();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long pexpireAt(String key, long millisecondsTimestamp) {
    Span span = helper.buildSpan("pexpireAt", key);
    span.setTag("millisecondsTimestamp", millisecondsTimestamp);
    try {
      return super.pexpireAt(key, millisecondsTimestamp);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long pttl(String key) {
    Span span = helper.buildSpan("pttl", key);
    try {
      return super.pttl(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String psetex(String key, int milliseconds, String value) {
    Span span = helper.buildSpan("psetex", key);
    span.setTag("value", value);
    span.setTag("milliseconds", milliseconds);
    try {
      return super.psetex(key, milliseconds, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String psetex(String key, long milliseconds, String value) {
    Span span = helper.buildSpan("psetex", key);
    span.setTag("value", value);
    span.setTag("milliseconds", milliseconds);
    try {
      return super.psetex(key, milliseconds, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String set(String key, String value, String nxxx) {
    Span span = helper.buildSpan("set", key);
    span.setTag("value", value);
    span.setTag("nxxx", nxxx);
    try {
      return super.set(key, value, nxxx);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String set(String key, String value, String nxxx, String expx, int time) {
    Span span = helper.buildSpan("set", key);
    span.setTag("value", value);
    span.setTag("nxxx", nxxx);
    span.setTag("expx", expx);
    span.setTag("time", time);
    try {
      return super.set(key, value, nxxx, expx, time);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String clientKill(String client) {
    Span span = helper.buildSpan("clientKill");
    span.setTag("client", client);
    try {
      return super.clientKill(client);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String clientSetname(String name) {
    Span span = helper.buildSpan("clientSetname");
    span.setTag("name", name);
    try {
      return super.clientSetname(name);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String migrate(String host, int port, String key, int destinationDb, int timeout) {
    Span span = helper.buildSpan("migrate", key);
    span.setTag("host", host);
    span.setTag("destinationDb", destinationDb);
    span.setTag("timeout", timeout);
    span.setTag("port", port);
    try {
      return super.migrate(host, port, key, destinationDb, timeout);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public ScanResult<String> scan(int cursor) {
    Span span = helper.buildSpan("scan");
    span.setTag("cursor", cursor);
    try {
      return super.scan(cursor);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public ScanResult<String> scan(int cursor, ScanParams params) {
    Span span = helper.buildSpan("scan");
    span.setTag("cursor", cursor);
    span.setTag("params", TracingHelper.toString(params.getParams()));
    try {
      return super.scan(cursor, params);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public ScanResult<Entry<String, String>> hscan(String key, int cursor) {
    Span span = helper.buildSpan("hscan", key);
    span.setTag("cursor", cursor);
    try {
      return super.hscan(key, cursor);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public ScanResult<Entry<String, String>> hscan(String key, int cursor, ScanParams params) {
    Span span = helper.buildSpan("hscan", key);
    span.setTag("cursor", cursor);
    span.setTag("params", TracingHelper.toString(params.getParams()));
    try {
      return super.hscan(key, cursor, params);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public ScanResult<String> sscan(String key, int cursor) {
    Span span = helper.buildSpan("sscan", key);
    span.setTag("cursor", cursor);
    try {
      return super.sscan(key, cursor);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public ScanResult<String> sscan(String key, int cursor, ScanParams params) {
    Span span = helper.buildSpan("sscan", key);
    span.setTag("cursor", cursor);
    span.setTag("params", TracingHelper.toString(params.getParams()));
    try {
      return super.sscan(key, cursor, params);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public ScanResult<Tuple> zscan(String key, int cursor) {
    Span span = helper.buildSpan("zscan", key);
    span.setTag("cursor", cursor);
    try {
      return super.zscan(key, cursor);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public ScanResult<Tuple> zscan(String key, int cursor, ScanParams params) {
    Span span = helper.buildSpan("zscan", key);
    span.setTag("cursor", cursor);
    span.setTag("params", TracingHelper.toString(params.getParams()));
    try {
      return super.zscan(key, cursor, params);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public ScanResult<String> scan(String cursor) {
    Span span = helper.buildSpan("scan");
    span.setTag("cursor", cursor);
    try {
      return super.scan(cursor);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public ScanResult<String> scan(String cursor, ScanParams params) {
    Span span = helper.buildSpan("scan");
    span.setTag("cursor", cursor);
    span.setTag("params", TracingHelper.toString(params.getParams()));
    try {
      return super.scan(cursor, params);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public ScanResult<Entry<String, String>> hscan(String key, String cursor) {
    Span span = helper.buildSpan("hscan", key);
    span.setTag("cursor", cursor);
    try {
      return super.hscan(key, cursor);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public ScanResult<Entry<String, String>> hscan(String key, String cursor, ScanParams params) {
    Span span = helper.buildSpan("hscan", key);
    span.setTag("cursor", cursor);
    span.setTag("params", TracingHelper.toString(params.getParams()));
    try {
      return super.hscan(key, cursor, params);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public ScanResult<String> sscan(String key, String cursor) {
    Span span = helper.buildSpan("sscan", key);
    span.setTag("cursor", cursor);
    try {
      return super.sscan(key, cursor);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public ScanResult<String> sscan(String key, String cursor, ScanParams params) {
    Span span = helper.buildSpan("sscan", key);
    span.setTag("cursor", cursor);
    span.setTag("params", TracingHelper.toString(params.getParams()));
    try {
      return super.sscan(key, cursor, params);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public ScanResult<Tuple> zscan(String key, String cursor) {
    Span span = helper.buildSpan("zscan", key);
    span.setTag("cursor", cursor);
    try {
      return super.zscan(key, cursor);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public ScanResult<Tuple> zscan(String key, String cursor, ScanParams params) {
    Span span = helper.buildSpan("zscan", key);
    span.setTag("cursor", cursor);
    span.setTag("params", TracingHelper.toString(params.getParams()));
    try {
      return super.zscan(key, cursor, params);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String clusterNodes() {
    Span span = helper.buildSpan("clusterNodes");
    try {
      return super.clusterNodes();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String readonly() {
    Span span = helper.buildSpan("readonly");
    try {
      return super.readonly();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String clusterMeet(String ip, int port) {
    Span span = helper.buildSpan("clusterMeet");
    span.setTag("ip", ip);
    span.setTag("port", port);
    try {
      return super.clusterMeet(ip, port);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String clusterReset(Reset resetType) {
    Span span = helper.buildSpan("clusterReset");
    span.setTag("resetType", resetType.name());
    try {
      return super.clusterReset(resetType);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String clusterAddSlots(int... slots) {
    Span span = helper.buildSpan("clusterAddSlots");
    span.setTag("slots", Arrays.toString(slots));
    try {
      return super.clusterAddSlots(slots);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String clusterDelSlots(int... slots) {
    Span span = helper.buildSpan("clusterDelSlots");
    span.setTag("slots", Arrays.toString(slots));
    try {
      return super.clusterDelSlots(slots);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String clusterInfo() {
    Span span = helper.buildSpan("clusterInfo");
    try {
      return super.clusterInfo();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<String> clusterGetKeysInSlot(int slot, int count) {
    Span span = helper.buildSpan("clusterGetKeysInSlot");
    span.setTag("slot", slot);
    span.setTag("count", count);
    try {
      return super.clusterGetKeysInSlot(slot, count);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String clusterSetSlotNode(int slot, String nodeId) {
    Span span = helper.buildSpan("clusterSetSlotNode");
    span.setTag("slot", slot);
    span.setTag("nodeId", nodeId);
    try {
      return super.clusterSetSlotNode(slot, nodeId);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String clusterSetSlotMigrating(int slot, String nodeId) {
    Span span = helper.buildSpan("clusterSetSlotMigrating");
    span.setTag("slot", slot);
    span.setTag("nodeId", nodeId);
    try {
      return super.clusterSetSlotMigrating(slot, nodeId);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String clusterSetSlotImporting(int slot, String nodeId) {
    Span span = helper.buildSpan("clusterSetSlotImporting");
    span.setTag("slot", slot);
    span.setTag("nodeId", nodeId);
    try {
      return super.clusterSetSlotImporting(slot, nodeId);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String clusterSetSlotStable(int slot) {
    Span span = helper.buildSpan("clusterSetSlotStable");
    span.setTag("slot", slot);
    try {
      return super.clusterSetSlotStable(slot);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String clusterForget(String nodeId) {
    Span span = helper.buildSpan("clusterForget");
    span.setTag("nodeId", nodeId);
    try {
      return super.clusterForget(nodeId);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String clusterFlushSlots() {
    Span span = helper.buildSpan("clusterFlushSlots");
    try {
      return super.clusterFlushSlots();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long clusterKeySlot(String key) {
    Span span = helper.buildSpan("clusterKeySlot", key);
    try {
      return super.clusterKeySlot(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long clusterCountKeysInSlot(int slot) {
    Span span = helper.buildSpan("clusterCountKeysInSlot");
    span.setTag("slot", slot);
    try {
      return super.clusterCountKeysInSlot(slot);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String clusterSaveConfig() {
    Span span = helper.buildSpan("clusterSaveConfig");
    try {
      return super.clusterSaveConfig();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String clusterReplicate(String nodeId) {
    Span span = helper.buildSpan("clusterReplicate");
    span.setTag("nodeId", nodeId);
    try {
      return super.clusterReplicate(nodeId);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<String> clusterSlaves(String nodeId) {
    Span span = helper.buildSpan("clusterSlaves");
    span.setTag("nodeId", nodeId);
    try {
      return super.clusterSlaves(nodeId);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String clusterFailover() {
    Span span = helper.buildSpan("clusterFailover");
    try {
      return super.clusterFailover();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<Object> clusterSlots() {
    Span span = helper.buildSpan("clusterSlots");
    try {
      return super.clusterSlots();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String asking() {
    Span span = helper.buildSpan("asking");
    try {
      return super.asking();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<String> pubsubChannels(String pattern) {
    Span span = helper.buildSpan("pubsubChannels");
    span.setTag("pattern", pattern);
    try {
      return super.pubsubChannels(pattern);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long pubsubNumPat() {
    Span span = helper.buildSpan("pubsubNumPat");
    try {
      return super.pubsubNumPat();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Map<String, String> pubsubNumSub(String... channels) {
    Span span = helper.buildSpan("pubsubNumSub");
    span.setTag("channels", Arrays.toString(channels));
    try {
      return super.pubsubNumSub(channels);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public void close() {
    super.close();
  }

  @Override
  public List<byte[]> sort(byte[] key) {
    Span span = helper.buildSpan("sort", key);
    try {
      return super.sort(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<byte[]> sort(byte[] key, SortingParams sortingParameters) {
    Span span = helper.buildSpan("sort", key);
    span.setTag("sortingParameters", TracingHelper.toString(sortingParameters.getParams()));
    try {
      return super.sort(key, sortingParameters);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<byte[]> blpop(int timeout, byte[]... keys) {
    Span span = helper.buildSpan("blpop");
    span.setTag("timeout", timeout);
    span.setTag("keys", TracingHelper.toString(keys));
    try {
      return super.blpop(timeout, keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long sort(byte[] key, SortingParams sortingParameters, byte[] dstkey) {
    Span span = helper.buildSpan("sort", key);
    span.setTag("sortingParameters", TracingHelper.toString(sortingParameters.getParams()));
    span.setTag("dstkey", Arrays.toString(dstkey));
    try {
      return super.sort(key, sortingParameters, dstkey);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long sort(byte[] key, byte[] dstkey) {
    Span span = helper.buildSpan("sort", key);
    span.setTag("dstkey", Arrays.toString(dstkey));
    try {
      return super.sort(key, dstkey);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<byte[]> brpop(int timeout, byte[]... keys) {
    Span span = helper.buildSpan("brpop");
    span.setTag("timeout", timeout);
    span.setTag("keys", TracingHelper.toString(keys));
    try {
      return super.brpop(timeout, keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<byte[]> blpop(byte[] arg) {
    Span span = helper.buildSpan("blpop");
    span.setTag("arg", Arrays.toString(arg));
    try {
      return super.blpop(arg);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<byte[]> brpop(byte[] arg) {
    Span span = helper.buildSpan("brpop");
    span.setTag("arg", Arrays.toString(arg));
    try {
      return super.brpop(arg);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<byte[]> blpop(byte[]... args) {
    Span span = helper.buildSpan("blpop");
    span.setTag("args", TracingHelper.toString(args));
    try {
      return super.blpop(args);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<byte[]> brpop(byte[]... args) {
    Span span = helper.buildSpan("brpop");
    span.setTag("args", TracingHelper.toString(args));
    try {
      return super.brpop(args);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String auth(String password) {
    Span span = helper.buildSpan("auth");
    try {
      return super.auth(password);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<Object> pipelined(PipelineBlock jedisPipeline) {
    Span span = helper.buildSpan("pipelined");
    try {
      return super.pipelined(jedisPipeline);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Pipeline pipelined() {
    Span span = helper.buildSpan("pipelined");
    try {
      return super.pipelined();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zcount(byte[] key, double min, double max) {
    Span span = helper.buildSpan("zcount", key);
    span.setTag("min", min);
    span.setTag("max", max);
    try {
      return super.zcount(key, min, max);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zcount(byte[] key, byte[] min, byte[] max) {
    Span span = helper.buildSpan("zcount", key);
    span.setTag("min", Arrays.toString(min));
    span.setTag("max", Arrays.toString(max));
    try {
      return super.zcount(key, min, max);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<byte[]> zrangeByScore(byte[] key, double min, double max) {
    Span span = helper.buildSpan("zrangeByScore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    try {
      return super.zrangeByScore(key, min, max);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<byte[]> zrangeByScore(byte[] key, byte[] min, byte[] max) {
    Span span = helper.buildSpan("zrangeByScore", key);
    span.setTag("min", Arrays.toString(min));
    span.setTag("max", Arrays.toString(max));
    try {
      return super.zrangeByScore(key, min, max);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<byte[]> zrangeByScore(byte[] key, double min, double max, int offset, int count) {
    Span span = helper.buildSpan("zrangeByScore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    try {
      return super.zrangeByScore(key, min, max, offset, count);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<byte[]> zrangeByScore(byte[] key, byte[] min, byte[] max, int offset, int count) {
    Span span = helper.buildSpan("zrangeByScore", key);
    span.setTag("min", Arrays.toString(min));
    span.setTag("max", Arrays.toString(max));
    span.setTag("offset", offset);
    span.setTag("count", count);
    try {
      return super.zrangeByScore(key, min, max, offset, count);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max) {
    Span span = helper.buildSpan("zrangeByScoreWithScores", key);
    span.setTag("min", min);
    span.setTag("max", max);
    try {
      return super.zrangeByScoreWithScores(key, min, max);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<Tuple> zrangeByScoreWithScores(byte[] key, byte[] min, byte[] max) {
    Span span = helper.buildSpan("zrangeByScoreWithScores", key);
    span.setTag("min", Arrays.toString(min));
    span.setTag("max", Arrays.toString(max));
    try {
      return super.zrangeByScoreWithScores(key, min, max);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max, int offset,
      int count) {
    Span span = helper.buildSpan("zrangeByScoreWithScores", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    try {
      return super.zrangeByScoreWithScores(key, min, max, offset, count);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<Tuple> zrangeByScoreWithScores(byte[] key, byte[] min, byte[] max, int offset,
      int count) {
    Span span = helper.buildSpan("zrangeByScoreWithScores", key);
    span.setTag("min", Arrays.toString(min));
    span.setTag("max", Arrays.toString(max));
    span.setTag("offset", offset);
    span.setTag("count", count);
    try {
      return super.zrangeByScoreWithScores(key, min, max, offset, count);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min) {
    Span span = helper.buildSpan("zrevrangeByScore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    try {
      return super.zrevrangeByScore(key, max, min);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<byte[]> zrevrangeByScore(byte[] key, byte[] max, byte[] min) {
    Span span = helper.buildSpan("zrevrangeByScore", key);
    span.setTag("max", Arrays.toString(max));
    span.setTag("min", Arrays.toString(min));
    try {
      return super.zrevrangeByScore(key, max, min);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min, int offset, int count) {
    Span span = helper.buildSpan("zrevrangeByScore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    try {
      return super.zrevrangeByScore(key, max, min, offset, count);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<byte[]> zrevrangeByScore(byte[] key, byte[] max, byte[] min, int offset, int count) {
    Span span = helper.buildSpan("zrevrangeByScore", key);
    span.setTag("min", Arrays.toString(min));
    span.setTag("max", Arrays.toString(max));
    span.setTag("offset", offset);
    span.setTag("count", count);
    try {
      return super.zrevrangeByScore(key, max, min, offset, count);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min) {
    Span span = helper.buildSpan("zrevrangeByScoreWithScores", key);
    span.setTag("min", min);
    span.setTag("max", max);
    try {
      return super.zrevrangeByScoreWithScores(key, max, min);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min, int offset,
      int count) {
    Span span = helper.buildSpan("zrevrangeByScoreWithScores", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    try {
      return super.zrevrangeByScoreWithScores(key, max, min, offset, count);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, byte[] max, byte[] min) {
    Span span = helper.buildSpan("zrevrangeByScoreWithScores", key);
    span.setTag("max", Arrays.toString(max));
    span.setTag("min", Arrays.toString(min));
    try {
      return super.zrevrangeByScoreWithScores(key, max, min);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, byte[] max, byte[] min, int offset,
      int count) {
    Span span = helper.buildSpan("zrevrangeByScoreWithScores", key);
    span.setTag("min", Arrays.toString(min));
    span.setTag("max", Arrays.toString(max));
    span.setTag("offset", offset);
    span.setTag("count", count);
    try {
      return super.zrevrangeByScoreWithScores(key, max, min, offset, count);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zremrangeByRank(byte[] key, long start, long end) {
    Span span = helper.buildSpan("zremrangeByRank", key);
    span.setTag("start", start);
    span.setTag("end", end);
    try {
      return super.zremrangeByRank(key, start, end);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zremrangeByScore(byte[] key, double start, double end) {
    Span span = helper.buildSpan("zremrangeByScore", key);
    span.setTag("start", start);
    span.setTag("end", end);
    try {
      return super.zremrangeByScore(key, start, end);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zremrangeByScore(byte[] key, byte[] start, byte[] end) {
    Span span = helper.buildSpan("zremrangeByScore", key);
    span.setTag("start", Arrays.toString(start));
    span.setTag("end", Arrays.toString(end));
    try {
      return super.zremrangeByScore(key, start, end);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zunionstore(byte[] dstkey, byte[]... sets) {
    Span span = helper.buildSpan("zunionstore");
    span.setTag("dstkey", Arrays.toString(dstkey));
    span.setTag("sets", TracingHelper.toString(sets));
    try {
      return super.zunionstore(dstkey, sets);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zunionstore(byte[] dstkey, ZParams params, byte[]... sets) {
    Span span = helper.buildSpan("zunionstore");
    span.setTag("dstkey", Arrays.toString(dstkey));
    span.setTag("params", TracingHelper.toString(params.getParams()));
    span.setTag("sets", TracingHelper.toString(sets));
    try {
      return super.zunionstore(dstkey, params, sets);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zinterstore(byte[] dstkey, byte[]... sets) {
    Span span = helper.buildSpan("zinterstore");
    span.setTag("dstkey", Arrays.toString(dstkey));
    span.setTag("sets", TracingHelper.toString(sets));
    try {
      return super.zinterstore(dstkey, sets);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zinterstore(byte[] dstkey, ZParams params, byte[]... sets) {
    Span span = helper.buildSpan("zinterstore");
    span.setTag("dstkey", Arrays.toString(dstkey));
    span.setTag("params", TracingHelper.toString(params.getParams()));
    span.setTag("sets", TracingHelper.toString(sets));
    try {
      return super.zinterstore(dstkey, params, sets);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zlexcount(byte[] key, byte[] min, byte[] max) {
    Span span = helper.buildSpan("zlexcount");
    span.setTag("min", Arrays.toString(min));
    span.setTag("max", Arrays.toString(max));
    try {
      return super.zlexcount(key, min, max);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<byte[]> zrangeByLex(byte[] key, byte[] min, byte[] max) {
    Span span = helper.buildSpan("zrangeByLex", key);
    span.setTag("min", Arrays.toString(min));
    span.setTag("max", Arrays.toString(max));
    try {
      return super.zrangeByLex(key, min, max);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<byte[]> zrangeByLex(byte[] key, byte[] min, byte[] max, int offset, int count) {
    Span span = helper.buildSpan("zrangeByLex", key);
    span.setTag("min", Arrays.toString(min));
    span.setTag("max", Arrays.toString(max));
    span.setTag("offset", offset);
    span.setTag("count", count);
    try {
      return super.zrangeByLex(key, min, max, offset, count);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<byte[]> zrevrangeByLex(byte[] key, byte[] max, byte[] min) {
    Span span = helper.buildSpan("zrevrangeByLex", key);
    span.setTag("max", Arrays.toString(max));
    span.setTag("min", Arrays.toString(min));
    try {
      return super.zrevrangeByLex(key, max, min);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<byte[]> zrevrangeByLex(byte[] key, byte[] max, byte[] min, int offset, int count) {
    Span span = helper.buildSpan("zrevrangeByLex", key);
    span.setTag("min", Arrays.toString(min));
    span.setTag("max", Arrays.toString(max));
    span.setTag("offset", offset);
    span.setTag("count", count);
    try {
      return super.zrevrangeByLex(key, max, min, offset, count);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zremrangeByLex(byte[] key, byte[] min, byte[] max) {
    Span span = helper.buildSpan("zremrangeByLex", key);
    span.setTag("min", Arrays.toString(min));
    span.setTag("max", Arrays.toString(max));
    try {
      return super.zremrangeByLex(key, min, max);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String save() {
    Span span = helper.buildSpan("save");
    try {
      return super.save();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String bgsave() {
    Span span = helper.buildSpan("bgsave");
    try {
      return super.bgsave();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String bgrewriteaof() {
    Span span = helper.buildSpan("bgrewriteaof");
    try {
      return super.bgrewriteaof();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long lastsave() {
    Span span = helper.buildSpan("lastsave");
    try {
      return super.lastsave();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String shutdown() {
    Span span = helper.buildSpan("shutdown");
    try {
      return super.shutdown();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String info() {
    Span span = helper.buildSpan("info");
    try {
      return super.info();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String info(String section) {
    Span span = helper.buildSpan("info");
    span.setTag("section", section);
    try {
      return super.info(section);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public void monitor(JedisMonitor jedisMonitor) {
    Span span = helper.buildSpan("monitor");
    try {
      super.monitor(jedisMonitor);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String slaveof(String host, int port) {
    Span span = helper.buildSpan("slaveof");
    span.setTag("host", host);
    span.setTag("port", port);
    try {
      return super.slaveof(host, port);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String slaveofNoOne() {
    Span span = helper.buildSpan("slaveofNoOne");
    try {
      return super.slaveofNoOne();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<byte[]> configGet(byte[] pattern) {
    Span span = helper.buildSpan("configGet");
    span.setTag("pattern", Arrays.toString(pattern));
    try {
      return super.configGet(pattern);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String configResetStat() {
    Span span = helper.buildSpan("configResetStat");
    try {
      return super.configResetStat();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public byte[] configSet(byte[] parameter, byte[] value) {
    Span span = helper.buildSpan("configSet");
    span.setTag("parameter", Arrays.toString(parameter));
    span.setTag("value", Arrays.toString(value));
    try {
      return super.configSet(parameter, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public boolean isConnected() {
    Span span = helper.buildSpan("isConnected");
    try {
      return super.isConnected();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long strlen(byte[] key) {
    Span span = helper.buildSpan("strlen", key);
    try {
      return super.strlen(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public void sync() {
    Span span = helper.buildSpan("sync");
    try {
      super.sync();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long lpushx(byte[] key, byte[]... string) {
    Span span = helper.buildSpan("lpushx", key);
    span.setTag("string", TracingHelper.toString(string));
    try {
      return super.lpushx(key, string);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long persist(byte[] key) {
    Span span = helper.buildSpan("persist", key);
    try {
      return super.persist(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long rpushx(byte[] key, byte[]... string) {
    Span span = helper.buildSpan("rpushx", key);
    span.setTag("string", TracingHelper.toString(string));
    try {
      return super.rpushx(key, string);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public byte[] echo(byte[] string) {
    Span span = helper.buildSpan("echo");
    span.setTag("string", Arrays.toString(string));
    try {
      return super.echo(string);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long linsert(byte[] key, LIST_POSITION where, byte[] pivot, byte[] value) {
    Span span = helper.buildSpan("linsert", key);
    span.setTag("where", where.name());
    span.setTag("pivot", Arrays.toString(pivot));
    span.setTag("value", Arrays.toString(value));
    try {
      return super.linsert(key, where, pivot, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String debug(DebugParams params) {
    Span span = helper.buildSpan("debug");
    span.setTag("params", Arrays.toString(params.getCommand()));
    try {
      return super.debug(params);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Client getClient() {
    Span span = helper.buildSpan("getClient");
    try {
      return super.getClient();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public byte[] brpoplpush(byte[] source, byte[] destination, int timeout) {
    Span span = helper.buildSpan("brpoplpush");
    span.setTag("timeout", timeout);
    span.setTag("source", Arrays.toString(source));
    span.setTag("destination", Arrays.toString(destination));
    try {
      return super.brpoplpush(source, destination, timeout);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Boolean setbit(byte[] key, long offset, boolean value) {
    Span span = helper.buildSpan("setbit", key);
    span.setTag("offset", offset);
    span.setTag("value", value);
    try {
      return super.setbit(key, offset, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Boolean setbit(byte[] key, long offset, byte[] value) {
    Span span = helper.buildSpan("setbit", key);
    span.setTag("offset", offset);
    span.setTag("value", Arrays.toString(value));
    try {
      return super.setbit(key, offset, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Boolean getbit(byte[] key, long offset) {
    Span span = helper.buildSpan("getbit", key);
    span.setTag("offset", offset);
    try {
      return super.getbit(key, offset);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long bitpos(byte[] key, boolean value) {
    Span span = helper.buildSpan("bitpos", key);
    span.setTag("value", value);
    try {
      return super.bitpos(key, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long bitpos(byte[] key, boolean value, BitPosParams params) {
    Span span = helper.buildSpan("bitpos", key);
    span.setTag("value", value);
    span.setTag("params", TracingHelper.toString(params.getParams()));
    try {
      return super.bitpos(key, value, params);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long setrange(byte[] key, long offset, byte[] value) {
    Span span = helper.buildSpan("setrange", key);
    span.setTag("offset", offset);
    span.setTag("value", Arrays.toString(value));
    try {
      return super.setrange(key, offset, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public byte[] getrange(byte[] key, long startOffset, long endOffset) {
    Span span = helper.buildSpan("getrange", key);
    span.setTag("startOffset", startOffset);
    span.setTag("endOffset", endOffset);
    try {
      return super.getrange(key, startOffset, endOffset);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long publish(byte[] channel, byte[] message) {
    Span span = helper.buildSpan("publish");
    span.setTag("channel", Arrays.toString(channel));
    span.setTag("message", Arrays.toString(message));
    try {
      return super.publish(channel, message);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public void subscribe(BinaryJedisPubSub jedisPubSub, byte[]... channels) {
    Span span = helper.buildSpan("subscribe");
    span.setTag("channels", Arrays.toString(channels));
    try {
      super.subscribe(jedisPubSub, channels);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public void psubscribe(BinaryJedisPubSub jedisPubSub, byte[]... patterns) {
    Span span = helper.buildSpan("psubscribe");
    span.setTag("patterns", Arrays.toString(patterns));
    try {
      super.psubscribe(jedisPubSub, patterns);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long getDB() {
    Span span = helper.buildSpan("getDB");
    try {
      return super.getDB();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Object eval(byte[] script, List<byte[]> keys, List<byte[]> args) {
    Span span = helper.buildSpan("eval");
    span.setTag("script", Arrays.toString(script));
    span.setTag("keys", TracingHelper.toString(keys));
    span.setTag("args", TracingHelper.toString(args));
    try {
      return super.eval(script, keys, args);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Object eval(byte[] script, byte[] keyCount, byte[]... params) {
    Span span = helper.buildSpan("eval");
    span.setTag("script", Arrays.toString(script));
    span.setTag("keyCount", Arrays.toString(keyCount));
    span.setTag("params", TracingHelper.toString(params));
    try {
      return super.eval(script, keyCount, params);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Object eval(byte[] script, int keyCount, byte[]... params) {
    Span span = helper.buildSpan("eval");
    span.setTag("script", Arrays.toString(script));
    span.setTag("keyCount", keyCount);
    span.setTag("params", TracingHelper.toString(params));
    try {
      return super.eval(script, keyCount, params);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Object eval(byte[] script) {
    Span span = helper.buildSpan("eval");
    span.setTag("script", Arrays.toString(script));
    try {
      return super.eval(script);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Object evalsha(byte[] sha1) {
    Span span = helper.buildSpan("evalsha");
    span.setTag("sha1", Arrays.toString(sha1));
    try {
      return super.evalsha(sha1);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Object evalsha(byte[] sha1, List<byte[]> keys, List<byte[]> args) {
    Span span = helper.buildSpan("evalsha");
    span.setTag("sha1", Arrays.toString(sha1));
    span.setTag("keys", TracingHelper.toString(keys));
    span.setTag("args", TracingHelper.toString(args));
    try {
      return super.evalsha(sha1, keys, args);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Object evalsha(byte[] sha1, int keyCount, byte[]... params) {
    Span span = helper.buildSpan("evalsha");
    span.setTag("params", TracingHelper.toString(params));
    span.setTag("sha1", Arrays.toString(sha1));
    span.setTag("keyCount", keyCount);
    try {
      return super.evalsha(sha1, keyCount, params);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String scriptFlush() {
    Span span = helper.buildSpan("scriptFlush");
    try {
      return super.scriptFlush();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long scriptExists(byte[] sha1) {
    Span span = helper.buildSpan("scriptExists");
    span.setTag("sha1", Arrays.toString(sha1));
    try {
      return super.scriptExists(sha1);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<Long> scriptExists(byte[]... sha1) {
    Span span = helper.buildSpan("scriptExists");
    span.setTag("sha1", TracingHelper.toString(sha1));
    try {
      return super.scriptExists(sha1);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public byte[] scriptLoad(byte[] script) {
    Span span = helper.buildSpan("scriptLoad");
    span.setTag("script", Arrays.toString(script));
    try {
      return super.scriptLoad(script);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String scriptKill() {
    Span span = helper.buildSpan("scriptKill");
    try {
      return super.scriptKill();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String slowlogReset() {
    Span span = helper.buildSpan("slowlogReset");
    try {
      return super.slowlogReset();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long slowlogLen() {
    Span span = helper.buildSpan("slowlogLen");
    return TracingHelper.doInScope(span, super::slowlogLen);
  }

  @Override
  public List<byte[]> slowlogGetBinary() {
    Span span = helper.buildSpan("slowlogGetBinary");
    return TracingHelper.doInScope(span, super::slowlogGetBinary);
  }

  @Override
  public List<byte[]> slowlogGetBinary(long entries) {
    Span span = helper.buildSpan("slowlogGetBinary");
    span.setTag("entries", entries);
    return TracingHelper.doInScope(span, () -> super.slowlogGetBinary(entries));
  }

  @Override
  public Long objectRefcount(byte[] key) {
    Span span = helper.buildSpan("objectRefcount", key);
    try {
      return super.objectRefcount(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public byte[] objectEncoding(byte[] key) {
    Span span = helper.buildSpan("objectEncoding", key);
    try {
      return super.objectEncoding(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long objectIdletime(byte[] key) {
    Span span = helper.buildSpan("objectIdletime", key);
    try {
      return super.objectIdletime(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long bitcount(byte[] key) {
    Span span = helper.buildSpan("bitcount", key);
    try {
      return super.bitcount(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long bitcount(byte[] key, long start, long end) {
    Span span = helper.buildSpan("bitcount", key);
    span.setTag("start", start);
    span.setTag("end", end);
    try {
      return super.bitcount(key, start, end);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long bitop(BitOP op, byte[] destKey, byte[]... srcKeys) {
    Span span = helper.buildSpan("bitop");
    span.setTag("destKey", Arrays.toString(destKey));
    span.setTag("srcKeys", TracingHelper.toString(srcKeys));
    try {
      return super.bitop(op, destKey, srcKeys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public byte[] dump(byte[] key) {
    Span span = helper.buildSpan("dump", key);
    try {
      return super.dump(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String restore(byte[] key, int ttl, byte[] serializedValue) {
    Span span = helper.buildSpan("restore", key);
    span.setTag("ttl", ttl);
    span.setTag("serializedValue", Arrays.toString(serializedValue));
    try {
      return super.restore(key, ttl, serializedValue);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long pexpire(byte[] key, int milliseconds) {
    Span span = helper.buildSpan("pexpire", key);
    span.setTag("milliseconds", milliseconds);
    try {
      return super.pexpire(key, milliseconds);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long pexpire(byte[] key, long milliseconds) {
    Span span = helper.buildSpan("pexpire", key);
    span.setTag("milliseconds", milliseconds);
    try {
      return super.pexpire(key, milliseconds);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long pexpireAt(byte[] key, long millisecondsTimestamp) {
    Span span = helper.buildSpan("pexpireAt", key);
    span.setTag("millisecondsTimestamp", millisecondsTimestamp);
    try {
      return super.pexpireAt(key, millisecondsTimestamp);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long pttl(byte[] key) {
    Span span = helper.buildSpan("pttl", key);
    try {
      return super.pttl(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String psetex(byte[] key, int milliseconds, byte[] value) {
    Span span = helper.buildSpan("psetex", key);
    span.setTag("milliseconds", milliseconds);
    span.setTag("value", Arrays.toString(value));
    try {
      return super.psetex(key, milliseconds, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String psetex(byte[] key, long milliseconds, byte[] value) {
    Span span = helper.buildSpan("psetex", key);
    span.setTag("milliseconds", milliseconds);
    span.setTag("value", Arrays.toString(value));
    try {
      return super.psetex(key, milliseconds, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String set(byte[] key, byte[] value, byte[] nxxx) {
    Span span = helper.buildSpan("set", key);
    span.setTag("nxxx", Arrays.toString(nxxx));
    span.setTag("value", Arrays.toString(value));
    try {
      return super.set(key, value, nxxx);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String set(byte[] key, byte[] value, byte[] nxxx, byte[] expx, int time) {
    Span span = helper.buildSpan("set", key);
    span.setTag("nxxx", Arrays.toString(nxxx));
    span.setTag("expx", Arrays.toString(expx));
    span.setTag("time", time);
    span.setTag("value", Arrays.toString(value));
    try {
      return super.set(key, value, nxxx, expx, time);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String clientKill(byte[] client) {
    Span span = helper.buildSpan("clientKill");
    span.setTag("client", Arrays.toString(client));
    try {
      return super.clientKill(client);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String clientGetname() {
    Span span = helper.buildSpan("clientGetname");
    try {
      return super.clientGetname();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String clientList() {
    Span span = helper.buildSpan("clientList");
    try {
      return super.clientList();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String clientSetname(byte[] name) {
    Span span = helper.buildSpan("clientSetname");
    span.setTag("name", Arrays.toString(name));
    try {
      return super.clientSetname(name);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<String> time() {
    Span span = helper.buildSpan("time");
    try {
      return super.time();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String migrate(byte[] host, int port, byte[] key, int destinationDb, int timeout) {
    Span span = helper.buildSpan("migrate", key);
    span.setTag("host", Arrays.toString(host));
    span.setTag("destinationDb", destinationDb);
    span.setTag("timeout", timeout);
    span.setTag("port", port);
    try {
      return super.migrate(host, port, key, destinationDb, timeout);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long waitReplicas(int replicas, long timeout) {
    Span span = helper.buildSpan("waitReplicas");
    span.setTag("replicas", replicas);
    span.setTag("timeout", timeout);
    try {
      return super.waitReplicas(replicas, timeout);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long pfadd(byte[] key, byte[]... elements) {
    Span span = helper.buildSpan("pfadd", key);
    span.setTag("elements", TracingHelper.toString(elements));
    try {
      return super.pfadd(key, elements);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public long pfcount(byte[] key) {
    Span span = helper.buildSpan("pfcount", key);
    try {
      return super.pfcount(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String pfmerge(byte[] destkey, byte[]... sourcekeys) {
    Span span = helper.buildSpan("pfmerge");
    span.setTag("destkey", Arrays.toString(destkey));
    span.setTag("sourcekeys", TracingHelper.toString(sourcekeys));
    try {
      return super.pfmerge(destkey, sourcekeys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long pfcount(byte[]... keys) {
    Span span = helper.buildSpan("pfcount");
    span.setTag("keys", TracingHelper.toString(keys));
    try {
      return super.pfcount(keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public ScanResult<byte[]> scan(byte[] cursor) {
    Span span = helper.buildSpan("scan");
    span.setTag("cursor", Arrays.toString(cursor));
    try {
      return super.scan(cursor);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public ScanResult<byte[]> scan(byte[] cursor, ScanParams params) {
    Span span = helper.buildSpan("scan");
    span.setTag("cursor", Arrays.toString(cursor));
    span.setTag("params", TracingHelper.toString(params.getParams()));
    try {
      return super.scan(cursor, params);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public ScanResult<Entry<byte[], byte[]>> hscan(byte[] key, byte[] cursor) {
    Span span = helper.buildSpan("hscan", key);
    span.setTag("cursor", Arrays.toString(cursor));
    try {
      return super.hscan(key, cursor);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public ScanResult<Entry<byte[], byte[]>> hscan(byte[] key, byte[] cursor, ScanParams params) {
    Span span = helper.buildSpan("hscan", key);
    span.setTag("cursor", Arrays.toString(cursor));
    span.setTag("params", TracingHelper.toString(params.getParams()));
    try {
      return super.hscan(key, cursor, params);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public ScanResult<byte[]> sscan(byte[] key, byte[] cursor) {
    Span span = helper.buildSpan("sscan", key);
    span.setTag("cursor", Arrays.toString(cursor));
    try {
      return super.sscan(key, cursor);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public ScanResult<byte[]> sscan(byte[] key, byte[] cursor, ScanParams params) {
    Span span = helper.buildSpan("sscan", key);
    span.setTag("cursor", Arrays.toString(cursor));
    span.setTag("params", TracingHelper.toString(params.getParams()));
    try {
      return super.sscan(key, cursor, params);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public ScanResult<Tuple> zscan(byte[] key, byte[] cursor) {
    Span span = helper.buildSpan("zscan", key);
    span.setTag("cursor", Arrays.toString(cursor));
    try {
      return super.zscan(key, cursor);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public ScanResult<Tuple> zscan(byte[] key, byte[] cursor, ScanParams params) {
    Span span = helper.buildSpan("zscan", key);
    span.setTag("cursor", Arrays.toString(cursor));
    span.setTag("params", TracingHelper.toString(params.getParams()));
    try {
      return super.zscan(key, cursor, params);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long geoadd(byte[] key, double longitude, double latitude, byte[] member) {
    Span span = helper.buildSpan("geoadd", key);
    span.setTag("longitude", longitude);
    span.setTag("latitude", latitude);
    span.setTag("member", Arrays.toString(member));
    try {
      return super.geoadd(key, longitude, latitude, member);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long geoadd(byte[] key, Map<byte[], GeoCoordinate> memberCoordinateMap) {
    Span span = helper.buildSpan("geoadd", key);
    span.setTag("memberCoordinateMap", TracingHelper.toStringMap2(memberCoordinateMap));
    try {
      return super.geoadd(key, memberCoordinateMap);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Double geodist(byte[] key, byte[] member1, byte[] member2) {
    Span span = helper.buildSpan("geodist", key);
    span.setTag("member1", Arrays.toString(member1));
    span.setTag("member2", Arrays.toString(member2));
    try {
      return super.geodist(key, member1, member2);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Double geodist(byte[] key, byte[] member1, byte[] member2, GeoUnit unit) {
    Span span = helper.buildSpan("geodist", key);
    span.setTag("member1", Arrays.toString(member1));
    span.setTag("member2", Arrays.toString(member2));
    span.setTag("unit", unit.name());
    try {
      return super.geodist(key, member1, member2, unit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<byte[]> geohash(byte[] key, byte[]... members) {
    Span span = helper.buildSpan("geohash", key);
    span.setTag("members", Arrays.toString(members));
    try {
      return super.geohash(key, members);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<GeoCoordinate> geopos(byte[] key, byte[]... members) {
    Span span = helper.buildSpan("geopos", key);
    span.setTag("members", Arrays.toString(members));
    try {
      return super.geopos(key, members);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<GeoRadiusResponse> georadius(byte[] key, double longitude, double latitude,
      double radius, GeoUnit unit) {
    Span span = helper.buildSpan("georadius", key);
    span.setTag("longitude", longitude);
    span.setTag("latitude", latitude);
    span.setTag("radius", radius);
    span.setTag("unit", unit.name());
    try {
      return super.georadius(key, longitude, latitude, radius, unit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
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
    try {
      return super.georadius(key, longitude, latitude, radius, unit, param);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<GeoRadiusResponse> georadiusByMember(byte[] key, byte[] member, double radius,
      GeoUnit unit) {
    Span span = helper.buildSpan("georadiusByMember", key);
    span.setTag("member", Arrays.toString(member));
    span.setTag("radius", radius);
    span.setTag("unit", unit.name());
    try {
      return super.georadiusByMember(key, member, radius, unit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<GeoRadiusResponse> georadiusByMember(byte[] key, byte[] member, double radius,
      GeoUnit unit, GeoRadiusParam param) {
    Span span = helper.buildSpan("georadiusByMember", key);
    span.setTag("member", Arrays.toString(member));
    span.setTag("radius", radius);
    span.setTag("unit", unit.name());
    span.setTag("param", TracingHelper.toString(param.getByteParams()));
    try {
      return super.georadiusByMember(key, member, radius, unit, param);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<byte[]> bitfield(byte[] key, byte[]... arguments) {
    Span span = helper.buildSpan("bitfield");
    span.setTag("arguments", TracingHelper.toString(arguments));
    try {
      return super.bitfield(key, arguments);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public void setDataSource(Pool<Jedis> jedisPool) {
    Span span = helper.buildSpan("setDataSource");
    try {
      super.setDataSource(jedisPool);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long pfadd(String key, String... elements) {
    Span span = helper.buildSpan("pfadd");
    span.setTag("elements", Arrays.toString(elements));
    try {
      return super.pfadd(key, elements);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public long pfcount(String key) {
    Span span = helper.buildSpan("pfcount", key);
    try {
      return super.pfcount(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public long pfcount(String... keys) {
    Span span = helper.buildSpan("pfcount", keys);
    try {
      return super.pfcount(keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String pfmerge(String destkey, String... sourcekeys) {
    Span span = helper.buildSpan("pfmerge");
    span.setTag("destkey", destkey);
    span.setTag("sourcekeys", Arrays.toString(sourcekeys));
    try {
      return super.pfmerge(destkey, sourcekeys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<String> blpop(int timeout, String key) {
    Span span = helper.buildSpan("blpop");
    span.setTag("timeout", timeout);
    try {
      return super.blpop(timeout, key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<String> brpop(int timeout, String key) {
    Span span = helper.buildSpan("brpop");
    span.setTag("timeout", timeout);
    try {
      return super.brpop(timeout, key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long geoadd(String key, double longitude, double latitude, String member) {
    Span span = helper.buildSpan("geoadd");
    span.setTag("longitude", longitude);
    span.setTag("latitude", latitude);
    span.setTag("member", member);
    try {
      return super.geoadd(key, longitude, latitude, member);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long geoadd(String key, Map<String, GeoCoordinate> memberCoordinateMap) {
    Span span = helper.buildSpan("geoadd");
    span.setTag("memberCoordinateMap", TracingHelper.toString(memberCoordinateMap));
    try {
      return super.geoadd(key, memberCoordinateMap);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Double geodist(String key, String member1, String member2) {
    Span span = helper.buildSpan("geodist", key);
    span.setTag("member1", member1);
    span.setTag("member2", member2);
    try {
      return super.geodist(key, member1, member2);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Double geodist(String key, String member1, String member2, GeoUnit unit) {
    Span span = helper.buildSpan("geodist", key);
    span.setTag("member1", member1);
    span.setTag("member2", member2);
    span.setTag("unit", unit.name());
    try {
      return super.geodist(key, member1, member2, unit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<String> geohash(String key, String... members) {
    Span span = helper.buildSpan("geohash", key);
    span.setTag("members", Arrays.toString(members));
    try {
      return super.geohash(key, members);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<GeoCoordinate> geopos(String key, String... members) {
    Span span = helper.buildSpan("geopos", key);
    span.setTag("members", Arrays.toString(members));
    try {
      return super.geopos(key, members);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<GeoRadiusResponse> georadius(String key, double longitude, double latitude,
      double radius, GeoUnit unit) {
    Span span = helper.buildSpan("georadius", key);
    span.setTag("longitude", longitude);
    span.setTag("latitude", latitude);
    span.setTag("radius", radius);
    span.setTag("unit", unit.name());
    try {
      return super.georadius(key, longitude, latitude, radius, unit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
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
    try {
      return super.georadius(key, longitude, latitude, radius, unit, param);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<GeoRadiusResponse> georadiusByMember(String key, String member, double radius,
      GeoUnit unit) {
    Span span = helper.buildSpan("georadiusByMember", key);
    span.setTag("member", member);
    span.setTag("radius", radius);
    span.setTag("unit", unit.name());
    try {
      return super.georadiusByMember(key, member, radius, unit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<GeoRadiusResponse> georadiusByMember(String key, String member, double radius,
      GeoUnit unit, GeoRadiusParam param) {
    Span span = helper.buildSpan("georadiusByMember", key);
    span.setTag("member", member);
    span.setTag("radius", radius);
    span.setTag("unit", unit.name());
    span.setTag("param", TracingHelper.toString(param.getByteParams()));
    try {
      return super.georadiusByMember(key, member, radius, unit, param);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<Long> bitfield(String key, String... arguments) {
    Span span = helper.buildSpan("bitfield", key);
    span.setTag("arguments", Arrays.toString(arguments));
    try {
      return super.bitfield(key, arguments);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }
}
