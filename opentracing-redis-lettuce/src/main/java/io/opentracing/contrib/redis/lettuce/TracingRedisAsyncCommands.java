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
package io.opentracing.contrib.redis.lettuce;

import static io.opentracing.contrib.redis.common.TracingHelper.nullable;
import static io.opentracing.contrib.redis.common.TracingHelper.onError;

import io.lettuce.core.BitFieldArgs;
import io.lettuce.core.GeoArgs;
import io.lettuce.core.GeoArgs.Unit;
import io.lettuce.core.GeoCoordinates;
import io.lettuce.core.GeoRadiusStoreArgs;
import io.lettuce.core.GeoWithin;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.KeyValue;
import io.lettuce.core.KillArgs;
import io.lettuce.core.Limit;
import io.lettuce.core.MapScanCursor;
import io.lettuce.core.MigrateArgs;
import io.lettuce.core.Range;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ScoredValueScanCursor;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.SetArgs;
import io.lettuce.core.SortArgs;
import io.lettuce.core.StreamScanCursor;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.Value;
import io.lettuce.core.ValueScanCursor;
import io.lettuce.core.ZAddArgs;
import io.lettuce.core.ZStoreArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.output.KeyStreamingChannel;
import io.lettuce.core.output.KeyValueStreamingChannel;
import io.lettuce.core.output.ScoredValueStreamingChannel;
import io.lettuce.core.output.ValueStreamingChannel;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;
import io.lettuce.core.protocol.ProtocolKeyword;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.contrib.redis.common.TracingHelper;
import io.opentracing.contrib.redis.common.RedisSpanNameProvider;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class TracingRedisAsyncCommands<K, V> implements RedisAsyncCommands<K, V> {

  private final RedisAsyncCommands<K, V> commands;
  private final Tracer tracer;
  private final boolean traceWithActiveSpanOnly;
  private final TracingHelper helper;
  private Function<String, String> redisSpanNameProvider;

  /**
   * @param commands redis async commands
   * @param tracer tracer
   * @param traceWithActiveSpanOnly if <code>true</code> then create new spans only if there is
   * active span
   */
  public TracingRedisAsyncCommands(RedisAsyncCommands<K, V> commands, Tracer tracer,
      boolean traceWithActiveSpanOnly) {
    this.commands = commands;
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    this.redisSpanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
    this.helper = new TracingHelper(tracer, traceWithActiveSpanOnly, this.redisSpanNameProvider);
  }

  @Override
  public String auth(String password) {
    Span span = helper.buildSpan("auth");
    try {
      return commands.auth(password);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String select(int db) {
    Span span = helper.buildSpan("select");
    span.setTag("db", db);
    try {
      return commands.select(db);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public RedisFuture<String> swapdb(int db1, int db2) {
    Span span = helper.buildSpan("swapdb");
    span.setTag("db1", db1);
    span.setTag("db2", db2);
    try {
      return setCompleteAction(commands.swapdb(db1, db2), span);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public StatefulRedisConnection<K, V> getStatefulConnection() {
    return new TracingStatefulRedisConnection<>(commands.getStatefulConnection(), tracer,
        traceWithActiveSpanOnly);
  }

  @Override
  public RedisFuture<Long> hdel(K key, K... fields) {
    Span span = helper.buildSpan("hdel", key);
    return setCompleteAction(commands.hdel(key, fields), span);
  }

  @Override
  public RedisFuture<Boolean> hexists(K key, K field) {
    Span span = helper.buildSpan("hexists", key);
    return setCompleteAction(commands.hexists(key, field), span);
  }

  @Override
  public RedisFuture<V> hget(K key, K field) {
    Span span = helper.buildSpan("hget", key);
    return setCompleteAction(commands.hget(key, field), span);
  }

  @Override
  public RedisFuture<Long> hincrby(K key, K field, long amount) {
    Span span = helper.buildSpan("hincrby", key);
    span.setTag("amount", amount);
    return setCompleteAction(commands.hincrby(key, field, amount), span);
  }

  @Override
  public RedisFuture<Double> hincrbyfloat(K key, K field, double amount) {
    Span span = helper.buildSpan("hincrbyfloat", key);
    span.setTag("amount", amount);
    return setCompleteAction(commands.hincrbyfloat(key, field, amount), span);
  }

  @Override
  public RedisFuture<Map<K, V>> hgetall(K key) {
    Span span = helper.buildSpan("hgetall", key);
    return setCompleteAction(commands.hgetall(key), span);
  }

  @Override
  public RedisFuture<Long> hgetall(
      KeyValueStreamingChannel<K, V> channel, K key) {
    Span span = helper.buildSpan("hgetall", key);
    return setCompleteAction(commands.hgetall(channel, key), span);
  }

  @Override
  public RedisFuture<List<K>> hkeys(K key) {
    Span span = helper.buildSpan("hkeys", key);
    return setCompleteAction(commands.hkeys(key), span);
  }

  @Override
  public RedisFuture<Long> hkeys(
      KeyStreamingChannel<K> channel, K key) {
    Span span = helper.buildSpan("hkeys", key);
    return setCompleteAction(commands.hkeys(channel, key), span);
  }

  @Override
  public RedisFuture<Long> hlen(K key) {
    Span span = helper.buildSpan("hlen", key);
    return setCompleteAction(commands.hlen(key), span);
  }

  @Override
  public RedisFuture<List<KeyValue<K, V>>> hmget(K key,
      K... fields) {
    Span span = helper.buildSpan("hmget", key);
    return setCompleteAction(commands.hmget(key, fields), span);
  }

  @Override
  public RedisFuture<Long> hmget(
      KeyValueStreamingChannel<K, V> channel, K key, K... fields) {
    Span span = helper.buildSpan("hmget", key);
    return setCompleteAction(commands.hmget(channel, key, fields), span);
  }

  @Override
  public RedisFuture<String> hmset(K key, Map<K, V> map) {
    Span span = helper.buildSpan("hmset", key);
    return setCompleteAction(commands.hmset(key, map), span);
  }

  @Override
  public RedisFuture<MapScanCursor<K, V>> hscan(K key) {
    Span span = helper.buildSpan("hscan", key);
    return setCompleteAction(commands.hscan(key), span);
  }

  @Override
  public RedisFuture<MapScanCursor<K, V>> hscan(K key,
      ScanArgs scanArgs) {
    Span span = helper.buildSpan("hscan", key);
    return setCompleteAction(commands.hscan(key, scanArgs), span);
  }

  @Override
  public RedisFuture<MapScanCursor<K, V>> hscan(K key,
      ScanCursor scanCursor, ScanArgs scanArgs) {
    Span span = helper.buildSpan("hscan", key);
    return setCompleteAction(commands.hscan(key, scanCursor, scanArgs), span);
  }

  @Override
  public RedisFuture<MapScanCursor<K, V>> hscan(K key,
      ScanCursor scanCursor) {
    Span span = helper.buildSpan("hscan", key);
    return setCompleteAction(commands.hscan(key, scanCursor), span);
  }

  @Override
  public RedisFuture<StreamScanCursor> hscan(
      KeyValueStreamingChannel<K, V> channel, K key) {
    Span span = helper.buildSpan("hscan", key);
    return setCompleteAction(commands.hscan(channel, key), span);
  }

  @Override
  public RedisFuture<StreamScanCursor> hscan(
      KeyValueStreamingChannel<K, V> channel, K key,
      ScanArgs scanArgs) {
    Span span = helper.buildSpan("hscan", key);
    return setCompleteAction(commands.hscan(channel, key, scanArgs), span);
  }

  @Override
  public RedisFuture<StreamScanCursor> hscan(
      KeyValueStreamingChannel<K, V> channel, K key,
      ScanCursor scanCursor, ScanArgs scanArgs) {
    Span span = helper.buildSpan("hscan", key);
    return setCompleteAction(commands.hscan(channel, key, scanCursor, scanArgs), span);
  }

  @Override
  public RedisFuture<StreamScanCursor> hscan(
      KeyValueStreamingChannel<K, V> channel, K key,
      ScanCursor scanCursor) {
    Span span = helper.buildSpan("hscan", key);
    return setCompleteAction(commands.hscan(channel, key, scanCursor), span);
  }

  @Override
  public RedisFuture<Boolean> hset(K key, K field, V value) {
    Span span = helper.buildSpan("hset", key);
    return setCompleteAction(commands.hset(key, field, value), span);
  }

  @Override
  public RedisFuture<Boolean> hsetnx(K key, K field, V value) {
    Span span = helper.buildSpan("hsetnx", key);
    return setCompleteAction(commands.hsetnx(key, field, value), span);
  }

  @Override
  public RedisFuture<Long> hstrlen(K key, K field) {
    Span span = helper.buildSpan("hstrlen", key);
    return setCompleteAction(commands.hstrlen(key, field), span);
  }

  @Override
  public RedisFuture<List<V>> hvals(K key) {
    Span span = helper.buildSpan("hvals", key);
    return setCompleteAction(commands.hvals(key), span);
  }

  @Override
  public RedisFuture<Long> hvals(
      ValueStreamingChannel<V> channel, K key) {
    Span span = helper.buildSpan("hvals", key);
    return setCompleteAction(commands.hvals(channel, key), span);
  }

  @Override
  public RedisFuture<Long> del(K... keys) {
    Span span = helper.buildSpan("del", keys);
    return setCompleteAction(commands.del(keys), span);
  }

  @Override
  public RedisFuture<Long> unlink(K... keys) {
    Span span = helper.buildSpan("unlink", keys);
    return setCompleteAction(commands.unlink(keys), span);
  }

  @Override
  public RedisFuture<byte[]> dump(K key) {
    Span span = helper.buildSpan("dump", key);
    return setCompleteAction(commands.dump(key), span);
  }

  @Override
  public RedisFuture<Long> exists(K... keys) {
    Span span = helper.buildSpan("exists", keys);
    return setCompleteAction(commands.exists(keys), span);
  }

  @Override
  public RedisFuture<Boolean> expire(K key, long seconds) {
    Span span = helper.buildSpan("expire", key);
    span.setTag("seconds", seconds);
    return setCompleteAction(commands.expire(key, seconds), span);
  }

  @Override
  public RedisFuture<Boolean> expireat(K key, Date timestamp) {
    Span span = helper.buildSpan("expireat", key);
    span.setTag("timestamp", nullable(timestamp));
    return setCompleteAction(commands.expireat(key, timestamp), span);
  }

  @Override
  public RedisFuture<Boolean> expireat(K key, long timestamp) {
    Span span = helper.buildSpan("expireat", key);
    span.setTag("timestamp", timestamp);
    return setCompleteAction(commands.expireat(key, timestamp), span);
  }

  @Override
  public RedisFuture<List<K>> keys(K pattern) {
    Span span = helper.buildSpan("keys");
    return setCompleteAction(commands.keys(pattern), span);
  }

  @Override
  public RedisFuture<Long> keys(KeyStreamingChannel<K> channel,
      K pattern) {
    Span span = helper.buildSpan("keys");
    return setCompleteAction(commands.keys(channel, pattern), span);
  }

  @Override
  public RedisFuture<String> migrate(String host, int port, K key, int db,
      long timeout) {
    Span span = helper.buildSpan("migrate", key);
    span.setTag("host", host);
    span.setTag("port", port);
    span.setTag("db", db);
    span.setTag("timeout", timeout);
    return setCompleteAction(commands.migrate(host, port, key, db, timeout), span);
  }

  @Override
  public RedisFuture<String> migrate(String host, int port, int db, long timeout,
      MigrateArgs<K> migrateArgs) {
    Span span = helper.buildSpan("migrate");
    span.setTag("host", host);
    span.setTag("port", port);
    span.setTag("db", db);
    span.setTag("timeout", timeout);
    return setCompleteAction(commands.migrate(host, port, db, timeout, migrateArgs), span);
  }

  @Override
  public RedisFuture<Boolean> move(K key, int db) {
    Span span = helper.buildSpan("move", key);
    span.setTag("db", db);
    return setCompleteAction(commands.move(key, db), span);
  }

  @Override
  public RedisFuture<String> objectEncoding(K key) {
    Span span = helper.buildSpan("objectEncoding", key);
    return setCompleteAction(commands.objectEncoding(key), span);
  }

  @Override
  public RedisFuture<Long> objectIdletime(K key) {
    Span span = helper.buildSpan("objectIdletime", key);
    return setCompleteAction(commands.objectIdletime(key), span);
  }

  @Override
  public RedisFuture<Long> objectRefcount(K key) {
    Span span = helper.buildSpan("objectRefcount", key);
    return setCompleteAction(commands.objectRefcount(key), span);
  }

  @Override
  public RedisFuture<Boolean> persist(K key) {
    Span span = helper.buildSpan("persist", key);
    return setCompleteAction(commands.persist(key), span);
  }

  @Override
  public RedisFuture<Boolean> pexpire(K key, long milliseconds) {
    Span span = helper.buildSpan("pexpire", key);
    span.setTag("milliseconds", milliseconds);
    return setCompleteAction(commands.pexpire(key, milliseconds), span);
  }

  @Override
  public RedisFuture<Boolean> pexpireat(K key, Date timestamp) {
    Span span = helper.buildSpan("pexpireat", key);
    span.setTag("timestamp", nullable(timestamp));
    return setCompleteAction(commands.pexpireat(key, timestamp), span);
  }

  @Override
  public RedisFuture<Boolean> pexpireat(K key, long timestamp) {
    Span span = helper.buildSpan("pexpireat", key);
    span.setTag("timestamp", timestamp);
    return setCompleteAction(commands.pexpireat(key, timestamp), span);
  }

  @Override
  public RedisFuture<Long> pttl(K key) {
    Span span = helper.buildSpan("pttl", key);
    return setCompleteAction(commands.pttl(key), span);
  }

  @Override
  public RedisFuture<V> randomkey() {
    Span span = helper.buildSpan("randomkey");
    return setCompleteAction(commands.randomkey(), span);
  }

  @Override
  public RedisFuture<String> rename(K key, K newKey) {
    Span span = helper.buildSpan("rename", key);
    return setCompleteAction(commands.rename(key, newKey), span);
  }

  @Override
  public RedisFuture<Boolean> renamenx(K key, K newKey) {
    Span span = helper.buildSpan("renamenx", key);
    return setCompleteAction(commands.renamenx(key, newKey), span);
  }

  @Override
  public RedisFuture<String> restore(K key, long ttl, byte[] value) {
    Span span = helper.buildSpan("restore", key);
    span.setTag("ttl", ttl);
    return setCompleteAction(commands.restore(key, ttl, value), span);
  }

  @Override
  public RedisFuture<List<V>> sort(K key) {
    Span span = helper.buildSpan("sort", key);
    return setCompleteAction(commands.sort(key), span);
  }

  @Override
  public RedisFuture<Long> sort(
      ValueStreamingChannel<V> channel, K key) {
    Span span = helper.buildSpan("sort", key);
    return setCompleteAction(commands.sort(channel, key), span);
  }

  @Override
  public RedisFuture<List<V>> sort(K key, SortArgs sortArgs) {
    Span span = helper.buildSpan("sort", key);
    return setCompleteAction(commands.sort(key, sortArgs), span);
  }

  @Override
  public RedisFuture<Long> sort(
      ValueStreamingChannel<V> channel, K key,
      SortArgs sortArgs) {
    Span span = helper.buildSpan("sort", key);
    return setCompleteAction(commands.sort(channel, key, sortArgs), span);
  }

  @Override
  public RedisFuture<Long> sortStore(K key, SortArgs sortArgs,
      K destination) {
    Span span = helper.buildSpan("sortStore", key);
    return setCompleteAction(commands.sortStore(key, sortArgs, destination), span);
  }

  @Override
  public RedisFuture<Long> touch(K... keys) {
    Span span = helper.buildSpan("touch", keys);
    return setCompleteAction(commands.touch(keys), span);
  }

  @Override
  public RedisFuture<Long> ttl(K key) {
    Span span = helper.buildSpan("ttl", key);
    return setCompleteAction(commands.ttl(key), span);
  }

  @Override
  public RedisFuture<String> type(K key) {
    Span span = helper.buildSpan("type", key);
    return setCompleteAction(commands.type(key), span);
  }

  @Override
  public RedisFuture<KeyScanCursor<K>> scan() {
    Span span = helper.buildSpan("scan");
    return setCompleteAction(commands.scan(), span);
  }

  @Override
  public RedisFuture<KeyScanCursor<K>> scan(
      ScanArgs scanArgs) {
    Span span = helper.buildSpan("scan");
    return setCompleteAction(commands.scan(scanArgs), span);
  }

  @Override
  public RedisFuture<KeyScanCursor<K>> scan(
      ScanCursor scanCursor, ScanArgs scanArgs) {
    Span span = helper.buildSpan("scan");
    return setCompleteAction(commands.scan(scanCursor, scanArgs), span);
  }

  @Override
  public RedisFuture<KeyScanCursor<K>> scan(
      ScanCursor scanCursor) {
    Span span = helper.buildSpan("scan");
    return setCompleteAction(commands.scan(scanCursor), span);
  }

  @Override
  public RedisFuture<StreamScanCursor> scan(
      KeyStreamingChannel<K> channel) {
    Span span = helper.buildSpan("scan");
    return setCompleteAction(commands.scan(channel), span);
  }

  @Override
  public RedisFuture<StreamScanCursor> scan(
      KeyStreamingChannel<K> channel, ScanArgs scanArgs) {
    Span span = helper.buildSpan("scan");
    return setCompleteAction(commands.scan(channel, scanArgs), span);
  }

  @Override
  public RedisFuture<StreamScanCursor> scan(
      KeyStreamingChannel<K> channel, ScanCursor scanCursor,
      ScanArgs scanArgs) {
    Span span = helper.buildSpan("scan");
    return setCompleteAction(commands.scan(channel, scanCursor, scanArgs), span);
  }

  @Override
  public RedisFuture<StreamScanCursor> scan(
      KeyStreamingChannel<K> channel, ScanCursor scanCursor) {
    Span span = helper.buildSpan("scan");
    return setCompleteAction(commands.scan(channel, scanCursor), span);
  }

  @Override
  public RedisFuture<Long> append(K key, V value) {
    Span span = helper.buildSpan("append", key);
    return setCompleteAction(commands.append(key, value), span);
  }

  @Override
  public RedisFuture<Long> bitcount(K key) {
    Span span = helper.buildSpan("bitcount", key);
    return setCompleteAction(commands.bitcount(key), span);
  }

  @Override
  public RedisFuture<Long> bitcount(K key, long start, long end) {
    Span span = helper.buildSpan("bitcount", key);
    span.setTag("start", start);
    span.setTag("end", end);
    return setCompleteAction(commands.bitcount(key, start, end), span);
  }

  @Override
  public RedisFuture<List<Long>> bitfield(K key,
      BitFieldArgs bitFieldArgs) {
    Span span = helper.buildSpan("bitfield", key);
    return setCompleteAction(commands.bitfield(key, bitFieldArgs), span);
  }

  @Override
  public RedisFuture<Long> bitpos(K key, boolean state) {
    Span span = helper.buildSpan("bitpos", key);
    span.setTag("state", state);
    return setCompleteAction(commands.bitpos(key, state), span);
  }

  @Override
  public RedisFuture<Long> bitpos(K key, boolean state, long start) {
    Span span = helper.buildSpan("bitpos", key);
    span.setTag("state", state);
    span.setTag("start", start);
    return setCompleteAction(commands.bitpos(key, state, start), span);
  }

  @Override
  public RedisFuture<Long> bitpos(K key, boolean state, long start, long end) {
    Span span = helper.buildSpan("bitpos", key);
    span.setTag("state", state);
    span.setTag("start", start);
    span.setTag("end", end);
    return setCompleteAction(commands.bitpos(key, state, start, end), span);
  }

  @Override
  public RedisFuture<Long> bitopAnd(K destination, K... keys) {
    Span span = helper.buildSpan("bitopAnd", keys);
    return setCompleteAction(commands.bitopAnd(destination, keys), span);
  }

  @Override
  public RedisFuture<Long> bitopNot(K destination, K source) {
    Span span = helper.buildSpan("bitopNot");
    return setCompleteAction(commands.bitopNot(destination, source), span);
  }

  @Override
  public RedisFuture<Long> bitopOr(K destination, K... keys) {
    Span span = helper.buildSpan("bitopOr", keys);
    return setCompleteAction(commands.bitopOr(destination, keys), span);
  }

  @Override
  public RedisFuture<Long> bitopXor(K destination, K... keys) {
    Span span = helper.buildSpan("bitopXor", keys);
    return setCompleteAction(commands.bitopXor(destination, keys), span);
  }

  @Override
  public RedisFuture<Long> decr(K key) {
    Span span = helper.buildSpan("decr", key);
    return setCompleteAction(commands.decr(key), span);
  }

  @Override
  public RedisFuture<Long> decrby(K key, long amount) {
    Span span = helper.buildSpan("decrby", key);
    span.setTag("amount", amount);
    return setCompleteAction(commands.decrby(key, amount), span);
  }

  @Override
  public RedisFuture<V> get(K key) {
    Span span = helper.buildSpan("get", key);
    return setCompleteAction(setCompleteAction(commands.get(key), span), span);
  }

  @Override
  public RedisFuture<Long> getbit(K key, long offset) {
    Span span = helper.buildSpan("getbit", key);
    span.setTag("offset", offset);
    return setCompleteAction(commands.getbit(key, offset), span);
  }

  @Override
  public RedisFuture<V> getrange(K key, long start, long end) {
    Span span = helper.buildSpan("getrange", key);
    span.setTag("start", start);
    span.setTag("end", end);
    return setCompleteAction(commands.getrange(key, start, end), span);
  }

  @Override
  public RedisFuture<V> getset(K key, V value) {
    Span span = helper.buildSpan("getset", key);
    return setCompleteAction(commands.getset(key, value), span);
  }

  @Override
  public RedisFuture<Long> incr(K key) {
    Span span = helper.buildSpan("incr", key);
    return setCompleteAction(commands.incr(key), span);
  }

  @Override
  public RedisFuture<Long> incrby(K key, long amount) {
    Span span = helper.buildSpan("incrby", key);
    span.setTag("amount", amount);
    return setCompleteAction(commands.incrby(key, amount), span);
  }

  @Override
  public RedisFuture<Double> incrbyfloat(K key, double amount) {
    Span span = helper.buildSpan("incrbyfloat", key);
    span.setTag("amount", amount);
    return setCompleteAction(commands.incrbyfloat(key, amount), span);
  }

  @Override
  public RedisFuture<List<KeyValue<K, V>>> mget(K... keys) {
    Span span = helper.buildSpan("mget", keys);
    return setCompleteAction(commands.mget(keys), span);
  }

  @Override
  public RedisFuture<Long> mget(
      KeyValueStreamingChannel<K, V> channel, K... keys) {
    Span span = helper.buildSpan("mget", keys);
    return setCompleteAction(commands.mget(channel, keys), span);
  }

  @Override
  public RedisFuture<String> mset(Map<K, V> map) {
    Span span = helper.buildSpan("mset");
    return setCompleteAction(commands.mset(map), span);
  }

  @Override
  public RedisFuture<Boolean> msetnx(Map<K, V> map) {
    Span span = helper.buildSpan("msetnx");
    return setCompleteAction(commands.msetnx(map), span);
  }

  @Override
  public RedisFuture<String> set(K key, V value) {
    Span span = helper.buildSpan("set", key);
    return setCompleteAction(setCompleteAction(commands.set(key, value), span), span);
  }

  @Override
  public RedisFuture<String> set(K key, V value, SetArgs setArgs) {
    Span span = helper.buildSpan("set", key);
    return setCompleteAction(commands.set(key, value, setArgs), span);
  }

  @Override
  public RedisFuture<Long> setbit(K key, long offset, int value) {
    Span span = helper.buildSpan("setbit", key);
    span.setTag("offset", offset);
    span.setTag("value", value);
    return setCompleteAction(commands.setbit(key, offset, value), span);
  }

  @Override
  public RedisFuture<String> setex(K key, long seconds, V value) {
    Span span = helper.buildSpan("setex", key);
    span.setTag("seconds", seconds);
    return setCompleteAction(commands.setex(key, seconds, value), span);
  }

  @Override
  public RedisFuture<String> psetex(K key, long milliseconds, V value) {
    Span span = helper.buildSpan("psetex", key);
    span.setTag("milliseconds", milliseconds);
    return setCompleteAction(commands.psetex(key, milliseconds, value), span);
  }

  @Override
  public RedisFuture<Boolean> setnx(K key, V value) {
    Span span = helper.buildSpan("setnx", key);
    return setCompleteAction(commands.setnx(key, value), span);
  }

  @Override
  public RedisFuture<Long> setrange(K key, long offset, V value) {
    Span span = helper.buildSpan("setrange", key);
    span.setTag("offset", offset);
    return setCompleteAction(commands.setrange(key, offset, value), span);
  }

  @Override
  public RedisFuture<Long> strlen(K key) {
    Span span = helper.buildSpan("strlen", key);
    return setCompleteAction(commands.strlen(key), span);
  }

  @Override
  public RedisFuture<KeyValue<K, V>> blpop(long timeout, K... keys) {
    Span span = helper.buildSpan("blpop", keys);
    span.setTag("timeout", timeout);
    return setCompleteAction(commands.blpop(timeout, keys), span);
  }

  @Override
  public RedisFuture<KeyValue<K, V>> brpop(long timeout, K... keys) {
    Span span = helper.buildSpan("brpop", keys);
    span.setTag("timeout", timeout);
    return setCompleteAction(commands.brpop(timeout, keys), span);
  }

  @Override
  public RedisFuture<V> brpoplpush(long timeout, K source, K destination) {
    Span span = helper.buildSpan("brpoplpush");
    span.setTag("timeout", timeout);
    return setCompleteAction(commands.brpoplpush(timeout, source, destination), span);
  }

  @Override
  public RedisFuture<V> lindex(K key, long index) {
    Span span = helper.buildSpan("lindex", key);
    span.setTag("index", index);
    return setCompleteAction(commands.lindex(key, index), span);
  }

  @Override
  public RedisFuture<Long> linsert(K key, boolean before, V pivot, V value) {
    Span span = helper.buildSpan("linsert", key);
    span.setTag("before", before);
    return setCompleteAction(commands.linsert(key, before, pivot, value), span);
  }

  @Override
  public RedisFuture<Long> llen(K key) {
    Span span = helper.buildSpan("llen", key);
    return setCompleteAction(commands.llen(key), span);
  }

  @Override
  public RedisFuture<V> lpop(K key) {
    Span span = helper.buildSpan("lpop", key);
    return setCompleteAction(commands.lpop(key), span);
  }

  @Override
  public RedisFuture<Long> lpush(K key, V... values) {
    Span span = helper.buildSpan("lpush", key);
    return setCompleteAction(commands.lpush(key, values), span);
  }

  @Override
  public RedisFuture<Long> lpushx(K key, V... values) {
    Span span = helper.buildSpan("lpushx", key);
    return setCompleteAction(commands.lpushx(key, values), span);
  }

  @Override
  public RedisFuture<List<V>> lrange(K key, long start, long stop) {
    Span span = helper.buildSpan("lrange", key);
    span.setTag("start", start);
    span.setTag("stop", stop);
    return setCompleteAction(commands.lrange(key, start, stop), span);
  }

  @Override
  public RedisFuture<Long> lrange(
      ValueStreamingChannel<V> channel, K key, long start, long stop) {
    Span span = helper.buildSpan("lrange", key);
    span.setTag("start", start);
    span.setTag("stop", stop);
    return setCompleteAction(commands.lrange(channel, key, start, stop), span);
  }

  @Override
  public RedisFuture<Long> lrem(K key, long count, V value) {
    Span span = helper.buildSpan("lrem", key);
    span.setTag("count", count);
    return setCompleteAction(commands.lrem(key, count, value), span);
  }

  @Override
  public RedisFuture<String> lset(K key, long index, V value) {
    Span span = helper.buildSpan("lset", key);
    span.setTag("index", index);
    return setCompleteAction(commands.lset(key, index, value), span);
  }

  @Override
  public RedisFuture<String> ltrim(K key, long start, long stop) {
    Span span = helper.buildSpan("ltrim", key);
    span.setTag("start", start);
    span.setTag("stop", stop);
    return setCompleteAction(commands.ltrim(key, start, stop), span);
  }

  @Override
  public RedisFuture<V> rpop(K key) {
    Span span = helper.buildSpan("rpop", key);
    return setCompleteAction(commands.rpop(key), span);
  }

  @Override
  public RedisFuture<V> rpoplpush(K source, K destination) {
    Span span = helper.buildSpan("rpoplpush");
    return setCompleteAction(commands.rpoplpush(source, destination), span);
  }

  @Override
  public RedisFuture<Long> rpush(K key, V... values) {
    Span span = helper.buildSpan("rpush", key);
    return setCompleteAction(commands.rpush(key, values), span);
  }

  @Override
  public RedisFuture<Long> rpushx(K key, V... values) {
    Span span = helper.buildSpan("rpushx", key);
    return setCompleteAction(commands.rpushx(key, values), span);
  }

  @Override
  public RedisFuture<Long> sadd(K key, V... members) {
    Span span = helper.buildSpan("sadd", key);
    return setCompleteAction(commands.sadd(key, members), span);
  }

  @Override
  public RedisFuture<Long> scard(K key) {
    Span span = helper.buildSpan("scard", key);
    return setCompleteAction(commands.scard(key), span);
  }

  @Override
  public RedisFuture<Set<V>> sdiff(K... keys) {
    Span span = helper.buildSpan("sdiff", keys);
    return setCompleteAction(commands.sdiff(keys), span);
  }

  @Override
  public RedisFuture<Long> sdiff(
      ValueStreamingChannel<V> channel, K... keys) {
    Span span = helper.buildSpan("sdiff", keys);
    return setCompleteAction(commands.sdiff(channel, keys), span);
  }

  @Override
  public RedisFuture<Long> sdiffstore(K destination, K... keys) {
    Span span = helper.buildSpan("sdiffstore", keys);
    return setCompleteAction(commands.sdiffstore(destination, keys), span);
  }

  @Override
  public RedisFuture<Set<V>> sinter(K... keys) {
    Span span = helper.buildSpan("sinter", keys);
    return setCompleteAction(commands.sinter(keys), span);
  }

  @Override
  public RedisFuture<Long> sinter(
      ValueStreamingChannel<V> channel, K... keys) {
    Span span = helper.buildSpan("sinter", keys);
    return setCompleteAction(commands.sinter(channel, keys), span);
  }

  @Override
  public RedisFuture<Long> sinterstore(K destination, K... keys) {
    Span span = helper.buildSpan("sinterstore", keys);
    return setCompleteAction(commands.sinterstore(destination, keys), span);
  }

  @Override
  public RedisFuture<Boolean> sismember(K key, V member) {
    Span span = helper.buildSpan("sismember", key);
    return setCompleteAction(commands.sismember(key, member), span);
  }

  @Override
  public RedisFuture<Boolean> smove(K source, K destination, V member) {
    Span span = helper.buildSpan("smove");
    return setCompleteAction(commands.smove(source, destination, member), span);
  }

  @Override
  public RedisFuture<Set<V>> smembers(K key) {
    Span span = helper.buildSpan("smembers", key);
    return setCompleteAction(commands.smembers(key), span);
  }

  @Override
  public RedisFuture<Long> smembers(
      ValueStreamingChannel<V> channel, K key) {
    Span span = helper.buildSpan("smembers", key);
    return setCompleteAction(commands.smembers(channel, key), span);
  }

  @Override
  public RedisFuture<V> spop(K key) {
    Span span = helper.buildSpan("spop", key);
    return setCompleteAction(commands.spop(key), span);
  }

  @Override
  public RedisFuture<Set<V>> spop(K key, long count) {
    Span span = helper.buildSpan("spop", key);
    span.setTag("count", count);
    return setCompleteAction(commands.spop(key, count), span);
  }

  @Override
  public RedisFuture<V> srandmember(K key) {
    Span span = helper.buildSpan("srandmember", key);
    return setCompleteAction(commands.srandmember(key), span);
  }

  @Override
  public RedisFuture<List<V>> srandmember(K key, long count) {
    Span span = helper.buildSpan("srandmember", key);
    span.setTag("count", count);
    return setCompleteAction(commands.srandmember(key, count), span);
  }

  @Override
  public RedisFuture<Long> srandmember(
      ValueStreamingChannel<V> channel, K key, long count) {
    Span span = helper.buildSpan("srandmember", key);
    span.setTag("count", count);
    return setCompleteAction(commands.srandmember(channel, key, count), span);
  }

  @Override
  public RedisFuture<Long> srem(K key, V... members) {
    Span span = helper.buildSpan("srem", key);
    return setCompleteAction(commands.srem(key, members), span);
  }

  @Override
  public RedisFuture<Set<V>> sunion(K... keys) {
    Span span = helper.buildSpan("sunion", keys);
    return setCompleteAction(commands.sunion(keys), span);
  }

  @Override
  public RedisFuture<Long> sunion(
      ValueStreamingChannel<V> channel, K... keys) {
    Span span = helper.buildSpan("sunion", keys);
    return setCompleteAction(commands.sunion(channel, keys), span);
  }

  @Override
  public RedisFuture<Long> sunionstore(K destination, K... keys) {
    Span span = helper.buildSpan("sunionstore", keys);
    return setCompleteAction(commands.sunionstore(destination, keys), span);
  }

  @Override
  public RedisFuture<ValueScanCursor<V>> sscan(K key) {
    Span span = helper.buildSpan("sscan", key);
    return setCompleteAction(commands.sscan(key), span);
  }

  @Override
  public RedisFuture<ValueScanCursor<V>> sscan(K key,
      ScanArgs scanArgs) {
    Span span = helper.buildSpan("sscan", key);
    return setCompleteAction(commands.sscan(key, scanArgs), span);
  }

  @Override
  public RedisFuture<ValueScanCursor<V>> sscan(K key,
      ScanCursor scanCursor, ScanArgs scanArgs) {
    Span span = helper.buildSpan("sscan", key);
    return setCompleteAction(commands.sscan(key, scanCursor, scanArgs), span);
  }

  @Override
  public RedisFuture<ValueScanCursor<V>> sscan(K key,
      ScanCursor scanCursor) {
    Span span = helper.buildSpan("sscan", key);
    return setCompleteAction(commands.sscan(key, scanCursor), span);
  }

  @Override
  public RedisFuture<StreamScanCursor> sscan(
      ValueStreamingChannel<V> channel, K key) {
    Span span = helper.buildSpan("sscan", key);
    return setCompleteAction(commands.sscan(channel, key), span);
  }

  @Override
  public RedisFuture<StreamScanCursor> sscan(
      ValueStreamingChannel<V> channel, K key,
      ScanArgs scanArgs) {
    Span span = helper.buildSpan("sscan", key);
    return setCompleteAction(commands.sscan(channel, key, scanArgs), span);
  }

  @Override
  public RedisFuture<StreamScanCursor> sscan(
      ValueStreamingChannel<V> channel, K key,
      ScanCursor scanCursor, ScanArgs scanArgs) {
    Span span = helper.buildSpan("sscan", key);
    return setCompleteAction(commands.sscan(channel, key, scanCursor, scanArgs), span);
  }

  @Override
  public RedisFuture<StreamScanCursor> sscan(
      ValueStreamingChannel<V> channel, K key,
      ScanCursor scanCursor) {
    Span span = helper.buildSpan("sscan", key);
    return setCompleteAction(commands.sscan(channel, key, scanCursor), span);
  }

  @Override
  public RedisFuture<Long> zadd(K key, double score, V member) {
    Span span = helper.buildSpan("zadd", key);
    span.setTag("score", score);
    return setCompleteAction(commands.zadd(key, score, member), span);
  }

  @Override
  public RedisFuture<Long> zadd(K key, Object... scoresAndValues) {
    Span span = helper.buildSpan("zadd", key);
    return setCompleteAction(commands.zadd(key, scoresAndValues), span);
  }

  @Override
  public RedisFuture<Long> zadd(K key, ScoredValue<V>... scoredValues) {
    Span span = helper.buildSpan("zadd", key);
    return setCompleteAction(commands.zadd(key, scoredValues), span);
  }

  @Override
  public RedisFuture<Long> zadd(K key, ZAddArgs zAddArgs,
      double score, V member) {
    Span span = helper.buildSpan("zadd", key);
    span.setTag("score", score);
    return setCompleteAction(commands.zadd(key, zAddArgs, score, member), span);
  }

  @Override
  public RedisFuture<Long> zadd(K key, ZAddArgs zAddArgs,
      Object... scoresAndValues) {
    Span span = helper.buildSpan("zadd", key);
    return setCompleteAction(commands.zadd(key, zAddArgs, scoresAndValues), span);
  }

  @Override
  public RedisFuture<Long> zadd(K key, ZAddArgs zAddArgs,
      ScoredValue<V>... scoredValues) {
    Span span = helper.buildSpan("zadd", key);
    return setCompleteAction(commands.zadd(key, zAddArgs, scoredValues), span);
  }

  @Override
  public RedisFuture<Double> zaddincr(K key, double score, V member) {
    Span span = helper.buildSpan("zaddincr", key);
    span.setTag("score", score);
    return setCompleteAction(commands.zaddincr(key, score, member), span);
  }

  @Override
  public RedisFuture<Double> zaddincr(K key, ZAddArgs zAddArgs,
      double score, V member) {
    Span span = helper.buildSpan("zaddincr", key);
    span.setTag("score", score);
    return setCompleteAction(commands.zaddincr(key, zAddArgs, score, member), span);
  }

  @Override
  public RedisFuture<Long> zcard(K key) {
    Span span = helper.buildSpan("zcard", key);
    return setCompleteAction(commands.zcard(key), span);
  }

  @Override
  @Deprecated
  public RedisFuture<Long> zcount(K key, double min, double max) {
    Span span = helper.buildSpan("zcount", key);
    span.setTag("min", min);
    span.setTag("max", max);
    return setCompleteAction(commands.zcount(key, min, max), span);
  }

  @Override
  @Deprecated
  public RedisFuture<Long> zcount(K key, String min, String max) {
    Span span = helper.buildSpan("zcount", key);
    span.setTag("min", min);
    span.setTag("max", max);
    return setCompleteAction(commands.zcount(key, min, max), span);
  }

  @Override
  public RedisFuture<Long> zcount(K key,
      Range<? extends Number> range) {
    Span span = helper.buildSpan("zcount", key);
    span.setTag("range", nullable(range));
    return setCompleteAction(commands.zcount(key, range), span);
  }

  @Override
  public RedisFuture<Double> zincrby(K key, double amount, K member) {
    Span span = helper.buildSpan("zincrby", key);
    span.setTag("amount", amount);
    return setCompleteAction(commands.zincrby(key, amount, member), span);
  }

  @Override
  public RedisFuture<Long> zinterstore(K destination, K... keys) {
    Span span = helper.buildSpan("zinterstore", keys);
    return setCompleteAction(commands.zinterstore(destination, keys), span);
  }

  @Override
  public RedisFuture<Long> zinterstore(K destination,
      ZStoreArgs storeArgs, K... keys) {
    Span span = helper.buildSpan("zinterstore", keys);
    return setCompleteAction(commands.zinterstore(destination, storeArgs, keys), span);
  }

  @Override
  @Deprecated
  public RedisFuture<Long> zlexcount(K key, String min, String max) {
    Span span = helper.buildSpan("zlexcount", key);
    span.setTag("min", min);
    span.setTag("max", max);
    return setCompleteAction(commands.zlexcount(key, min, max), span);
  }

  @Override
  public RedisFuture<Long> zlexcount(K key, Range<? extends V> range) {
    Span span = helper.buildSpan("zlexcount", key);
    span.setTag("range", nullable(range));
    return setCompleteAction(commands.zlexcount(key, range), span);
  }

  @Override
  public RedisFuture<List<V>> zrange(K key, long start, long stop) {
    Span span = helper.buildSpan("zrange", key);
    span.setTag("start", start);
    span.setTag("stop", stop);
    return setCompleteAction(commands.zrange(key, start, stop), span);
  }

  @Override
  public RedisFuture<Long> zrange(
      ValueStreamingChannel<V> channel, K key, long start, long stop) {
    Span span = helper.buildSpan("zrange", key);
    span.setTag("start", start);
    span.setTag("stop", stop);
    return setCompleteAction(commands.zrange(channel, key, start, stop), span);
  }

  @Override
  public RedisFuture<List<ScoredValue<V>>> zrangeWithScores(
      K key, long start, long stop) {
    Span span = helper.buildSpan("zrangeWithScores", key);
    span.setTag("start", start);
    span.setTag("stop", stop);
    return setCompleteAction(commands.zrangeWithScores(key, start, stop), span);
  }

  @Override
  public RedisFuture<Long> zrangeWithScores(
      ScoredValueStreamingChannel<V> channel, K key, long start, long stop) {
    Span span = helper.buildSpan("zrangeWithScores", key);
    span.setTag("start", start);
    span.setTag("stop", stop);
    return setCompleteAction(commands.zrangeWithScores(channel, key, start, stop), span);
  }

  @Override
  @Deprecated
  public RedisFuture<List<V>> zrangebylex(K key, String min,
      String max) {
    Span span = helper.buildSpan("zrangebylex", key);
    span.setTag("min", min);
    span.setTag("max", max);
    return setCompleteAction(commands.zrangebylex(key, min, max), span);
  }

  @Override
  public RedisFuture<List<V>> zrangebylex(K key,
      Range<? extends V> range) {
    Span span = helper.buildSpan("zrangebylex", key);
    span.setTag("range", nullable(range));
    return setCompleteAction(commands.zrangebylex(key, range), span);
  }

  @Override
  @Deprecated
  public RedisFuture<List<V>> zrangebylex(K key, String min,
      String max, long offset, long count) {
    Span span = helper.buildSpan("zrangebylex", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return setCompleteAction(commands.zrangebylex(key, min, max, offset, count), span);
  }

  @Override
  public RedisFuture<List<V>> zrangebylex(K key,
      Range<? extends V> range, Limit limit) {
    Span span = helper.buildSpan("zrangebylex", key);
    span.setTag("range", nullable(range));
    span.setTag("limit", nullable(limit));
    return setCompleteAction(commands.zrangebylex(key, range, limit), span);
  }

  @Override
  @Deprecated
  public RedisFuture<List<V>> zrangebyscore(K key, double min, double max) {
    Span span = helper.buildSpan("zrangebyscore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    return setCompleteAction(commands.zrangebyscore(key, min, max), span);
  }

  @Override
  @Deprecated
  public RedisFuture<List<V>> zrangebyscore(K key, String min,
      String max) {
    Span span = helper.buildSpan("zrangebyscore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    return setCompleteAction(commands.zrangebyscore(key, min, max), span);
  }

  @Override
  public RedisFuture<List<V>> zrangebyscore(K key,
      Range<? extends Number> range) {
    Span span = helper.buildSpan("zrangebyscore", key);
    span.setTag("range", nullable(range));
    return setCompleteAction(commands.zrangebyscore(key, range), span);
  }

  @Override
  @Deprecated
  public RedisFuture<List<V>> zrangebyscore(K key, double min, double max,
      long offset, long count) {
    Span span = helper.buildSpan("zrangebyscore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return setCompleteAction(commands.zrangebyscore(key, min, max, offset, count), span);
  }

  @Override
  @Deprecated
  public RedisFuture<List<V>> zrangebyscore(K key, String min,
      String max, long offset, long count) {
    Span span = helper.buildSpan("zrangebyscore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return setCompleteAction(commands.zrangebyscore(key, min, max, offset, count), span);
  }

  @Override
  public RedisFuture<List<V>> zrangebyscore(K key,
      Range<? extends Number> range, Limit limit) {
    Span span = helper.buildSpan("zrangebyscore", key);
    span.setTag("range", nullable(range));
    span.setTag("limit", nullable(limit));
    return setCompleteAction(commands.zrangebyscore(key, range, limit), span);
  }

  @Override
  @Deprecated
  public RedisFuture<Long> zrangebyscore(
      ValueStreamingChannel<V> channel, K key, double min, double max) {
    Span span = helper.buildSpan("zrangebyscore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    return setCompleteAction(commands.zrangebyscore(channel, key, min, max), span);
  }

  @Override
  @Deprecated
  public RedisFuture<Long> zrangebyscore(
      ValueStreamingChannel<V> channel, K key, String min,
      String max) {
    Span span = helper.buildSpan("zrangebyscore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    return setCompleteAction(commands.zrangebyscore(channel, key, min, max), span);
  }

  @Override
  public RedisFuture<Long> zrangebyscore(
      ValueStreamingChannel<V> channel, K key,
      Range<? extends Number> range) {
    Span span = helper.buildSpan("zrangebyscore", key);
    span.setTag("range", nullable(range));
    return setCompleteAction(commands.zrangebyscore(channel, key, range), span);
  }

  @Override
  @Deprecated
  public RedisFuture<Long> zrangebyscore(
      ValueStreamingChannel<V> channel, K key, double min, double max,
      long offset, long count) {
    Span span = helper.buildSpan("zrangebyscore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return setCompleteAction(commands.zrangebyscore(channel, key, min, max, offset, count), span);
  }

  @Override
  @Deprecated
  public RedisFuture<Long> zrangebyscore(
      ValueStreamingChannel<V> channel, K key, String min,
      String max, long offset, long count) {
    Span span = helper.buildSpan("zrangebyscore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return setCompleteAction(commands.zrangebyscore(channel, key, min, max, offset, count), span);
  }

  @Override
  public RedisFuture<Long> zrangebyscore(
      ValueStreamingChannel<V> channel, K key,
      Range<? extends Number> range, Limit limit) {
    Span span = helper.buildSpan("zrangebyscore", key);
    span.setTag("range", nullable(range));
    span.setTag("limit", nullable(limit));
    return setCompleteAction(commands.zrangebyscore(channel, key, range, limit), span);
  }

  @Override
  @Deprecated
  public RedisFuture<List<ScoredValue<V>>> zrangebyscoreWithScores(
      K key, double min, double max) {
    Span span = helper.buildSpan("zrangebyscoreWithScores", key);
    span.setTag("min", min);
    span.setTag("max", max);
    return setCompleteAction(commands.zrangebyscoreWithScores(key, min, max), span);
  }

  @Override
  @Deprecated
  public RedisFuture<List<ScoredValue<V>>> zrangebyscoreWithScores(
      K key, String min, String max) {
    Span span = helper.buildSpan("zrangebyscoreWithScores", key);
    span.setTag("min", min);
    span.setTag("max", max);
    return setCompleteAction(commands.zrangebyscoreWithScores(key, min, max), span);
  }

  @Override
  public RedisFuture<List<ScoredValue<V>>> zrangebyscoreWithScores(
      K key, Range<? extends Number> range) {
    Span span = helper.buildSpan("zrangebyscoreWithScores", key);
    span.setTag("range", nullable(range));
    return setCompleteAction(commands.zrangebyscoreWithScores(key, range), span);
  }

  @Override
  @Deprecated
  public RedisFuture<List<ScoredValue<V>>> zrangebyscoreWithScores(
      K key, double min, double max, long offset, long count) {
    Span span = helper.buildSpan("zrangebyscoreWithScores", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return setCompleteAction(commands.zrangebyscoreWithScores(key, min, max, offset, count), span);
  }

  @Override
  @Deprecated
  public RedisFuture<List<ScoredValue<V>>> zrangebyscoreWithScores(
      K key, String min, String max, long offset, long count) {
    Span span = helper.buildSpan("zrangebyscoreWithScores", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return setCompleteAction(commands.zrangebyscoreWithScores(key, min, max, offset, count), span);
  }

  @Override
  public RedisFuture<List<ScoredValue<V>>> zrangebyscoreWithScores(
      K key, Range<? extends Number> range, Limit limit) {
    Span span = helper.buildSpan("zrangebyscoreWithScores", key);
    span.setTag("range", nullable(range));
    span.setTag("limit", nullable(limit));
    return setCompleteAction(commands.zrangebyscoreWithScores(key, range, limit), span);
  }

  @Override
  @Deprecated
  public RedisFuture<Long> zrangebyscoreWithScores(
      ScoredValueStreamingChannel<V> channel, K key, double min, double max) {
    Span span = helper.buildSpan("zrangebyscoreWithScores", key);
    span.setTag("min", min);
    span.setTag("max", max);
    return setCompleteAction(commands.zrangebyscoreWithScores(channel, key, min, max), span);
  }

  @Override
  @Deprecated
  public RedisFuture<Long> zrangebyscoreWithScores(
      ScoredValueStreamingChannel<V> channel, K key, String min,
      String max) {
    Span span = helper.buildSpan("zrangebyscoreWithScores", key);
    span.setTag("min", min);
    span.setTag("max", max);
    return setCompleteAction(commands.zrangebyscoreWithScores(channel, key, min, max), span);
  }

  @Override
  public RedisFuture<Long> zrangebyscoreWithScores(
      ScoredValueStreamingChannel<V> channel, K key,
      Range<? extends Number> range) {
    Span span = helper.buildSpan("zrangebyscoreWithScores", key);
    span.setTag("range", nullable(range));
    return setCompleteAction(commands.zrangebyscoreWithScores(channel, key, range), span);
  }

  @Override
  @Deprecated
  public RedisFuture<Long> zrangebyscoreWithScores(
      ScoredValueStreamingChannel<V> channel, K key, double min, double max,
      long offset, long count) {
    Span span = helper.buildSpan("zrangebyscoreWithScores", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return setCompleteAction(
        commands.zrangebyscoreWithScores(channel, key, min, max, offset, count), span);
  }

  @Override
  @Deprecated
  public RedisFuture<Long> zrangebyscoreWithScores(
      ScoredValueStreamingChannel<V> channel, K key, String min,
      String max, long offset, long count) {
    Span span = helper.buildSpan("zrangebyscoreWithScores", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return setCompleteAction(
        commands.zrangebyscoreWithScores(channel, key, min, max, offset, count), span);
  }

  @Override
  public RedisFuture<Long> zrangebyscoreWithScores(
      ScoredValueStreamingChannel<V> channel, K key,
      Range<? extends Number> range, Limit limit) {
    Span span = helper.buildSpan("zrangebyscoreWithScores", key);
    span.setTag("range", nullable(range));
    span.setTag("limit", nullable(limit));
    return setCompleteAction(commands.zrangebyscoreWithScores(channel, key, range, limit), span);
  }

  @Override
  public RedisFuture<Long> zrank(K key, V member) {
    Span span = helper.buildSpan("zrank", key);
    return setCompleteAction(commands.zrank(key, member), span);
  }

  @Override
  public RedisFuture<Long> zrem(K key, V... members) {
    Span span = helper.buildSpan("zrem", key);
    return setCompleteAction(commands.zrem(key, members), span);
  }

  @Override
  @Deprecated
  public RedisFuture<Long> zremrangebylex(K key, String min, String max) {
    Span span = helper.buildSpan("zremrangebylex", key);
    span.setTag("min", min);
    span.setTag("max", max);
    return setCompleteAction(commands.zremrangebylex(key, min, max), span);
  }

  @Override
  public RedisFuture<Long> zremrangebylex(K key,
      Range<? extends V> range) {
    Span span = helper.buildSpan("zremrangebylex", key);
    span.setTag("range", nullable(range));
    return setCompleteAction(commands.zremrangebylex(key, range), span);
  }

  @Override
  public RedisFuture<Long> zremrangebyrank(K key, long start, long stop) {
    Span span = helper.buildSpan("zremrangebyrank", key);
    span.setTag("start", start);
    span.setTag("stop", stop);
    return setCompleteAction(commands.zremrangebyrank(key, start, stop), span);
  }

  @Override
  @Deprecated
  public RedisFuture<Long> zremrangebyscore(K key, double min, double max) {
    Span span = helper.buildSpan("zremrangebyscore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    return setCompleteAction(commands.zremrangebyscore(key, min, max), span);
  }

  @Override
  @Deprecated
  public RedisFuture<Long> zremrangebyscore(K key, String min, String max) {
    Span span = helper.buildSpan("zremrangebyscore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    return setCompleteAction(commands.zremrangebyscore(key, min, max), span);
  }

  @Override
  public RedisFuture<Long> zremrangebyscore(K key,
      Range<? extends Number> range) {
    Span span = helper.buildSpan("zremrangebyscore", key);
    span.setTag("range", nullable(range));
    return setCompleteAction(commands.zremrangebyscore(key, range), span);
  }

  @Override
  public RedisFuture<List<V>> zrevrange(K key, long start, long stop) {
    Span span = helper.buildSpan("zrevrange", key);
    span.setTag("start", start);
    span.setTag("stop", stop);
    return setCompleteAction(commands.zrevrange(key, start, stop), span);
  }

  @Override
  public RedisFuture<Long> zrevrange(
      ValueStreamingChannel<V> channel, K key, long start, long stop) {
    Span span = helper.buildSpan("zrevrange", key);
    span.setTag("start", start);
    span.setTag("stop", stop);
    return setCompleteAction(commands.zrevrange(channel, key, start, stop), span);
  }

  @Override
  public RedisFuture<List<ScoredValue<V>>> zrevrangeWithScores(
      K key, long start, long stop) {
    Span span = helper.buildSpan("zrevrangeWithScores", key);
    span.setTag("start", start);
    span.setTag("stop", stop);
    return setCompleteAction(commands.zrevrangeWithScores(key, start, stop), span);
  }

  @Override
  public RedisFuture<Long> zrevrangeWithScores(
      ScoredValueStreamingChannel<V> channel, K key, long start, long stop) {
    Span span = helper.buildSpan("zrevrangeWithScores", key);
    span.setTag("start", start);
    span.setTag("stop", stop);
    return setCompleteAction(commands.zrevrangeWithScores(channel, key, start, stop), span);
  }

  @Override
  public RedisFuture<List<V>> zrevrangebylex(K key,
      Range<? extends V> range) {
    Span span = helper.buildSpan("zrevrangebylex", key);
    span.setTag("range", nullable(range));
    return setCompleteAction(commands.zrevrangebylex(key, range), span);
  }

  @Override
  public RedisFuture<List<V>> zrevrangebylex(K key,
      Range<? extends V> range, Limit limit) {
    Span span = helper.buildSpan("zrevrangebylex", key);
    span.setTag("range", nullable(range));
    span.setTag("limit", nullable(limit));
    return setCompleteAction(commands.zrevrangebylex(key, range, limit), span);
  }

  @Override
  @Deprecated
  public RedisFuture<List<V>> zrevrangebyscore(K key, double max,
      double min) {
    Span span = helper.buildSpan("zrevrangebyscore", key);
    span.setTag("max", max);
    span.setTag("min", min);
    return setCompleteAction(commands.zrevrangebyscore(key, max, min), span);
  }

  @Override
  @Deprecated
  public RedisFuture<List<V>> zrevrangebyscore(K key, String max,
      String min) {
    Span span = helper.buildSpan("zrevrangebyscore", key);
    span.setTag("max", max);
    span.setTag("min", min);
    return setCompleteAction(commands.zrevrangebyscore(key, max, min), span);
  }

  @Override
  public RedisFuture<List<V>> zrevrangebyscore(K key,
      Range<? extends Number> range) {
    Span span = helper.buildSpan("zrevrangebyscore", key);
    span.setTag("range", nullable(range));
    return setCompleteAction(commands.zrevrangebyscore(key, range), span);
  }

  @Override
  @Deprecated
  public RedisFuture<List<V>> zrevrangebyscore(K key, double max,
      double min, long offset, long count) {
    Span span = helper.buildSpan("zrevrangebyscore", key);
    span.setTag("max", max);
    span.setTag("min", min);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return setCompleteAction(commands.zrevrangebyscore(key, max, min, offset, count), span);
  }

  @Override
  @Deprecated
  public RedisFuture<List<V>> zrevrangebyscore(K key, String max,
      String min, long offset, long count) {
    Span span = helper.buildSpan("zrevrangebyscore", key);
    span.setTag("max", max);
    span.setTag("min", min);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return setCompleteAction(commands.zrevrangebyscore(key, max, min, offset, count), span);
  }

  @Override
  public RedisFuture<List<V>> zrevrangebyscore(K key,
      Range<? extends Number> range, Limit limit) {
    Span span = helper.buildSpan("zrevrangebyscore", key);
    span.setTag("range", nullable(range));
    span.setTag("limit", nullable(limit));
    return setCompleteAction(commands.zrevrangebyscore(key, range, limit), span);
  }

  @Override
  @Deprecated
  public RedisFuture<Long> zrevrangebyscore(
      ValueStreamingChannel<V> channel, K key, double max, double min) {
    Span span = helper.buildSpan("zrevrangebyscore", key);
    span.setTag("max", max);
    span.setTag("min", min);
    return setCompleteAction(commands.zrevrangebyscore(channel, key, max, min), span);
  }

  @Override
  @Deprecated
  public RedisFuture<Long> zrevrangebyscore(
      ValueStreamingChannel<V> channel, K key, String max,
      String min) {
    Span span = helper.buildSpan("zrevrangebyscore", key);
    span.setTag("max", max);
    span.setTag("min", min);
    return setCompleteAction(commands.zrevrangebyscore(channel, key, max, min), span);
  }

  @Override
  public RedisFuture<Long> zrevrangebyscore(
      ValueStreamingChannel<V> channel, K key,
      Range<? extends Number> range) {
    Span span = helper.buildSpan("zrevrangebyscore", key);
    span.setTag("range", nullable(range));
    return setCompleteAction(commands.zrevrangebyscore(channel, key, range), span);
  }

  @Override
  @Deprecated
  public RedisFuture<Long> zrevrangebyscore(
      ValueStreamingChannel<V> channel, K key, double max, double min,
      long offset, long count) {
    Span span = helper.buildSpan("zrevrangebyscore", key);
    span.setTag("max", max);
    span.setTag("min", min);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return setCompleteAction(commands.zrevrangebyscore(channel, key, max, min, offset, count),
        span);
  }

  @Override
  @Deprecated
  public RedisFuture<Long> zrevrangebyscore(
      ValueStreamingChannel<V> channel, K key, String max,
      String min, long offset, long count) {
    Span span = helper.buildSpan("zrevrangebyscore", key);
    span.setTag("max", max);
    span.setTag("min", min);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return setCompleteAction(commands.zrevrangebyscore(channel, key, max, min, offset, count),
        span);
  }

  @Override
  public RedisFuture<Long> zrevrangebyscore(
      ValueStreamingChannel<V> channel, K key,
      Range<? extends Number> range, Limit limit) {
    Span span = helper.buildSpan("zrevrangebyscore", key);
    span.setTag("range", nullable(range));
    span.setTag("limit", nullable(limit));
    return setCompleteAction(commands.zrevrangebyscore(channel, key, range, limit), span);
  }

  @Override
  @Deprecated
  public RedisFuture<List<ScoredValue<V>>> zrevrangebyscoreWithScores(
      K key, double max, double min) {
    Span span = helper.buildSpan("zrevrangebyscoreWithScores", key);
    span.setTag("max", max);
    span.setTag("min", min);
    return setCompleteAction(commands.zrevrangebyscoreWithScores(key, max, min), span);
  }

  @Override
  @Deprecated
  public RedisFuture<List<ScoredValue<V>>> zrevrangebyscoreWithScores(
      K key, String max, String min) {
    Span span = helper.buildSpan("zrevrangebyscoreWithScores", key);
    span.setTag("max", max);
    span.setTag("min", min);
    return setCompleteAction(commands.zrevrangebyscoreWithScores(key, max, min), span);
  }

  @Override
  public RedisFuture<List<ScoredValue<V>>> zrevrangebyscoreWithScores(
      K key, Range<? extends Number> range) {
    Span span = helper.buildSpan("zrevrangebyscoreWithScores", key);
    span.setTag("range", nullable(range));
    return setCompleteAction(commands.zrevrangebyscoreWithScores(key, range), span);
  }

  @Override
  @Deprecated
  public RedisFuture<List<ScoredValue<V>>> zrevrangebyscoreWithScores(
      K key, double max, double min, long offset, long count) {
    Span span = helper.buildSpan("zrevrangebyscoreWithScores", key);
    span.setTag("max", max);
    span.setTag("min", min);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return setCompleteAction(commands.zrevrangebyscoreWithScores(key, max, min, offset, count),
        span);
  }

  @Override
  @Deprecated
  public RedisFuture<List<ScoredValue<V>>> zrevrangebyscoreWithScores(
      K key, String max, String min, long offset, long count) {
    Span span = helper.buildSpan("zrevrangebyscoreWithScores", key);
    span.setTag("max", max);
    span.setTag("min", min);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return setCompleteAction(commands.zrevrangebyscoreWithScores(key, max, min, offset, count),
        span);
  }

  @Override
  public RedisFuture<List<ScoredValue<V>>> zrevrangebyscoreWithScores(
      K key, Range<? extends Number> range, Limit limit) {
    Span span = helper.buildSpan("zrevrangebyscoreWithScores", key);
    span.setTag("range", nullable(range));
    span.setTag("limit", nullable(limit));
    return setCompleteAction(commands.zrevrangebyscoreWithScores(key, range, limit), span);
  }

  @Override
  @Deprecated
  public RedisFuture<Long> zrevrangebyscoreWithScores(
      ScoredValueStreamingChannel<V> channel, K key, double max, double min) {
    Span span = helper.buildSpan("zrevrangebyscoreWithScores", key);
    span.setTag("max", max);
    span.setTag("min", min);
    return setCompleteAction(commands.zrevrangebyscoreWithScores(channel, key, max, min), span);
  }

  @Override
  @Deprecated
  public RedisFuture<Long> zrevrangebyscoreWithScores(
      ScoredValueStreamingChannel<V> channel, K key, String max,
      String min) {
    Span span = helper.buildSpan("zrevrangebyscoreWithScores", key);
    span.setTag("max", max);
    span.setTag("min", min);
    return setCompleteAction(commands.zrevrangebyscoreWithScores(channel, key, max, min), span);
  }

  @Override
  public RedisFuture<Long> zrevrangebyscoreWithScores(
      ScoredValueStreamingChannel<V> channel, K key,
      Range<? extends Number> range) {
    Span span = helper.buildSpan("zrevrangebyscoreWithScores", key);
    span.setTag("range", nullable(range));
    return setCompleteAction(commands.zrevrangebyscoreWithScores(channel, key, range), span);
  }

  @Override
  @Deprecated
  public RedisFuture<Long> zrevrangebyscoreWithScores(
      ScoredValueStreamingChannel<V> channel, K key, double max, double min,
      long offset, long count) {
    Span span = helper.buildSpan("zrevrangebyscoreWithScores", key);
    span.setTag("max", max);
    span.setTag("min", min);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return setCompleteAction(
        commands.zrevrangebyscoreWithScores(channel, key, max, min, offset, count), span);
  }

  @Override
  @Deprecated
  public RedisFuture<Long> zrevrangebyscoreWithScores(
      ScoredValueStreamingChannel<V> channel, K key, String max,
      String min, long offset, long count) {
    Span span = helper.buildSpan("zrevrangebyscoreWithScores", key);
    span.setTag("max", max);
    span.setTag("min", min);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return setCompleteAction(
        commands.zrevrangebyscoreWithScores(channel, key, max, min, offset, count), span);
  }

  @Override
  public RedisFuture<Long> zrevrangebyscoreWithScores(
      ScoredValueStreamingChannel<V> channel, K key,
      Range<? extends Number> range, Limit limit) {
    Span span = helper.buildSpan("zrevrangebyscoreWithScores", key);
    span.setTag("range", nullable(range));
    span.setTag("limit", nullable(limit));
    return setCompleteAction(commands.zrevrangebyscoreWithScores(channel, key, range, limit), span);
  }

  @Override
  public RedisFuture<Long> zrevrank(K key, V member) {
    Span span = helper.buildSpan("zrevrank", key);
    return setCompleteAction(commands.zrevrank(key, member), span);
  }

  @Override
  public RedisFuture<ScoredValueScanCursor<V>> zscan(K key) {
    Span span = helper.buildSpan("zscan", key);
    return setCompleteAction(commands.zscan(key), span);
  }

  @Override
  public RedisFuture<ScoredValueScanCursor<V>> zscan(K key,
      ScanArgs scanArgs) {
    Span span = helper.buildSpan("zscan", key);
    return setCompleteAction(commands.zscan(key, scanArgs), span);
  }

  @Override
  public RedisFuture<ScoredValueScanCursor<V>> zscan(K key,
      ScanCursor scanCursor, ScanArgs scanArgs) {
    Span span = helper.buildSpan("zscan", key);
    return setCompleteAction(commands.zscan(key, scanCursor, scanArgs), span);
  }

  @Override
  public RedisFuture<ScoredValueScanCursor<V>> zscan(K key,
      ScanCursor scanCursor) {
    Span span = helper.buildSpan("zscan", key);
    return setCompleteAction(commands.zscan(key, scanCursor), span);
  }

  @Override
  public RedisFuture<StreamScanCursor> zscan(
      ScoredValueStreamingChannel<V> channel, K key) {
    Span span = helper.buildSpan("zscan", key);
    return setCompleteAction(commands.zscan(channel, key), span);
  }

  @Override
  public RedisFuture<StreamScanCursor> zscan(
      ScoredValueStreamingChannel<V> channel, K key,
      ScanArgs scanArgs) {
    Span span = helper.buildSpan("zscan", key);
    return setCompleteAction(commands.zscan(channel, key, scanArgs), span);
  }

  @Override
  public RedisFuture<StreamScanCursor> zscan(
      ScoredValueStreamingChannel<V> channel, K key,
      ScanCursor scanCursor, ScanArgs scanArgs) {
    Span span = helper.buildSpan("zscan", key);
    return setCompleteAction(commands.zscan(channel, key, scanCursor, scanArgs), span);
  }

  @Override
  public RedisFuture<StreamScanCursor> zscan(
      ScoredValueStreamingChannel<V> channel, K key,
      ScanCursor scanCursor) {
    Span span = helper.buildSpan("zscan", key);
    return setCompleteAction(commands.zscan(channel, key, scanCursor), span);
  }

  @Override
  public RedisFuture<Double> zscore(K key, V member) {
    Span span = helper.buildSpan("zscore", key);
    return setCompleteAction(commands.zscore(key, member), span);
  }

  @Override
  public RedisFuture<Long> zunionstore(K destination, K... keys) {
    Span span = helper.buildSpan("zunionstore", keys);
    return setCompleteAction(commands.zunionstore(destination, keys), span);
  }

  @Override
  public RedisFuture<Long> zunionstore(K destination,
      ZStoreArgs storeArgs, K... keys) {
    Span span = helper.buildSpan("zunionstore", keys);
    return setCompleteAction(commands.zunionstore(destination, storeArgs, keys), span);
  }

  @Override
  public <T> RedisFuture<T> eval(String script, ScriptOutputType type,
      K... keys) {
    Span span = helper.buildSpan("eval", keys);
    return setCompleteAction(commands.eval(script, type, keys), span);
  }

  @Override
  public <T> RedisFuture<T> eval(String script, ScriptOutputType type,
      K[] keys, V... values) {
    Span span = helper.buildSpan("eval", keys);
    return setCompleteAction(commands.eval(script, type, keys, values), span);
  }

  @Override
  public <T> RedisFuture<T> evalsha(String digest,
      ScriptOutputType type, K... keys) {
    Span span = helper.buildSpan("evalsha", keys);
    return setCompleteAction(commands.evalsha(digest, type, keys), span);
  }

  @Override
  public <T> RedisFuture<T> evalsha(String digest,
      ScriptOutputType type, K[] keys, V... values) {
    Span span = helper.buildSpan("evalsha", keys);
    return setCompleteAction(commands.evalsha(digest, type, keys, values), span);
  }

  @Override
  public RedisFuture<List<Boolean>> scriptExists(
      String... digests) {
    Span span = helper.buildSpan("scriptExists");
    return setCompleteAction(commands.scriptExists(digests), span);
  }

  @Override
  public RedisFuture<String> scriptFlush() {
    Span span = helper.buildSpan("scriptFlush");
    return setCompleteAction(commands.scriptFlush(), span);
  }

  @Override
  public RedisFuture<String> scriptKill() {
    Span span = helper.buildSpan("scriptKill");
    return setCompleteAction(commands.scriptKill(), span);
  }

  @Override
  public RedisFuture<String> scriptLoad(V script) {
    Span span = helper.buildSpan("scriptLoad");
    return setCompleteAction(commands.scriptLoad(script), span);
  }

  @Override
  public String digest(V script) {
    Span span = helper.buildSpan("digest");
    try {
      return commands.digest(script);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public RedisFuture<String> bgrewriteaof() {
    Span span = helper.buildSpan("bgrewriteaof");
    return setCompleteAction(commands.bgrewriteaof(), span);
  }

  @Override
  public RedisFuture<String> bgsave() {
    Span span = helper.buildSpan("bgsave");
    return setCompleteAction(commands.bgsave(), span);
  }

  @Override
  public RedisFuture<K> clientGetname() {
    Span span = helper.buildSpan("clientGetname");
    return setCompleteAction(commands.clientGetname(), span);
  }

  @Override
  public RedisFuture<String> clientSetname(K name) {
    Span span = helper.buildSpan("clientSetname");
    return setCompleteAction(commands.clientSetname(name), span);
  }

  @Override
  public RedisFuture<String> clientKill(String addr) {
    Span span = helper.buildSpan("clientKill");
    return setCompleteAction(commands.clientKill(addr), span);
  }

  @Override
  public RedisFuture<Long> clientKill(KillArgs killArgs) {
    Span span = helper.buildSpan("clientKill");
    return setCompleteAction(commands.clientKill(killArgs), span);
  }

  @Override
  public RedisFuture<String> clientPause(long timeout) {
    Span span = helper.buildSpan("clientPause");
    return setCompleteAction(commands.clientPause(timeout), span);
  }

  @Override
  public RedisFuture<String> clientList() {
    Span span = helper.buildSpan("clientList");
    return setCompleteAction(commands.clientList(), span);
  }

  @Override
  public RedisFuture<List<Object>> command() {
    Span span = helper.buildSpan("command");
    return setCompleteAction(commands.command(), span);
  }

  @Override
  public RedisFuture<List<Object>> commandInfo(String... commands) {
    Span span = helper.buildSpan("commandInfo");
    return this.commands.commandInfo(commands);
  }

  @Override
  public RedisFuture<List<Object>> commandInfo(
      CommandType... commands) {
    Span span = helper.buildSpan("commandInfo");
    return this.commands.commandInfo(commands);
  }

  @Override
  public RedisFuture<Long> commandCount() {
    Span span = helper.buildSpan("commandCount");
    return setCompleteAction(commands.commandCount(), span);
  }

  @Override
  public RedisFuture<Map<String, String>> configGet(
      String parameter) {
    Span span = helper.buildSpan("configGet");
    return setCompleteAction(commands.configGet(parameter), span);
  }

  @Override
  public RedisFuture<String> configResetstat() {
    Span span = helper.buildSpan("configResetstat");
    return setCompleteAction(commands.configResetstat(), span);
  }

  @Override
  public RedisFuture<String> configRewrite() {
    Span span = helper.buildSpan("configRewrite");
    return setCompleteAction(commands.configRewrite(), span);
  }

  @Override
  public RedisFuture<String> configSet(String parameter, String value) {
    Span span = helper.buildSpan("configSet");
    return setCompleteAction(commands.configSet(parameter, value), span);
  }

  @Override
  public RedisFuture<Long> dbsize() {
    Span span = helper.buildSpan("dbsize");
    return setCompleteAction(commands.dbsize(), span);
  }

  @Override
  public RedisFuture<String> debugCrashAndRecover(Long delay) {
    Span span = helper.buildSpan("debugCrashAndRecover");
    return setCompleteAction(commands.debugCrashAndRecover(delay), span);
  }

  @Override
  public RedisFuture<String> debugHtstats(int db) {
    Span span = helper.buildSpan("debugHtstats");
    return setCompleteAction(commands.debugHtstats(db), span);
  }

  @Override
  public RedisFuture<String> debugObject(K key) {
    Span span = helper.buildSpan("debugObject", key);
    return setCompleteAction(commands.debugObject(key), span);
  }

  @Override
  public void debugOom() {
    Span span = helper.buildSpan("debugOom");
    try {
      debugOom();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public void debugSegfault() {
    Span span = helper.buildSpan("debugSegfault");
    try {
      commands.debugSegfault();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public RedisFuture<String> debugReload() {
    Span span = helper.buildSpan("debugReload");
    return setCompleteAction(commands.debugReload(), span);
  }

  @Override
  public RedisFuture<String> debugRestart(Long delay) {
    Span span = helper.buildSpan("debugRestart");
    return setCompleteAction(commands.debugRestart(delay), span);
  }

  @Override
  public RedisFuture<String> debugSdslen(K key) {
    Span span = helper.buildSpan("debugSdslen", key);
    return setCompleteAction(commands.debugSdslen(key), span);
  }

  @Override
  public RedisFuture<String> flushall() {
    Span span = helper.buildSpan("flushall");
    return setCompleteAction(commands.flushall(), span);
  }

  @Override
  public RedisFuture<String> flushallAsync() {
    Span span = helper.buildSpan("flushallAsync");
    return setCompleteAction(commands.flushallAsync(), span);
  }

  @Override
  public RedisFuture<String> flushdb() {
    Span span = helper.buildSpan("flushdb");
    return setCompleteAction(commands.flushdb(), span);
  }

  @Override
  public RedisFuture<String> flushdbAsync() {
    Span span = helper.buildSpan("flushdbAsync");
    return setCompleteAction(commands.flushdbAsync(), span);
  }

  @Override
  public RedisFuture<String> info() {
    Span span = helper.buildSpan("info");
    return setCompleteAction(commands.info(), span);
  }

  @Override
  public RedisFuture<String> info(String section) {
    Span span = helper.buildSpan("info");
    return setCompleteAction(commands.info(section), span);
  }

  @Override
  public RedisFuture<Date> lastsave() {
    Span span = helper.buildSpan("lastsave");
    return setCompleteAction(commands.lastsave(), span);
  }

  @Override
  public RedisFuture<String> save() {
    Span span = helper.buildSpan("save");
    return setCompleteAction(commands.save(), span);
  }

  @Override
  public void shutdown(boolean save) {
    Span span = helper.buildSpan("shutdown");
    try {
      commands.shutdown(save);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public RedisFuture<String> slaveof(String host, int port) {
    Span span = helper.buildSpan("slaveof");
    return setCompleteAction(commands.slaveof(host, port), span);
  }

  @Override
  public RedisFuture<String> slaveofNoOne() {
    Span span = helper.buildSpan("slaveofNoOne");
    return setCompleteAction(commands.slaveofNoOne(), span);
  }

  @Override
  public RedisFuture<List<Object>> slowlogGet() {
    Span span = helper.buildSpan("slowlogGet");
    return setCompleteAction(commands.slowlogGet(), span);
  }

  @Override
  public RedisFuture<List<Object>> slowlogGet(int count) {
    Span span = helper.buildSpan("slowlogGet");
    return setCompleteAction(commands.slowlogGet(count), span);
  }

  @Override
  public RedisFuture<Long> slowlogLen() {
    Span span = helper.buildSpan("slowlogLen");
    return setCompleteAction(commands.slowlogLen(), span);
  }

  @Override
  public RedisFuture<String> slowlogReset() {
    Span span = helper.buildSpan("slowlogReset");
    return setCompleteAction(commands.slowlogReset(), span);
  }

  @Override
  public RedisFuture<List<V>> time() {
    Span span = helper.buildSpan("time");
    return setCompleteAction(commands.time(), span);
  }

  @Override
  public RedisFuture<Long> pfadd(K key, V... values) {
    Span span = helper.buildSpan("pfadd", key);
    return setCompleteAction(commands.pfadd(key, values), span);
  }

  @Override
  public RedisFuture<String> pfmerge(K destkey, K... sourcekeys) {
    Span span = helper.buildSpan("pfmerge");
    return setCompleteAction(commands.pfmerge(destkey, sourcekeys), span);
  }

  @Override
  public RedisFuture<Long> pfcount(K... keys) {
    Span span = helper.buildSpan("pfcount", keys);
    return setCompleteAction(commands.pfcount(keys), span);
  }

  @Override
  public RedisFuture<Long> publish(K channel, V message) {
    Span span = helper.buildSpan("publish");
    return setCompleteAction(commands.publish(channel, message), span);
  }

  @Override
  public RedisFuture<List<K>> pubsubChannels() {
    Span span = helper.buildSpan("pubsubChannels");
    return setCompleteAction(commands.pubsubChannels(), span);
  }

  @Override
  public RedisFuture<List<K>> pubsubChannels(K channel) {
    Span span = helper.buildSpan("pubsubChannels");
    return setCompleteAction(commands.pubsubChannels(channel), span);
  }

  @Override
  public RedisFuture<Map<K, Long>> pubsubNumsub(K... channels) {
    Span span = helper.buildSpan("pubsubNumsub");
    return setCompleteAction(commands.pubsubNumsub(channels), span);
  }

  @Override
  public RedisFuture<Long> pubsubNumpat() {
    Span span = helper.buildSpan("pubsubNumpat");
    return setCompleteAction(commands.pubsubNumpat(), span);
  }

  @Override
  public RedisFuture<V> echo(V msg) {
    Span span = helper.buildSpan("echo");
    return setCompleteAction(commands.echo(msg), span);
  }

  @Override
  public RedisFuture<List<Object>> role() {
    Span span = helper.buildSpan("role");
    return setCompleteAction(commands.role(), span);
  }

  @Override
  public RedisFuture<String> ping() {
    Span span = helper.buildSpan("ping");
    return setCompleteAction(commands.ping(), span);
  }

  @Override
  public RedisFuture<String> readOnly() {
    Span span = helper.buildSpan("readOnly");
    return setCompleteAction(commands.readOnly(), span);
  }

  @Override
  public RedisFuture<String> readWrite() {
    Span span = helper.buildSpan("readWrite");
    return setCompleteAction(commands.readWrite(), span);
  }

  @Override
  public RedisFuture<String> quit() {
    Span span = helper.buildSpan("quit");
    return setCompleteAction(commands.quit(), span);
  }

  @Override
  public RedisFuture<Long> waitForReplication(int replicas, long timeout) {
    Span span = helper.buildSpan("waitForReplication");
    return setCompleteAction(commands.waitForReplication(replicas, timeout), span);
  }

  @Override
  public <T> RedisFuture<T> dispatch(ProtocolKeyword type,
      CommandOutput<K, V, T> output) {
    Span span = helper.buildSpan("dispatch");
    return setCompleteAction(commands.dispatch(type, output), span);
  }

  @Override
  public <T> RedisFuture<T> dispatch(ProtocolKeyword type,
      CommandOutput<K, V, T> output,
      CommandArgs<K, V> args) {
    Span span = helper.buildSpan("dispatch");
    return setCompleteAction(commands.dispatch(type, output, args), span);
  }

  @Override
  public boolean isOpen() {
    Span span = helper.buildSpan("isOpen");
    try {
      return commands.isOpen();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public void reset() {
    Span span = helper.buildSpan("reset");
    try {
      commands.reset();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public void setAutoFlushCommands(boolean autoFlush) {
    Span span = helper.buildSpan("setAutoFlushCommands");
    try {
      commands.setAutoFlushCommands(autoFlush);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public void flushCommands() {
    Span span = helper.buildSpan("flushCommands");
    try {
      commands.flushCommands();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public void setTimeout(Duration timeout) {
    Span span = helper.buildSpan("setTimeout");
    try {
      commands.setTimeout(timeout);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Deprecated
  public void setTimeout(long timeout, TimeUnit unit) {
    Span span = helper.buildSpan("setTimeout");
    span.setTag("unit", nullable(unit));
    try {
      commands.setTimeout(timeout, unit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }

  }

  @Override
  public RedisFuture<String> clusterBumpepoch() {
    Span span = helper.buildSpan("clusterBumpepoch");
    return setCompleteAction(commands.clusterBumpepoch(), span);
  }

  @Override
  public RedisFuture<String> clusterMeet(String ip, int port) {
    Span span = helper.buildSpan("clusterMeet");
    return setCompleteAction(commands.clusterMeet(ip, port), span);
  }

  @Override
  public RedisFuture<String> clusterForget(String nodeId) {
    Span span = helper.buildSpan("clusterForget");
    span.setTag("nodeId", nodeId);
    return setCompleteAction(commands.clusterForget(nodeId), span);
  }

  @Override
  public RedisFuture<String> clusterAddSlots(int... slots) {
    Span span = helper.buildSpan("clusterAddSlots");
    span.setTag("slots", Arrays.toString(slots));
    return setCompleteAction(commands.clusterAddSlots(slots), span);
  }

  @Override
  public RedisFuture<String> clusterDelSlots(int... slots) {
    Span span = helper.buildSpan("clusterDelSlots");
    span.setTag("slots", Arrays.toString(slots));
    return setCompleteAction(commands.clusterDelSlots(slots), span);
  }

  @Override
  public RedisFuture<String> clusterSetSlotNode(int slot, String nodeId) {
    Span span = helper.buildSpan("clusterSetSlotNode");
    span.setTag("slot", slot);
    span.setTag("nodeId", nodeId);
    return setCompleteAction(commands.clusterSetSlotNode(slot, nodeId), span);
  }

  @Override
  public RedisFuture<String> clusterSetSlotStable(int slot) {
    Span span = helper.buildSpan("clusterSetSlotStable");
    span.setTag("slot", slot);
    return setCompleteAction(commands.clusterSetSlotStable(slot), span);
  }

  @Override
  public RedisFuture<String> clusterSetSlotMigrating(int slot,
      String nodeId) {
    Span span = helper.buildSpan("clusterSetSlotMigrating");
    span.setTag("slot", slot);
    span.setTag("nodeId", nodeId);
    return setCompleteAction(commands.clusterSetSlotMigrating(slot, nodeId), span);
  }

  @Override
  public RedisFuture<String> clusterSetSlotImporting(int slot,
      String nodeId) {
    Span span = helper.buildSpan("clusterSetSlotImporting");
    span.setTag("slot", slot);
    span.setTag("nodeId", nodeId);
    return setCompleteAction(commands.clusterSetSlotImporting(slot, nodeId), span);
  }

  @Override
  public RedisFuture<String> clusterInfo() {
    Span span = helper.buildSpan("clusterInfo");
    return setCompleteAction(commands.clusterInfo(), span);
  }

  @Override
  public RedisFuture<String> clusterMyId() {
    Span span = helper.buildSpan("clusterMyId");
    return setCompleteAction(commands.clusterMyId(), span);
  }

  @Override
  public RedisFuture<String> clusterNodes() {
    Span span = helper.buildSpan("clusterNodes");
    return setCompleteAction(commands.clusterNodes(), span);
  }

  @Override
  public RedisFuture<List<String>> clusterSlaves(String nodeId) {
    Span span = helper.buildSpan("clusterSlaves");
    span.setTag("nodeId", nodeId);
    return setCompleteAction(commands.clusterSlaves(nodeId), span);
  }

  @Override
  public RedisFuture<List<K>> clusterGetKeysInSlot(int slot, int count) {
    Span span = helper.buildSpan("clusterGetKeysInSlot");
    span.setTag("slot", slot);
    return setCompleteAction(commands.clusterGetKeysInSlot(slot, count), span);
  }

  @Override
  public RedisFuture<Long> clusterCountKeysInSlot(int slot) {
    Span span = helper.buildSpan("clusterCountKeysInSlot");
    span.setTag("slot", slot);
    return setCompleteAction(commands.clusterCountKeysInSlot(slot), span);
  }

  @Override
  public RedisFuture<Long> clusterCountFailureReports(String nodeId) {
    Span span = helper.buildSpan("clusterCountFailureReports");
    span.setTag("nodeId", nodeId);
    return setCompleteAction(commands.clusterCountFailureReports(nodeId), span);
  }

  @Override
  public RedisFuture<Long> clusterKeyslot(K key) {
    Span span = helper.buildSpan("clusterKeyslot", key);
    return setCompleteAction(commands.clusterKeyslot(key), span);
  }

  @Override
  public RedisFuture<String> clusterSaveconfig() {
    Span span = helper.buildSpan("clusterSaveconfig");
    return setCompleteAction(commands.clusterSaveconfig(), span);
  }

  @Override
  public RedisFuture<String> clusterSetConfigEpoch(long configEpoch) {
    Span span = helper.buildSpan("clusterSetConfigEpoch");
    return setCompleteAction(commands.clusterSetConfigEpoch(configEpoch), span);
  }

  @Override
  public RedisFuture<List<Object>> clusterSlots() {
    Span span = helper.buildSpan("clusterSlots");
    return setCompleteAction(commands.clusterSlots(), span);
  }

  @Override
  public RedisFuture<String> asking() {
    Span span = helper.buildSpan("asking");
    return setCompleteAction(commands.asking(), span);
  }

  @Override
  public RedisFuture<String> clusterReplicate(String nodeId) {
    Span span = helper.buildSpan("clusterReplicate");
    span.setTag("nodeId", nodeId);
    return setCompleteAction(commands.clusterReplicate(nodeId), span);
  }

  @Override
  public RedisFuture<String> clusterFailover(boolean force) {
    Span span = helper.buildSpan("clusterFailover");
    span.setTag("force", force);
    return setCompleteAction(commands.clusterFailover(force), span);
  }

  @Override
  public RedisFuture<String> clusterReset(boolean hard) {
    Span span = helper.buildSpan("clusterReset");
    span.setTag("hard", hard);
    return setCompleteAction(commands.clusterReset(hard), span);
  }

  @Override
  public RedisFuture<String> clusterFlushslots() {
    Span span = helper.buildSpan("clusterFlushslots");
    return setCompleteAction(commands.clusterFlushslots(), span);
  }

  @Override
  public RedisFuture<Long> geoadd(K key, double longitude, double latitude, V member) {
    Span span = helper.buildSpan("geoadd", key);
    span.setTag("longitude", longitude);
    span.setTag("latitude", latitude);
    return setCompleteAction(commands.geoadd(key, longitude, latitude, member), span);
  }

  @Override
  public RedisFuture<Long> geoadd(K key, Object... lngLatMember) {
    Span span = helper.buildSpan("geoadd", key);
    return setCompleteAction(commands.geoadd(key, lngLatMember), span);
  }

  @Override
  public RedisFuture<List<Value<String>>> geohash(K key,
      V... members) {
    Span span = helper.buildSpan("geohash", key);
    return setCompleteAction(commands.geohash(key, members), span);
  }

  @Override
  public RedisFuture<Set<V>> georadius(K key, double longitude,
      double latitude, double distance, Unit unit) {
    Span span = helper.buildSpan("georadius", key);
    span.setTag("longitude", longitude);
    span.setTag("latitude", latitude);
    span.setTag("unit", nullable(unit));
    span.setTag("distance", distance);
    return setCompleteAction(commands.georadius(key, longitude, latitude, distance, unit), span);
  }

  @Override
  public RedisFuture<List<GeoWithin<V>>> georadius(K key,
      double longitude, double latitude, double distance, Unit unit,
      GeoArgs geoArgs) {
    Span span = helper.buildSpan("georadius", key);
    span.setTag("longitude", longitude);
    span.setTag("latitude", latitude);
    span.setTag("unit", nullable(unit));
    span.setTag("distance", distance);
    return setCompleteAction(commands.georadius(key, longitude, latitude, distance, unit, geoArgs),
        span);
  }

  @Override
  public RedisFuture<Long> georadius(K key, double longitude, double latitude,
      double distance, Unit unit,
      GeoRadiusStoreArgs<K> geoRadiusStoreArgs) {
    Span span = helper.buildSpan("georadius", key);
    span.setTag("longitude", longitude);
    span.setTag("latitude", latitude);
    span.setTag("unit", nullable(unit));
    span.setTag("distance", distance);
    return setCompleteAction(
        commands.georadius(key, longitude, latitude, distance, unit, geoRadiusStoreArgs), span);
  }

  @Override
  public RedisFuture<Set<V>> georadiusbymember(K key, V member,
      double distance, Unit unit) {
    Span span = helper.buildSpan("georadiusbymember", key);
    span.setTag("unit", nullable(unit));
    span.setTag("distance", distance);
    return setCompleteAction(commands.georadiusbymember(key, member, distance, unit), span);
  }

  @Override
  public RedisFuture<List<GeoWithin<V>>> georadiusbymember(
      K key, V member, double distance, Unit unit,
      GeoArgs geoArgs) {
    Span span = helper.buildSpan("georadiusbymember", key);
    span.setTag("unit", nullable(unit));
    span.setTag("distance", distance);
    return setCompleteAction(commands.georadiusbymember(key, member, distance, unit, geoArgs),
        span);
  }

  @Override
  public RedisFuture<Long> georadiusbymember(K key, V member, double distance,
      Unit unit, GeoRadiusStoreArgs<K> geoRadiusStoreArgs) {
    Span span = helper.buildSpan("georadiusbymember", key);
    span.setTag("unit", nullable(unit));
    span.setTag("distance", distance);
    return setCompleteAction(
        commands.georadiusbymember(key, member, distance, unit, geoRadiusStoreArgs), span);
  }

  @Override
  public RedisFuture<List<GeoCoordinates>> geopos(K key,
      V... members) {
    Span span = helper.buildSpan("geopos", key);
    return setCompleteAction(commands.geopos(key, members), span);
  }

  @Override
  public RedisFuture<Double> geodist(K key, V from, V to,
      Unit unit) {
    Span span = helper.buildSpan("geodist", key);
    span.setTag("unit", nullable(unit));
    return setCompleteAction(commands.geodist(key, from, to, unit), span);
  }

  @Override
  public RedisFuture<String> discard() {
    Span span = helper.buildSpan("discard");
    return setCompleteAction(commands.discard(), span);
  }

  @Override
  public RedisFuture<TransactionResult> exec() {
    Span span = helper.buildSpan("exec");
    return setCompleteAction(commands.exec(), span);
  }

  @Override
  public RedisFuture<String> multi() {
    Span span = helper.buildSpan("multi");
    return setCompleteAction(commands.multi(), span);
  }

  @Override
  public RedisFuture<String> watch(K... keys) {
    Span span = helper.buildSpan("watch", keys);
    return setCompleteAction(commands.watch(keys), span);
  }

  @Override
  public RedisFuture<String> unwatch() {
    Span span = helper.buildSpan("unwatch");
    return setCompleteAction(commands.unwatch(), span);
  }

  static <V> RedisFuture<V> setCompleteAction(RedisFuture<V> future, Span span) {
    future.whenComplete((v, throwable) -> {
      if (throwable != null) {
        onError(throwable, span);
      }
      span.finish();
    });

    return future;
  }
}
