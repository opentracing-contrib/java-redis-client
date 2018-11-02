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
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.output.KeyStreamingChannel;
import io.lettuce.core.output.KeyValueStreamingChannel;
import io.lettuce.core.output.ScoredValueStreamingChannel;
import io.lettuce.core.output.ValueStreamingChannel;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;
import io.lettuce.core.protocol.ProtocolKeyword;
import io.opentracing.Span;
import io.opentracing.contrib.redis.common.TracingConfiguration;
import io.opentracing.contrib.redis.common.TracingHelper;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class TracingRedisCommands<K, V> implements RedisCommands<K, V> {

  private final RedisCommands<K, V> commands;
  private final TracingHelper helper;
  private final TracingConfiguration tracingConfiguration;

  public TracingRedisCommands(RedisCommands<K, V> commands,
      TracingConfiguration tracingConfiguration) {
    this.commands = commands;
    this.helper = new TracingHelper(tracingConfiguration);
    this.tracingConfiguration = tracingConfiguration;
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
  public String swapdb(int db1, int db2) {
    Span span = helper.buildSpan("swapdb");
    span.setTag("db1", db1);
    span.setTag("db2", db2);
    try {
      return commands.swapdb(db1, db2);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public StatefulRedisConnection<K, V> getStatefulConnection() {
    return new TracingStatefulRedisConnection<>(commands.getStatefulConnection(),
        tracingConfiguration);
  }

  @Override
  public Long hdel(K key, K... fields) {
    Span span = helper.buildSpan("hdel", key);
    try {
      return commands.hdel(key, fields);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Boolean hexists(K key, K field) {
    Span span = helper.buildSpan("hexists", key);
    try {
      return commands.hexists(key, field);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public V hget(K key, K field) {
    Span span = helper.buildSpan("hget", key);
    try {
      return commands.hget(key, field);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long hincrby(K key, K field, long amount) {
    Span span = helper.buildSpan("hincrby", key);
    span.setTag("amount", amount);
    try {
      return commands.hincrby(key, field, amount);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Double hincrbyfloat(K key, K field, double amount) {
    Span span = helper.buildSpan("hincrbyfloat", key);
    span.setTag("amount", amount);
    try {
      return commands.hincrbyfloat(key, field, amount);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Map<K, V> hgetall(K key) {
    Span span = helper.buildSpan("hgetall", key);
    try {
      return commands.hgetall(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long hgetall(KeyValueStreamingChannel<K, V> channel, K key) {
    Span span = helper.buildSpan("hgetall", key);
    try {
      return commands.hgetall(channel, key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<K> hkeys(K key) {
    Span span = helper.buildSpan("hkeys", key);
    try {
      return commands.hkeys(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long hkeys(KeyStreamingChannel<K> channel, K key) {
    Span span = helper.buildSpan("hkeys", key);
    try {
      return commands.hkeys(channel, key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long hlen(K key) {
    Span span = helper.buildSpan("hlen", key);
    try {
      return commands.hlen(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<KeyValue<K, V>> hmget(K key, K... fields) {
    Span span = helper.buildSpan("hmget", key);
    try {
      return commands.hmget(key, fields);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long hmget(KeyValueStreamingChannel<K, V> channel, K key, K... fields) {
    Span span = helper.buildSpan("hmget", key);
    try {
      return commands.hmget(channel, key, fields);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String hmset(K key, Map<K, V> map) {
    Span span = helper.buildSpan("hmset", key);
    try {
      return commands.hmset(key, map);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public MapScanCursor<K, V> hscan(K key) {
    Span span = helper.buildSpan("hscan", key);
    try {
      return commands.hscan(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public MapScanCursor<K, V> hscan(K key, ScanArgs scanArgs) {
    Span span = helper.buildSpan("hscan", key);
    try {
      return commands.hscan(key, scanArgs);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public MapScanCursor<K, V> hscan(K key, ScanCursor scanCursor,
      ScanArgs scanArgs) {
    Span span = helper.buildSpan("hscan", key);
    try {
      return commands.hscan(key, scanCursor, scanArgs);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public MapScanCursor<K, V> hscan(K key, ScanCursor scanCursor) {
    Span span = helper.buildSpan("hscan", key);
    try {
      return commands.hscan(key, scanCursor);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public StreamScanCursor hscan(
      KeyValueStreamingChannel<K, V> channel, K key) {
    Span span = helper.buildSpan("hscan", key);
    try {
      return commands.hscan(channel, key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public StreamScanCursor hscan(
      KeyValueStreamingChannel<K, V> channel, K key,
      ScanArgs scanArgs) {
    Span span = helper.buildSpan("hscan", key);
    try {
      return commands.hscan(channel, key, scanArgs);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public StreamScanCursor hscan(
      KeyValueStreamingChannel<K, V> channel, K key,
      ScanCursor scanCursor, ScanArgs scanArgs) {
    Span span = helper.buildSpan("hscan", key);
    try {
      return commands.hscan(channel, key, scanCursor, scanArgs);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public StreamScanCursor hscan(
      KeyValueStreamingChannel<K, V> channel, K key,
      ScanCursor scanCursor) {
    Span span = helper.buildSpan("hscan", key);
    try {
      return commands.hscan(channel, key, scanCursor);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Boolean hset(K key, K field, V value) {
    Span span = helper.buildSpan("hset", key);
    try {
      return commands.hset(key, field, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Boolean hsetnx(K key, K field, V value) {
    Span span = helper.buildSpan("hsetnx", key);
    try {
      return commands.hsetnx(key, field, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long hstrlen(K key, K field) {
    Span span = helper.buildSpan("hstrlen", key);
    try {
      return commands.hstrlen(key, field);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<V> hvals(K key) {
    Span span = helper.buildSpan("hvals", key);
    try {
      return commands.hvals(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long hvals(ValueStreamingChannel<V> channel, K key) {
    Span span = helper.buildSpan("hvals", key);
    try {
      return commands.hvals(channel, key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long del(K... keys) {
    Span span = helper.buildSpan("del", keys);
    try {
      return commands.del(keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long unlink(K... keys) {
    Span span = helper.buildSpan("unlink", keys);
    try {
      return commands.unlink(keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public byte[] dump(K key) {
    Span span = helper.buildSpan("dump", key);
    try {
      return commands.dump(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long exists(K... keys) {
    Span span = helper.buildSpan("exists", keys);
    try {
      return commands.exists(keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Boolean expire(K key, long seconds) {
    Span span = helper.buildSpan("expire", key);
    span.setTag("seconds", seconds);
    try {
      return commands.expire(key, seconds);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Boolean expireat(K key, Date timestamp) {
    Span span = helper.buildSpan("expireat", key);
    span.setTag("timestamp", nullable(timestamp));
    try {
      return commands.expireat(key, timestamp);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Boolean expireat(K key, long timestamp) {
    Span span = helper.buildSpan("expireat", key);
    span.setTag("timestamp", timestamp);
    try {
      return commands.expireat(key, timestamp);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<K> keys(K pattern) {
    Span span = helper.buildSpan("keys");
    span.setTag("pattern", nullable(pattern));
    try {
      return commands.keys(pattern);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long keys(KeyStreamingChannel<K> channel, K pattern) {
    Span span = helper.buildSpan("keys");
    span.setTag("pattern", nullable(pattern));
    try {
      return commands.keys(channel, pattern);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String migrate(String host, int port, K key, int db, long timeout) {
    Span span = helper.buildSpan("migrate", key);
    span.setTag("host", host);
    span.setTag("port", port);
    span.setTag("db", db);
    span.setTag("timeout", timeout);
    try {
      return commands.migrate(host, port, key, db, timeout);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String migrate(String host, int port, int db, long timeout,
      MigrateArgs<K> migrateArgs) {
    Span span = helper.buildSpan("migrate");
    span.setTag("host", host);
    span.setTag("port", port);
    span.setTag("db", db);
    span.setTag("timeout", timeout);
    try {
      return commands.migrate(host, port, db, timeout, migrateArgs);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Boolean move(K key, int db) {
    Span span = helper.buildSpan("move", key);
    span.setTag("db", db);
    try {
      return commands.move(key, db);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String objectEncoding(K key) {
    Span span = helper.buildSpan("objectEncoding", key);
    try {
      return commands.objectEncoding(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long objectIdletime(K key) {
    Span span = helper.buildSpan("objectIdletime", key);
    try {
      return commands.objectIdletime(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long objectRefcount(K key) {
    Span span = helper.buildSpan("objectRefcount", key);
    try {
      return commands.objectRefcount(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Boolean persist(K key) {
    Span span = helper.buildSpan("persist", key);
    try {
      return commands.persist(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Boolean pexpire(K key, long milliseconds) {
    Span span = helper.buildSpan("pexpire", key);
    span.setTag("milliseconds", milliseconds);
    try {
      return commands.pexpire(key, milliseconds);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Boolean pexpireat(K key, Date timestamp) {
    Span span = helper.buildSpan("pexpireat", key);
    span.setTag("timestamp", timestamp == null ? "null" : timestamp.toString());
    try {
      return commands.pexpireat(key, timestamp);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Boolean pexpireat(K key, long timestamp) {
    Span span = helper.buildSpan("pexpireat", key);
    span.setTag("timestamp", timestamp);
    try {
      return commands.pexpireat(key, timestamp);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long pttl(K key) {
    Span span = helper.buildSpan("pttl", key);
    try {
      return commands.pttl(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public V randomkey() {
    Span span = helper.buildSpan("randomkey");
    try {
      return commands.randomkey();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String rename(K key, K newKey) {
    Span span = helper.buildSpan("rename", key);
    span.setTag("newKey", nullable(newKey));
    try {
      return commands.rename(key, newKey);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Boolean renamenx(K key, K newKey) {
    Span span = helper.buildSpan("renamenx", key);
    span.setTag("newKey", nullable(newKey));
    try {
      return commands.renamenx(key, newKey);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String restore(K key, long ttl, byte[] value) {
    Span span = helper.buildSpan("restore", key);
    span.setTag("ttl", ttl);
    try {
      return commands.restore(key, ttl, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<V> sort(K key) {
    Span span = helper.buildSpan("sort", key);
    try {
      return commands.sort(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long sort(ValueStreamingChannel<V> channel, K key) {
    Span span = helper.buildSpan("sort", key);
    try {
      return commands.sort(channel, key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<V> sort(K key, SortArgs sortArgs) {
    Span span = helper.buildSpan("sort", key);
    try {
      return commands.sort(key, sortArgs);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long sort(ValueStreamingChannel<V> channel, K key,
      SortArgs sortArgs) {
    Span span = helper.buildSpan("sort", key);
    try {
      return commands.sort(channel, key, sortArgs);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long sortStore(K key, SortArgs sortArgs, K destination) {
    Span span = helper.buildSpan("sortStore", key);
    try {
      return commands.sortStore(key, sortArgs, destination);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long touch(K... keys) {
    Span span = helper.buildSpan("touch", keys);
    try {
      return commands.touch(keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long ttl(K key) {
    Span span = helper.buildSpan("ttl", key);
    try {
      return commands.ttl(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String type(K key) {
    Span span = helper.buildSpan("type", key);
    try {
      return commands.type(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public KeyScanCursor<K> scan() {
    Span span = helper.buildSpan("scan");
    try {
      return commands.scan();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public KeyScanCursor<K> scan(ScanArgs scanArgs) {
    Span span = helper.buildSpan("scan");
    try {
      return commands.scan(scanArgs);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public KeyScanCursor<K> scan(ScanCursor scanCursor,
      ScanArgs scanArgs) {
    Span span = helper.buildSpan("scan");
    try {
      return commands.scan(scanCursor, scanArgs);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public KeyScanCursor<K> scan(ScanCursor scanCursor) {
    Span span = helper.buildSpan("scan");
    try {
      return commands.scan(scanCursor);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public StreamScanCursor scan(KeyStreamingChannel<K> channel) {
    Span span = helper.buildSpan("scan");
    try {
      return commands.scan(channel);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public StreamScanCursor scan(KeyStreamingChannel<K> channel,
      ScanArgs scanArgs) {
    Span span = helper.buildSpan("scan");
    try {
      return commands.scan(channel, scanArgs);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public StreamScanCursor scan(KeyStreamingChannel<K> channel,
      ScanCursor scanCursor, ScanArgs scanArgs) {
    Span span = helper.buildSpan("scan");
    try {
      return commands.scan(channel, scanCursor, scanArgs);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public StreamScanCursor scan(KeyStreamingChannel<K> channel,
      ScanCursor scanCursor) {
    Span span = helper.buildSpan("scan");
    try {
      return commands.scan(channel, scanCursor);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long append(K key, V value) {
    Span span = helper.buildSpan("append", key);
    try {
      return commands.append(key, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long bitcount(K key) {
    Span span = helper.buildSpan("bitcount", key);
    try {
      return commands.bitcount(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long bitcount(K key, long start, long end) {
    Span span = helper.buildSpan("bitcount", key);
    span.setTag("start", start);
    span.setTag("end", end);
    try {
      return commands.bitcount(key, start, end);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<Long> bitfield(K key, BitFieldArgs bitFieldArgs) {
    Span span = helper.buildSpan("bitfield", key);
    try {
      return commands.bitfield(key, bitFieldArgs);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long bitpos(K key, boolean state) {
    Span span = helper.buildSpan("bitpos", key);
    span.setTag("state", state);
    try {
      return commands.bitpos(key, state);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long bitpos(K key, boolean state, long start) {
    Span span = helper.buildSpan("bitpos", key);
    span.setTag("state", state);
    span.setTag("start", start);
    try {
      return commands.bitpos(key, state, start);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long bitpos(K key, boolean state, long start, long end) {
    Span span = helper.buildSpan("bitpos", key);
    span.setTag("state", state);
    span.setTag("start", start);
    span.setTag("end", end);
    try {
      return commands.bitpos(key, state, start, end);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long bitopAnd(K destination, K... keys) {
    Span span = helper.buildSpan("bitopAnd", keys);
    try {
      return commands.bitopAnd(destination, keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long bitopNot(K destination, K source) {
    Span span = helper.buildSpan("bitopNot");
    try {
      return commands.bitopNot(destination, source);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long bitopOr(K destination, K... keys) {
    Span span = helper.buildSpan("bitopOr", keys);
    try {
      return commands.bitopOr(destination, keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long bitopXor(K destination, K... keys) {
    Span span = helper.buildSpan("bitopXor", keys);
    try {
      return commands.bitopXor(destination, keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long decr(K key) {
    Span span = helper.buildSpan("decr", key);
    try {
      return commands.decr(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long decrby(K key, long amount) {
    Span span = helper.buildSpan("decrby", key);
    span.setTag("amount", amount);
    try {
      return commands.decrby(key, amount);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public V get(K key) {
    Span span = helper.buildSpan("get", key);
    try {
      return commands.get(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long getbit(K key, long offset) {
    Span span = helper.buildSpan("getbit", key);
    span.setTag("offset", offset);
    try {
      return commands.getbit(key, offset);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public V getrange(K key, long start, long end) {
    Span span = helper.buildSpan("getrange", key);
    span.setTag("start", start);
    span.setTag("end", end);
    try {
      return commands.getrange(key, start, end);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public V getset(K key, V value) {
    Span span = helper.buildSpan("getset", key);
    try {
      return commands.getset(key, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long incr(K key) {
    Span span = helper.buildSpan("incr", key);
    try {
      return commands.incr(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long incrby(K key, long amount) {
    Span span = helper.buildSpan("incrby", key);
    span.setTag("amount", amount);
    try {
      return commands.incrby(key, amount);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Double incrbyfloat(K key, double amount) {
    Span span = helper.buildSpan("incrbyfloat", key);
    span.setTag("amount", amount);
    try {
      return commands.incrbyfloat(key, amount);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<KeyValue<K, V>> mget(K... keys) {
    Span span = helper.buildSpan("mget", keys);
    try {
      return commands.mget(keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long mget(KeyValueStreamingChannel<K, V> channel, K... keys) {
    Span span = helper.buildSpan("mget", keys);
    try {
      return commands.mget(channel, keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String mset(Map<K, V> map) {
    Span span = helper.buildSpan("mset");
    try {
      return commands.mset(map);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Boolean msetnx(Map<K, V> map) {
    Span span = helper.buildSpan("msetnx");
    try {
      return commands.msetnx(map);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String set(K key, V value) {
    Span span = helper.buildSpan("set", key);
    try {
      return commands.set(key, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String set(K key, V value, SetArgs setArgs) {
    Span span = helper.buildSpan("set", key);
    try {
      return commands.set(key, value, setArgs);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long setbit(K key, long offset, int value) {
    Span span = helper.buildSpan("setbit", key);
    span.setTag("offset", offset);
    span.setTag("value", value);
    try {
      return commands.setbit(key, offset, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String setex(K key, long seconds, V value) {
    Span span = helper.buildSpan("setex", key);
    span.setTag("seconds", seconds);
    try {
      return commands.setex(key, seconds, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String psetex(K key, long milliseconds, V value) {
    Span span = helper.buildSpan("psetex", key);
    span.setTag("milliseconds", milliseconds);
    try {
      return commands.psetex(key, milliseconds, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Boolean setnx(K key, V value) {
    Span span = helper.buildSpan("setnx", key);
    try {
      return commands.setnx(key, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long setrange(K key, long offset, V value) {
    Span span = helper.buildSpan("setrange", key);
    span.setTag("offset", offset);
    try {
      return commands.setrange(key, offset, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long strlen(K key) {
    Span span = helper.buildSpan("strlen", key);
    try {
      return commands.strlen(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public KeyValue<K, V> blpop(long timeout, K... keys) {
    Span span = helper.buildSpan("blpop", keys);
    span.setTag("timeout", timeout);
    try {
      return commands.blpop(timeout, keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public KeyValue<K, V> brpop(long timeout, K... keys) {
    Span span = helper.buildSpan("brpop", keys);
    span.setTag("timeout", timeout);
    try {
      return commands.brpop(timeout, keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public V brpoplpush(long timeout, K source, K destination) {
    Span span = helper.buildSpan("brpoplpush");
    span.setTag("timeout", timeout);
    try {
      return commands.brpoplpush(timeout, source, destination);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public V lindex(K key, long index) {
    Span span = helper.buildSpan("lindex", key);
    span.setTag("index", index);
    try {
      return commands.lindex(key, index);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long linsert(K key, boolean before, V pivot, V value) {
    Span span = helper.buildSpan("linsert", key);
    span.setTag("before", before);
    try {
      return commands.linsert(key, before, pivot, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long llen(K key) {
    Span span = helper.buildSpan("llen", key);
    try {
      return commands.llen(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public V lpop(K key) {
    Span span = helper.buildSpan("lpop", key);
    try {
      return commands.lpop(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long lpush(K key, V... values) {
    Span span = helper.buildSpan("lpush", key);
    try {
      return commands.lpush(key, values);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long lpushx(K key, V... values) {
    Span span = helper.buildSpan("lpushx", key);
    try {
      return commands.lpushx(key, values);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<V> lrange(K key, long start, long stop) {
    Span span = helper.buildSpan("lrange", key);
    span.setTag("start", start);
    span.setTag("stop", stop);
    try {
      return commands.lrange(key, start, stop);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long lrange(ValueStreamingChannel<V> channel, K key, long start,
      long stop) {
    Span span = helper.buildSpan("lrange", key);
    span.setTag("start", start);
    span.setTag("stop", stop);
    try {
      return commands.lrange(channel, key, start, stop);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long lrem(K key, long count, V value) {
    Span span = helper.buildSpan("lrem", key);
    span.setTag("count", count);
    try {
      return commands.lrem(key, count, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String lset(K key, long index, V value) {
    Span span = helper.buildSpan("lset", key);
    span.setTag("index", index);
    try {
      return commands.lset(key, index, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String ltrim(K key, long start, long stop) {
    Span span = helper.buildSpan("ltrim", key);
    span.setTag("start", start);
    span.setTag("stop", stop);
    try {
      return commands.ltrim(key, start, stop);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public V rpop(K key) {
    Span span = helper.buildSpan("rpop", key);
    try {
      return commands.rpop(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public V rpoplpush(K source, K destination) {
    Span span = helper.buildSpan("rpoplpush");
    try {
      return commands.rpoplpush(source, destination);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long rpush(K key, V... values) {
    Span span = helper.buildSpan("rpush", key);
    try {
      return commands.rpush(key, values);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long rpushx(K key, V... values) {
    Span span = helper.buildSpan("rpushx", key);
    try {
      return commands.rpushx(key, values);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long sadd(K key, V... members) {
    Span span = helper.buildSpan("sadd", key);
    try {
      return commands.sadd(key, members);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long scard(K key) {
    Span span = helper.buildSpan("scard", key);
    try {
      return commands.scard(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<V> sdiff(K... keys) {
    Span span = helper.buildSpan("sdiff", keys);
    try {
      return commands.sdiff(keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long sdiff(ValueStreamingChannel<V> channel, K... keys) {
    Span span = helper.buildSpan("sdiff", keys);
    try {
      return commands.sdiff(channel, keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long sdiffstore(K destination, K... keys) {
    Span span = helper.buildSpan("sdiffstore", keys);
    try {
      return commands.sdiffstore(destination, keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<V> sinter(K... keys) {
    Span span = helper.buildSpan("sinter", keys);
    try {
      return commands.sinter(keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long sinter(ValueStreamingChannel<V> channel, K... keys) {
    Span span = helper.buildSpan("sinter", keys);
    try {
      return commands.sinter(channel, keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long sinterstore(K destination, K... keys) {
    Span span = helper.buildSpan("sinterstore", keys);
    try {
      return commands.sinterstore(destination, keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Boolean sismember(K key, V member) {
    Span span = helper.buildSpan("sismember", key);
    try {
      return commands.sismember(key, member);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Boolean smove(K source, K destination, V member) {
    Span span = helper.buildSpan("smove");
    try {
      return commands.smove(source, destination, member);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<V> smembers(K key) {
    Span span = helper.buildSpan("smembers", key);
    try {
      return commands.smembers(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long smembers(ValueStreamingChannel<V> channel, K key) {
    Span span = helper.buildSpan("smembers", key);
    try {
      return commands.smembers(channel, key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public V spop(K key) {
    Span span = helper.buildSpan("spop", key);
    try {
      return commands.spop(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<V> spop(K key, long count) {
    Span span = helper.buildSpan("spop", key);
    span.setTag("count", count);
    try {
      return commands.spop(key, count);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public V srandmember(K key) {
    Span span = helper.buildSpan("srandmember", key);
    try {
      return commands.srandmember(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<V> srandmember(K key, long count) {
    Span span = helper.buildSpan("srandmember", key);
    span.setTag("count", count);
    try {
      return commands.srandmember(key, count);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long srandmember(ValueStreamingChannel<V> channel, K key, long count) {
    Span span = helper.buildSpan("srandmember", key);
    span.setTag("count", count);
    try {
      return commands.srandmember(channel, key, count);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long srem(K key, V... members) {
    Span span = helper.buildSpan("srem", key);
    try {
      return commands.srem(key, members);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<V> sunion(K... keys) {
    Span span = helper.buildSpan("sunion", keys);
    try {
      return commands.sunion(keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long sunion(ValueStreamingChannel<V> channel, K... keys) {
    Span span = helper.buildSpan("sunion", keys);
    try {
      return commands.sunion(channel, keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long sunionstore(K destination, K... keys) {
    Span span = helper.buildSpan("sunionstore", keys);
    try {
      return commands.sunionstore(destination, keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public ValueScanCursor<V> sscan(K key) {
    Span span = helper.buildSpan("sscan", key);
    try {
      return commands.sscan(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public ValueScanCursor<V> sscan(K key, ScanArgs scanArgs) {
    Span span = helper.buildSpan("sscan", key);
    try {
      return commands.sscan(key, scanArgs);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public ValueScanCursor<V> sscan(K key, ScanCursor scanCursor,
      ScanArgs scanArgs) {
    Span span = helper.buildSpan("sscan", key);
    try {
      return commands.sscan(key, scanCursor, scanArgs);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public ValueScanCursor<V> sscan(K key, ScanCursor scanCursor) {
    Span span = helper.buildSpan("sscan", key);
    try {
      return commands.sscan(key, scanCursor);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public StreamScanCursor sscan(
      ValueStreamingChannel<V> channel, K key) {
    Span span = helper.buildSpan("sscan", key);
    try {
      return commands.sscan(channel, key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public StreamScanCursor sscan(
      ValueStreamingChannel<V> channel, K key,
      ScanArgs scanArgs) {
    Span span = helper.buildSpan("sscan", key);
    try {
      return commands.sscan(channel, key, scanArgs);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public StreamScanCursor sscan(
      ValueStreamingChannel<V> channel, K key,
      ScanCursor scanCursor, ScanArgs scanArgs) {
    Span span = helper.buildSpan("sscan", key);
    try {
      return commands.sscan(channel, key, scanCursor, scanArgs);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public StreamScanCursor sscan(
      ValueStreamingChannel<V> channel, K key,
      ScanCursor scanCursor) {
    Span span = helper.buildSpan("sscan", key);
    try {
      return commands.sscan(channel, key, scanCursor);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zadd(K key, double score, V member) {
    Span span = helper.buildSpan("zadd", key);
    span.setTag("score", score);
    try {
      return commands.zadd(key, score, member);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zadd(K key, Object... scoresAndValues) {
    Span span = helper.buildSpan("zadd", key);
    try {
      return commands.zadd(key, scoresAndValues);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zadd(K key, ScoredValue<V>... scoredValues) {
    Span span = helper.buildSpan("zadd", key);
    try {
      return commands.zadd(key, scoredValues);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zadd(K key, ZAddArgs zAddArgs, double score, V member) {
    Span span = helper.buildSpan("zadd", key);
    span.setTag("score", score);
    try {
      return commands.zadd(key, zAddArgs, score, member);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zadd(K key, ZAddArgs zAddArgs, Object... scoresAndValues) {
    Span span = helper.buildSpan("zadd", key);
    try {
      return commands.zadd(key, zAddArgs, scoresAndValues);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zadd(K key, ZAddArgs zAddArgs,
      ScoredValue<V>... scoredValues) {
    Span span = helper.buildSpan("zadd", key);
    try {
      return commands.zadd(key, zAddArgs, scoredValues);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Double zaddincr(K key, double score, V member) {
    Span span = helper.buildSpan("zaddincr", key);
    span.setTag("score", score);
    try {
      return commands.zaddincr(key, score, member);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Double zaddincr(K key, ZAddArgs zAddArgs, double score, V member) {
    Span span = helper.buildSpan("zaddincr", key);
    span.setTag("score", score);
    try {
      return commands.zaddincr(key, zAddArgs, score, member);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zcard(K key) {
    Span span = helper.buildSpan("zcard", key);
    try {
      return commands.zcard(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Deprecated
  public Long zcount(K key, double min, double max) {
    Span span = helper.buildSpan("zcount", key);
    span.setTag("min", min);
    span.setTag("max", max);
    try {
      return commands.zcount(key, min, max);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Deprecated
  public Long zcount(K key, String min, String max) {
    Span span = helper.buildSpan("zcount", key);
    span.setTag("min", min);
    span.setTag("max", max);
    try {
      return commands.zcount(key, min, max);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zcount(K key, Range<? extends Number> range) {
    Span span = helper.buildSpan("zcount", key);
    try {
      return commands.zcount(key, range);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Double zincrby(K key, double amount, K member) {
    Span span = helper.buildSpan("zincrby", key);
    span.setTag("amount", amount);
    try {
      return commands.zincrby(key, amount, member);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zinterstore(K destination, K... keys) {
    Span span = helper.buildSpan("zinterstore", keys);
    try {
      return commands.zinterstore(destination, keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zinterstore(K destination, ZStoreArgs storeArgs, K... keys) {
    Span span = helper.buildSpan("zinterstore", keys);
    try {
      return commands.zinterstore(destination, storeArgs, keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Deprecated
  public Long zlexcount(K key, String min, String max) {
    Span span = helper.buildSpan("zlexcount", key);
    span.setTag("min", min);
    span.setTag("max", max);
    try {
      return commands.zlexcount(key, min, max);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zlexcount(K key, Range<? extends V> range) {
    Span span = helper.buildSpan("zlexcount", key);
    span.setTag("range", range == null ? "null" : range.toString());
    try {
      return commands.zlexcount(key, range);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<V> zrange(K key, long start, long stop) {
    Span span = helper.buildSpan("zrange", key);
    span.setTag("start", start);
    span.setTag("stop", stop);
    try {
      return commands.zrange(key, start, stop);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zrange(ValueStreamingChannel<V> channel, K key, long start,
      long stop) {
    Span span = helper.buildSpan("zrange", key);
    span.setTag("start", start);
    span.setTag("stop", stop);
    try {
      return commands.zrange(channel, key, start, stop);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<ScoredValue<V>> zrangeWithScores(K key, long start, long stop) {
    Span span = helper.buildSpan("zrangeWithScores", key);
    span.setTag("start", start);
    span.setTag("stop", stop);
    try {
      return commands.zrangeWithScores(key, start, stop);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zrangeWithScores(ScoredValueStreamingChannel<V> channel, K key,
      long start, long stop) {
    Span span = helper.buildSpan("zrangeWithScores", key);
    span.setTag("start", start);
    span.setTag("stop", stop);
    try {
      return commands.zrangeWithScores(channel, key, start, stop);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Deprecated
  public List<V> zrangebylex(K key, String min, String max) {
    Span span = helper.buildSpan("zrangebylex", key);
    span.setTag("min", min);
    span.setTag("max", max);
    try {
      return commands.zrangebylex(key, min, max);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<V> zrangebylex(K key, Range<? extends V> range) {
    Span span = helper.buildSpan("zrangebylex", key);
    span.setTag("range", range == null ? "null" : range.toString());
    try {
      return commands.zrangebylex(key, range);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Deprecated
  public List<V> zrangebylex(K key, String min, String max, long offset, long count) {
    Span span = helper.buildSpan("zrangebylex", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    try {
      return commands.zrangebylex(key, min, max, offset, count);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<V> zrangebylex(K key, Range<? extends V> range,
      Limit limit) {
    Span span = helper.buildSpan("zrangebylex", key);
    span.setTag("range", range == null ? "null" : range.toString());
    span.setTag("limit", limit == null ? "null" : limit.toString());
    try {
      return commands.zrangebylex(key, range, limit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Deprecated
  public List<V> zrangebyscore(K key, double min, double max) {
    Span span = helper.buildSpan("zrangebyscore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    try {
      return commands.zrangebyscore(key, min, max);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Deprecated
  public List<V> zrangebyscore(K key, String min, String max) {
    Span span = helper.buildSpan("zrangebyscore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    try {
      return commands.zrangebyscore(key, min, max);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<V> zrangebyscore(K key,
      Range<? extends Number> range) {
    Span span = helper.buildSpan("zrangebyscore", key);
    span.setTag("range", range == null ? "null" : range.toString());
    try {
      return commands.zrangebyscore(key, range);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Deprecated
  public List<V> zrangebyscore(K key, double min, double max, long offset, long count) {
    Span span = helper.buildSpan("zrangebyscore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    try {
      return commands.zrangebyscore(key, min, max, offset, count);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Deprecated
  public List<V> zrangebyscore(K key, String min, String max, long offset, long count) {
    Span span = helper.buildSpan("zrangebyscore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    try {
      return commands.zrangebyscore(key, min, max, offset, count);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<V> zrangebyscore(K key,
      Range<? extends Number> range, Limit limit) {
    Span span = helper.buildSpan("zrangebyscore", key);
    span.setTag("range", range == null ? "null" : range.toString());
    span.setTag("limit", limit == null ? "null" : limit.toString());
    try {
      return commands.zrangebyscore(key, range, limit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Deprecated
  public Long zrangebyscore(ValueStreamingChannel<V> channel, K key,
      double min, double max) {
    Span span = helper.buildSpan("zrangebyscore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    try {
      return commands.zrangebyscore(channel, key, min, max);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Deprecated
  public Long zrangebyscore(ValueStreamingChannel<V> channel, K key,
      String min, String max) {
    Span span = helper.buildSpan("zrangebyscore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    try {
      return commands.zrangebyscore(channel, key, min, max);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zrangebyscore(ValueStreamingChannel<V> channel, K key,
      Range<? extends Number> range) {
    Span span = helper.buildSpan("zrangebyscore", key);
    span.setTag("range", range == null ? "null" : range.toString());
    try {
      return commands.zrangebyscore(channel, key, range);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Deprecated
  public Long zrangebyscore(ValueStreamingChannel<V> channel, K key,
      double min, double max, long offset, long count) {
    Span span = helper.buildSpan("zrangebyscore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    try {
      return commands.zrangebyscore(channel, key, min, max, offset, count);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Deprecated
  public Long zrangebyscore(ValueStreamingChannel<V> channel, K key,
      String min, String max, long offset, long count) {
    Span span = helper.buildSpan("zrangebyscore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    try {
      return commands.zrangebyscore(channel, key, min, max, offset, count);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zrangebyscore(ValueStreamingChannel<V> channel, K key,
      Range<? extends Number> range, Limit limit) {
    Span span = helper.buildSpan("zrangebyscore", key);
    span.setTag("range", range == null ? "null" : range.toString());
    span.setTag("limit", limit == null ? "null" : limit.toString());
    try {
      return commands.zrangebyscore(channel, key, range, limit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Deprecated
  public List<ScoredValue<V>> zrangebyscoreWithScores(K key, double min,
      double max) {
    Span span = helper.buildSpan("zrangebyscoreWithScores", key);
    span.setTag("min", min);
    span.setTag("max", max);
    try {
      return commands.zrangebyscoreWithScores(key, min, max);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Deprecated
  public List<ScoredValue<V>> zrangebyscoreWithScores(K key,
      String min, String max) {
    Span span = helper.buildSpan("zrangebyscoreWithScores", key);
    span.setTag("min", min);
    span.setTag("max", max);
    try {
      return commands.zrangebyscoreWithScores(key, min, max);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<ScoredValue<V>> zrangebyscoreWithScores(K key,
      Range<? extends Number> range) {
    Span span = helper.buildSpan("zrangebyscoreWithScores", key);
    span.setTag("range", range == null ? "null" : range.toString());
    try {
      return commands.zrangebyscoreWithScores(key, range);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Deprecated
  public List<ScoredValue<V>> zrangebyscoreWithScores(K key, double min,
      double max, long offset, long count) {
    Span span = helper.buildSpan("zrangebyscoreWithScores", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    try {
      return commands.zrangebyscoreWithScores(key, min, max, offset, count);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Deprecated
  public List<ScoredValue<V>> zrangebyscoreWithScores(K key,
      String min, String max, long offset, long
      count) {
    Span span = helper.buildSpan("zrangebyscoreWithScores", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    try {
      return commands.zrangebyscoreWithScores(key, min, max, offset, count);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<ScoredValue<V>> zrangebyscoreWithScores(K key,
      Range<? extends Number> range, Limit
      limit) {
    Span span = helper.buildSpan("zrangebyscoreWithScores", key);
    span.setTag("range", range == null ? "null" : range.toString());
    span.setTag("limit", limit == null ? "null" : limit.toString());
    try {
      return commands.zrangebyscoreWithScores(key, range, limit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Deprecated
  public Long zrangebyscoreWithScores(ScoredValueStreamingChannel<V> channel,
      K key, double min, double max) {
    Span span = helper.buildSpan("zrangebyscoreWithScores", key);
    span.setTag("min", min);
    span.setTag("max", max);
    try {
      return commands.zrangebyscoreWithScores(channel, key, min, max);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Deprecated
  public Long zrangebyscoreWithScores(ScoredValueStreamingChannel<V> channel,
      K key, String min, String max) {
    Span span = helper.buildSpan("zrangebyscoreWithScores", key);
    span.setTag("min", min);
    span.setTag("max", max);
    try {
      return commands.zrangebyscoreWithScores(channel, key, min, max);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zrangebyscoreWithScores(ScoredValueStreamingChannel<V> channel,
      K key, Range<? extends Number> range) {
    Span span = helper.buildSpan("zrangebyscoreWithScores", key);
    span.setTag("range", range == null ? "null" : range.toString());
    try {
      return commands.zrangebyscoreWithScores(channel, key, range);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Deprecated
  public Long zrangebyscoreWithScores(ScoredValueStreamingChannel<V> channel,
      K key, double min, double max, long offset, long count) {
    Span span = helper.buildSpan("zrangebyscoreWithScores", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    try {
      return commands.zrangebyscoreWithScores(channel, key, min, max, offset, count);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Deprecated
  public Long zrangebyscoreWithScores(ScoredValueStreamingChannel<V> channel,
      K key, String min, String max, long offset, long count) {
    Span span = helper.buildSpan("zrangebyscoreWithScores", key);
    span.setTag("min", min);
    span.setTag("max", max);
    span.setTag("offset", offset);
    span.setTag("count", count);
    try {
      return commands.zrangebyscoreWithScores(channel, key, min, max, offset, count);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zrangebyscoreWithScores(ScoredValueStreamingChannel<V> channel,
      K key, Range<? extends Number> range, Limit limit) {
    Span span = helper.buildSpan("zrangebyscoreWithScores", key);
    span.setTag("range", nullable(range));
    span.setTag("limit", nullable(limit));
    try {
      return commands.zrangebyscoreWithScores(channel, key, range, limit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zrank(K key, V member) {
    Span span = helper.buildSpan("zrank", key);
    try {
      return commands.zrank(key, member);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zrem(K key, V... members) {
    Span span = helper.buildSpan("zrem", key);
    try {
      return commands.zrem(key, members);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Deprecated
  public Long zremrangebylex(K key, String min, String max) {
    Span span = helper.buildSpan("zremrangebylex", key);
    span.setTag("min", min);
    span.setTag("max", max);
    try {
      return commands.zremrangebylex(key, min, max);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zremrangebylex(K key, Range<? extends V> range) {
    Span span = helper.buildSpan("zremrangebylex", key);
    span.setTag("range", nullable(range));
    try {
      return commands.zremrangebylex(key, range);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zremrangebyrank(K key, long start, long stop) {
    Span span = helper.buildSpan("zremrangebyrank", key);
    span.setTag("start", start);
    span.setTag("stop", stop);
    try {
      return commands.zremrangebyrank(key, start, stop);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Deprecated
  public Long zremrangebyscore(K key, double min, double max) {
    Span span = helper.buildSpan("zremrangebyscore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    try {
      return commands.zremrangebyscore(key, min, max);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Deprecated
  public Long zremrangebyscore(K key, String min, String max) {
    Span span = helper.buildSpan("zremrangebyscore", key);
    span.setTag("min", min);
    span.setTag("max", max);
    try {
      return commands.zremrangebyscore(key, min, max);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zremrangebyscore(K key, Range<? extends Number> range) {
    Span span = helper.buildSpan("zremrangebyscore", key);
    span.setTag("range", nullable(range));
    try {
      return commands.zremrangebyscore(key, range);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<V> zrevrange(K key, long start, long stop) {
    Span span = helper.buildSpan("zrevrange", key);
    span.setTag("start", start);
    span.setTag("stop", stop);
    try {
      return commands.zrevrange(key, start, stop);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zrevrange(ValueStreamingChannel<V> channel, K key, long start,
      long stop) {
    Span span = helper.buildSpan("zrevrange", key);
    span.setTag("start", start);
    span.setTag("stop", stop);
    try {
      return commands.zrevrange(channel, key, start, stop);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<ScoredValue<V>> zrevrangeWithScores(K key, long start,
      long stop) {
    Span span = helper.buildSpan("zrevrangeWithScores", key);
    span.setTag("start", start);
    span.setTag("stop", stop);
    try {
      return commands.zrevrangeWithScores(key, start, stop);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zrevrangeWithScores(ScoredValueStreamingChannel<V> channel,
      K key, long start, long stop) {
    Span span = helper.buildSpan("zrevrangeWithScores", key);
    span.setTag("start", start);
    span.setTag("stop", stop);
    try {
      return commands.zrevrangeWithScores(channel, key, start, stop);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<V> zrevrangebylex(K key, Range<? extends V> range) {
    Span span = helper.buildSpan("zrevrangebylex", key);
    span.setTag("range", nullable(range));
    try {
      return commands.zrevrangebylex(key, range);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<V> zrevrangebylex(K key, Range<? extends V> range,
      Limit limit) {
    Span span = helper.buildSpan("zrevrangebylex", key);
    span.setTag("range", nullable(range));
    span.setTag("limit", nullable(limit));
    try {
      return commands.zrevrangebylex(key, range, limit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Deprecated
  public List<V> zrevrangebyscore(K key, double max, double min) {
    Span span = helper.buildSpan("zrevrangebyscore", key);
    span.setTag("max", max);
    span.setTag("min", min);
    try {
      return commands.zrevrangebyscore(key, max, min);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Deprecated
  public List<V> zrevrangebyscore(K key, String max, String min) {
    Span span = helper.buildSpan("zrevrangebyscore", key);
    span.setTag("max", max);
    span.setTag("min", min);
    try {
      return commands.zrevrangebyscore(key, max, min);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<V> zrevrangebyscore(K key,
      Range<? extends Number> range) {
    Span span = helper.buildSpan("zrevrangebyscore", key);
    span.setTag("range", nullable(range));
    try {
      return commands.zrevrangebyscore(key, range);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Deprecated
  public List<V> zrevrangebyscore(K key, double max, double min, long offset, long count) {
    Span span = helper.buildSpan("zrevrangebyscore", key);
    span.setTag("max", max);
    span.setTag("min", min);
    span.setTag("offset", offset);
    span.setTag("count", count);
    try {
      return commands.zrevrangebyscore(key, max, min, offset, count);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Deprecated
  public List<V> zrevrangebyscore(K key, String max, String min, long offset, long count) {
    Span span = helper.buildSpan("zrevrangebyscore", key);
    span.setTag("max", max);
    span.setTag("min", min);
    span.setTag("offset", offset);
    span.setTag("count", count);
    try {
      return commands.zrevrangebyscore(key, max, min, offset, count);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<V> zrevrangebyscore(K key,
      Range<? extends Number> range, Limit limit) {
    Span span = helper.buildSpan("zrevrangebyscore", key);
    span.setTag("range", nullable(range));
    span.setTag("limit", nullable(limit));
    try {
      return commands.zrevrangebyscore(key, range, limit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Deprecated
  public Long zrevrangebyscore(ValueStreamingChannel<V> channel, K key,
      double max, double min) {
    Span span = helper.buildSpan("zrevrangebyscore", key);
    span.setTag("max", max);
    span.setTag("min", min);
    try {
      return commands.zrevrangebyscore(channel, key, max, min);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Deprecated
  public Long zrevrangebyscore(ValueStreamingChannel<V> channel, K key,
      String max, String min) {
    Span span = helper.buildSpan("zrevrangebyscore", key);
    span.setTag("max", max);
    span.setTag("min", min);
    try {
      return commands.zrevrangebyscore(channel, key, max, min);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zrevrangebyscore(ValueStreamingChannel<V> channel, K key,
      Range<? extends Number> range) {
    Span span = helper.buildSpan("zrevrangebyscore", key);
    span.setTag("range", nullable(range));
    try {
      return commands.zrevrangebyscore(channel, key, range);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Deprecated
  public Long zrevrangebyscore(ValueStreamingChannel<V> channel, K key,
      double max, double min, long offset, long count) {
    Span span = helper.buildSpan("zrevrangebyscore", key);
    span.setTag("max", max);
    span.setTag("min", min);
    span.setTag("offset", offset);
    span.setTag("count", count);
    try {
      return commands.zrevrangebyscore(channel, key, max, min, offset, count);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Deprecated
  public Long zrevrangebyscore(ValueStreamingChannel<V> channel, K key,
      String max, String min, long offset, long count) {
    Span span = helper.buildSpan("zrevrangebyscore", key);
    span.setTag("max", max);
    span.setTag("min", min);
    span.setTag("offset", offset);
    span.setTag("count", count);
    try {
      return commands.zrevrangebyscore(channel, key, max, min, offset, count);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zrevrangebyscore(ValueStreamingChannel<V> channel, K key,
      Range<? extends Number> range, Limit limit) {
    Span span = helper.buildSpan("zrevrangebyscore", key);
    span.setTag("range", nullable(range));
    span.setTag("limit", nullable(limit));
    try {
      return commands.zrevrangebyscore(channel, key, range, limit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Deprecated
  public List<ScoredValue<V>> zrevrangebyscoreWithScores(K key, double max,
      double min) {
    Span span = helper.buildSpan("zrevrangebyscoreWithScores", key);
    span.setTag("max", max);
    span.setTag("min", min);
    try {
      return commands.zrevrangebyscoreWithScores(key, max, min);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Deprecated
  public List<ScoredValue<V>> zrevrangebyscoreWithScores(K key,
      String max, String min) {
    Span span = helper.buildSpan("zrevrangebyscoreWithScores", key);
    span.setTag("max", max);
    span.setTag("min", min);
    try {
      return commands.zrevrangebyscoreWithScores(key, max, min);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<ScoredValue<V>> zrevrangebyscoreWithScores(K key,
      Range<? extends Number> range) {
    Span span = helper.buildSpan("zrevrangebyscoreWithScores", key);
    span.setTag("range", nullable(range));
    try {
      return commands.zrevrangebyscoreWithScores(key, range);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Deprecated
  public List<ScoredValue<V>> zrevrangebyscoreWithScores(K key, double max,
      double min, long offset, long count) {
    Span span = helper.buildSpan("zrevrangebyscoreWithScores", key);
    span.setTag("max", max);
    span.setTag("min", min);
    span.setTag("offset", offset);
    span.setTag("count", count);
    try {
      return commands.zrevrangebyscoreWithScores(key, max, min, offset, count);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Deprecated
  public List<ScoredValue<V>> zrevrangebyscoreWithScores(K key,
      String max, String min, long offset,
      long count) {
    Span span = helper.buildSpan("zrevrangebyscoreWithScores", key);
    span.setTag("max", max);
    span.setTag("min", min);
    span.setTag("offset", offset);
    span.setTag("count", count);
    try {
      return commands.zrevrangebyscoreWithScores(key, max, min, offset, count);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<ScoredValue<V>> zrevrangebyscoreWithScores(K key,
      Range<? extends Number> range, Limit
      limit) {
    Span span = helper.buildSpan("zrevrangebyscoreWithScores", key);
    span.setTag("range", nullable(range));
    span.setTag("limit", nullable(limit));
    try {
      return commands.zrevrangebyscoreWithScores(key, range, limit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Deprecated
  public Long zrevrangebyscoreWithScores(
      ScoredValueStreamingChannel<V> channel, K key, double max, double min) {
    Span span = helper.buildSpan("zrevrangebyscoreWithScores", key);
    span.setTag("max", max);
    span.setTag("min", min);
    try {
      return commands.zrevrangebyscoreWithScores(channel, key, max, min);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Deprecated
  public Long zrevrangebyscoreWithScores(
      ScoredValueStreamingChannel<V> channel, K key, String max,
      String min) {
    Span span = helper.buildSpan("zrevrangebyscoreWithScores", key);
    span.setTag("max", max);
    span.setTag("min", min);
    try {
      return commands.zrevrangebyscoreWithScores(channel, key, max, min);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zrevrangebyscoreWithScores(
      ScoredValueStreamingChannel<V> channel, K key,
      Range<? extends Number> range) {
    Span span = helper.buildSpan("zrevrangebyscoreWithScores", key);
    span.setTag("range", nullable(range));
    try {
      return commands.zrevrangebyscoreWithScores(channel, key, range);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Deprecated
  public Long zrevrangebyscoreWithScores(
      ScoredValueStreamingChannel<V> channel, K key, double max, double min,
      long offset, long count) {
    Span span = helper.buildSpan("zrevrangebyscoreWithScores", key);
    span.setTag("max", max);
    span.setTag("min", min);
    span.setTag("offset", offset);
    span.setTag("count", count);
    try {
      return commands.zrevrangebyscoreWithScores(channel, key, max, min, offset, count);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Deprecated
  public Long zrevrangebyscoreWithScores(
      ScoredValueStreamingChannel<V> channel, K key, String max,
      String min, long offset, long count) {
    Span span = helper.buildSpan("zrevrangebyscoreWithScores", key);
    span.setTag("max", max);
    span.setTag("min", min);
    span.setTag("offset", offset);
    span.setTag("count", count);
    try {
      return commands.zrevrangebyscoreWithScores(channel, key, max, min, offset, count);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zrevrangebyscoreWithScores(
      ScoredValueStreamingChannel<V> channel, K key,
      Range<? extends Number> range, Limit limit) {
    Span span = helper.buildSpan("zrevrangebyscoreWithScores", key);
    span.setTag("range", nullable(range));
    span.setTag("limit", nullable(limit));
    try {
      return commands.zrevrangebyscoreWithScores(channel, key, range, limit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zrevrank(K key, V member) {
    Span span = helper.buildSpan("zrevrank", key);
    try {
      return commands.zrevrank(key, member);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public ScoredValueScanCursor<V> zscan(K key) {
    Span span = helper.buildSpan("zscan", key);
    try {
      return commands.zscan(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public ScoredValueScanCursor<V> zscan(K key, ScanArgs scanArgs) {
    Span span = helper.buildSpan("zscan", key);
    try {
      return commands.zscan(key, scanArgs);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public ScoredValueScanCursor<V> zscan(K key, ScanCursor scanCursor,
      ScanArgs scanArgs) {
    Span span = helper.buildSpan("zscan", key);
    try {
      return commands.zscan(key, scanCursor, scanArgs);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public ScoredValueScanCursor<V> zscan(K key, ScanCursor scanCursor) {
    Span span = helper.buildSpan("zscan", key);
    try {
      return commands.zscan(key, scanCursor);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public StreamScanCursor zscan(
      ScoredValueStreamingChannel<V> channel, K key) {
    Span span = helper.buildSpan("zscan", key);
    try {
      return commands.zscan(channel, key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public StreamScanCursor zscan(
      ScoredValueStreamingChannel<V> channel, K key,
      ScanArgs scanArgs) {
    Span span = helper.buildSpan("zscan", key);
    try {
      return commands.zscan(channel, key, scanArgs);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public StreamScanCursor zscan(
      ScoredValueStreamingChannel<V> channel, K key,
      ScanCursor scanCursor, ScanArgs scanArgs) {
    Span span = helper.buildSpan("zscan", key);
    try {
      return commands.zscan(channel, key, scanCursor, scanArgs);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public StreamScanCursor zscan(
      ScoredValueStreamingChannel<V> channel, K key,
      ScanCursor scanCursor) {
    Span span = helper.buildSpan("zscan", key);
    try {
      return commands.zscan(channel, key, scanCursor);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Double zscore(K key, V member) {
    Span span = helper.buildSpan("zscore", key);
    try {
      return commands.zscore(key, member);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zunionstore(K destination, K... keys) {
    Span span = helper.buildSpan("zunionstore", keys);
    try {
      return commands.zunionstore(destination, keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long zunionstore(K destination, ZStoreArgs storeArgs, K... keys) {
    Span span = helper.buildSpan("zunionstore", keys);
    try {
      return commands.zunionstore(destination, storeArgs, keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <T> T eval(String script, ScriptOutputType type, K... keys) {
    Span span = helper.buildSpan("eval", keys);
    try {
      return commands.eval(script, type, keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <T> T eval(String script, ScriptOutputType type, K[] keys, V... values) {
    Span span = helper.buildSpan("eval", keys);
    try {
      return commands.eval(script, type, keys, values);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <T> T evalsha(String digest, ScriptOutputType type, K... keys) {
    Span span = helper.buildSpan("evalsha", keys);
    try {
      return commands.evalsha(digest, type, keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <T> T evalsha(String digest, ScriptOutputType type, K[] keys, V... values) {
    Span span = helper.buildSpan("evalsha", keys);
    try {
      return commands.evalsha(digest, type, keys, values);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<Boolean> scriptExists(String... digests) {
    Span span = helper.buildSpan("scriptExists");
    try {
      return commands.scriptExists(digests);
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
      return commands.scriptFlush();
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
      return commands.scriptKill();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String scriptLoad(V script) {
    Span span = helper.buildSpan("scriptLoad");
    try {
      return commands.scriptLoad(script);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
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
  public String bgrewriteaof() {
    Span span = helper.buildSpan("bgrewriteaof");
    try {
      return commands.bgrewriteaof();
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
      return commands.bgsave();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public K clientGetname() {
    Span span = helper.buildSpan("clientGetname");
    try {
      return commands.clientGetname();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String clientSetname(K name) {
    Span span = helper.buildSpan("clientSetname");
    try {
      return commands.clientSetname(name);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String clientKill(String addr) {
    Span span = helper.buildSpan("clientKill");
    span.setTag("addr", addr);
    try {
      return commands.clientKill(addr);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long clientKill(KillArgs killArgs) {
    Span span = helper.buildSpan("clientKill");
    try {
      return commands.clientKill(killArgs);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String clientPause(long timeout) {
    Span span = helper.buildSpan("clientPause");
    span.setTag("timeout", timeout);
    try {
      return commands.clientPause(timeout);
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
      return commands.clientList();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<Object> command() {
    Span span = helper.buildSpan("command");
    try {
      return commands.command();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<Object> commandInfo(String... commands) {
    Span span = helper.buildSpan("commandInfo");
    try {
      return this.commands.commandInfo(commands);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<Object> commandInfo(CommandType... commands) {
    Span span = helper.buildSpan("commandInfo");
    try {
      return this.commands.commandInfo(commands);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long commandCount() {
    Span span = helper.buildSpan("commandCount");
    try {
      return commands.commandCount();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Map<String, String> configGet(String parameter) {
    Span span = helper.buildSpan("configGet");
    span.setTag("parameter", parameter);
    try {
      return commands.configGet(parameter);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String configResetstat() {
    Span span = helper.buildSpan("configResetstat");
    try {
      return commands.configResetstat();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String configRewrite() {
    Span span = helper.buildSpan("configRewrite");
    try {
      return commands.configRewrite();
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
    span.setTag("parameter", parameter);
    try {
      return commands.configSet(parameter, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long dbsize() {
    Span span = helper.buildSpan("dbsize");
    try {
      return commands.dbsize();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String debugCrashAndRecover(Long delay) {
    Span span = helper.buildSpan("debugCrashAndRecover");
    span.setTag("delay", nullable(delay));
    try {
      return commands.debugCrashAndRecover(delay);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String debugHtstats(int db) {
    Span span = helper.buildSpan("debugHtstats");
    span.setTag("db", db);
    try {
      return commands.debugHtstats(db);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String debugObject(K key) {
    Span span = helper.buildSpan("debugObject", key);
    try {
      return commands.debugObject(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public void debugOom() {
    Span span = helper.buildSpan("debugOom");
    try {
      commands.debugOom();
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
  public String debugReload() {
    Span span = helper.buildSpan("debugReload");
    try {
      return commands.debugReload();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String debugRestart(Long delay) {
    Span span = helper.buildSpan("debugRestart");
    span.setTag("delay", nullable(delay));
    try {
      return commands.debugRestart(delay);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String debugSdslen(K key) {
    Span span = helper.buildSpan("debugSdslen", key);
    try {
      return commands.debugSdslen(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String flushall() {
    Span span = helper.buildSpan("flushall");
    try {
      return commands.flushall();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String flushallAsync() {
    Span span = helper.buildSpan("flushallAsync");
    try {
      return commands.flushallAsync();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String flushdb() {
    Span span = helper.buildSpan("flushdb");
    try {
      return commands.flushdb();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String flushdbAsync() {
    Span span = helper.buildSpan("flushdbAsync");
    try {
      return commands.flushdbAsync();
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
      return commands.info();
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
      return commands.info(section);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Date lastsave() {
    Span span = helper.buildSpan("lastsave");
    try {
      return commands.lastsave();
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
      return commands.save();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public void shutdown(boolean save) {
    Span span = helper.buildSpan("save");
    span.setTag("save", save);
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
  public String slaveof(String host, int port) {
    Span span = helper.buildSpan("slaveof");
    span.setTag("host", host);
    span.setTag("port", port);
    try {
      return commands.slaveof(host, port);
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
      return commands.slaveofNoOne();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<Object> slowlogGet() {
    Span span = helper.buildSpan("slowlogGet");
    try {
      return commands.slowlogGet();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<Object> slowlogGet(int count) {
    Span span = helper.buildSpan("slowlogGet");
    span.setTag("count", count);
    try {
      return commands.slowlogGet(count);
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
    try {
      return commands.slowlogLen();
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
      return commands.slowlogReset();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<V> time() {
    Span span = helper.buildSpan("time");
    try {
      return commands.time();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long pfadd(K key, V... values) {
    Span span = helper.buildSpan("pfadd", key);
    try {
      return commands.pfadd(key, values);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String pfmerge(K destkey, K... sourcekeys) {
    Span span = helper.buildSpan("pfmerge");
    try {
      return commands.pfmerge(destkey, sourcekeys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long pfcount(K... keys) {
    Span span = helper.buildSpan("pfcount", keys);
    try {
      return commands.pfcount(keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long publish(K channel, V message) {
    Span span = helper.buildSpan("publish");
    try {
      return commands.publish(channel, message);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<K> pubsubChannels() {
    Span span = helper.buildSpan("pubsubChannels");
    try {
      return commands.pubsubChannels();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<K> pubsubChannels(K channel) {
    Span span = helper.buildSpan("pubsubChannels");
    try {
      return commands.pubsubChannels(channel);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Map<K, Long> pubsubNumsub(K... channels) {
    Span span = helper.buildSpan("pubsubNumsub");
    try {
      return commands.pubsubNumsub(channels);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long pubsubNumpat() {
    Span span = helper.buildSpan("pubsubNumpat");
    try {
      return commands.pubsubNumpat();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public V echo(V msg) {
    Span span = helper.buildSpan("echo");
    try {
      return commands.echo(msg);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<Object> role() {
    Span span = helper.buildSpan("role");
    try {
      return commands.role();
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
      return commands.ping();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String readOnly() {
    Span span = helper.buildSpan("readOnly");
    try {
      return commands.readOnly();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String readWrite() {
    Span span = helper.buildSpan("readWrite");
    try {
      return commands.readWrite();
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
      return commands.quit();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long waitForReplication(int replicas, long timeout) {
    Span span = helper.buildSpan("waitForReplication");
    span.setTag("replicas", replicas);
    span.setTag("timeout", timeout);
    try {
      return commands.waitForReplication(replicas, timeout);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <T> T dispatch(ProtocolKeyword type,
      CommandOutput<K, V, T> output) {
    Span span = helper.buildSpan("dispatch");
    try {
      return commands.dispatch(type, output);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <T> T dispatch(ProtocolKeyword type,
      CommandOutput<K, V, T> output,
      CommandArgs<K, V> args) {
    Span span = helper.buildSpan("dispatch");
    try {
      return commands.dispatch(type, output, args);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
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
  public void setTimeout(Duration timeout) {
    Span span = helper.buildSpan("setTimeout");
    span.setTag("timeout", nullable(timeout));
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
    span.setTag("timeout", timeout);
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
  public String clusterBumpepoch() {
    Span span = helper.buildSpan("clusterBumpepoch");
    try {
      return commands.clusterBumpepoch();
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
      return commands.clusterMeet(ip, port);
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
      return commands.clusterForget(nodeId);
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
      return commands.clusterAddSlots(slots);
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
      return commands.clusterDelSlots(slots);
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
      return commands.clusterSetSlotNode(slot, nodeId);
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
      return commands.clusterSetSlotStable(slot);
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
      return commands.clusterSetSlotMigrating(slot, nodeId);
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
      return commands.clusterSetSlotImporting(slot, nodeId);
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
      return commands.clusterInfo();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String clusterMyId() {
    Span span = helper.buildSpan("clusterMyId");
    try {
      return commands.clusterMyId();
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
      return commands.clusterNodes();
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
      return commands.clusterSlaves(nodeId);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<K> clusterGetKeysInSlot(int slot, int count) {
    Span span = helper.buildSpan("clusterGetKeysInSlot");
    span.setTag("slot", slot);
    span.setTag("count", count);
    try {
      return commands.clusterGetKeysInSlot(slot, count);
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
      return commands.clusterCountKeysInSlot(slot);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long clusterCountFailureReports(String nodeId) {
    Span span = helper.buildSpan("clusterCountFailureReports");
    span.setTag("nodeId", nodeId);
    try {
      return commands.clusterCountFailureReports(nodeId);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long clusterKeyslot(K key) {
    Span span = helper.buildSpan("clusterKeyslot");
    try {
      return commands.clusterKeyslot(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String clusterSaveconfig() {
    Span span = helper.buildSpan("clusterSaveconfig");
    try {
      return commands.clusterSaveconfig();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String clusterSetConfigEpoch(long configEpoch) {
    Span span = helper.buildSpan("clusterSetConfigEpoch");
    span.setTag("configEpoch", configEpoch);
    try {
      return commands.clusterSetConfigEpoch(configEpoch);
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
      return commands.clusterSlots();
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
      return commands.asking();
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
      return commands.clusterReplicate(nodeId);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String clusterFailover(boolean force) {
    Span span = helper.buildSpan("clusterFailover");
    span.setTag("force", force);
    try {
      return commands.clusterFailover(force);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String clusterReset(boolean hard) {
    Span span = helper.buildSpan("clusterReset");
    span.setTag("hard", hard);
    try {
      return commands.clusterReset(hard);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String clusterFlushslots() {
    Span span = helper.buildSpan("clusterFlushslots");
    try {
      return commands.clusterFlushslots();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long geoadd(K key, double longitude, double latitude, V member) {
    Span span = helper.buildSpan("geoadd", key);
    span.setTag("longitude", longitude);
    span.setTag("latitude", latitude);
    try {
      return commands.geoadd(key, longitude, latitude, member);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long geoadd(K key, Object... lngLatMember) {
    Span span = helper.buildSpan("geoadd", key);
    try {
      return commands.geoadd(key, lngLatMember);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<Value<String>> geohash(K key, V... members) {
    Span span = helper.buildSpan("geohash", key);
    try {
      return commands.geohash(key, members);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<V> georadius(K key, double longitude, double latitude, double distance,
      Unit unit) {
    Span span = helper.buildSpan("georadius", key);
    span.setTag("longitude", longitude);
    span.setTag("latitude", latitude);
    span.setTag("distance", distance);
    span.setTag("unit", nullable(unit));
    try {
      return commands.georadius(key, longitude, latitude, distance, unit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<GeoWithin<V>> georadius(K key, double longitude,
      double latitude, double distance, Unit unit,
      GeoArgs geoArgs) {
    Span span = helper.buildSpan("georadius", key);
    span.setTag("longitude", longitude);
    span.setTag("latitude", latitude);
    span.setTag("distance", distance);
    span.setTag("unit", nullable(unit));
    try {
      return commands.georadius(key, longitude, latitude, distance, unit, geoArgs);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long georadius(K key, double longitude, double latitude, double distance,
      Unit unit, GeoRadiusStoreArgs<K> geoRadiusStoreArgs) {
    Span span = helper.buildSpan("georadius", key);
    span.setTag("longitude", longitude);
    span.setTag("latitude", latitude);
    span.setTag("distance", distance);
    span.setTag("unit", nullable(unit));
    try {
      return commands.georadius(key, longitude, latitude, distance, unit, geoRadiusStoreArgs);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Set<V> georadiusbymember(K key, V member, double distance,
      Unit unit) {
    Span span = helper.buildSpan("georadiusbymember", key);
    span.setTag("distance", distance);
    span.setTag("unit", nullable(unit));
    try {
      return commands.georadiusbymember(key, member, distance, unit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<GeoWithin<V>> georadiusbymember(K key, V member,
      double distance, Unit unit, GeoArgs geoArgs) {
    Span span = helper.buildSpan("georadiusbymember", key);
    span.setTag("distance", distance);
    span.setTag("unit", nullable(unit));
    try {
      return commands.georadiusbymember(key, member, distance, unit, geoArgs);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Long georadiusbymember(K key, V member, double distance, Unit unit,
      GeoRadiusStoreArgs<K> geoRadiusStoreArgs) {
    Span span = helper.buildSpan("georadiusbymember", key);
    span.setTag("distance", distance);
    span.setTag("unit", nullable(unit));
    try {
      return commands.georadiusbymember(key, member, distance, unit, geoRadiusStoreArgs);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<GeoCoordinates> geopos(K key, V... members) {
    Span span = helper.buildSpan("geopos", key);
    try {
      return commands.geopos(key, members);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Double geodist(K key, V from, V to, Unit unit) {
    Span span = helper.buildSpan("geodist", key);
    span.setTag("unit", nullable(unit));
    try {
      return commands.geodist(key, from, to, unit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String discard() {
    Span span = helper.buildSpan("discard");
    try {
      return commands.discard();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public TransactionResult exec() {
    Span span = helper.buildSpan("exec");
    try {
      return commands.exec();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String multi() {
    Span span = helper.buildSpan("multi");
    try {
      return commands.multi();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public String watch(K... keys) {
    Span span = helper.buildSpan("watch", keys);
    try {
      return commands.watch(keys);
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
      return commands.unwatch();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }
}
