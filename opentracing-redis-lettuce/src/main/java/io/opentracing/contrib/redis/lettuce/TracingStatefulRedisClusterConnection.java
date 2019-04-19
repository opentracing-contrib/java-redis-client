/*
 * Copyright 2017-2019 The OpenTracing Authors
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

import io.lettuce.core.ClientOptions;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.cluster.api.reactive.RedisAdvancedClusterReactiveCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.lettuce.core.cluster.models.partitions.Partitions;
import io.lettuce.core.protocol.RedisCommand;
import io.lettuce.core.resource.ClientResources;
import io.opentracing.contrib.redis.common.TracingConfiguration;
import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class TracingStatefulRedisClusterConnection<K, V> implements
    StatefulRedisClusterConnection<K, V> {

  private final StatefulRedisClusterConnection<K, V> connection;
  private final TracingConfiguration tracingConfiguration;

  /**
   * @param connection redis connection
   * @param tracingConfiguration tracing configuration
   */
  public TracingStatefulRedisClusterConnection(StatefulRedisClusterConnection<K, V> connection,
      TracingConfiguration tracingConfiguration) {
    this.connection = connection;
    this.tracingConfiguration = tracingConfiguration;
  }

  @Override
  public RedisAdvancedClusterCommands<K, V> sync() {
    return new TracingRedisAdvancedClusterCommands<>(connection.sync(), tracingConfiguration);
  }

  @Override
  public RedisAdvancedClusterAsyncCommands<K, V> async() {
    return new TracingRedisAdvancedClusterAsyncCommands<>(connection.async(), tracingConfiguration);
  }

  @Override
  public RedisAdvancedClusterReactiveCommands<K, V> reactive() {
    return connection.reactive();
  }

  @Override
  public StatefulRedisConnection<K, V> getConnection(String s) {
    return new TracingStatefulRedisConnection<>(connection.getConnection(s), tracingConfiguration);
  }

  @Override
  public CompletableFuture<StatefulRedisConnection<K, V>> getConnectionAsync(String s) {
    return CompletableFuture.supplyAsync(
        () -> new TracingStatefulRedisConnection<>(connection.getConnection(s),
            tracingConfiguration));
  }

  @Override
  public StatefulRedisConnection<K, V> getConnection(String s, int i) {
    return new TracingStatefulRedisConnection<>(connection.getConnection(s, i),
        tracingConfiguration);
  }

  @Override
  public CompletableFuture<StatefulRedisConnection<K, V>> getConnectionAsync(String s, int i) {
    return CompletableFuture.supplyAsync(
        () -> new TracingStatefulRedisConnection<>(connection.getConnection(s, i),
            tracingConfiguration));
  }

  @Override
  public void setReadFrom(ReadFrom readFrom) {
    connection.setReadFrom(readFrom);
  }

  @Override
  public ReadFrom getReadFrom() {
    return connection.getReadFrom();
  }

  @Override
  public Partitions getPartitions() {
    return connection.getPartitions();
  }

  @Override
  public void setTimeout(Duration duration) {
    connection.setTimeout(duration);
  }

  @Override
  public void setTimeout(long l, TimeUnit timeUnit) {
    connection.setTimeout(l, timeUnit);
  }

  @Override
  public Duration getTimeout() {
    return connection.getTimeout();
  }

  @Override
  public <T> RedisCommand<K, V, T> dispatch(RedisCommand<K, V, T> redisCommand) {
    return connection.dispatch(redisCommand);
  }

  @Override
  public Collection<RedisCommand<K, V, ?>> dispatch(
      Collection<? extends RedisCommand<K, V, ?>> collection) {
    return connection.dispatch(collection);
  }

  @Override
  public void close() {
    connection.close();
  }

  @Override
  public CompletableFuture<Void> closeAsync() {
    return connection.closeAsync();
  }

  @Override
  public boolean isOpen() {
    return connection.isOpen();
  }

  @Override
  public ClientOptions getOptions() {
    return connection.getOptions();
  }

  @Override
  public ClientResources getResources() {
    return connection.getResources();
  }

  @Override
  public void reset() {
    connection.reset();
  }

  @Override
  public void setAutoFlushCommands(boolean b) {
    connection.setAutoFlushCommands(b);
  }

  @Override
  public void flushCommands() {
    connection.flushCommands();
  }
}
