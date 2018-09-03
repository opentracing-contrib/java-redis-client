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

import io.lettuce.core.ClientOptions;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.protocol.RedisCommand;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;
import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

public class TracingStatefulRedisConnection<K, V> implements StatefulRedisConnection<K, V> {

  private final StatefulRedisConnection<K, V> connection;
  private final Tracer tracer;
  private final boolean traceWithActiveSpanOnly;

  /**
   * @param connection redis connection
   * @param tracer tracer
   * @param traceWithActiveSpanOnly if <code>true</code> then create new spans only if there is
   * active span
   */
  public TracingStatefulRedisConnection(StatefulRedisConnection<K, V> connection, Tracer tracer,
      boolean traceWithActiveSpanOnly) {
    this.connection = connection;
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingStatefulRedisConnection(StatefulRedisConnection<K, V> connection,
      boolean traceWithActiveSpanOnly) {
    this(connection, GlobalTracer.get(), traceWithActiveSpanOnly);
  }

  @Override
  public boolean isMulti() {
    return connection.isMulti();
  }

  @Override
  public RedisCommands<K, V> sync() {
    return new TracingRedisCommands<>(connection.sync(), tracer, traceWithActiveSpanOnly);
  }

  @Override
  public RedisAsyncCommands<K, V> async() {
    return new TracingRedisAsyncCommands<>(connection.async(), tracer, traceWithActiveSpanOnly);
  }

  @Override
  public RedisReactiveCommands<K, V> reactive() {
    return connection.reactive();
  }

  @Override
  public void setTimeout(Duration timeout) {
    connection.setTimeout(timeout);
  }

  @Override
  @Deprecated
  public void setTimeout(long timeout, TimeUnit unit) {
    connection.setTimeout(timeout, unit);
  }

  @Override
  public Duration getTimeout() {
    return connection.getTimeout();
  }

  @Override
  public <T> RedisCommand<K, V, T> dispatch(
      RedisCommand<K, V, T> command) {
    return connection.dispatch(command);
  }

  @Override
  public Collection<RedisCommand<K, V, ?>> dispatch(
      Collection<? extends RedisCommand<K, V, ?>> redisCommands) {
    return connection.dispatch(redisCommands);
  }

  @Override
  public void close() {
    connection.close();
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
  public void reset() {
    connection.reset();
  }

  @Override
  public void setAutoFlushCommands(boolean autoFlush) {
    connection.setAutoFlushCommands(autoFlush);
  }

  @Override
  public void flushCommands() {
    connection.flushCommands();
  }
}
