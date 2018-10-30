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
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.ReactiveRedisClusterConnection;
import org.springframework.data.redis.connection.ReactiveRedisConnection;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisSentinelConnection;

/**
 * OpenTracing instrumentation of a {@link RedisConnectionFactory}.
 *
 * This class delegates invocations to the given {@link RedisConnectionFactory}, returning OpenTracing wrappers for
 * {@link RedisConnection} and {@link RedisClusterConnection} only.
 *
 * @author Daniel del Castillo
 */
public class TracingRedisConnectionFactory implements RedisConnectionFactory,
    ReactiveRedisConnectionFactory {

  private final RedisConnectionFactory delegate;
  private final boolean withActiveSpanOnly;
  private final Tracer tracer;

  public TracingRedisConnectionFactory(RedisConnectionFactory delegate, boolean withActiveSpanOnly,
      Tracer tracer) {
    this.delegate = delegate;
    this.withActiveSpanOnly = withActiveSpanOnly;
    this.tracer = tracer;
  }

  @Override
  public RedisConnection getConnection() {
    return new TracingRedisConnection(delegate.getConnection(), withActiveSpanOnly, tracer);
  }

  @Override
  public RedisClusterConnection getClusterConnection() {
    return new TracingRedisClusterConnection(delegate.getClusterConnection(), withActiveSpanOnly,
        tracer);
  }

  @Override
  public boolean getConvertPipelineAndTxResults() {
    return delegate.getConvertPipelineAndTxResults();
  }

  @Override
  public RedisSentinelConnection getSentinelConnection() {
    return new TracingRedisSentinelConnection(delegate.getSentinelConnection(), withActiveSpanOnly,
        tracer);
  }

  @Override
  public DataAccessException translateExceptionIfPossible(RuntimeException e) {
    return delegate.translateExceptionIfPossible(e);
  }

  @Override
  public ReactiveRedisConnection getReactiveConnection() {
    if (delegate instanceof ReactiveRedisConnectionFactory) {
      return new TracingReactiveRedisConnection((ReactiveRedisConnection) delegate,
          withActiveSpanOnly, tracer);
    }
    // TODO: shouldn't we throw an exception?
    return null;
  }

  @Override
  public ReactiveRedisClusterConnection getReactiveClusterConnection() {
    if (delegate instanceof ReactiveRedisConnectionFactory) {
      return ((ReactiveRedisConnectionFactory) delegate).getReactiveClusterConnection();
    }
    // TODO: shouldn't we throw an exception?
    return null;
  }
}
