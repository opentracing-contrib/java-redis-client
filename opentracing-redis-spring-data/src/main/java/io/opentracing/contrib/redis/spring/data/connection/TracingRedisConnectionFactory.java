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
package io.opentracing.contrib.redis.spring.data.connection;

import io.opentracing.contrib.redis.common.TracingConfiguration;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisSentinelConnection;

/**
 * OpenTracing instrumentation of a {@link RedisConnectionFactory}.
 * <p>
 * This class delegates invocations to the given {@link RedisConnectionFactory}, returning
 * OpenTracing wrappers for {@link RedisConnection} and {@link RedisClusterConnection} only.
 *
 * @author Daniel del Castillo
 */
public class TracingRedisConnectionFactory implements RedisConnectionFactory {

  private final RedisConnectionFactory delegate;
  private final TracingConfiguration tracingConfiguration;

  public TracingRedisConnectionFactory(RedisConnectionFactory delegate,
      TracingConfiguration tracingConfiguration) {
    this.delegate = delegate;
    this.tracingConfiguration = tracingConfiguration;
  }

  @Override
  public RedisConnection getConnection() {
    // support cluster connection
    RedisConnection connection = this.delegate.getConnection();
    if (connection instanceof RedisClusterConnection) {
      return new TracingRedisClusterConnection((RedisClusterConnection) connection,
          tracingConfiguration);
    }
    return new TracingRedisConnection(connection, tracingConfiguration);
  }

  @Override
  public RedisClusterConnection getClusterConnection() {
    return new TracingRedisClusterConnection(delegate.getClusterConnection(), tracingConfiguration);
  }

  @Override
  public boolean getConvertPipelineAndTxResults() {
    return delegate.getConvertPipelineAndTxResults();
  }

  @Override
  public RedisSentinelConnection getSentinelConnection() {
    return new TracingRedisSentinelConnection(delegate.getSentinelConnection(),
        tracingConfiguration);
  }

  @Override
  public DataAccessException translateExceptionIfPossible(RuntimeException e) {
    return delegate.translateExceptionIfPossible(e);
  }
}
