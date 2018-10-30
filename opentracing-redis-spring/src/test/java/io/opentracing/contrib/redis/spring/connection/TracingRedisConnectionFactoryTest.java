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

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.opentracing.Tracer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisSentinelConnection;

/**
 * @author Daniel del Castillo
 */
@RunWith(MockitoJUnitRunner.class)
public class TracingRedisConnectionFactoryTest {

  private @Mock
  Tracer tracer;
  private @Mock
  RedisConnectionFactory delegate;

  private TracingRedisConnectionFactory connectionFactory;

  @Before
  public void init() {
    connectionFactory = new TracingRedisConnectionFactory(delegate, false, tracer);
  }

  @Test
  public void connectionIsTracingRedisConnection() {
    RedisConnection connection = connectionFactory.getConnection();
    verify(delegate).getConnection();
    assertTrue(connection instanceof TracingRedisConnection);
  }

  @Test
  public void connectionIsTracingRedisClusterConnection() {
    RedisClusterConnection connection = connectionFactory.getClusterConnection();
    verify(delegate).getClusterConnection();
    assertTrue(connection instanceof TracingRedisClusterConnection);
  }

  @Test
  public void delegatesCallToGetConvertPipelineAndTxResults() {
    connectionFactory.getConvertPipelineAndTxResults();
    verify(delegate).getConvertPipelineAndTxResults();
  }

  @Test
  public void delegatesCallToGetSentinelConnection() {
    RedisSentinelConnection connection = connectionFactory.getSentinelConnection();
    verify(delegate).getSentinelConnection();
    assertTrue(connection instanceof TracingRedisSentinelConnection);
  }

  @Test
  public void delegatesCallToTranslateExceptionIfPossible() {
    RuntimeException e = mock(RuntimeException.class);
    delegate.translateExceptionIfPossible(e);
    verify(delegate).translateExceptionIfPossible(e);
  }

}