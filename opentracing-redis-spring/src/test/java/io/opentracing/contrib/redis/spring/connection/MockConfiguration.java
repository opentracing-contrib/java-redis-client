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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.opentracing.mock.MockTracer;
import io.opentracing.util.GlobalTracerTestUtil;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;

/**
 * @author Daniel del Castillo
 */
@Configuration
public class MockConfiguration {

  @Bean
  public MockTracer mockTracer() {
    GlobalTracerTestUtil.resetGlobalTracer();
    return new MockTracer();
  }

  @Bean
  public RedisConnection mockRedisConnection() {
    return mock(RedisConnection.class);
  }

  @Bean
  public RedisClusterConnection mockRedisClusterConnection() {
    return mock(RedisClusterConnection.class);
  }

  @Bean
  public RedisConnectionFactory redisConnectionFactory() {
    RedisConnectionFactory factory = mock(RedisConnectionFactory.class);
    when(factory.getConnection())
        .thenReturn(new TracingRedisConnection(mockRedisConnection(), false, mockTracer()));
    when(factory.getClusterConnection()).thenReturn(
        new TracingRedisClusterConnection(mockRedisClusterConnection(), false, mockTracer()));
    return factory;
  }

}
