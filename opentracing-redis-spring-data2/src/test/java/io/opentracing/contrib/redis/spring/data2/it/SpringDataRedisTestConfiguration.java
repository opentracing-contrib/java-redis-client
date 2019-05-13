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
package io.opentracing.contrib.redis.spring.data2.it;

import io.opentracing.contrib.redis.common.TracingConfiguration;
import io.opentracing.contrib.redis.spring.data2.connection.TracingRedisConnectionFactory;
import io.opentracing.mock.MockTracer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

@Configuration
public class SpringDataRedisTestConfiguration {

  @Bean
  public StringRedisTemplate stringRedisTemplate() {
    return new StringRedisTemplate(redisConnectionFactory());
  }

  @Bean
  public MockTracer tracer() {
    return new MockTracer();
  }

  @Bean
  public RedisConnectionFactory redisConnectionFactory() {
    LettuceConnectionFactory lettuceConnectionFactory = new LettuceConnectionFactory();
    lettuceConnectionFactory.afterPropertiesSet();
    return new TracingRedisConnectionFactory(lettuceConnectionFactory,
        new TracingConfiguration.Builder(tracer()).build());
  }
}
