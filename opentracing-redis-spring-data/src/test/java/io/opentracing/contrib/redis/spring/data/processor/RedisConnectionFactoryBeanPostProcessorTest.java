/*
 * Copyright 2017-2020 The OpenTracing Authors
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
package io.opentracing.contrib.redis.spring.data.processor;

import static org.junit.Assert.assertTrue;

import io.opentracing.contrib.redis.spring.data.connection.TracingRedisConnectionFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = RedisConnectionFactoryBeanPostProcessorConfiguration.class)
@EnableAutoConfiguration
public class RedisConnectionFactoryBeanPostProcessorTest {

  @Autowired
  private RedisConnectionFactory redisConnectionFactory;

  @Test
  public void test() {
    assertTrue(redisConnectionFactory instanceof TracingRedisConnectionFactory);
  }
}
