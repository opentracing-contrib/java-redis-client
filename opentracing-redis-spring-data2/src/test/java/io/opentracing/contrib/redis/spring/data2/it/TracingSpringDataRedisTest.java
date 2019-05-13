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

import static org.junit.Assert.assertEquals;

import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import redis.embedded.RedisServer;

@ContextConfiguration(classes = {SpringDataRedisTestConfiguration.class})
@RunWith(SpringJUnit4ClassRunner.class)
public class TracingSpringDataRedisTest {
  private RedisServer redisServer;

  @Before
  public void before() {
    redisServer = RedisServer.builder().setting("bind 127.0.0.1").build();
    redisServer.start();
  }

  @After
  public void after() {
    if (redisServer != null) {
      redisServer.stop();
    }
  }

  @Autowired
  private StringRedisTemplate template;

  @Autowired
  private MockTracer tracer;

  @Test
  public void test() {
    template.opsForValue().set("key", "value");
    assertEquals("value", template.opsForValue().get("key"));

    List<MockSpan> spans = tracer.finishedSpans();
    assertEquals(2, spans.size());

    for (MockSpan span : spans) {
      assertEquals("key", span.tags().get("key"));
    }
  }
}
