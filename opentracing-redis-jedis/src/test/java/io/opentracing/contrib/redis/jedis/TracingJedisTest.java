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
package io.opentracing.contrib.redis.jedis;

import static org.junit.Assert.assertEquals;

import io.opentracing.contrib.redis.common.TracingConfiguration;
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.util.ThreadLocalScopeManager;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.embedded.RedisServer;

public class TracingJedisTest {

  private MockTracer mockTracer = new MockTracer(new ThreadLocalScopeManager(),
      MockTracer.Propagator.TEXT_MAP);

  private RedisServer redisServer;

  @Before
  public void before() throws Exception {
    mockTracer.reset();

    redisServer = RedisServer.builder().setting("bind 127.0.0.1").build();
    redisServer.start();
  }

  @After
  public void after() {
    if (redisServer != null) {
      redisServer.stop();
    }
  }

  @Test
  public void test() {
    Jedis jedis = new TracingJedis(new TracingConfiguration.Builder(mockTracer).build());

    assertEquals("OK", jedis.set("key", "value"));
    assertEquals("value", jedis.get("key"));

    jedis.close();

    List<MockSpan> spans = mockTracer.finishedSpans();
    assertEquals(2, spans.size());
  }

}
