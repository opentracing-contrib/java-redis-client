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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.Protocol;
import redis.embedded.RedisSentinel;
import redis.embedded.RedisServer;

public class TracingJedisSentinelPoolTest {

  private MockTracer mockTracer = new MockTracer(new ThreadLocalScopeManager(),
      MockTracer.Propagator.TEXT_MAP);

  private RedisServer redisServer;

  private RedisSentinel redisSentinel;

  private static final String MASTER_NAME = "mymaster";

  private Set<String> sentinels = new HashSet<String>() {{
    add("127.0.0.1:" + (Protocol.DEFAULT_PORT + 1));
  }};

  @Before
  public void before() {
    mockTracer.reset();

    redisServer = RedisServer.builder().build();
    redisServer.start();
    redisSentinel = RedisSentinel.builder().port(Protocol.DEFAULT_PORT + 1).build();
    redisSentinel.start();
  }

  @After
  public void after() {
    if (redisSentinel != null) {
      redisSentinel.stop();
    }
    if (redisServer != null) {
      redisServer.stop();
    }
  }

  @Test
  public void testSentinelPoolReturnsTracedJedis() {
    JedisSentinelPool pool = new TracingJedisSentinelPool(
        new TracingConfiguration.Builder(mockTracer).build(), MASTER_NAME, sentinels,
        new GenericObjectPoolConfig());
    Jedis jedis = pool.getResource();
    assertEquals("OK", jedis.set("key", "value"));
    assertEquals("value", jedis.get("key"));

    jedis.close();
    pool.destroy();

    List<MockSpan> spans = mockTracer.finishedSpans();
    assertEquals(2, spans.size());
  }

  @Test
  public void testClosingTracedJedisClosesUnderlyingJedis() {
    JedisSentinelPool pool = new TracingJedisSentinelPool(
        new TracingConfiguration.Builder(mockTracer).build(), MASTER_NAME, sentinels,
        new GenericObjectPoolConfig());
    Jedis resource = pool.getResource();
    assertEquals(1, pool.getNumActive());

    resource.close();
    assertEquals(0, pool.getNumActive());
    assertEquals(1, pool.getNumIdle());

    // ensure that resource is reused
    Jedis nextResource = pool.getResource();
    assertEquals(1, pool.getNumActive());
    assertEquals(0, pool.getNumIdle());
    nextResource.close();

    pool.destroy();
  }
}