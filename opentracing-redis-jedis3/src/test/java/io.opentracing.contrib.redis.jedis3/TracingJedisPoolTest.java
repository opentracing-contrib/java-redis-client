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
package io.opentracing.contrib.redis.jedis3;

import io.opentracing.contrib.redis.common.TracingConfiguration;
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.util.ThreadLocalScopeManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.embedded.RedisServer;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class TracingJedisPoolTest {

    private MockTracer mockTracer = new MockTracer(new ThreadLocalScopeManager(),
            MockTracer.Propagator.TEXT_MAP);

    private RedisServer redisServer;

    @Before
    public void before() {
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
    public void testPoolReturnsTracedJedis() {
        JedisPool pool = new TracingJedisPool(new TracingConfiguration.Builder(mockTracer).build());

        Jedis jedis = pool.getResource();
        assertEquals("OK", jedis.set("key", "value"));
        assertEquals("value", jedis.get("key"));

        jedis.close();

        List<MockSpan> spans = mockTracer.finishedSpans();
        assertEquals(2, spans.size());
    }

    @Test
    public void testClosingTracedJedisClosesUnderlyingJedis() {
        JedisPool pool = new TracingJedisPool(new TracingConfiguration.Builder(mockTracer).build());
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
    }
}
