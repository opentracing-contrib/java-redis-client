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
package io.opentracing.contrib.redis.lettuce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.contrib.redis.common.TracingConfiguration;
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.util.ThreadLocalScopeManager;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.embedded.RedisServer;

public class TracingLettuceTest {

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
  public void sync() {
    RedisClient client = RedisClient.create("redis://localhost");

    StatefulRedisConnection<String, String> connection =
        new TracingStatefulRedisConnection<>(client.connect(),
            new TracingConfiguration.Builder(mockTracer).build());
    RedisCommands<String, String> commands = connection.sync();

    assertEquals("OK", commands.set("key", "value"));
    assertEquals("value", commands.get("key"));

    connection.close();

    client.shutdown();

    List<MockSpan> spans = mockTracer.finishedSpans();
    assertEquals(2, spans.size());
  }

  @Test
  public void async() throws Exception {
    RedisClient client = RedisClient.create("redis://localhost");

    StatefulRedisConnection<String, String> connection =
        new TracingStatefulRedisConnection<>(client.connect(),
            new TracingConfiguration.Builder(mockTracer).build());

    RedisAsyncCommands<String, String> commands = connection.async();

    assertEquals("OK", commands.set("key2", "value2").get(15, TimeUnit.SECONDS));

    assertEquals("value2", commands.get("key2").get(15, TimeUnit.SECONDS));

    connection.close();

    client.shutdown();

    List<MockSpan> spans = mockTracer.finishedSpans();
    assertEquals(2, spans.size());
  }

  @Test
  public void async_continue_span() throws Exception {
    try (Scope scope = mockTracer.buildSpan("test").startActive(true)) {
      Span activeSpan = mockTracer.activeSpan();

      RedisClient client = RedisClient.create("redis://localhost");

      StatefulRedisConnection<String, String> connection =
          new TracingStatefulRedisConnection<>(client.connect(),
              new TracingConfiguration.Builder(mockTracer).build());

      RedisAsyncCommands<String, String> commands = connection.async();

      assertEquals("OK",
          commands.set("key2", "value2").toCompletableFuture().thenApply(s -> {
            assertSame(activeSpan, mockTracer.activeSpan());
            return s;
          }).get(15, TimeUnit.SECONDS));

      connection.close();

      client.shutdown();
    }
    List<MockSpan> spans = mockTracer.finishedSpans();
    assertEquals(2, spans.size());
  }
}
