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
package io.opentracing.contrib.redis.redisson;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.contrib.redis.common.TracingConfiguration;
import io.opentracing.contrib.redis.common.TracingHelper;
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.tag.Tags;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.redisson.Redisson;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RList;
import org.redisson.api.RListMultimap;
import org.redisson.api.RLock;
import org.redisson.api.RMap;
import org.redisson.api.RSet;
import org.redisson.api.RSetMultimap;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import redis.embedded.RedisServer;

public class TracingRedissonTest {
  private MockTracer tracer = new MockTracer();
  private RedisServer redisServer;
  private RedissonClient client;

  @Before
  public void before() {
    tracer.reset();

    redisServer = RedisServer.builder().setting("bind 127.0.0.1").build();
    redisServer.start();

    Config config = new Config();
    config.useSingleServer().setAddress("redis://127.0.0.1:6379");

    client = new TracingRedissonClient(Redisson.create(config), tracer, false);
  }

  @After
  public void after() {
    if (client != null) {
      client.shutdown();
    }
    if (redisServer != null) {
      redisServer.stop();
    }
  }

  @Test
  public void test_map() {
    RMap<String, String> map = client.getMap("map");

    map.put("key", "value");
    assertEquals("value", map.get("key"));

    List<MockSpan> spans = tracer.finishedSpans();
    assertEquals(2, spans.size());
    checkSpans(spans);
    assertNull(tracer.activeSpan());
  }

  @Test
  public void test_list() {
    RList<Object> list = client.getList("list");

    list.add("key");
    assertTrue(list.contains("key"));

    List<MockSpan> spans = tracer.finishedSpans();
    assertEquals(2, spans.size());
    checkSpans(spans);
    assertNull(tracer.activeSpan());
  }

  @Test
  public void test_set() {
    RSet<Object> set = client.getSet("set");

    set.add("key");
    assertTrue(set.contains("key"));

    List<MockSpan> spans = tracer.finishedSpans();
    assertEquals(2, spans.size());
    checkSpans(spans);
    assertNull(tracer.activeSpan());
  }

  @Test
  public void test_lock() {
    RLock lock = client.getLock("lock");

    lock.lock(10, TimeUnit.SECONDS);
    lock.unlock();

    List<MockSpan> spans = tracer.finishedSpans();
    assertEquals(2, spans.size());
    checkSpans(spans);
    assertNull(tracer.activeSpan());
  }

  @Test
  public void test_atomic_long() {
    RAtomicLong atomicLong = client.getAtomicLong("long");

    atomicLong.set(10);
    assertEquals(10, atomicLong.get());

    List<MockSpan> spans = tracer.finishedSpans();
    assertEquals(2, spans.size());
    checkSpans(spans);
    assertNull(tracer.activeSpan());
  }

  @Test
  public void test_list_multi_map() {
    RListMultimap<String, String> map = client.getListMultimap("map");

    map.put("key", "value");
    assertEquals("value", map.get("key").get(0));

    List<MockSpan> spans = tracer.finishedSpans();
    assertEquals(3, spans.size());
    checkSpans(spans);
    assertNull(tracer.activeSpan());
  }

  @Test
  public void test_set_multi_map() {
    RSetMultimap<String, String> map = client.getSetMultimap("map");

    map.put("key", "value");
    assertEquals("value", map.get("key").iterator().next());

    List<MockSpan> spans = tracer.finishedSpans();
    assertEquals(2, spans.size());
    checkSpans(spans);
    assertNull(tracer.activeSpan());
  }

  @Test
  public void async_continue_span() throws Exception {
    try (Scope ignore = tracer.buildSpan("test").startActive(true)) {
      Span activeSpan = tracer.activeSpan();

      RMap<String, String> map = client.getMap("map");

      assertFalse(map.containsKeyAsync("key").toCompletableFuture().thenApply(s -> {
        System.out.println(
            "active span: " + tracer.activeSpan() + " in thread: " + Thread.currentThread()
                .getName());
        assertSame(activeSpan, tracer.activeSpan());
        return s;
      }).get(15, TimeUnit.SECONDS));

    }

    await().atMost(15, TimeUnit.SECONDS).until(reportedSpansSize(), equalTo(2));
    List<MockSpan> spans = tracer.finishedSpans();
    assertEquals(2, spans.size());

    assertNull(tracer.activeSpan());
  }

  @Test
  public void test_config_span_name() throws Exception {
    Config config = new Config();
    config.useSingleServer().setAddress("redis://127.0.0.1:6379");

    RedissonClient customClient = new TracingRedissonClient(Redisson.create(config),
            new TracingConfiguration.Builder(tracer)
                    .traceWithActiveSpanOnly(true)
                    .withSpanNameProvider(operation -> "Redis." + operation)
                    .build());

    try (Scope ignore = tracer.buildSpan("test").startActive(true)) {
      RMap<String, String> map = customClient.getMap("map");
      map.getAsync("key").get(15, TimeUnit.SECONDS);
    }

    await().atMost(15, TimeUnit.SECONDS).until(reportedSpansSize(), equalTo(2));

    List<MockSpan> spans = tracer.finishedSpans();
    assertEquals(2, spans.size());
    MockSpan redisSpan = spans.get(0);
    assertEquals("Redis.getAsync", redisSpan.operationName());

    assertNull(tracer.activeSpan());
    customClient.shutdown();
  }

  private void checkSpans(List<MockSpan> spans) {
    for (MockSpan span : spans) {
      assertEquals(span.tags().get(Tags.SPAN_KIND.getKey()), Tags.SPAN_KIND_CLIENT);
      assertEquals(TracingHelper.COMPONENT_NAME, span.tags().get(Tags.COMPONENT.getKey()));
      assertEquals(TracingHelper.DB_TYPE, span.tags().get(Tags.DB_TYPE.getKey()));
      assertEquals(0, span.generatedErrors().size());
    }
  }

  private Callable<Integer> reportedSpansSize() {
    return () -> tracer.finishedSpans().size();
  }
}
