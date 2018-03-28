package io.opentracing.contrib.redis.jedis;

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
  public void testPoolReturnsTracedJedis() {
    JedisPool pool = new TracingJedisPool(mockTracer, false);

    Jedis jedis = pool.getResource();
    assertEquals("OK", jedis.set("key", "value"));
    assertEquals("value", jedis.get("key"));

    jedis.close();

    List<MockSpan> spans = mockTracer.finishedSpans();
    assertEquals(2, spans.size());
  }

  @Test
  public void testClosingTracedJedisClosesUnderlyingJedis() {
    JedisPool pool = new TracingJedisPool(mockTracer, false);
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
