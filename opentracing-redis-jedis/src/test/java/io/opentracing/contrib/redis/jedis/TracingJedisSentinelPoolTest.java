package io.opentracing.contrib.redis.jedis;

import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.util.ThreadLocalScopeManager;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.Protocol;
import redis.embedded.RedisServer;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class TracingJedisSentinelPoolTest {

  private MockTracer mockTracer = new MockTracer(new ThreadLocalScopeManager(),
      MockTracer.Propagator.TEXT_MAP);

  private RedisServer redisServer;

  private static final String MASTER_NAME = "mymaster";


  private Set<String> sentinels = new HashSet<String>();
  protected static TracingJedis sentinelJedis1;
  protected static TracingJedis sentinelJedis2;
  HostAndPort sentinel1 = new HostAndPort("localhost", Protocol.DEFAULT_PORT);
  HostAndPort sentinel2 = new HostAndPort("localhost", Protocol.DEFAULT_PORT+1);
  HostAndPort master = new HostAndPort("localhost", Protocol.DEFAULT_PORT + 2);


  @Before
  public void before() throws Exception {
    mockTracer.reset();

    redisServer = RedisServer.builder().setting("bind 127.0.0.1").build();
    redisServer.start();
    sentinels.add(sentinel1.toString());
    sentinels.add(sentinel2.toString());

//    sentinelJedis1 = new TracingJedis(sentinel1.getHost(), sentinel1.getPort(), mockTracer, false);
//    sentinelJedis2 = new TracingJedis(sentinel2.getHost(), sentinel2.getPort(), mockTracer, false);
  }

  @After
  public void after() {
    if (redisServer != null) {
      redisServer.stop();
    }
  }

  @Test
  public void testSentinelPoolReturnsTracedJedis() {
    JedisSentinelPool pool = new TracingJedisSentinelPool(mockTracer, false, MASTER_NAME, sentinels, new GenericObjectPoolConfig());
    Jedis jedis = pool.getResource();
    assertEquals("OK", jedis.set("key", "value"));
    assertEquals("value", jedis.get("key"));

    jedis.close();

    List<MockSpan> spans = mockTracer.finishedSpans();
    assertEquals(2, spans.size());
  }

  @Test
  public void testClosingTracedJedisClosesUnderlyingJedis() {
    JedisSentinelPool pool = new TracingJedisSentinelPool(mockTracer, false, "name", new HashSet<String>(), new GenericObjectPoolConfig());
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
