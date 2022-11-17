package io.opentracing.contrib.redis.jediscluster.commands;

import org.junit.Test;
import redis.clients.util.SafeEncoder;

import static io.opentracing.contrib.redis.jediscluster.commands.TestKeys.*;
import static org.junit.Assert.assertEquals;

public class HyperLogLogCommandsTest extends JedisCommandTestBase {

  @Test
  public void pfadd() {
    long status = jedisCluster.pfadd("foo", "a");
    assertEquals(1, status);

    status = jedisCluster.pfadd("foo", "a");
    assertEquals(0, status);
  }

  @Test
  public void pfaddBinary() {
    byte[] bFoo = SafeEncoder.encode("foo");
    byte[] bBar = SafeEncoder.encode("bar");
    byte[] bBar2 = SafeEncoder.encode("bar2");

    long status = jedisCluster.pfadd(bFoo, bBar, bBar2);
    assertEquals(1, status);

    status = jedisCluster.pfadd(bFoo, bBar, bBar2);
    assertEquals(0, status);
  }

  @Test
  public void pfcount() {
    long status = jedisCluster.pfadd("hll", "foo", "bar", "zap");
    assertEquals(1, status);

    status = jedisCluster.pfadd("hll", "zap", "zap", "zap");
    assertEquals(0, status);

    status = jedisCluster.pfadd("hll", "foo", "bar");
    assertEquals(0, status);

    status = jedisCluster.pfcount("hll");
    assertEquals(3, status);
  }

  @Test
  public void pfcounts() {
    long status = jedisCluster.pfadd(foo1, "foo", "bar", "zap");
    assertEquals(1, status);
    status = jedisCluster.pfadd(foo2, "foo", "bar", "zap");
    assertEquals(1, status);

    status = jedisCluster.pfadd(foo3, "foo", "bar", "baz");
    assertEquals(1, status);
    status = jedisCluster.pfcount(foo1);
    assertEquals(3, status);
    status = jedisCluster.pfcount(foo2);
    assertEquals(3, status);
    status = jedisCluster.pfcount(foo3);
    assertEquals(3, status);

    status = jedisCluster.pfcount(foo1, foo2);
    assertEquals(3, status);

    status = jedisCluster.pfcount(foo1, foo2, foo3);
    assertEquals(4, status);

  }

  @Test
  public void pfcountBinary() {
    byte[] bHll = SafeEncoder.encode("hll");
    byte[] bFoo = SafeEncoder.encode("foo");
    byte[] bBar = SafeEncoder.encode("bar");
    byte[] bZap = SafeEncoder.encode("zap");

    long status = jedisCluster.pfadd(bHll, bFoo, bBar, bZap);
    assertEquals(1, status);

    status = jedisCluster.pfadd(bHll, bZap, bZap, bZap);
    assertEquals(0, status);

    status = jedisCluster.pfadd(bHll, bFoo, bBar);
    assertEquals(0, status);

    status = jedisCluster.pfcount(bHll);
    assertEquals(3, status);
  }

  @Test
  public void pfmerge() {
    long status = jedisCluster.pfadd(foo1, "foo", "bar", "zap", "a");
    assertEquals(1, status);

    status = jedisCluster.pfadd(foo2, "a", "b", "c", "foo");
    assertEquals(1, status);

    String mergeStatus = jedisCluster.pfmerge(foo3, foo1, foo2);
    assertEquals("OK", mergeStatus);

    status = jedisCluster.pfcount(foo3);
    assertEquals(6, status);
  }

  @Test
  public void pfmergeBinary() {
    byte[] bHll1 = bfoo1;
    byte[] bHll2 = bfoo2;
    byte[] bHll3 = bfoo3;
    byte[] bFoo = SafeEncoder.encode("foo");
    byte[] bBar = SafeEncoder.encode("bar");
    byte[] bZap = SafeEncoder.encode("zap");
    byte[] bA = SafeEncoder.encode("a");
    byte[] bB = SafeEncoder.encode("b");
    byte[] bC = SafeEncoder.encode("c");

    long status = jedisCluster.pfadd(bHll1, bFoo, bBar, bZap, bA);
    assertEquals(1, status);

    status = jedisCluster.pfadd(bHll2, bA, bB, bC, bFoo);
    assertEquals(1, status);

    String mergeStatus = jedisCluster.pfmerge(bHll3, bHll1, bHll2);
    assertEquals("OK", mergeStatus);

    status = jedisCluster.pfcount(bHll3);
    assertEquals(6, status);
  }
}
