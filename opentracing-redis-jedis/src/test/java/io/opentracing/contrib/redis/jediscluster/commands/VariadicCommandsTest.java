package io.opentracing.contrib.redis.jediscluster.commands;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.opentracing.contrib.redis.jediscluster.commands.TestKeys.*;
import static io.opentracing.contrib.redis.utils.AssertUtil.assertByteArrayListEquals;
import static io.opentracing.contrib.redis.utils.AssertUtil.assertByteArraySetEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class VariadicCommandsTest extends JedisCommandTestBase {

  @Test
  public void hdel() {
    Map<String, String> hash = new HashMap<String, String>();
    hash.put("bar", "car");
    hash.put("car", "bar");
    hash.put("foo2", "bar");
    jedisCluster.hmset("foo", hash);

    assertEquals(0, jedisCluster.hdel("bar", "foo", "foo1").intValue());
    assertEquals(0, jedisCluster.hdel("foo", "foo", "foo1").intValue());
    assertEquals(2, jedisCluster.hdel("foo", "bar", "foo2").intValue());
    assertNull(jedisCluster.hget("foo", "bar"));

    // Binary
    Map<byte[], byte[]> bhash = new HashMap<byte[], byte[]>();
    bhash.put(bbar, bcar);
    bhash.put(bcar, bbar);
    bhash.put(bfoo2, bbar);
    jedisCluster.hmset(bfoo, bhash);

    assertEquals(0, jedisCluster.hdel(bbar, bfoo, bfoo1).intValue());
    assertEquals(0, jedisCluster.hdel(bfoo, bfoo, bfoo1).intValue());
    assertEquals(2, jedisCluster.hdel(bfoo, bbar, bfoo2).intValue());
    assertNull(jedisCluster.hget(bfoo, bbar));

  }

  @Test
  public void rpush() {
    long size = jedisCluster.rpush("foo", "bar", "foo");
    assertEquals(2, size);

    List<String> expected = new ArrayList<String>();
    expected.add("bar");
    expected.add("foo");

    List<String> values = jedisCluster.lrange("foo", 0, -1);
    assertEquals(expected, values);

    // Binary
    size = jedisCluster.rpush(bfoo, bbar, bfoo);
    assertEquals(2, size);

    List<byte[]> bexpected = new ArrayList<byte[]>();
    bexpected.add(bbar);
    bexpected.add(bfoo);

    List<byte[]> bvalues = jedisCluster.lrange(bfoo, 0, -1);
    assertByteArrayListEquals(bexpected, bvalues);

  }

  @Test
  public void lpush() {
    long size = jedisCluster.lpush("foo", "bar", "foo");
    assertEquals(2, size);

    List<String> expected = new ArrayList<String>();
    expected.add("foo");
    expected.add("bar");

    List<String> values = jedisCluster.lrange("foo", 0, -1);
    assertEquals(expected, values);

    // Binary
    size = jedisCluster.lpush(bfoo, bbar, bfoo);
    assertEquals(2, size);

    List<byte[]> bexpected = new ArrayList<byte[]>();
    bexpected.add(bfoo);
    bexpected.add(bbar);

    List<byte[]> bvalues = jedisCluster.lrange(bfoo, 0, -1);
    assertByteArrayListEquals(bexpected, bvalues);

  }

  @Test
  public void sadd() {
    long status = jedisCluster.sadd("foo", "bar", "foo1");
    assertEquals(2, status);

    status = jedisCluster.sadd("foo", "bar", "car");
    assertEquals(1, status);

    status = jedisCluster.sadd("foo", "bar", "foo1");
    assertEquals(0, status);

    status = jedisCluster.sadd(bfoo, bbar, bfoo1);
    assertEquals(2, status);

    status = jedisCluster.sadd(bfoo, bbar, bcar);
    assertEquals(1, status);

    status = jedisCluster.sadd(bfoo, bbar, bfoo1);
    assertEquals(0, status);

  }

  @Test
  public void zadd() {
    Map<String, Double> scoreMembers = new HashMap<String, Double>();
    scoreMembers.put("bar", 1d);
    scoreMembers.put("foo", 10d);

    long status = jedisCluster.zadd("foo", scoreMembers);
    assertEquals(2, status);

    scoreMembers.clear();
    scoreMembers.put("car", 0.1d);
    scoreMembers.put("bar", 2d);

    status = jedisCluster.zadd("foo", scoreMembers);
    assertEquals(1, status);

    Map<byte[], Double> bscoreMembers = new HashMap<byte[], Double>();
    bscoreMembers.put(bbar, 1d);
    bscoreMembers.put(bfoo, 10d);

    status = jedisCluster.zadd(bfoo, bscoreMembers);
    assertEquals(2, status);

    bscoreMembers.clear();
    bscoreMembers.put(bcar, 0.1d);
    bscoreMembers.put(bbar, 2d);

    status = jedisCluster.zadd(bfoo, bscoreMembers);
    assertEquals(1, status);

  }

  @Test
  public void zrem() {
    jedisCluster.zadd("foo", 1d, "bar");
    jedisCluster.zadd("foo", 2d, "car");
    jedisCluster.zadd("foo", 3d, "foo1");

    long status = jedisCluster.zrem("foo", "bar", "car");

    Set<String> expected = new LinkedHashSet<String>();
    expected.add("foo1");

    assertEquals(2, status);
    assertEquals(expected, jedisCluster.zrange("foo", 0, 100));

    status = jedisCluster.zrem("foo", "bar", "car");
    assertEquals(0, status);

    status = jedisCluster.zrem("foo", "bar", "foo1");
    assertEquals(1, status);

    // Binary
    jedisCluster.zadd(bfoo, 1d, bbar);
    jedisCluster.zadd(bfoo, 2d, bcar);
    jedisCluster.zadd(bfoo, 3d, bfoo1);

    status = jedisCluster.zrem(bfoo, bbar, bcar);

    Set<byte[]> bexpected = new LinkedHashSet<byte[]>();
    bexpected.add(bfoo);

    assertEquals(2, status);
    assertByteArraySetEquals(bexpected, jedisCluster.zrange(bfoo, 0, 100));

    status = jedisCluster.zrem(bfoo, bbar, bcar);
    assertEquals(0, status);

    status = jedisCluster.zrem(bfoo, bbar, bfoo1);
    assertEquals(1, status);

  }
}