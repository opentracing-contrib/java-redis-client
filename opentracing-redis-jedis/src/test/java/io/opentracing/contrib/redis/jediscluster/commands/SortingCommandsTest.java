package io.opentracing.contrib.redis.jediscluster.commands;

import org.junit.Test;
import redis.clients.jedis.SortingParams;

import java.util.ArrayList;
import java.util.List;

import static io.opentracing.contrib.redis.jediscluster.commands.TestKeys.*;
import static io.opentracing.contrib.redis.utils.AssertUtil.assertByteArrayListEquals;
import static org.junit.Assert.assertEquals;

public class SortingCommandsTest extends JedisCommandTestBase {

  final byte[] b1 = { '1' };
  final byte[] b2 = { '2' };
  final byte[] b3 = { '3' };
  final byte[] b10 = { '1', '0' };

  @Test
  public void sort() {
    jedisCluster.lpush("foo", "3");
    jedisCluster.lpush("foo", "2");
    jedisCluster.lpush("foo", "1");

    List<String> result = jedisCluster.sort("foo");

    List<String> expected = new ArrayList<String>();
    expected.add("1");
    expected.add("2");
    expected.add("3");

    assertEquals(expected, result);

    // Binary
    jedisCluster.lpush(bfoo, b3);
    jedisCluster.lpush(bfoo, b2);
    jedisCluster.lpush(bfoo, b1);

    List<byte[]> bresult = jedisCluster.sort(bfoo);

    List<byte[]> bexpected = new ArrayList<byte[]>();
    bexpected.add(b1);
    bexpected.add(b2);
    bexpected.add(b3);

    assertByteArrayListEquals(bexpected, bresult);
  }

//  @Test
//  public void sortBy() {
//    jedisCluster.lpush("foo", "2");
//    jedisCluster.lpush("foo", "3");
//    jedisCluster.lpush("foo", "1");
//
//    jedisCluster.set("bar1", "3");
//    jedisCluster.set("bar2", "2");
//    jedisCluster.set("bar3", "1");
//
//    SortingParams sp = new SortingParams();
//    sp.by("bar*");
//
//    List<String> result = jedisCluster.sort("foo", sp);
//
//    List<String> expected = new ArrayList<String>();
//    expected.add("3");
//    expected.add("2");
//    expected.add("1");
//
//    assertEquals(expected, result);
//
//    // Binary
//    jedisCluster.lpush(bfoo, b2);
//    jedisCluster.lpush(bfoo, b3);
//    jedisCluster.lpush(bfoo, b1);
//
//    jedisCluster.set(bbar1, b3);
//    jedisCluster.set(bbar2, b2);
//    jedisCluster.set(bbar3, b1);
//
//    SortingParams bsp = new SortingParams();
//    bsp.by(bbarstar);
//
//    List<byte[]> bresult = jedisCluster.sort(bfoo, bsp);
//
//    List<byte[]> bexpected = new ArrayList<byte[]>();
//    bexpected.add(b3);
//    bexpected.add(b2);
//    bexpected.add(b1);
//
//    assertByteArrayListEquals(bexpected, bresult);
//
//  }

  @Test
  public void sortDesc() {
    jedisCluster.lpush("foo", "3");
    jedisCluster.lpush("foo", "2");
    jedisCluster.lpush("foo", "1");

    SortingParams sp = new SortingParams();
    sp.desc();

    List<String> result = jedisCluster.sort("foo", sp);

    List<String> expected = new ArrayList<String>();
    expected.add("3");
    expected.add("2");
    expected.add("1");

    assertEquals(expected, result);

    // Binary
    jedisCluster.lpush(bfoo, b3);
    jedisCluster.lpush(bfoo, b2);
    jedisCluster.lpush(bfoo, b1);

    SortingParams bsp = new SortingParams();
    bsp.desc();

    List<byte[]> bresult = jedisCluster.sort(bfoo, bsp);

    List<byte[]> bexpected = new ArrayList<byte[]>();
    bexpected.add(b3);
    bexpected.add(b2);
    bexpected.add(b1);

    assertByteArrayListEquals(bexpected, bresult);
  }

  @Test
  public void sortLimit() {
    for (int n = 10; n > 0; n--) {
      jedisCluster.lpush("foo", String.valueOf(n));
    }

    SortingParams sp = new SortingParams();
    sp.limit(0, 3);

    List<String> result = jedisCluster.sort("foo", sp);

    List<String> expected = new ArrayList<String>();
    expected.add("1");
    expected.add("2");
    expected.add("3");

    assertEquals(expected, result);

    // Binary
    jedisCluster.rpush(bfoo, new byte[] { (byte) '4' });
    jedisCluster.rpush(bfoo, new byte[] { (byte) '3' });
    jedisCluster.rpush(bfoo, new byte[] { (byte) '2' });
    jedisCluster.rpush(bfoo, new byte[] { (byte) '1' });

    SortingParams bsp = new SortingParams();
    bsp.limit(0, 3);

    List<byte[]> bresult = jedisCluster.sort(bfoo, bsp);

    List<byte[]> bexpected = new ArrayList<byte[]>();
    bexpected.add(b1);
    bexpected.add(b2);
    bexpected.add(b3);

    assertByteArrayListEquals(bexpected, bresult);
  }

  @Test
  public void sortAlpha() {
    jedisCluster.lpush("foo", "1");
    jedisCluster.lpush("foo", "2");
    jedisCluster.lpush("foo", "10");

    SortingParams sp = new SortingParams();
    sp.alpha();

    List<String> result = jedisCluster.sort("foo", sp);

    List<String> expected = new ArrayList<String>();
    expected.add("1");
    expected.add("10");
    expected.add("2");

    assertEquals(expected, result);

    // Binary
    jedisCluster.lpush(bfoo, b1);
    jedisCluster.lpush(bfoo, b2);
    jedisCluster.lpush(bfoo, b10);

    SortingParams bsp = new SortingParams();
    bsp.alpha();

    List<byte[]> bresult = jedisCluster.sort(bfoo, bsp);

    List<byte[]> bexpected = new ArrayList<byte[]>();
    bexpected.add(b1);
    bexpected.add(b10);
    bexpected.add(b2);

    assertByteArrayListEquals(bexpected, bresult);
  }

//  @Test
//  public void sortGet() {
//    jedisCluster.lpush(foo, "1");
//    jedisCluster.lpush(foo, "2");
//    jedisCluster.lpush(foo, "10");
//
//    jedisCluster.set(bar1, "bar1");
//    jedisCluster.set(bar2, "bar2");
//    jedisCluster.set(bar10, "bar10");
//
//    jedisCluster.set(car1, "car1");
//    jedisCluster.set(car2, "car2");
//    jedisCluster.set(car10, "car10");
//
//    SortingParams sp = new SortingParams();
//    sp.get(carstar, barstar);
//
//    List<String> result = jedisCluster.sort(foo, sp);
//
//    List<String> expected = new ArrayList<String>();
//    expected.add("car1");
//    expected.add("bar1");
//    expected.add("car2");
//    expected.add("bar2");
//    expected.add("car10");
//    expected.add("bar10");
//
//    assertEquals(expected, result);
//
//    // Binary
//    jedisCluster.lpush(bfoo, b1);
//    jedisCluster.lpush(bfoo, b2);
//    jedisCluster.lpush(bfoo, b10);
//
//    jedisCluster.set(bbar1, bbar1);
//    jedisCluster.set(bbar2, bbar2);
//    jedisCluster.set(bbar10, bbar10);
//
//    jedisCluster.set(bcar1, bcar1);
//    jedisCluster.set(bcar2, bcar2);
//    jedisCluster.set(bcar10, bcar10);
//
//    SortingParams bsp = new SortingParams();
//    bsp.get(bcarstar, bbarstar);
//
//    List<byte[]> bresult = jedisCluster.sort(bfoo, bsp);
//
//    List<byte[]> bexpected = new ArrayList<byte[]>();
//    bexpected.add(bcar1);
//    bexpected.add(bbar1);
//    bexpected.add(bcar2);
//    bexpected.add(bbar2);
//    bexpected.add(bcar10);
//    bexpected.add(bbar10);
//
//    assertByteArrayListEquals(bexpected, bresult);
//  }

  @Test
  public void sortStore() {
    jedisCluster.lpush(foo, "1");
    jedisCluster.lpush(foo, "2");
    jedisCluster.lpush(foo, "10");

    long result = jedisCluster.sort(foo, dst);

    List<String> expected = new ArrayList<String>();
    expected.add("1");
    expected.add("2");
    expected.add("10");

    assertEquals(3, result);
    assertEquals(expected, jedisCluster.lrange(dst, 0, 1000));

    // Binary
    jedisCluster.lpush(bfoo, b1);
    jedisCluster.lpush(bfoo, b2);
    jedisCluster.lpush(bfoo, b10);

    long bresult = jedisCluster.sort(bfoo, bdst);

    List<byte[]> bexpected = new ArrayList<byte[]>();
    bexpected.add(b1);
    bexpected.add(b2);
    bexpected.add(b10);

    assertEquals(3, bresult);
    assertByteArrayListEquals(bexpected, jedisCluster.lrange(bdst, 0, 1000));
  }

}