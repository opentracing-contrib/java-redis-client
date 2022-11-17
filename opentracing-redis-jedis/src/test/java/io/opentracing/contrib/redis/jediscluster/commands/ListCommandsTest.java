package io.opentracing.contrib.redis.jediscluster.commands;

import org.junit.Test;
import redis.clients.jedis.ListPosition;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.ArrayList;
import java.util.List;

import static io.opentracing.contrib.redis.jediscluster.commands.TestKeys.*;
import static io.opentracing.contrib.redis.utils.AssertUtil.assertByteArrayListEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class ListCommandsTest extends JedisCommandTestBase {
  final byte[] bA = { 0x0A };
  final byte[] bB = { 0x0B };
  final byte[] bC = { 0x0C };
  final byte[] b1 = { 0x01 };
  final byte[] b2 = { 0x02 };
  final byte[] b3 = { 0x03 };
  final byte[] bhello = { 0x04, 0x02 };
  final byte[] bx = { 0x02, 0x04 };

  @Test
  public void rpush() {
    long size = jedisCluster.rpush("foo", "bar");
    assertEquals(1, size);
    size = jedisCluster.rpush("foo", "foo");
    assertEquals(2, size);
    size = jedisCluster.rpush("foo", "bar", "foo");
    assertEquals(4, size);

    // Binary
    long bsize = jedisCluster.rpush(bfoo, bbar);
    assertEquals(1, bsize);
    bsize = jedisCluster.rpush(bfoo, bfoo);
    assertEquals(2, bsize);
    bsize = jedisCluster.rpush(bfoo, bbar, bfoo);
    assertEquals(4, bsize);

  }

  @Test
  public void lpush() {
    long size = jedisCluster.lpush("foo", "bar");
    assertEquals(1, size);
    size = jedisCluster.lpush("foo", "foo");
    assertEquals(2, size);
    size = jedisCluster.lpush("foo", "bar", "foo");
    assertEquals(4, size);

    // Binary
    long bsize = jedisCluster.lpush(bfoo, bbar);
    assertEquals(1, bsize);
    bsize = jedisCluster.lpush(bfoo, bfoo);
    assertEquals(2, bsize);
    bsize = jedisCluster.lpush(bfoo, bbar, bfoo);
    assertEquals(4, bsize);

  }

  @Test
  public void llen() {
    assertEquals(0, jedisCluster.llen("foo").intValue());
    jedisCluster.lpush("foo", "bar");
    jedisCluster.lpush("foo", "car");
    assertEquals(2, jedisCluster.llen("foo").intValue());

    // Binary
    assertEquals(0, jedisCluster.llen(bfoo).intValue());
    jedisCluster.lpush(bfoo, bbar);
    jedisCluster.lpush(bfoo, bcar);
    assertEquals(2, jedisCluster.llen(bfoo).intValue());

  }

  @Test
  public void llenNotOnList() {
    try {
      jedisCluster.set("foo", "bar");
      jedisCluster.llen("foo");
      fail("JedisDataException expected");
    } catch (final JedisDataException e) {
    }

    // Binary
    try {
      jedisCluster.set(bfoo, bbar);
      jedisCluster.llen(bfoo);
      fail("JedisDataException expected");
    } catch (final JedisDataException e) {
    }

  }

  @Test
  public void lrange() {
    jedisCluster.rpush("foo", "a");
    jedisCluster.rpush("foo", "b");
    jedisCluster.rpush("foo", "c");

    List<String> expected = new ArrayList<String>();
    expected.add("a");
    expected.add("b");
    expected.add("c");

    List<String> range = jedisCluster.lrange("foo", 0, 2);
    assertEquals(expected, range);

    range = jedisCluster.lrange("foo", 0, 20);
    assertEquals(expected, range);

    expected = new ArrayList<String>();
    expected.add("b");
    expected.add("c");

    range = jedisCluster.lrange("foo", 1, 2);
    assertEquals(expected, range);

    expected = new ArrayList<String>();
    range = jedisCluster.lrange("foo", 2, 1);
    assertEquals(expected, range);

    // Binary
    jedisCluster.rpush(bfoo, bA);
    jedisCluster.rpush(bfoo, bB);
    jedisCluster.rpush(bfoo, bC);

    List<byte[]> bexpected = new ArrayList<byte[]>();
    bexpected.add(bA);
    bexpected.add(bB);
    bexpected.add(bC);

    List<byte[]> brange = jedisCluster.lrange(bfoo, 0, 2);
    assertByteArrayListEquals(bexpected, brange);

    brange = jedisCluster.lrange(bfoo, 0, 20);
    assertByteArrayListEquals(bexpected, brange);

    bexpected = new ArrayList<byte[]>();
    bexpected.add(bB);
    bexpected.add(bC);

    brange = jedisCluster.lrange(bfoo, 1, 2);
    assertByteArrayListEquals(bexpected, brange);

    bexpected = new ArrayList<byte[]>();
    brange = jedisCluster.lrange(bfoo, 2, 1);
    assertByteArrayListEquals(bexpected, brange);

  }

  @Test
  public void ltrim() {
    jedisCluster.lpush("foo", "1");
    jedisCluster.lpush("foo", "2");
    jedisCluster.lpush("foo", "3");
    String status = jedisCluster.ltrim("foo", 0, 1);

    List<String> expected = new ArrayList<String>();
    expected.add("3");
    expected.add("2");

    assertEquals("OK", status);
    assertEquals(2, jedisCluster.llen("foo").intValue());
    assertEquals(expected, jedisCluster.lrange("foo", 0, 100));

    // Binary
    jedisCluster.lpush(bfoo, b1);
    jedisCluster.lpush(bfoo, b2);
    jedisCluster.lpush(bfoo, b3);
    String bstatus = jedisCluster.ltrim(bfoo, 0, 1);

    List<byte[]> bexpected = new ArrayList<byte[]>();
    bexpected.add(b3);
    bexpected.add(b2);

    assertEquals("OK", bstatus);
    assertEquals(2, jedisCluster.llen(bfoo).intValue());
    assertByteArrayListEquals(bexpected, jedisCluster.lrange(bfoo, 0, 100));

  }

  @Test
  public void lset() {
    jedisCluster.lpush("foo", "1");
    jedisCluster.lpush("foo", "2");
    jedisCluster.lpush("foo", "3");

    List<String> expected = new ArrayList<String>();
    expected.add("3");
    expected.add("bar");
    expected.add("1");

    String status = jedisCluster.lset("foo", 1, "bar");

    assertEquals("OK", status);
    assertEquals(expected, jedisCluster.lrange("foo", 0, 100));

    // Binary
    jedisCluster.lpush(bfoo, b1);
    jedisCluster.lpush(bfoo, b2);
    jedisCluster.lpush(bfoo, b3);

    List<byte[]> bexpected = new ArrayList<byte[]>();
    bexpected.add(b3);
    bexpected.add(bbar);
    bexpected.add(b1);

    String bstatus = jedisCluster.lset(bfoo, 1, bbar);

    assertEquals("OK", bstatus);
    assertByteArrayListEquals(bexpected, jedisCluster.lrange(bfoo, 0, 100));
  }

  @Test
  public void lindex() {
    jedisCluster.lpush("foo", "1");
    jedisCluster.lpush("foo", "2");
    jedisCluster.lpush("foo", "3");

    assertEquals("3", jedisCluster.lindex("foo", 0));
    assertNull(jedisCluster.lindex("foo", 100));

    // Binary
    jedisCluster.lpush(bfoo, b1);
    jedisCluster.lpush(bfoo, b2);
    jedisCluster.lpush(bfoo, b3);

    assertArrayEquals(b3, jedisCluster.lindex(bfoo, 0));
    assertNull(jedisCluster.lindex(bfoo, 100));

  }

  @Test
  public void lrem() {
    jedisCluster.lpush("foo", "hello");
    jedisCluster.lpush("foo", "hello");
    jedisCluster.lpush("foo", "x");
    jedisCluster.lpush("foo", "hello");
    jedisCluster.lpush("foo", "c");
    jedisCluster.lpush("foo", "b");
    jedisCluster.lpush("foo", "a");

    long count = jedisCluster.lrem("foo", -2, "hello");

    List<String> expected = new ArrayList<String>();
    expected.add("a");
    expected.add("b");
    expected.add("c");
    expected.add("hello");
    expected.add("x");

    assertEquals(2, count);
    assertEquals(expected, jedisCluster.lrange("foo", 0, 1000));
    assertEquals(0, jedisCluster.lrem("bar", 100, "foo").intValue());

    // Binary
    jedisCluster.lpush(bfoo, bhello);
    jedisCluster.lpush(bfoo, bhello);
    jedisCluster.lpush(bfoo, bx);
    jedisCluster.lpush(bfoo, bhello);
    jedisCluster.lpush(bfoo, bC);
    jedisCluster.lpush(bfoo, bB);
    jedisCluster.lpush(bfoo, bA);

    long bcount = jedisCluster.lrem(bfoo, -2, bhello);

    List<byte[]> bexpected = new ArrayList<byte[]>();
    bexpected.add(bA);
    bexpected.add(bB);
    bexpected.add(bC);
    bexpected.add(bhello);
    bexpected.add(bx);

    assertEquals(2, bcount);
    assertByteArrayListEquals(bexpected, jedisCluster.lrange(bfoo, 0, 1000));
    assertEquals(0, jedisCluster.lrem(bbar, 100, bfoo).intValue());

  }

  @Test
  public void lpop() {
    jedisCluster.rpush("foo", "a");
    jedisCluster.rpush("foo", "b");
    jedisCluster.rpush("foo", "c");

    String element = jedisCluster.lpop("foo");
    assertEquals("a", element);

    List<String> expected = new ArrayList<String>();
    expected.add("b");
    expected.add("c");

    assertEquals(expected, jedisCluster.lrange("foo", 0, 1000));
    jedisCluster.lpop("foo");
    jedisCluster.lpop("foo");

    element = jedisCluster.lpop("foo");
    assertNull(element);

    // Binary
    jedisCluster.rpush(bfoo, bA);
    jedisCluster.rpush(bfoo, bB);
    jedisCluster.rpush(bfoo, bC);

    byte[] belement = jedisCluster.lpop(bfoo);
    assertArrayEquals(bA, belement);

    List<byte[]> bexpected = new ArrayList<byte[]>();
    bexpected.add(bB);
    bexpected.add(bC);

    assertByteArrayListEquals(bexpected, jedisCluster.lrange(bfoo, 0, 1000));
    jedisCluster.lpop(bfoo);
    jedisCluster.lpop(bfoo);

    belement = jedisCluster.lpop(bfoo);
    assertNull(belement);

  }

  @Test
  public void rpop() {
    jedisCluster.rpush("foo", "a");
    jedisCluster.rpush("foo", "b");
    jedisCluster.rpush("foo", "c");

    String element = jedisCluster.rpop("foo");
    assertEquals("c", element);

    List<String> expected = new ArrayList<String>();
    expected.add("a");
    expected.add("b");

    assertEquals(expected, jedisCluster.lrange("foo", 0, 1000));
    jedisCluster.rpop("foo");
    jedisCluster.rpop("foo");

    element = jedisCluster.rpop("foo");
    assertNull(element);

    // Binary
    jedisCluster.rpush(bfoo, bA);
    jedisCluster.rpush(bfoo, bB);
    jedisCluster.rpush(bfoo, bC);

    byte[] belement = jedisCluster.rpop(bfoo);
    assertArrayEquals(bC, belement);

    List<byte[]> bexpected = new ArrayList<byte[]>();
    bexpected.add(bA);
    bexpected.add(bB);

    assertByteArrayListEquals(bexpected, jedisCluster.lrange(bfoo, 0, 1000));
    jedisCluster.rpop(bfoo);
    jedisCluster.rpop(bfoo);

    belement = jedisCluster.rpop(bfoo);
    assertNull(belement);

  }

  @Test
  public void rpoplpush() {
    jedisCluster.rpush(foo, "a");
    jedisCluster.rpush(foo, "b");
    jedisCluster.rpush(foo, "c");

    jedisCluster.rpush(dst, "foo");
    jedisCluster.rpush(dst, "bar");

    String element = jedisCluster.rpoplpush(foo, dst);

    assertEquals("c", element);

    List<String> srcExpected = new ArrayList<String>();
    srcExpected.add("a");
    srcExpected.add("b");

    List<String> dstExpected = new ArrayList<String>();
    dstExpected.add("c");
    dstExpected.add("foo");
    dstExpected.add("bar");

    assertEquals(srcExpected, jedisCluster.lrange(foo, 0, 1000));
    assertEquals(dstExpected, jedisCluster.lrange(dst, 0, 1000));

    // Binary
    jedisCluster.rpush(bfoo, bA);
    jedisCluster.rpush(bfoo, bB);
    jedisCluster.rpush(bfoo, bC);

    jedisCluster.rpush(bdst, bfoo);
    jedisCluster.rpush(bdst, bbar);

    byte[] belement = jedisCluster.rpoplpush(bfoo, bdst);

    assertArrayEquals(bC, belement);

    List<byte[]> bsrcExpected = new ArrayList<byte[]>();
    bsrcExpected.add(bA);
    bsrcExpected.add(bB);

    List<byte[]> bdstExpected = new ArrayList<byte[]>();
    bdstExpected.add(bC);
    bdstExpected.add(bfoo);
    bdstExpected.add(bbar);

    assertByteArrayListEquals(bsrcExpected, jedisCluster.lrange(bfoo, 0, 1000));
    assertByteArrayListEquals(bdstExpected, jedisCluster.lrange(bdst, 0, 1000));

  }

  @Test
  public void blpop() throws InterruptedException {
    List<String> result = jedisCluster.blpop(1, "foo");
    assertNull(result);

    jedisCluster.lpush("foo", "bar");
    result = jedisCluster.blpop(1, "foo");

    assertNotNull(result);
    assertEquals(2, result.size());
    assertEquals("foo", result.get(0));
    assertEquals("bar", result.get(1));

    // Binary
    jedisCluster.lpush(bfoo, bbar);
    List<byte[]> bresult = jedisCluster.blpop(1, bfoo);

    assertNotNull(bresult);
    assertEquals(2, bresult.size());
    assertArrayEquals(bfoo, bresult.get(0));
    assertArrayEquals(bbar, bresult.get(1));

  }

  @Test
  public void brpop() throws InterruptedException {
    List<String> result = jedisCluster.brpop(1, "foo");
    assertNull(result);

    jedisCluster.lpush("foo", "bar");
    result = jedisCluster.brpop(1, "foo");
    assertNotNull(result);
    assertEquals(2, result.size());
    assertEquals("foo", result.get(0));
    assertEquals("bar", result.get(1));

    // Binary

    jedisCluster.lpush(bfoo, bbar);
    List<byte[]> bresult = jedisCluster.brpop(1, bfoo);
    assertNotNull(bresult);
    assertEquals(2, bresult.size());
    assertArrayEquals(bfoo, bresult.get(0));
    assertArrayEquals(bbar, bresult.get(1));

  }

  @Test
  public void lpushx() {
    long status = jedisCluster.lpushx("foo", "bar");
    assertEquals(0, status);

    jedisCluster.lpush("foo", "a");
    status = jedisCluster.lpushx("foo", "b");
    assertEquals(2, status);

    // Binary
    long bstatus = jedisCluster.lpushx(bfoo, bbar);
    assertEquals(0, bstatus);

    jedisCluster.lpush(bfoo, bA);
    bstatus = jedisCluster.lpushx(bfoo, bB);
    assertEquals(2, bstatus);

  }

  @Test
  public void rpushx() {
    long status = jedisCluster.rpushx("foo", "bar");
    assertEquals(0, status);

    jedisCluster.lpush("foo", "a");
    status = jedisCluster.rpushx("foo", "b");
    assertEquals(2, status);

    // Binary
    long bstatus = jedisCluster.rpushx(bfoo, bbar);
    assertEquals(0, bstatus);

    jedisCluster.lpush(bfoo, bA);
    bstatus = jedisCluster.rpushx(bfoo, bB);
    assertEquals(2, bstatus);
  }

  @Test
  public void linsert() {
    long status = jedisCluster.linsert("foo", ListPosition.BEFORE, "bar", "car");
    assertEquals(0, status);

    jedisCluster.lpush("foo", "a");
    status = jedisCluster.linsert("foo", ListPosition.AFTER, "a", "b");
    assertEquals(2, status);

    List<String> actual = jedisCluster.lrange("foo", 0, 100);
    List<String> expected = new ArrayList<String>();
    expected.add("a");
    expected.add("b");

    assertEquals(expected, actual);

    status = jedisCluster.linsert("foo", ListPosition.BEFORE, "bar", "car");
    assertEquals(-1, status);

    // Binary
    long bstatus = jedisCluster.linsert(bfoo, ListPosition.BEFORE, bbar, bcar);
    assertEquals(0, bstatus);

    jedisCluster.lpush(bfoo, bA);
    bstatus = jedisCluster.linsert(bfoo, ListPosition.AFTER, bA, bB);
    assertEquals(2, bstatus);

    List<byte[]> bactual = jedisCluster.lrange(bfoo, 0, 100);
    List<byte[]> bexpected = new ArrayList<byte[]>();
    bexpected.add(bA);
    bexpected.add(bB);

    assertByteArrayListEquals(bexpected, bactual);

    bstatus = jedisCluster.linsert(bfoo, ListPosition.BEFORE, bbar, bcar);
    assertEquals(-1, bstatus);

  }

//  @Test
//  public void brpoplpush() {
//    (new Thread(new Runnable() {
//      public void run() {
//        try {
//          Thread.sleep(100);
//          Jedis j = createJedis();
//          j.lpush("foo", "a");
//        } catch (InterruptedException e) {
//          e.printStackTrace();
//        }
//      }
//    })).start();
//
//    String element = jedis.brpoplpush("foo", "bar", 0);
//
//    assertEquals("a", element);
//    assertEquals(1, jedis.llen("bar").longValue());
//    assertEquals("a", jedis.lrange("bar", 0, -1).get(0));
//
//    (new Thread(new Runnable() {
//      public void run() {
//        try {
//          Thread.sleep(100);
//          Jedis j = createJedis();
//          j.lpush("foo", "a");
//        } catch (InterruptedException e) {
//          e.printStackTrace();
//        }
//      }
//    })).start();
//
//    byte[] brpoplpush = jedis.brpoplpush("foo".getBytes(), "bar".getBytes(), 0);
//
//    assertTrue(Arrays.equals("a".getBytes(), brpoplpush));
//    assertEquals(1, jedis.llen("bar").longValue());
//    assertEquals("a", jedis.lrange("bar", 0, -1).get(0));
//
//  }
}
