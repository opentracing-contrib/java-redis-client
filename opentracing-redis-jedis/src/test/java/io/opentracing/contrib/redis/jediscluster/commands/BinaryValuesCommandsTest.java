package io.opentracing.contrib.redis.jediscluster.commands;

import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Protocol.Keyword;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.opentracing.contrib.redis.jediscluster.commands.TestKeys.*;
import static io.opentracing.contrib.redis.utils.AssertUtil.assertByteArrayListEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class BinaryValuesCommandsTest extends JedisCommandTestBase {
  byte[] bxx = { 0x78, 0x78 };
  byte[] bnx = { 0x6E, 0x78 };
  byte[] bex = { 0x65, 0x78 };
  byte[] bpx = { 0x70, 0x78 };
  long expireSeconds = 2;
  long expireMillis = expireSeconds * 1000;
  byte[] binaryValue;

  @Before
  public void startUp() {
    StringBuilder sb = new StringBuilder();

    for (int n = 0; n < 1000; n++) {
      sb.append("A");
    }

    binaryValue = sb.toString().getBytes();
  }

  @Test
  public void setAndGet() {
    String status = jedisCluster.set(bfoo, binaryValue);
    assertTrue(Keyword.OK.name().equalsIgnoreCase(status));

    byte[] value = jedisCluster.get(bfoo);
    assertTrue(Arrays.equals(binaryValue, value));

    assertNull(jedisCluster.get(bbar));
  }

  @Test
  public void setNxExAndGet() {
    String status = jedisCluster.set(bfoo, binaryValue, bnx, bex, expireSeconds);
    assertTrue(Keyword.OK.name().equalsIgnoreCase(status));
    byte[] value = jedisCluster.get(bfoo);
    assertTrue(Arrays.equals(binaryValue, value));

    assertNull(jedisCluster.get(bbar));
  }

  @Test
  public void setIfNotExistAndGet() {
    String status = jedisCluster.set(bfoo, binaryValue);
    assertTrue(Keyword.OK.name().equalsIgnoreCase(status));
    // nx should fail if value exists
    String statusFail = jedisCluster.set(bfoo, binaryValue, bnx, bex, expireSeconds);
    assertNull(statusFail);

    byte[] value = jedisCluster.get(bfoo);
    assertTrue(Arrays.equals(binaryValue, value));

    assertNull(jedisCluster.get(bbar));
  }

  @Test
  public void setIfExistAndGet() {
    String status = jedisCluster.set(bfoo, binaryValue);
    assertTrue(Keyword.OK.name().equalsIgnoreCase(status));
    // nx should fail if value exists
    String statusSuccess = jedisCluster.set(bfoo, binaryValue, bxx, bex, expireSeconds);
    assertTrue(Keyword.OK.name().equalsIgnoreCase(statusSuccess));

    byte[] value = jedisCluster.get(bfoo);
    assertTrue(Arrays.equals(binaryValue, value));

    assertNull(jedisCluster.get(bbar));
  }

  @Test
  public void setFailIfNotExistAndGet() {
    // xx should fail if value does NOT exists
    String statusFail = jedisCluster.set(bfoo, binaryValue, bxx, bex, expireSeconds);
    assertNull(statusFail);
  }

  @Test
  public void setAndExpireMillis() {
    String status = jedisCluster.set(bfoo, binaryValue, bnx, bpx, expireMillis);
    assertTrue(Keyword.OK.name().equalsIgnoreCase(status));
    long ttl = jedisCluster.ttl(bfoo);
    assertTrue(ttl > 0 && ttl <= expireSeconds);
  }

  @Test
  public void setAndExpire() {
    String status = jedisCluster.set(bfoo, binaryValue, bnx, bex, expireSeconds);
    assertTrue(Keyword.OK.name().equalsIgnoreCase(status));
    long ttl = jedisCluster.ttl(bfoo);
    assertTrue(ttl > 0 && ttl <= expireSeconds);
  }

  @Test
  public void getSet() {
    byte[] value = jedisCluster.getSet(bfoo, binaryValue);
    assertNull(value);
    value = jedisCluster.get(bfoo);
    assertTrue(Arrays.equals(binaryValue, value));
  }

  @Test
  public void mget() {
    List<byte[]> values = jedisCluster.mget(bfoo, bbar);
    List<byte[]> expected = new ArrayList<byte[]>();
    expected.add(null);
    expected.add(null);

    assertByteArrayListEquals(expected, values);

    jedisCluster.set(bfoo, binaryValue);

    expected = new ArrayList<byte[]>();
    expected.add(binaryValue);
    expected.add(null);
    values = jedisCluster.mget(bfoo, bbar);

    assertByteArrayListEquals(expected, values);

    jedisCluster.set(bbar, bfoo);

    expected = new ArrayList<byte[]>();
    expected.add(binaryValue);
    expected.add(bfoo);
    values = jedisCluster.mget(bfoo, bbar);

    assertByteArrayListEquals(expected, values);
  }

  @Test
  public void setnx() {
    long status = jedisCluster.setnx(bfoo, binaryValue);
    assertEquals(1, status);
    assertTrue(Arrays.equals(binaryValue, jedisCluster.get(bfoo)));

    status = jedisCluster.setnx(bfoo, bbar);
    assertEquals(0, status);
    assertTrue(Arrays.equals(binaryValue, jedisCluster.get(bfoo)));
  }

  @Test
  public void setex() {
    String status = jedisCluster.setex(bfoo, 20, binaryValue);
    assertEquals(Keyword.OK.name(), status);
    long ttl = jedisCluster.ttl(bfoo);
    assertTrue(ttl > 0 && ttl <= 20);
  }

  @Test
  public void mset() {
    String status = jedisCluster.mset(bfoo, binaryValue, bbar, bfoo);
    assertEquals(Keyword.OK.name(), status);
    assertTrue(Arrays.equals(binaryValue, jedisCluster.get(bfoo)));
    assertTrue(Arrays.equals(bfoo, jedisCluster.get(bbar)));
  }

  @Test
  public void msetnx() {
    long status = jedisCluster.msetnx(bfoo, binaryValue, bbar, bfoo);
    assertEquals(1, status);
    assertTrue(Arrays.equals(binaryValue, jedisCluster.get(bfoo)));
    assertTrue(Arrays.equals(bfoo, jedisCluster.get(bbar)));

    status = jedisCluster.msetnx(bfoo, bbar, bbar2, "foo2".getBytes());
    assertEquals(0, status);
    assertTrue(Arrays.equals(binaryValue, jedisCluster.get(bfoo)));
    assertTrue(Arrays.equals(bfoo, jedisCluster.get(bbar)));
  }

  @Test(expected = JedisDataException.class)
  public void incrWrongValue() {
    jedisCluster.set(bfoo, binaryValue);
    jedisCluster.incr(bfoo);
  }

  @Test
  public void incr() {
    long value = jedisCluster.incr(bfoo);
    assertEquals(1, value);
    value = jedisCluster.incr(bfoo);
    assertEquals(2, value);
  }

  @Test(expected = JedisDataException.class)
  public void incrByWrongValue() {
    jedisCluster.set(bfoo, binaryValue);
    jedisCluster.incrBy(bfoo, 2);
  }

  @Test
  public void incrBy() {
    long value = jedisCluster.incrBy(bfoo, 2);
    assertEquals(2, value);
    value = jedisCluster.incrBy(bfoo, 2);
    assertEquals(4, value);
  }

  @Test(expected = JedisDataException.class)
  public void decrWrongValue() {
    jedisCluster.set(bfoo, binaryValue);
    jedisCluster.decr(bfoo);
  }

  @Test
  public void decr() {
    long value = jedisCluster.decr(bfoo);
    assertEquals(-1, value);
    value = jedisCluster.decr(bfoo);
    assertEquals(-2, value);
  }

  @Test(expected = JedisDataException.class)
  public void decrByWrongValue() {
    jedisCluster.set(bfoo, binaryValue);
    jedisCluster.decrBy(bfoo, 2);
  }

  @Test
  public void decrBy() {
    long value = jedisCluster.decrBy(bfoo, 2);
    assertEquals(-2, value);
    value = jedisCluster.decrBy(bfoo, 2);
    assertEquals(-4, value);
  }

  @Test
  public void append() {
    byte[] first512 = new byte[512];
    System.arraycopy(binaryValue, 0, first512, 0, 512);
    long value = jedisCluster.append(bfoo, first512);
    assertEquals(512, value);
    assertTrue(Arrays.equals(first512, jedisCluster.get(bfoo)));

    byte[] rest = new byte[binaryValue.length - 512];
    System.arraycopy(binaryValue, 512, rest, 0, binaryValue.length - 512);
    value = jedisCluster.append(bfoo, rest);
    assertEquals(binaryValue.length, value);

    assertTrue(Arrays.equals(binaryValue, jedisCluster.get(bfoo)));
  }

  @Test
  public void substr() {
    jedisCluster.set(bfoo, binaryValue);

    byte[] first512 = new byte[512];
    System.arraycopy(binaryValue, 0, first512, 0, 512);
    byte[] rfirst512 = jedisCluster.substr(bfoo, 0, 511);
    assertTrue(Arrays.equals(first512, rfirst512));

    byte[] last512 = new byte[512];
    System.arraycopy(binaryValue, binaryValue.length - 512, last512, 0, 512);
    assertTrue(Arrays.equals(last512, jedisCluster.substr(bfoo, -512, -1)));

    assertTrue(Arrays.equals(binaryValue, jedisCluster.substr(bfoo, 0, -1)));

    assertTrue(Arrays.equals(last512, jedisCluster.substr(bfoo, binaryValue.length - 512, 100000)));
  }

  @Test
  public void strlen() {
    jedisCluster.set(bfoo, binaryValue);
    assertEquals(binaryValue.length, jedisCluster.strlen(bfoo).intValue());
  }
}