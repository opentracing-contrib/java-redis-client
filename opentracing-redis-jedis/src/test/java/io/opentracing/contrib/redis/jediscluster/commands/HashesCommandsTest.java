package io.opentracing.contrib.redis.jediscluster.commands;

import org.junit.Test;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.opentracing.contrib.redis.utils.AssertUtil.assertByteArrayListEquals;
import static io.opentracing.contrib.redis.utils.AssertUtil.assertByteArraySetEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static redis.clients.jedis.ScanParams.SCAN_POINTER_START;
import static redis.clients.jedis.ScanParams.SCAN_POINTER_START_BINARY;


public class HashesCommandsTest extends JedisCommandTestBase {
  final byte[] bfoo = { 0x01, 0x02, 0x03, 0x04 };
  final byte[] bbar = { 0x05, 0x06, 0x07, 0x08 };
  final byte[] bcar = { 0x09, 0x0A, 0x0B, 0x0C };

  final byte[] bbar1 = { 0x05, 0x06, 0x07, 0x08, 0x0A };
  final byte[] bbar2 = { 0x05, 0x06, 0x07, 0x08, 0x0B };
  final byte[] bbar3 = { 0x05, 0x06, 0x07, 0x08, 0x0C };
  final byte[] bbarstar = { 0x05, 0x06, 0x07, 0x08, '*' };

  @Test
  public void hset() {
    long status = jedisCluster.hset("foo", "bar", "car");
    assertEquals(1, status);
    status = jedisCluster.hset("foo", "bar", "foo");
    assertEquals(0, status);

    // Binary
    long bstatus = jedisCluster.hset(bfoo, bbar, bcar);
    assertEquals(1, bstatus);
    bstatus = jedisCluster.hset(bfoo, bbar, bfoo);
    assertEquals(0, bstatus);

  }

  @Test
  public void hget() {
    jedisCluster.hset("foo", "bar", "car");
    assertNull(jedisCluster.hget("bar", "foo"));
    assertNull(jedisCluster.hget("foo", "car"));
    assertEquals("car", jedisCluster.hget("foo", "bar"));

    // Binary
    jedisCluster.hset(bfoo, bbar, bcar);
    assertNull(jedisCluster.hget(bbar, bfoo));
    assertNull(jedisCluster.hget(bfoo, bcar));
    assertArrayEquals(bcar, jedisCluster.hget(bfoo, bbar));
  }

  @Test
  public void hsetnx() {
    long status = jedisCluster.hsetnx("foo", "bar", "car");
    assertEquals(1, status);
    assertEquals("car", jedisCluster.hget("foo", "bar"));

    status = jedisCluster.hsetnx("foo", "bar", "foo");
    assertEquals(0, status);
    assertEquals("car", jedisCluster.hget("foo", "bar"));

    status = jedisCluster.hsetnx("foo", "car", "bar");
    assertEquals(1, status);
    assertEquals("bar", jedisCluster.hget("foo", "car"));

    // Binary
    long bstatus = jedisCluster.hsetnx(bfoo, bbar, bcar);
    assertEquals(1, bstatus);
    assertArrayEquals(bcar, jedisCluster.hget(bfoo, bbar));

    bstatus = jedisCluster.hsetnx(bfoo, bbar, bfoo);
    assertEquals(0, bstatus);
    assertArrayEquals(bcar, jedisCluster.hget(bfoo, bbar));

    bstatus = jedisCluster.hsetnx(bfoo, bcar, bbar);
    assertEquals(1, bstatus);
    assertArrayEquals(bbar, jedisCluster.hget(bfoo, bcar));

  }

  @Test
  public void hmset() {
    Map<String, String> hash = new HashMap<String, String>();
    hash.put("bar", "car");
    hash.put("car", "bar");
    String status = jedisCluster.hmset("foo", hash);
    assertEquals("OK", status);
    assertEquals("car", jedisCluster.hget("foo", "bar"));
    assertEquals("bar", jedisCluster.hget("foo", "car"));

    // Binary
    Map<byte[], byte[]> bhash = new HashMap<byte[], byte[]>();
    bhash.put(bbar, bcar);
    bhash.put(bcar, bbar);
    String bstatus = jedisCluster.hmset(bfoo, bhash);
    assertEquals("OK", bstatus);
    assertArrayEquals(bcar, jedisCluster.hget(bfoo, bbar));
    assertArrayEquals(bbar, jedisCluster.hget(bfoo, bcar));

  }

  @Test
  public void hsetVariadic() {
    Map<String, String> hash = new HashMap<String, String>();
    hash.put("bar", "car");
    hash.put("car", "bar");
    long status = jedisCluster.hset("foo", hash);
    assertEquals(2, status);
    assertEquals("car", jedisCluster.hget("foo", "bar"));
    assertEquals("bar", jedisCluster.hget("foo", "car"));

    // Binary
    Map<byte[], byte[]> bhash = new HashMap<byte[], byte[]>();
    bhash.put(bbar, bcar);
    bhash.put(bcar, bbar);
    status = jedisCluster.hset(bfoo, bhash);
    assertEquals(2, status);
    assertArrayEquals(bcar, jedisCluster.hget(bfoo, bbar));
    assertArrayEquals(bbar, jedisCluster.hget(bfoo, bcar));
  }

  @Test
  public void hmget() {
    Map<String, String> hash = new HashMap<String, String>();
    hash.put("bar", "car");
    hash.put("car", "bar");
    jedisCluster.hmset("foo", hash);

    List<String> values = jedisCluster.hmget("foo", "bar", "car", "foo");
    List<String> expected = new ArrayList<String>();
    expected.add("car");
    expected.add("bar");
    expected.add(null);

    assertEquals(expected, values);

    // Binary
    Map<byte[], byte[]> bhash = new HashMap<byte[], byte[]>();
    bhash.put(bbar, bcar);
    bhash.put(bcar, bbar);
    jedisCluster.hmset(bfoo, bhash);

    List<byte[]> bvalues = jedisCluster.hmget(bfoo, bbar, bcar, bfoo);
    List<byte[]> bexpected = new ArrayList<byte[]>();
    bexpected.add(bcar);
    bexpected.add(bbar);
    bexpected.add(null);

    assertByteArrayListEquals(bexpected, bvalues);
  }

  @Test
  public void hincrBy() {
    long value = jedisCluster.hincrBy("foo", "bar", 1);
    assertEquals(1, value);
    value = jedisCluster.hincrBy("foo", "bar", -1);
    assertEquals(0, value);
    value = jedisCluster.hincrBy("foo", "bar", -10);
    assertEquals(-10, value);

    // Binary
    long bvalue = jedisCluster.hincrBy(bfoo, bbar, 1);
    assertEquals(1, bvalue);
    bvalue = jedisCluster.hincrBy(bfoo, bbar, -1);
    assertEquals(0, bvalue);
    bvalue = jedisCluster.hincrBy(bfoo, bbar, -10);
    assertEquals(-10, bvalue);

  }

  @Test
  public void hincrByFloat() {
    Double value = jedisCluster.hincrByFloat("foo", "bar", 1.5d);
    assertEquals((Double) 1.5d, value);
    value = jedisCluster.hincrByFloat("foo", "bar", -1.5d);
    assertEquals((Double) 0d, value);
    value = jedisCluster.hincrByFloat("foo", "bar", -10.7d);
    assertEquals(Double.valueOf(-10.7d), value);

    // Binary
    double bvalue = jedisCluster.hincrByFloat(bfoo, bbar, 1.5d);
    assertEquals(1.5d, bvalue, 0d);
    bvalue = jedisCluster.hincrByFloat(bfoo, bbar, -1.5d);
    assertEquals(0d, bvalue, 0d);
    bvalue = jedisCluster.hincrByFloat(bfoo, bbar, -10.7d);
    assertEquals(-10.7d, bvalue, 0d);

  }

  @Test
  public void hexists() {
    Map<String, String> hash = new HashMap<String, String>();
    hash.put("bar", "car");
    hash.put("car", "bar");
    jedisCluster.hmset("foo", hash);

    assertFalse(jedisCluster.hexists("bar", "foo"));
    assertFalse(jedisCluster.hexists("foo", "foo"));
    assertTrue(jedisCluster.hexists("foo", "bar"));

    // Binary
    Map<byte[], byte[]> bhash = new HashMap<byte[], byte[]>();
    bhash.put(bbar, bcar);
    bhash.put(bcar, bbar);
    jedisCluster.hmset(bfoo, bhash);

    assertFalse(jedisCluster.hexists(bbar, bfoo));
    assertFalse(jedisCluster.hexists(bfoo, bfoo));
    assertTrue(jedisCluster.hexists(bfoo, bbar));

  }

  @Test
  public void hdel() {
    Map<String, String> hash = new HashMap<String, String>();
    hash.put("bar", "car");
    hash.put("car", "bar");
    jedisCluster.hmset("foo", hash);

    assertEquals(0, jedisCluster.hdel("bar", "foo").intValue());
    assertEquals(0, jedisCluster.hdel("foo", "foo").intValue());
    assertEquals(1, jedisCluster.hdel("foo", "bar").intValue());
    assertNull(jedisCluster.hget("foo", "bar"));

    // Binary
    Map<byte[], byte[]> bhash = new HashMap<byte[], byte[]>();
    bhash.put(bbar, bcar);
    bhash.put(bcar, bbar);
    jedisCluster.hmset(bfoo, bhash);

    assertEquals(0, jedisCluster.hdel(bbar, bfoo).intValue());
    assertEquals(0, jedisCluster.hdel(bfoo, bfoo).intValue());
    assertEquals(1, jedisCluster.hdel(bfoo, bbar).intValue());
    assertNull(jedisCluster.hget(bfoo, bbar));

  }

  @Test
  public void hlen() {
    Map<String, String> hash = new HashMap<String, String>();
    hash.put("bar", "car");
    hash.put("car", "bar");
    jedisCluster.hmset("foo", hash);

    assertEquals(0, jedisCluster.hlen("bar").intValue());
    assertEquals(2, jedisCluster.hlen("foo").intValue());

    // Binary
    Map<byte[], byte[]> bhash = new HashMap<byte[], byte[]>();
    bhash.put(bbar, bcar);
    bhash.put(bcar, bbar);
    jedisCluster.hmset(bfoo, bhash);

    assertEquals(0, jedisCluster.hlen(bbar).intValue());
    assertEquals(2, jedisCluster.hlen(bfoo).intValue());

  }

  @Test
  public void hkeys() {
    Map<String, String> hash = new LinkedHashMap<String, String>();
    hash.put("bar", "car");
    hash.put("car", "bar");
    jedisCluster.hmset("foo", hash);

    Set<String> keys = jedisCluster.hkeys("foo");
    Set<String> expected = new LinkedHashSet<String>();
    expected.add("bar");
    expected.add("car");
    assertEquals(expected, keys);

    // Binary
    Map<byte[], byte[]> bhash = new LinkedHashMap<byte[], byte[]>();
    bhash.put(bbar, bcar);
    bhash.put(bcar, bbar);
    jedisCluster.hmset(bfoo, bhash);

    Set<byte[]> bkeys = jedisCluster.hkeys(bfoo);
    Set<byte[]> bexpected = new LinkedHashSet<byte[]>();
    bexpected.add(bbar);
    bexpected.add(bcar);
    assertByteArraySetEquals(bexpected, bkeys);
  }

  @Test
  public void hvals() {
    Map<String, String> hash = new LinkedHashMap<String, String>();
    hash.put("bar", "car");
    hash.put("car", "bar");
    jedisCluster.hmset("foo", hash);

    List<String> vals = jedisCluster.hvals("foo");
    assertEquals(2, vals.size());
    assertTrue(vals.contains("bar"));
    assertTrue(vals.contains("car"));

    // Binary
    Map<byte[], byte[]> bhash = new LinkedHashMap<byte[], byte[]>();
    bhash.put(bbar, bcar);
    bhash.put(bcar, bbar);
    jedisCluster.hmset(bfoo, bhash);

    List<byte[]> bvals = new ArrayList<>(jedisCluster.hvals(bfoo));

    assertEquals(2, bvals.size());
    assertTrue(arrayContains(bvals, bbar));
    assertTrue(arrayContains(bvals, bcar));
  }

  @Test
  public void hgetAll() {
    Map<String, String> h = new HashMap<String, String>();
    h.put("bar", "car");
    h.put("car", "bar");
    jedisCluster.hmset("foo", h);

    Map<String, String> hash = jedisCluster.hgetAll("foo");
    assertEquals(2, hash.size());
    assertEquals("car", hash.get("bar"));
    assertEquals("bar", hash.get("car"));

    // Binary
    Map<byte[], byte[]> bh = new HashMap<byte[], byte[]>();
    bh.put(bbar, bcar);
    bh.put(bcar, bbar);
    jedisCluster.hmset(bfoo, bh);
    Map<byte[], byte[]> bhash = jedisCluster.hgetAll(bfoo);

    assertEquals(2, bhash.size());
    assertArrayEquals(bcar, bhash.get(bbar));
    assertArrayEquals(bbar, bhash.get(bcar));
  }

//  @Test
//  public void hgetAllPipeline() {
//    Map<byte[], byte[]> bh = new HashMap<byte[], byte[]>();
//    bh.put(bbar, bcar);
//    bh.put(bcar, bbar);
//    jedis.hmset(bfoo, bh);
//    Pipeline pipeline = jedis.pipelined();
//    Response<Map<byte[], byte[]>> bhashResponse = pipeline.hgetAll(bfoo);
//    pipeline.sync();
//    Map<byte[], byte[]> bhash = bhashResponse.get();
//
//    assertEquals(2, bhash.size());
//    assertArrayEquals(bcar, bhash.get(bbar));
//    assertArrayEquals(bbar, bhash.get(bcar));
//  }

  @Test
  public void hscan() {
    jedisCluster.hset("foo", "b", "b");
    jedisCluster.hset("foo", "a", "a");

    ScanResult<Map.Entry<String, String>> result = jedisCluster.hscan("foo", SCAN_POINTER_START);

    assertEquals(SCAN_POINTER_START, result.getStringCursor());
    assertFalse(result.getResult().isEmpty());

    // binary
    jedisCluster.hset(bfoo, bbar, bcar);

    ScanResult<Map.Entry<byte[], byte[]>> bResult = jedisCluster.hscan(bfoo, SCAN_POINTER_START_BINARY);

    assertArrayEquals(SCAN_POINTER_START_BINARY, bResult.getCursorAsBytes());
    assertFalse(bResult.getResult().isEmpty());
  }

  @Test
  public void hscanMatch() {
    ScanParams params = new ScanParams();
    params.match("a*");

    jedisCluster.hset("foo", "b", "b");
    jedisCluster.hset("foo", "a", "a");
    jedisCluster.hset("foo", "aa", "aa");
    ScanResult<Map.Entry<String, String>> result = jedisCluster.hscan("foo", SCAN_POINTER_START, params);

    assertEquals(SCAN_POINTER_START, result.getStringCursor());
    assertFalse(result.getResult().isEmpty());

    // binary
    params = new ScanParams();
    params.match(bbarstar);

    jedisCluster.hset(bfoo, bbar, bcar);
    jedisCluster.hset(bfoo, bbar1, bcar);
    jedisCluster.hset(bfoo, bbar2, bcar);
    jedisCluster.hset(bfoo, bbar3, bcar);

    ScanResult<Map.Entry<byte[], byte[]>> bResult = jedisCluster.hscan(bfoo, SCAN_POINTER_START_BINARY,
      params);

    assertArrayEquals(SCAN_POINTER_START_BINARY, bResult.getCursorAsBytes());
    assertFalse(bResult.getResult().isEmpty());
  }

  @Test
  public void hscanCount() {
    ScanParams params = new ScanParams();
    params.count(2);

    for (int i = 0; i < 10; i++) {
      jedisCluster.hset("foo", "a" + i, "a" + i);
    }

    ScanResult<Map.Entry<String, String>> result = jedisCluster.hscan("foo", SCAN_POINTER_START, params);

    assertFalse(result.getResult().isEmpty());

    // binary
    params = new ScanParams();
    params.count(2);

    jedisCluster.hset(bfoo, bbar, bcar);
    jedisCluster.hset(bfoo, bbar1, bcar);
    jedisCluster.hset(bfoo, bbar2, bcar);
    jedisCluster.hset(bfoo, bbar3, bcar);

    ScanResult<Map.Entry<byte[], byte[]>> bResult = jedisCluster.hscan(bfoo, SCAN_POINTER_START_BINARY,
      params);

    assertFalse(bResult.getResult().isEmpty());
  }

  @Test
  public void testHstrLen_EmptyHash() {
    Long response = jedisCluster.hstrlen("myhash", "k1");
    assertEquals(0l, response.longValue());
  }

  @Test
  public void testHstrLen() {
    Map<String, String> values = new HashMap<String, String>();
    values.put("key", "value");
    jedisCluster.hmset("myhash", values);
    Long response = jedisCluster.hstrlen("myhash", "key");
    assertEquals(5l, response.longValue());

  }

  @Test
  public void testBinaryHstrLen() {
    Map<byte[], byte[]> values = new HashMap<byte[], byte[]>();
    values.put(bbar, bcar);
    jedisCluster.hmset(bfoo, values);
    Long response = jedisCluster.hstrlen(bfoo, bbar);
    assertEquals(4l, response.longValue());
  }

}
