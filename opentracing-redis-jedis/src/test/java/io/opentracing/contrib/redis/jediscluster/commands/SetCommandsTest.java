package io.opentracing.contrib.redis.jediscluster.commands;

import org.junit.Test;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static io.opentracing.contrib.redis.jediscluster.commands.TestKeys.*;
import static io.opentracing.contrib.redis.utils.AssertUtil.assertByteArraySetEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static redis.clients.jedis.ScanParams.SCAN_POINTER_START;
import static redis.clients.jedis.ScanParams.SCAN_POINTER_START_BINARY;

public class SetCommandsTest extends JedisCommandTestBase {
  final byte[] ba = { 0x0A };
  final byte[] bb = { 0x0B };
  final byte[] bc = { 0x0C };
  final byte[] bd = { 0x0D };
  final byte[] bx = { 0x42 };


  @Test
  public void sadd() {
    long status = jedisCluster.sadd("foo", "a");
    assertEquals(1, status);

    status = jedisCluster.sadd("foo", "a");
    assertEquals(0, status);

    long bstatus = jedisCluster.sadd(bfoo, ba);
    assertEquals(1, bstatus);

    bstatus = jedisCluster.sadd(bfoo, ba);
    assertEquals(0, bstatus);

  }

  @Test
  public void smembers() {
    jedisCluster.sadd("foo", "a");
    jedisCluster.sadd("foo", "b");

    Set<String> expected = new HashSet<String>();
    expected.add("a");
    expected.add("b");

    Set<String> members = jedisCluster.smembers("foo");

    assertEquals(expected, members);

    // Binary
    jedisCluster.sadd(bfoo, ba);
    jedisCluster.sadd(bfoo, bb);

    Set<byte[]> bexpected = new HashSet<byte[]>();
    bexpected.add(bb);
    bexpected.add(ba);

    Set<byte[]> bmembers = jedisCluster.smembers(bfoo);

    assertByteArraySetEquals(bexpected, bmembers);
  }

  @Test
  public void srem() {
    jedisCluster.sadd("foo", "a");
    jedisCluster.sadd("foo", "b");

    long status = jedisCluster.srem("foo", "a");

    Set<String> expected = new HashSet<String>();
    expected.add("b");

    assertEquals(1, status);
    assertEquals(expected, jedisCluster.smembers("foo"));

    status = jedisCluster.srem("foo", "bar");

    assertEquals(0, status);

    // Binary

    jedisCluster.sadd(bfoo, ba);
    jedisCluster.sadd(bfoo, bb);

    long bstatus = jedisCluster.srem(bfoo, ba);

    Set<byte[]> bexpected = new HashSet<byte[]>();
    bexpected.add(bb);

    assertEquals(1, bstatus);
    assertByteArraySetEquals(bexpected, jedisCluster.smembers(bfoo));

    bstatus = jedisCluster.srem(bfoo, bbar);

    assertEquals(0, bstatus);

  }

  @Test
  public void spop() {
    jedisCluster.sadd("foo", "a");
    jedisCluster.sadd("foo", "b");

    String member = jedisCluster.spop("foo");

    assertTrue("a".equals(member) || "b".equals(member));
    assertEquals(1, jedisCluster.smembers("foo").size());

    member = jedisCluster.spop("bar");
    assertNull(member);

    // Binary
    jedisCluster.sadd(bfoo, ba);
    jedisCluster.sadd(bfoo, bb);

    byte[] bmember = jedisCluster.spop(bfoo);

    assertTrue(Arrays.equals(ba, bmember) || Arrays.equals(bb, bmember));
    assertEquals(1, jedisCluster.smembers(bfoo).size());

    bmember = jedisCluster.spop(bbar);
    assertNull(bmember);

  }

  @Test
  public void spopWithCount() {
    jedisCluster.sadd("foo", "a");
    jedisCluster.sadd("foo", "b");

    Set<String> expected = new HashSet<String>();
    expected.add("a");
    expected.add("b");

    Set<String> members = jedisCluster.spop("foo", 2);

    assertEquals(2, members.size());
    assertEquals(expected, members);

    members = jedisCluster.spop("foo", 2);
    assertTrue(members.isEmpty());

    // Binary
    jedisCluster.sadd(bfoo, ba);
    jedisCluster.sadd(bfoo, bb);

    Set<byte[]> bexpected = new HashSet<byte[]>();
    bexpected.add(bb);
    bexpected.add(ba);

    Set<byte[]> bmembers = jedisCluster.spop(bfoo, 2);

    assertEquals(2, bmembers.size());
    assertByteArraySetEquals(bexpected, bmembers);

    bmembers = jedisCluster.spop(bfoo, 2);
    assertTrue(bmembers.isEmpty());
  }

  @Test
  public void smove() {
    jedisCluster.sadd(foo, "a");
    jedisCluster.sadd(foo, "b");

    jedisCluster.sadd(bar, "c");

    long status = jedisCluster.smove(foo, bar, "a");

    Set<String> expectedSrc = new HashSet<String>();
    expectedSrc.add("b");

    Set<String> expectedDst = new HashSet<String>();
    expectedDst.add("c");
    expectedDst.add("a");

    assertEquals(status, 1);
    assertEquals(expectedSrc, jedisCluster.smembers(foo));
    assertEquals(expectedDst, jedisCluster.smembers(bar));

    status = jedisCluster.smove(foo, bar, "a");

    assertEquals(status, 0);

    // Binary
    jedisCluster.sadd(bfoo, ba);
    jedisCluster.sadd(bfoo, bb);

    jedisCluster.sadd(bbar, bc);

    long bstatus = jedisCluster.smove(bfoo, bbar, ba);

    Set<byte[]> bexpectedSrc = new HashSet<byte[]>();
    bexpectedSrc.add(bb);

    Set<byte[]> bexpectedDst = new HashSet<byte[]>();
    bexpectedDst.add(bc);
    bexpectedDst.add(ba);

    assertEquals(bstatus, 1);
    assertByteArraySetEquals(bexpectedSrc, jedisCluster.smembers(bfoo));
    assertByteArraySetEquals(bexpectedDst, jedisCluster.smembers(bbar));

    bstatus = jedisCluster.smove(bfoo, bbar, ba);
    assertEquals(bstatus, 0);

  }

  @Test
  public void scard() {
    jedisCluster.sadd(foo, "a");
    jedisCluster.sadd(foo, "b");

    long card = jedisCluster.scard(foo);

    assertEquals(2, card);

    card = jedisCluster.scard(bar);
    assertEquals(0, card);

    // Binary
    jedisCluster.sadd(bfoo, ba);
    jedisCluster.sadd(bfoo, bb);

    long bcard = jedisCluster.scard(bfoo);

    assertEquals(2, bcard);

    bcard = jedisCluster.scard(bbar);
    assertEquals(0, bcard);

  }

  @Test
  public void sismember() {
    jedisCluster.sadd("foo", "a");
    jedisCluster.sadd("foo", "b");

    assertTrue(jedisCluster.sismember("foo", "a"));

    assertFalse(jedisCluster.sismember("foo", "c"));

    // Binary
    jedisCluster.sadd(bfoo, ba);
    jedisCluster.sadd(bfoo, bb);

    assertTrue(jedisCluster.sismember(bfoo, ba));

    assertFalse(jedisCluster.sismember(bfoo, bc));

  }

  @Test
  public void sinter() {
    jedisCluster.sadd(foo, "a");
    jedisCluster.sadd(foo, "b");

    jedisCluster.sadd(bar, "b");
    jedisCluster.sadd(bar, "c");

    Set<String> expected = new HashSet<String>();
    expected.add("b");

    Set<String> intersection = jedisCluster.sinter(foo, bar);
    assertEquals(expected, intersection);

    // Binary
    jedisCluster.sadd(bfoo, ba);
    jedisCluster.sadd(bfoo, bb);

    jedisCluster.sadd(bbar, bb);
    jedisCluster.sadd(bbar, bc);

    Set<byte[]> bexpected = new HashSet<byte[]>();
    bexpected.add(bb);

    Set<byte[]> bintersection = jedisCluster.sinter(bfoo, bbar);
    assertByteArraySetEquals(bexpected, bintersection);
  }

  @Test
  public void sinterstore() {
    jedisCluster.sadd(foo, "a");
    jedisCluster.sadd(foo, "b");

    jedisCluster.sadd(bar, "b");
    jedisCluster.sadd(bar, "c");

    Set<String> expected = new HashSet<String>();
    expected.add("b");

    long status = jedisCluster.sinterstore(car, foo, bar);
    assertEquals(1, status);

    assertEquals(expected, jedisCluster.smembers(car));

    // Binary
    jedisCluster.sadd(bfoo, ba);
    jedisCluster.sadd(bfoo, bb);

    jedisCluster.sadd(bbar, bb);
    jedisCluster.sadd(bbar, bc);

    Set<byte[]> bexpected = new HashSet<byte[]>();
    bexpected.add(bb);

    long bstatus = jedisCluster.sinterstore(bcar, bfoo, bbar);
    assertEquals(1, bstatus);

    assertByteArraySetEquals(bexpected, jedisCluster.smembers(bcar));

  }

  @Test
  public void sunion() {
    jedisCluster.sadd(foo, "a");
    jedisCluster.sadd(foo, "b");

    jedisCluster.sadd(bar, "b");
    jedisCluster.sadd(bar, "c");

    Set<String> expected = new HashSet<String>();
    expected.add("a");
    expected.add("b");
    expected.add("c");

    Set<String> union = jedisCluster.sunion(foo, bar);
    assertEquals(expected, union);

    // Binary
    jedisCluster.sadd(bfoo, ba);
    jedisCluster.sadd(bfoo, bb);

    jedisCluster.sadd(bbar, bb);
    jedisCluster.sadd(bbar, bc);

    Set<byte[]> bexpected = new HashSet<byte[]>();
    bexpected.add(bb);
    bexpected.add(bc);
    bexpected.add(ba);

    Set<byte[]> bunion = jedisCluster.sunion(bfoo, bbar);
    assertByteArraySetEquals(bexpected, bunion);

  }

  @Test
  public void sunionstore() {
    jedisCluster.sadd(foo, "a");
    jedisCluster.sadd(foo, "b");

    jedisCluster.sadd(bar, "b");
    jedisCluster.sadd(bar, "c");

    Set<String> expected = new HashSet<String>();
    expected.add("a");
    expected.add("b");
    expected.add("c");

    long status = jedisCluster.sunionstore(car, foo, bar);
    assertEquals(3, status);

    assertEquals(expected, jedisCluster.smembers(car));

    // Binary
    jedisCluster.sadd(bfoo, ba);
    jedisCluster.sadd(bfoo, bb);

    jedisCluster.sadd(bbar, bb);
    jedisCluster.sadd(bbar, bc);

    Set<byte[]> bexpected = new HashSet<byte[]>();
    bexpected.add(bb);
    bexpected.add(bc);
    bexpected.add(ba);

    long bstatus = jedisCluster.sunionstore(bcar, bfoo, bbar);
    assertEquals(3, bstatus);

    assertByteArraySetEquals(bexpected, jedisCluster.smembers(bcar));

  }

  @Test
  public void sdiff() {
    jedisCluster.sadd(foo, "x");
    jedisCluster.sadd(foo, "a");
    jedisCluster.sadd(foo, "b");
    jedisCluster.sadd(foo, "c");

    jedisCluster.sadd(bar, "c");

    jedisCluster.sadd(car, "a");
    jedisCluster.sadd(car, "d");

    Set<String> expected = new HashSet<String>();
    expected.add("x");
    expected.add("b");

    Set<String> diff = jedisCluster.sdiff(foo, bar, car);
    assertEquals(expected, diff);

    // Binary
    jedisCluster.sadd(bfoo, bx);
    jedisCluster.sadd(bfoo, ba);
    jedisCluster.sadd(bfoo, bb);
    jedisCluster.sadd(bfoo, bc);

    jedisCluster.sadd(bbar, bc);

    jedisCluster.sadd(bcar, ba);
    jedisCluster.sadd(bcar, bd);

    Set<byte[]> bexpected = new HashSet<byte[]>();
    bexpected.add(bb);
    bexpected.add(bx);

    Set<byte[]> bdiff = jedisCluster.sdiff(bfoo, bbar, bcar);
    assertByteArraySetEquals(bexpected, bdiff);

  }

  @Test
  public void sdiffstore() {
    jedisCluster.sadd(foo, "x");
    jedisCluster.sadd(foo, "a");
    jedisCluster.sadd(foo, "b");
    jedisCluster.sadd(foo, "c");

    jedisCluster.sadd(bar, "c");

    jedisCluster.sadd(car, "a");
    jedisCluster.sadd(car, "d");

    Set<String> expected = new HashSet<String>();
    expected.add("d");
    expected.add("a");

    long status = jedisCluster.sdiffstore(dst, foo, bar, car);
    assertEquals(2, status);
    assertEquals(expected, jedisCluster.smembers(car));

    // Binary
    jedisCluster.sadd(bfoo, bx);
    jedisCluster.sadd(bfoo, ba);
    jedisCluster.sadd(bfoo, bb);
    jedisCluster.sadd(bfoo, bc);

    jedisCluster.sadd(bbar, bc);

    jedisCluster.sadd(bcar, ba);
    jedisCluster.sadd(bcar, bd);

    Set<byte[]> bexpected = new HashSet<byte[]>();
    bexpected.add(bd);
    bexpected.add(ba);

    long bstatus = jedisCluster.sdiffstore(bdst, bfoo, bbar, bcar);
    assertEquals(2, bstatus);
    assertByteArraySetEquals(bexpected, jedisCluster.smembers(bcar));

  }

  @Test
  public void srandmember() {
    jedisCluster.sadd("foo", "a");
    jedisCluster.sadd("foo", "b");

    String member = jedisCluster.srandmember("foo");

    assertTrue("a".equals(member) || "b".equals(member));
    assertEquals(2, jedisCluster.smembers("foo").size());

    member = jedisCluster.srandmember("bar");
    assertNull(member);

    // Binary
    jedisCluster.sadd(bfoo, ba);
    jedisCluster.sadd(bfoo, bb);

    byte[] bmember = jedisCluster.srandmember(bfoo);

    assertTrue(Arrays.equals(ba, bmember) || Arrays.equals(bb, bmember));
    assertEquals(2, jedisCluster.smembers(bfoo).size());

    bmember = jedisCluster.srandmember(bbar);
    assertNull(bmember);
  }

  @Test
  public void sscan() {
    jedisCluster.sadd("foo", "a", "b");

    ScanResult<String> result = jedisCluster.sscan("foo", SCAN_POINTER_START);

    assertEquals(SCAN_POINTER_START, result.getStringCursor());
    assertFalse(result.getResult().isEmpty());

    // binary
    jedisCluster.sadd(bfoo, ba, bb);

    ScanResult<byte[]> bResult = jedisCluster.sscan(bfoo, SCAN_POINTER_START_BINARY);

    assertArrayEquals(SCAN_POINTER_START_BINARY, bResult.getCursorAsBytes());
    assertFalse(bResult.getResult().isEmpty());
  }

  @Test
  public void sscanMatch() {
    ScanParams params = new ScanParams();
    params.match("a*");

    jedisCluster.sadd("foo", "b", "a", "aa");
    ScanResult<String> result = jedisCluster.sscan("foo", SCAN_POINTER_START, params);

    assertEquals(SCAN_POINTER_START, result.getStringCursor());
    assertFalse(result.getResult().isEmpty());

    // binary
    params = new ScanParams();
    params.match(bbarstar);

    jedisCluster.sadd(bfoo, bbar1, bbar2, bbar3);
    ScanResult<byte[]> bResult = jedisCluster.sscan(bfoo, SCAN_POINTER_START_BINARY, params);

    assertArrayEquals(SCAN_POINTER_START_BINARY, bResult.getCursorAsBytes());
    assertFalse(bResult.getResult().isEmpty());
  }

  @Test
  public void sscanCount() {
    ScanParams params = new ScanParams();
    params.count(2);

    jedisCluster.sadd("foo", "a1", "a2", "a3", "a4", "a5");

    ScanResult<String> result = jedisCluster.sscan("foo", SCAN_POINTER_START, params);

    assertFalse(result.getResult().isEmpty());

    // binary
    params = new ScanParams();
    params.count(2);

    jedisCluster.sadd(bfoo, bbar1, bbar2, bbar3);
    ScanResult<byte[]> bResult = jedisCluster.sscan(bfoo, SCAN_POINTER_START_BINARY, params);

    assertFalse(bResult.getResult().isEmpty());
  }
}
