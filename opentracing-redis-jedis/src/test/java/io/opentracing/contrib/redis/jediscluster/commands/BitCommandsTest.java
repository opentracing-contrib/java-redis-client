package io.opentracing.contrib.redis.jediscluster.commands;

import org.junit.Test;
import redis.clients.jedis.BitOP;
import redis.clients.jedis.BitPosParams;
import redis.clients.jedis.Protocol;
import redis.clients.util.SafeEncoder;

import java.util.List;

import static io.opentracing.contrib.redis.jediscluster.commands.TestKeys.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BitCommandsTest extends JedisCommandTestBase {
  @Test
  public void setAndgetbit() {
    boolean bit = jedisCluster.setbit("foo", 0, true);
    assertEquals(false, bit);

    bit = jedisCluster.getbit("foo", 0);
    assertEquals(true, bit);

    boolean bbit = jedisCluster.setbit("bfoo".getBytes(), 0, "1".getBytes());
    assertFalse(bbit);

    bbit = jedisCluster.getbit("bfoo".getBytes(), 0);
    assertTrue(bbit);
  }

  @Test
  public void bitpos() {
    String foo = "foo";

    jedisCluster.set(foo, String.valueOf(0));

    jedisCluster.setbit(foo, 3, true);
    jedisCluster.setbit(foo, 7, true);
    jedisCluster.setbit(foo, 13, true);
    jedisCluster.setbit(foo, 39, true);

    /*
     * byte: 0 1 2 3 4 bit: 00010001 / 00000100 / 00000000 / 00000000 / 00000001
     */
    long offset = jedisCluster.bitpos(foo, true);
    assertEquals(2, offset);
    offset = jedisCluster.bitpos(foo, false);
    assertEquals(0, offset);

    offset = jedisCluster.bitpos(foo, true, new BitPosParams(1));
    assertEquals(13, offset);
    offset = jedisCluster.bitpos(foo, false, new BitPosParams(1));
    assertEquals(8, offset);

    offset = jedisCluster.bitpos(foo, true, new BitPosParams(2, 3));
    assertEquals(-1, offset);
    offset = jedisCluster.bitpos(foo, false, new BitPosParams(2, 3));
    assertEquals(16, offset);

    offset = jedisCluster.bitpos(foo, true, new BitPosParams(3, 4));
    assertEquals(39, offset);
  }

  @Test
  public void bitposBinary() {
    // binary
    byte[] bfoo = { 0x01, 0x02, 0x03, 0x04 };
    String foo = new String(bfoo);

    jedisCluster.set(bfoo, Protocol.toByteArray(0));

    jedisCluster.setbit(bfoo, 3, true);
    jedisCluster.setbit(bfoo, 7, true);
    jedisCluster.setbit(bfoo, 13, true);
    jedisCluster.setbit(bfoo, 39, true);

    /*
     * byte: 0 1 2 3 4 bit: 00010001 / 00000100 / 00000000 / 00000000 / 00000001
     */
    long offset = jedisCluster.bitpos(foo, true);
    assertEquals(2, offset);
    offset = jedisCluster.bitpos(foo, false);
    assertEquals(0, offset);

    offset = jedisCluster.bitpos(foo, true, new BitPosParams(1));
    assertEquals(13, offset);
    offset = jedisCluster.bitpos(foo, false, new BitPosParams(1));
    assertEquals(8, offset);

    offset = jedisCluster.bitpos(foo, true, new BitPosParams(2, 3));
    assertEquals(-1, offset);
    offset = jedisCluster.bitpos(foo, false, new BitPosParams(2, 3));
    assertEquals(16, offset);

    offset = jedisCluster.bitpos(foo, true, new BitPosParams(3, 4));
    assertEquals(39, offset);
  }

  @Test
  public void bitposWithNoMatchingBitExist() {
    String foo = "foo";

    jedisCluster.set(foo, String.valueOf(0));
    for (int idx = 0; idx < 8; idx++) {
      jedisCluster.setbit(foo, idx, true);
    }

    /*
     * byte: 0 bit: 11111111
     */
    long offset = jedisCluster.bitpos(foo, false);
    // offset should be last index + 1
    assertEquals(8, offset);
  }

  @Test
  public void bitposWithNoMatchingBitExistWithinRange() {
    String foo = "foo";

    jedisCluster.set(foo, String.valueOf(0));
    for (int idx = 0; idx < 8 * 5; idx++) {
      jedisCluster.setbit(foo, idx, true);
    }

    /*
     * byte: 0 1 2 3 4 bit: 11111111 / 11111111 / 11111111 / 11111111 / 11111111
     */
    long offset = jedisCluster.bitpos(foo, false, new BitPosParams(2, 3));
    // offset should be -1
    assertEquals(-1, offset);
  }

  @Test
  public void setAndgetrange() {
    jedisCluster.set("key1", "Hello World");
    long reply = jedisCluster.setrange("key1", 6, "Jedis");
    assertEquals(11, reply);

    assertEquals("Hello Jedis", jedisCluster.get("key1"));

    assertEquals("Hello", jedisCluster.getrange("key1", 0, 4));
    assertEquals("Jedis", jedisCluster.getrange("key1", 6, 11));
  }

  @Test
  public void bitCount() {
    jedisCluster.setbit("foo", 16, true);
    jedisCluster.setbit("foo", 24, true);
    jedisCluster.setbit("foo", 40, true);
    jedisCluster.setbit("foo", 56, true);

    long c4 = jedisCluster.bitcount("foo");
    assertEquals(4, c4);

    long c3 = jedisCluster.bitcount("foo", 2L, 5L);
    assertEquals(3, c3);
  }

  @Test
  public void bitOp() {
    jedisCluster.set(foo1, "\u0060");
    jedisCluster.set(foo2, "\u0044");

    jedisCluster.bitop(BitOP.AND, Key_resultAnd, foo1, foo2);
    String resultAnd = jedisCluster.get(Key_resultAnd);
    assertEquals("\u0040", resultAnd);

    jedisCluster.bitop(BitOP.OR, Key_resultOr,  foo1, foo2);
    String resultOr = jedisCluster.get(Key_resultOr);
    assertEquals("\u0064", resultOr);

    jedisCluster.bitop(BitOP.XOR, Key_resultXor, foo1, foo2);
    String resultXor = jedisCluster.get(Key_resultXor);
    assertEquals("\u0024", resultXor);
  }

  @Test
  public void bitOpNot() {
    jedisCluster.setbit(foo, 0, true);
    jedisCluster.setbit(foo, 4, true);

    jedisCluster.bitop(BitOP.NOT, Key_resultNot, foo);

    String resultNot = jedisCluster.get(Key_resultNot);
    assertEquals("\u0077", resultNot);
  }

  @Test(expected = redis.clients.jedis.exceptions.JedisDataException.class)
  public void bitOpNotMultiSourceShouldFail() {
    jedisCluster.bitop(BitOP.NOT, "dest", "src1", "src2");
  }

  @Test
  public void testBitfield() {
    List<Long> responses = jedisCluster.bitfield("mykey", "INCRBY","i5","100","1", "GET", "u4", "0");
    assertEquals(1L, responses.get(0).longValue());
    assertEquals(0L, responses.get(1).longValue());
  }

  @Test
  public void testBinaryBitfield() {
    List<Long> responses = jedisCluster.bitfield(SafeEncoder.encode("mykey"), SafeEncoder.encode("INCRBY"),
            SafeEncoder.encode("i5"), SafeEncoder.encode("100"), SafeEncoder.encode("1"),
            SafeEncoder.encode("GET"), SafeEncoder.encode("u4"), SafeEncoder.encode("0")
    );
    assertEquals(1L, responses.get(0).longValue());
    assertEquals(0L, responses.get(1).longValue());
  }

}
