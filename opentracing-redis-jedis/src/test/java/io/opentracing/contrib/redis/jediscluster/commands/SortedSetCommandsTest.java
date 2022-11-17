package io.opentracing.contrib.redis.jediscluster.commands;

import org.junit.Test;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.ZParams;
import redis.clients.jedis.params.sortedset.ZAddParams;
import redis.clients.jedis.params.sortedset.ZIncrByParams;
import redis.clients.util.SafeEncoder;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static io.opentracing.contrib.redis.utils.AssertUtil.assertByteArraySetEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static redis.clients.jedis.ScanParams.SCAN_POINTER_START;
import static redis.clients.jedis.ScanParams.SCAN_POINTER_START_BINARY;

public class SortedSetCommandsTest extends JedisCommandTestBase {
//  final byte[] bfoo = { 0x01, 0x02, 0x03, 0x04 };
//  final byte[] bbar = { 0x05, 0x06, 0x07, 0x08 };
  final byte[] bcar = { 0x09, 0x0A, 0x0B, 0x0C };
  final byte[] ba = { 0x0A };
  final byte[] bb = { 0x0B };
  final byte[] bc = { 0x0C };

  final byte[] bbar1 = { 0x05, 0x06, 0x07, 0x08, 0x0A };
  final byte[] bbar2 = { 0x05, 0x06, 0x07, 0x08, 0x0B };
  final byte[] bbar3 = { 0x05, 0x06, 0x07, 0x08, 0x0C };

  final byte[] bbarstar = { 0x05, 0x06, 0x07, 0x08, '*' };
  final byte[] bInclusiveB = { 0x5B, 0x0B };
  final byte[] bExclusiveC = { 0x28, 0x0C };
  final byte[] bLexMinusInf = { 0x2D };
  final byte[] bLexPlusInf = { 0x2B };

  private final String foo = "foo{zset}";
  private final String bar = "bar{zset}";
  private final String dst = "dst{zset}";

  private final byte[] bfoo = SafeEncoder.encode("b" + foo);
  private final byte[] bbar = SafeEncoder.encode("b" + bar);
  private final byte[] bdst =  SafeEncoder.encode("b" + dst);

  @Test
  public void zadd() {
    long status = jedisCluster.zadd("foo", 1d, "a");
    assertEquals(1, status);

    status = jedisCluster.zadd("foo", 10d, "b");
    assertEquals(1, status);

    status = jedisCluster.zadd("foo", 0.1d, "c");
    assertEquals(1, status);

    status = jedisCluster.zadd("foo", 2d, "a");
    assertEquals(0, status);

    // Binary
    long bstatus = jedisCluster.zadd(bfoo, 1d, ba);
    assertEquals(1, bstatus);

    bstatus = jedisCluster.zadd(bfoo, 10d, bb);
    assertEquals(1, bstatus);

    bstatus = jedisCluster.zadd(bfoo, 0.1d, bc);
    assertEquals(1, bstatus);

    bstatus = jedisCluster.zadd(bfoo, 2d, ba);
    assertEquals(0, bstatus);

  }

  @Test
  public void zaddWithParams() {
    jedisCluster.del("foo");

    // xx: never add new member
    long status = jedisCluster.zadd("foo", 1d, "a", ZAddParams.zAddParams().xx());
    assertEquals(0L, status);

    jedisCluster.zadd("foo", 1d, "a");
    // nx: never update current member
    status = jedisCluster.zadd("foo", 2d, "a", ZAddParams.zAddParams().nx());
    assertEquals(0L, status);
    assertEquals(Double.valueOf(1d), jedisCluster.zscore("foo", "a"));

    Map<String, Double> scoreMembers = new HashMap<String, Double>();
    scoreMembers.put("a", 2d);
    scoreMembers.put("b", 1d);
    // ch: return count of members not only added, but also updated
    status = jedisCluster.zadd("foo", scoreMembers, ZAddParams.zAddParams().ch());
    assertEquals(2L, status);

    // binary
    jedisCluster.del(bfoo);

    // xx: never add new member
    status = jedisCluster.zadd(bfoo, 1d, ba, ZAddParams.zAddParams().xx());
    assertEquals(0L, status);

    jedisCluster.zadd(bfoo, 1d, ba);
    // nx: never update current member
    status = jedisCluster.zadd(bfoo, 2d, ba, ZAddParams.zAddParams().nx());
    assertEquals(0L, status);
    assertEquals(Double.valueOf(1d), jedisCluster.zscore(bfoo, ba));

    Map<byte[], Double> binaryScoreMembers = new HashMap<byte[], Double>();
    binaryScoreMembers.put(ba, 2d);
    binaryScoreMembers.put(bb, 1d);
    // ch: return count of members not only added, but also updated
    status = jedisCluster.zadd(bfoo, binaryScoreMembers, ZAddParams.zAddParams().ch());
    assertEquals(2L, status);
  }

  @Test
  public void zrange() {
    jedisCluster.zadd("foo", 1d, "a");
    jedisCluster.zadd("foo", 10d, "b");
    jedisCluster.zadd("foo", 0.1d, "c");
    jedisCluster.zadd("foo", 2d, "a");

    Set<String> expected = new LinkedHashSet<String>();
    expected.add("c");
    expected.add("a");

    Set<String> range = jedisCluster.zrange("foo", 0, 1);
    assertEquals(expected, range);

    expected.add("b");
    range = jedisCluster.zrange("foo", 0, 100);
    assertEquals(expected, range);

    // Binary
    jedisCluster.zadd(bfoo, 1d, ba);
    jedisCluster.zadd(bfoo, 10d, bb);
    jedisCluster.zadd(bfoo, 0.1d, bc);
    jedisCluster.zadd(bfoo, 2d, ba);

    Set<byte[]> bexpected = new LinkedHashSet<byte[]>();
    bexpected.add(bc);
    bexpected.add(ba);

    Set<byte[]> brange = jedisCluster.zrange(bfoo, 0, 1);
    assertByteArraySetEquals(bexpected, brange);

    bexpected.add(bb);
    brange = jedisCluster.zrange(bfoo, 0, 100);
    assertByteArraySetEquals(bexpected, brange);

  }

  @Test
  public void zrangeByLex() {
    jedisCluster.zadd("foo", 1, "aa");
    jedisCluster.zadd("foo", 1, "c");
    jedisCluster.zadd("foo", 1, "bb");
    jedisCluster.zadd("foo", 1, "d");

    Set<String> expected = new LinkedHashSet<String>();
    expected.add("bb");
    expected.add("c");

    // exclusive aa ~ inclusive c
    assertEquals(expected, jedisCluster.zrangeByLex("foo", "(aa", "[c"));

    expected.clear();
    expected.add("bb");
    expected.add("c");

    // with LIMIT
    assertEquals(expected, jedisCluster.zrangeByLex("foo", "-", "+", 1, 2));
  }

  @Test
  public void zrangeByLexBinary() {
    // binary
    jedisCluster.zadd(bfoo, 1, ba);
    jedisCluster.zadd(bfoo, 1, bc);
    jedisCluster.zadd(bfoo, 1, bb);

    Set<byte[]> bExpected = new LinkedHashSet<byte[]>();
    bExpected.add(bb);

    assertByteArraySetEquals(bExpected, jedisCluster.zrangeByLex(bfoo, bInclusiveB, bExclusiveC));

    bExpected.clear();
    bExpected.add(ba);
    bExpected.add(bb);

    // with LIMIT
    assertByteArraySetEquals(bExpected, jedisCluster.zrangeByLex(bfoo, bLexMinusInf, bLexPlusInf, 0, 2));
  }

  @Test
  public void zrevrangeByLex() {
    jedisCluster.zadd("foo", 1, "aa");
    jedisCluster.zadd("foo", 1, "c");
    jedisCluster.zadd("foo", 1, "bb");
    jedisCluster.zadd("foo", 1, "d");

    Set<String> expected = new LinkedHashSet<String>();
    expected.add("c");
    expected.add("bb");

    // exclusive aa ~ inclusive c
    assertEquals(expected, jedisCluster.zrevrangeByLex("foo", "[c", "(aa"));

    expected.clear();
    expected.add("c");
    expected.add("bb");

    // with LIMIT
    assertEquals(expected, jedisCluster.zrevrangeByLex("foo", "+", "-", 1, 2));
  }

  @Test
  public void zrevrangeByLexBinary() {
    // binary
    jedisCluster.zadd(bfoo, 1, ba);
    jedisCluster.zadd(bfoo, 1, bc);
    jedisCluster.zadd(bfoo, 1, bb);

    Set<byte[]> bExpected = new LinkedHashSet<byte[]>();
    bExpected.add(bb);

    assertByteArraySetEquals(bExpected, jedisCluster.zrevrangeByLex(bfoo, bExclusiveC, bInclusiveB));

    bExpected.clear();
    bExpected.add(bb);
    bExpected.add(ba);

    // with LIMIT
    assertByteArraySetEquals(bExpected, jedisCluster.zrevrangeByLex(bfoo, bLexPlusInf, bLexMinusInf, 0, 2));
  }

  @Test
  public void zrevrange() {
    jedisCluster.zadd("foo", 1d, "a");
    jedisCluster.zadd("foo", 10d, "b");
    jedisCluster.zadd("foo", 0.1d, "c");
    jedisCluster.zadd("foo", 2d, "a");

    Set<String> expected = new LinkedHashSet<String>();
    expected.add("b");
    expected.add("a");

    Set<String> range = jedisCluster.zrevrange("foo", 0, 1);
    assertEquals(expected, range);

    expected.add("c");
    range = jedisCluster.zrevrange("foo", 0, 100);
    assertEquals(expected, range);

    // Binary
    jedisCluster.zadd(bfoo, 1d, ba);
    jedisCluster.zadd(bfoo, 10d, bb);
    jedisCluster.zadd(bfoo, 0.1d, bc);
    jedisCluster.zadd(bfoo, 2d, ba);

    Set<byte[]> bexpected = new LinkedHashSet<byte[]>();
    bexpected.add(bb);
    bexpected.add(ba);

    Set<byte[]> brange = jedisCluster.zrevrange(bfoo, 0, 1);
    assertByteArraySetEquals(bexpected, brange);

    bexpected.add(bc);
    brange = jedisCluster.zrevrange(bfoo, 0, 100);
    assertByteArraySetEquals(bexpected, brange);

  }

  @Test
  public void zrem() {
    jedisCluster.zadd("foo", 1d, "a");
    jedisCluster.zadd("foo", 2d, "b");

    long status = jedisCluster.zrem("foo", "a");

    Set<String> expected = new LinkedHashSet<String>();
    expected.add("b");

    assertEquals(1, status);
    assertEquals(expected, jedisCluster.zrange("foo", 0, 100));

    status = jedisCluster.zrem("foo", "bar");

    assertEquals(0, status);

    // Binary
    jedisCluster.zadd(bfoo, 1d, ba);
    jedisCluster.zadd(bfoo, 2d, bb);

    long bstatus = jedisCluster.zrem(bfoo, ba);

    Set<byte[]> bexpected = new LinkedHashSet<byte[]>();
    bexpected.add(bb);

    assertEquals(1, bstatus);
    assertByteArraySetEquals(bexpected, jedisCluster.zrange(bfoo, 0, 100));

    bstatus = jedisCluster.zrem(bfoo, bbar);

    assertEquals(0, bstatus);

  }

  @Test
  public void zincrby() {
    jedisCluster.zadd("foo", 1d, "a");
    jedisCluster.zadd("foo", 2d, "b");

    double score = jedisCluster.zincrby("foo", 2d, "a");

    Set<String> expected = new LinkedHashSet<String>();
    expected.add("a");
    expected.add("b");

    assertEquals(3d, score, 0);
    assertEquals(expected, jedisCluster.zrange("foo", 0, 100));

    // Binary
    jedisCluster.zadd(bfoo, 1d, ba);
    jedisCluster.zadd(bfoo, 2d, bb);

    double bscore = jedisCluster.zincrby(bfoo, 2d, ba);

    Set<byte[]> bexpected = new LinkedHashSet<byte[]>();
    bexpected.add(bb);
    bexpected.add(ba);

    assertEquals(3d, bscore, 0);
    assertByteArraySetEquals(bexpected, jedisCluster.zrange(bfoo, 0, 100));

  }

  @Test
  public void zincrbyWithParams() {
    jedisCluster.del("foo");

    // xx: never add new member
    Double score = jedisCluster.zincrby("foo", 2d, "a", ZIncrByParams.zIncrByParams().xx());
    assertNull(score);

    jedisCluster.zadd("foo", 2d, "a");

    // nx: never update current member
    score = jedisCluster.zincrby("foo", 1d, "a", ZIncrByParams.zIncrByParams().nx());
    assertNull(score);
    assertEquals(Double.valueOf(2d), jedisCluster.zscore("foo", "a"));

    // Binary

    jedisCluster.del(bfoo);

    // xx: never add new member
    score = jedisCluster.zincrby(bfoo, 2d, ba, ZIncrByParams.zIncrByParams().xx());
    assertNull(score);

    jedisCluster.zadd(bfoo, 2d, ba);

    // nx: never update current member
    score = jedisCluster.zincrby(bfoo, 1d, ba, ZIncrByParams.zIncrByParams().nx());
    assertNull(score);
    assertEquals(Double.valueOf(2d), jedisCluster.zscore(bfoo, ba));
  }

  @Test
  public void zrank() {
    jedisCluster.zadd("foo", 1d, "a");
    jedisCluster.zadd("foo", 2d, "b");

    long rank = jedisCluster.zrank("foo", "a");
    assertEquals(0, rank);

    rank = jedisCluster.zrank("foo", "b");
    assertEquals(1, rank);

    assertNull(jedisCluster.zrank("car", "b"));

    // Binary
    jedisCluster.zadd(bfoo, 1d, ba);
    jedisCluster.zadd(bfoo, 2d, bb);

    long brank = jedisCluster.zrank(bfoo, ba);
    assertEquals(0, brank);

    brank = jedisCluster.zrank(bfoo, bb);
    assertEquals(1, brank);

    assertNull(jedisCluster.zrank(bcar, bb));

  }

  @Test
  public void zrevrank() {
    jedisCluster.zadd("foo", 1d, "a");
    jedisCluster.zadd("foo", 2d, "b");

    long rank = jedisCluster.zrevrank("foo", "a");
    assertEquals(1, rank);

    rank = jedisCluster.zrevrank("foo", "b");
    assertEquals(0, rank);

    // Binary
    jedisCluster.zadd(bfoo, 1d, ba);
    jedisCluster.zadd(bfoo, 2d, bb);

    long brank = jedisCluster.zrevrank(bfoo, ba);
    assertEquals(1, brank);

    brank = jedisCluster.zrevrank(bfoo, bb);
    assertEquals(0, brank);

  }

  @Test
  public void zrangeWithScores() {
    jedisCluster.zadd("foo", 1d, "a");
    jedisCluster.zadd("foo", 10d, "b");
    jedisCluster.zadd("foo", 0.1d, "c");
    jedisCluster.zadd("foo", 2d, "a");

    Set<Tuple> expected = new LinkedHashSet<Tuple>();
    expected.add(new Tuple("c", 0.1d));
    expected.add(new Tuple("a", 2d));

    Set<Tuple> range = jedisCluster.zrangeWithScores("foo", 0, 1);
    assertEquals(expected, range);

    expected.add(new Tuple("b", 10d));
    range = jedisCluster.zrangeWithScores("foo", 0, 100);
    assertEquals(expected, range);

    // Binary
    jedisCluster.zadd(bfoo, 1d, ba);
    jedisCluster.zadd(bfoo, 10d, bb);
    jedisCluster.zadd(bfoo, 0.1d, bc);
    jedisCluster.zadd(bfoo, 2d, ba);

    Set<Tuple> bexpected = new LinkedHashSet<Tuple>();
    bexpected.add(new Tuple(bc, 0.1d));
    bexpected.add(new Tuple(ba, 2d));

    Set<Tuple> brange = jedisCluster.zrangeWithScores(bfoo, 0, 1);
    assertEquals(bexpected, brange);

    bexpected.add(new Tuple(bb, 10d));
    brange = jedisCluster.zrangeWithScores(bfoo, 0, 100);
    assertEquals(bexpected, brange);

  }

  @Test
  public void zrevrangeWithScores() {
    jedisCluster.zadd("foo", 1d, "a");
    jedisCluster.zadd("foo", 10d, "b");
    jedisCluster.zadd("foo", 0.1d, "c");
    jedisCluster.zadd("foo", 2d, "a");

    Set<Tuple> expected = new LinkedHashSet<Tuple>();
    expected.add(new Tuple("b", 10d));
    expected.add(new Tuple("a", 2d));

    Set<Tuple> range = jedisCluster.zrevrangeWithScores("foo", 0, 1);
    assertEquals(expected, range);

    expected.add(new Tuple("c", 0.1d));
    range = jedisCluster.zrevrangeWithScores("foo", 0, 100);
    assertEquals(expected, range);

    // Binary
    jedisCluster.zadd(bfoo, 1d, ba);
    jedisCluster.zadd(bfoo, 10d, bb);
    jedisCluster.zadd(bfoo, 0.1d, bc);
    jedisCluster.zadd(bfoo, 2d, ba);

    Set<Tuple> bexpected = new LinkedHashSet<Tuple>();
    bexpected.add(new Tuple(bb, 10d));
    bexpected.add(new Tuple(ba, 2d));

    Set<Tuple> brange = jedisCluster.zrevrangeWithScores(bfoo, 0, 1);
    assertEquals(bexpected, brange);

    bexpected.add(new Tuple(bc, 0.1d));
    brange = jedisCluster.zrevrangeWithScores(bfoo, 0, 100);
    assertEquals(bexpected, brange);

  }

  @Test
  public void zcard() {
    jedisCluster.zadd("foo", 1d, "a");
    jedisCluster.zadd("foo", 10d, "b");
    jedisCluster.zadd("foo", 0.1d, "c");
    jedisCluster.zadd("foo", 2d, "a");

    long size = jedisCluster.zcard("foo");
    assertEquals(3, size);

    // Binary
    jedisCluster.zadd(bfoo, 1d, ba);
    jedisCluster.zadd(bfoo, 10d, bb);
    jedisCluster.zadd(bfoo, 0.1d, bc);
    jedisCluster.zadd(bfoo, 2d, ba);

    long bsize = jedisCluster.zcard(bfoo);
    assertEquals(3, bsize);

  }

  @Test
  public void zscore() {
    jedisCluster.zadd("foo", 1d, "a");
    jedisCluster.zadd("foo", 10d, "b");
    jedisCluster.zadd("foo", 0.1d, "c");
    jedisCluster.zadd("foo", 2d, "a");

    Double score = jedisCluster.zscore("foo", "b");
    assertEquals((Double) 10d, score);

    score = jedisCluster.zscore("foo", "c");
    assertEquals((Double) 0.1d, score);

    score = jedisCluster.zscore("foo", "s");
    assertNull(score);

    // Binary
    jedisCluster.zadd(bfoo, 1d, ba);
    jedisCluster.zadd(bfoo, 10d, bb);
    jedisCluster.zadd(bfoo, 0.1d, bc);
    jedisCluster.zadd(bfoo, 2d, ba);

    Double bscore = jedisCluster.zscore(bfoo, bb);
    assertEquals((Double) 10d, bscore);

    bscore = jedisCluster.zscore(bfoo, bc);
    assertEquals((Double) 0.1d, bscore);

    bscore = jedisCluster.zscore(bfoo, SafeEncoder.encode("s"));
    assertNull(bscore);

  }

  @Test
  public void zcount() {
    jedisCluster.zadd("foo", 1d, "a");
    jedisCluster.zadd("foo", 10d, "b");
    jedisCluster.zadd("foo", 0.1d, "c");
    jedisCluster.zadd("foo", 2d, "a");

    long result = jedisCluster.zcount("foo", 0.01d, 2.1d);

    assertEquals(2, result);

    result = jedisCluster.zcount("foo", "(0.01", "+inf");

    assertEquals(3, result);

    // Binary
    jedisCluster.zadd(bfoo, 1d, ba);
    jedisCluster.zadd(bfoo, 10d, bb);
    jedisCluster.zadd(bfoo, 0.1d, bc);
    jedisCluster.zadd(bfoo, 2d, ba);

    long bresult = jedisCluster.zcount(bfoo, 0.01d, 2.1d);

    assertEquals(2, bresult);

    bresult = jedisCluster.zcount(bfoo, SafeEncoder.encode("(0.01"), SafeEncoder.encode("+inf"));

    assertEquals(3, bresult);
  }

  @Test
  public void zlexcount() {
    jedisCluster.zadd("foo", 1, "a");
    jedisCluster.zadd("foo", 1, "b");
    jedisCluster.zadd("foo", 1, "c");
    jedisCluster.zadd("foo", 1, "aa");

    long result = jedisCluster.zlexcount("foo", "[aa", "(c");
    assertEquals(2, result);

    result = jedisCluster.zlexcount("foo", "-", "+");
    assertEquals(4, result);

    result = jedisCluster.zlexcount("foo", "-", "(c");
    assertEquals(3, result);

    result = jedisCluster.zlexcount("foo", "[aa", "+");
    assertEquals(3, result);
  }

  @Test
  public void zlexcountBinary() {
    // Binary
    jedisCluster.zadd(bfoo, 1, ba);
    jedisCluster.zadd(bfoo, 1, bc);
    jedisCluster.zadd(bfoo, 1, bb);

    long result = jedisCluster.zlexcount(bfoo, bInclusiveB, bExclusiveC);
    assertEquals(1, result);

    result = jedisCluster.zlexcount(bfoo, bLexMinusInf, bLexPlusInf);
    assertEquals(3, result);
  }

  @Test
  public void zrangebyscore() {
    jedisCluster.zadd("foo", 1d, "a");
    jedisCluster.zadd("foo", 10d, "b");
    jedisCluster.zadd("foo", 0.1d, "c");
    jedisCluster.zadd("foo", 2d, "a");

    Set<String> range = jedisCluster.zrangeByScore("foo", 0d, 2d);

    Set<String> expected = new LinkedHashSet<String>();
    expected.add("c");
    expected.add("a");

    assertEquals(expected, range);

    range = jedisCluster.zrangeByScore("foo", 0d, 2d, 0, 1);

    expected = new LinkedHashSet<String>();
    expected.add("c");

    assertEquals(expected, range);

    range = jedisCluster.zrangeByScore("foo", 0d, 2d, 1, 1);
    Set<String> range2 = jedisCluster.zrangeByScore("foo", "-inf", "(2");
    assertEquals(expected, range2);

    expected = new LinkedHashSet<String>();
    expected.add("a");

    assertEquals(expected, range);

    // Binary
    jedisCluster.zadd(bfoo, 1d, ba);
    jedisCluster.zadd(bfoo, 10d, bb);
    jedisCluster.zadd(bfoo, 0.1d, bc);
    jedisCluster.zadd(bfoo, 2d, ba);

    Set<byte[]> brange = jedisCluster.zrangeByScore(bfoo, 0d, 2d);

    Set<byte[]> bexpected = new LinkedHashSet<byte[]>();
    bexpected.add(bc);
    bexpected.add(ba);

    assertByteArraySetEquals(bexpected, brange);

    brange = jedisCluster.zrangeByScore(bfoo, 0d, 2d, 0, 1);

    bexpected = new LinkedHashSet<byte[]>();
    bexpected.add(bc);

    assertByteArraySetEquals(bexpected, brange);

    brange = jedisCluster.zrangeByScore(bfoo, 0d, 2d, 1, 1);
    Set<byte[]> brange2 = jedisCluster.zrangeByScore(bfoo, SafeEncoder.encode("-inf"),
      SafeEncoder.encode("(2"));
    assertByteArraySetEquals(bexpected, brange2);

    bexpected = new LinkedHashSet<byte[]>();
    bexpected.add(ba);

    assertByteArraySetEquals(bexpected, brange);

  }

  @Test
  public void zrevrangebyscore() {
    jedisCluster.zadd("foo", 1.0d, "a");
    jedisCluster.zadd("foo", 2.0d, "b");
    jedisCluster.zadd("foo", 3.0d, "c");
    jedisCluster.zadd("foo", 4.0d, "d");
    jedisCluster.zadd("foo", 5.0d, "e");

    Set<String> range = jedisCluster.zrevrangeByScore("foo", 3d, Double.NEGATIVE_INFINITY, 0, 1);
    Set<String> expected = new LinkedHashSet<String>();
    expected.add("c");

    assertEquals(expected, range);

    range = jedisCluster.zrevrangeByScore("foo", 3.5d, Double.NEGATIVE_INFINITY, 0, 2);
    expected = new LinkedHashSet<String>();
    expected.add("c");
    expected.add("b");

    assertEquals(expected, range);

    range = jedisCluster.zrevrangeByScore("foo", 3.5d, Double.NEGATIVE_INFINITY, 1, 1);
    expected = new LinkedHashSet<String>();
    expected.add("b");

    assertEquals(expected, range);

    range = jedisCluster.zrevrangeByScore("foo", 4d, 2d);
    expected = new LinkedHashSet<String>();
    expected.add("d");
    expected.add("c");
    expected.add("b");

    assertEquals(expected, range);

    range = jedisCluster.zrevrangeByScore("foo", "+inf", "(4");
    expected = new LinkedHashSet<String>();
    expected.add("e");

    assertEquals(expected, range);

    // Binary
    jedisCluster.zadd(bfoo, 1d, ba);
    jedisCluster.zadd(bfoo, 10d, bb);
    jedisCluster.zadd(bfoo, 0.1d, bc);
    jedisCluster.zadd(bfoo, 2d, ba);

    Set<byte[]> brange = jedisCluster.zrevrangeByScore(bfoo, 2d, 0d);

    Set<byte[]> bexpected = new LinkedHashSet<byte[]>();
    bexpected.add(bc);
    bexpected.add(ba);

    assertByteArraySetEquals(bexpected, brange);

    brange = jedisCluster.zrevrangeByScore(bfoo, 2d, 0d, 0, 1);

    bexpected = new LinkedHashSet<byte[]>();
    bexpected.add(ba);

    assertByteArraySetEquals(bexpected, brange);

    Set<byte[]> brange2 = jedisCluster.zrevrangeByScore(bfoo, SafeEncoder.encode("+inf"),
      SafeEncoder.encode("(2"));

    bexpected = new LinkedHashSet<byte[]>();
    bexpected.add(bb);

    assertByteArraySetEquals(bexpected, brange2);

    brange = jedisCluster.zrevrangeByScore(bfoo, 2d, 0d, 1, 1);
    bexpected = new LinkedHashSet<byte[]>();
    bexpected.add(bc);

    assertByteArraySetEquals(bexpected, brange);
  }

  @Test
  public void zrangebyscoreWithScores() {
    jedisCluster.zadd("foo", 1d, "a");
    jedisCluster.zadd("foo", 10d, "b");
    jedisCluster.zadd("foo", 0.1d, "c");
    jedisCluster.zadd("foo", 2d, "a");

    Set<Tuple> range = jedisCluster.zrangeByScoreWithScores("foo", 0d, 2d);

    Set<Tuple> expected = new LinkedHashSet<Tuple>();
    expected.add(new Tuple("c", 0.1d));
    expected.add(new Tuple("a", 2d));

    assertEquals(expected, range);

    range = jedisCluster.zrangeByScoreWithScores("foo", 0d, 2d, 0, 1);

    expected = new LinkedHashSet<Tuple>();
    expected.add(new Tuple("c", 0.1d));

    assertEquals(expected, range);

    range = jedisCluster.zrangeByScoreWithScores("foo", 0d, 2d, 1, 1);

    expected = new LinkedHashSet<Tuple>();
    expected.add(new Tuple("a", 2d));

    assertEquals(expected, range);

    // Binary

    jedisCluster.zadd(bfoo, 1d, ba);
    jedisCluster.zadd(bfoo, 10d, bb);
    jedisCluster.zadd(bfoo, 0.1d, bc);
    jedisCluster.zadd(bfoo, 2d, ba);

    Set<Tuple> brange = jedisCluster.zrangeByScoreWithScores(bfoo, 0d, 2d);

    Set<Tuple> bexpected = new LinkedHashSet<Tuple>();
    bexpected.add(new Tuple(bc, 0.1d));
    bexpected.add(new Tuple(ba, 2d));

    assertEquals(bexpected, brange);

    brange = jedisCluster.zrangeByScoreWithScores(bfoo, 0d, 2d, 0, 1);

    bexpected = new LinkedHashSet<Tuple>();
    bexpected.add(new Tuple(bc, 0.1d));

    assertEquals(bexpected, brange);

    brange = jedisCluster.zrangeByScoreWithScores(bfoo, 0d, 2d, 1, 1);

    bexpected = new LinkedHashSet<Tuple>();
    bexpected.add(new Tuple(ba, 2d));

    assertEquals(bexpected, brange);

  }

  @Test
  public void zrevrangebyscoreWithScores() {
    jedisCluster.zadd("foo", 1.0d, "a");
    jedisCluster.zadd("foo", 2.0d, "b");
    jedisCluster.zadd("foo", 3.0d, "c");
    jedisCluster.zadd("foo", 4.0d, "d");
    jedisCluster.zadd("foo", 5.0d, "e");

    Set<Tuple> range = jedisCluster.zrevrangeByScoreWithScores("foo", 3d, Double.NEGATIVE_INFINITY, 0, 1);
    Set<Tuple> expected = new LinkedHashSet<Tuple>();
    expected.add(new Tuple("c", 3.0d));

    assertEquals(expected, range);

    range = jedisCluster.zrevrangeByScoreWithScores("foo", 3.5d, Double.NEGATIVE_INFINITY, 0, 2);
    expected = new LinkedHashSet<Tuple>();
    expected.add(new Tuple("c", 3.0d));
    expected.add(new Tuple("b", 2.0d));

    assertEquals(expected, range);

    range = jedisCluster.zrevrangeByScoreWithScores("foo", 3.5d, Double.NEGATIVE_INFINITY, 1, 1);
    expected = new LinkedHashSet<Tuple>();
    expected.add(new Tuple("b", 2.0d));

    assertEquals(expected, range);

    range = jedisCluster.zrevrangeByScoreWithScores("foo", 4d, 2d);
    expected = new LinkedHashSet<Tuple>();
    expected.add(new Tuple("d", 4.0d));
    expected.add(new Tuple("c", 3.0d));
    expected.add(new Tuple("b", 2.0d));

    assertEquals(expected, range);

    // Binary
    jedisCluster.zadd(bfoo, 1d, ba);
    jedisCluster.zadd(bfoo, 10d, bb);
    jedisCluster.zadd(bfoo, 0.1d, bc);
    jedisCluster.zadd(bfoo, 2d, ba);

    Set<Tuple> brange = jedisCluster.zrevrangeByScoreWithScores(bfoo, 2d, 0d);

    Set<Tuple> bexpected = new LinkedHashSet<Tuple>();
    bexpected.add(new Tuple(bc, 0.1d));
    bexpected.add(new Tuple(ba, 2d));

    assertEquals(bexpected, brange);

    brange = jedisCluster.zrevrangeByScoreWithScores(bfoo, 2d, 0d, 0, 1);

    bexpected = new LinkedHashSet<Tuple>();
    bexpected.add(new Tuple(ba, 2d));

    assertEquals(bexpected, brange);

    brange = jedisCluster.zrevrangeByScoreWithScores(bfoo, 2d, 0d, 1, 1);

    bexpected = new LinkedHashSet<Tuple>();
    bexpected.add(new Tuple(bc, 0.1d));

    assertEquals(bexpected, brange);
  }

  @Test
  public void zremrangeByRank() {
    jedisCluster.zadd("foo", 1d, "a");
    jedisCluster.zadd("foo", 10d, "b");
    jedisCluster.zadd("foo", 0.1d, "c");
    jedisCluster.zadd("foo", 2d, "a");

    long result = jedisCluster.zremrangeByRank("foo", 0, 0);

    assertEquals(1, result);

    Set<String> expected = new LinkedHashSet<String>();
    expected.add("a");
    expected.add("b");

    assertEquals(expected, jedisCluster.zrange("foo", 0, 100));

    // Binary
    jedisCluster.zadd(bfoo, 1d, ba);
    jedisCluster.zadd(bfoo, 10d, bb);
    jedisCluster.zadd(bfoo, 0.1d, bc);
    jedisCluster.zadd(bfoo, 2d, ba);

    long bresult = jedisCluster.zremrangeByRank(bfoo, 0, 0);

    assertEquals(1, bresult);

    Set<byte[]> bexpected = new LinkedHashSet<byte[]>();
    bexpected.add(ba);
    bexpected.add(bb);

    assertByteArraySetEquals(bexpected, jedisCluster.zrange(bfoo, 0, 100));

  }

  @Test
  public void zremrangeByScore() {
    jedisCluster.zadd("foo", 1d, "a");
    jedisCluster.zadd("foo", 10d, "b");
    jedisCluster.zadd("foo", 0.1d, "c");
    jedisCluster.zadd("foo", 2d, "a");

    long result = jedisCluster.zremrangeByScore("foo", 0, 2);

    assertEquals(2, result);

    Set<String> expected = new LinkedHashSet<String>();
    expected.add("b");

    assertEquals(expected, jedisCluster.zrange("foo", 0, 100));

    // Binary
    jedisCluster.zadd(bfoo, 1d, ba);
    jedisCluster.zadd(bfoo, 10d, bb);
    jedisCluster.zadd(bfoo, 0.1d, bc);
    jedisCluster.zadd(bfoo, 2d, ba);

    long bresult = jedisCluster.zremrangeByScore(bfoo, 0, 2);

    assertEquals(2, bresult);

    Set<byte[]> bexpected = new LinkedHashSet<byte[]>();
    bexpected.add(bb);

    assertByteArraySetEquals(bexpected, jedisCluster.zrange(bfoo, 0, 100));
  }

  @Test
  public void zremrangeByLex() {
    jedisCluster.zadd("foo", 1, "a");
    jedisCluster.zadd("foo", 1, "b");
    jedisCluster.zadd("foo", 1, "c");
    jedisCluster.zadd("foo", 1, "aa");

    long result = jedisCluster.zremrangeByLex("foo", "[aa", "(c");

    assertEquals(2, result);

    Set<String> expected = new LinkedHashSet<String>();
    expected.add("a");
    expected.add("c");

    assertEquals(expected, jedisCluster.zrangeByLex("foo", "-", "+"));
  }

  @Test
  public void zremrangeByLexBinary() {
    jedisCluster.zadd(bfoo, 1, ba);
    jedisCluster.zadd(bfoo, 1, bc);
    jedisCluster.zadd(bfoo, 1, bb);

    long bresult = jedisCluster.zremrangeByLex(bfoo, bInclusiveB, bExclusiveC);

    assertEquals(1, bresult);

    Set<byte[]> bexpected = new LinkedHashSet<byte[]>();
    bexpected.add(ba);
    bexpected.add(bc);

    assertByteArraySetEquals(bexpected, jedisCluster.zrangeByLex(bfoo, bLexMinusInf, bLexPlusInf));
  }

  @Test
  public void zunionstore() {
    jedisCluster.zadd(foo, 1, "a");
    jedisCluster.zadd(foo, 2, "b");
    jedisCluster.zadd(bar, 2, "a");
    jedisCluster.zadd(bar, 2, "b");

    long result = jedisCluster.zunionstore(dst, foo, bar);

    assertEquals(2, result);

    Set<Tuple> expected = new LinkedHashSet<Tuple>();
    expected.add(new Tuple("b", new Double(4)));
    expected.add(new Tuple("a", new Double(3)));

    assertEquals(expected, jedisCluster.zrangeWithScores(dst, 0, 100));

    // Binary
    jedisCluster.zadd(bfoo, 1, ba);
    jedisCluster.zadd(bfoo, 2, bb);
    jedisCluster.zadd(bbar, 2, ba);
    jedisCluster.zadd(bbar, 2, bb);

    long bresult = jedisCluster.zunionstore(bdst, bfoo, bbar);

    assertEquals(2, bresult);

    Set<Tuple> bexpected = new LinkedHashSet<Tuple>();
    bexpected.add(new Tuple(bb, new Double(4)));
    bexpected.add(new Tuple(ba, new Double(3)));

    assertEquals(bexpected, jedisCluster.zrangeWithScores(bdst, 0, 100));
  }

  @Test
  public void zunionstoreParams() {
    jedisCluster.zadd(foo, 1, "a");
    jedisCluster.zadd(foo, 2, "b");
    jedisCluster.zadd(bar, 2, "a");
    jedisCluster.zadd(bar, 2, "b");

    ZParams params = new ZParams();
    params.weightsByDouble(2, 2.5);
    params.aggregate(ZParams.Aggregate.SUM);
    long result = jedisCluster.zunionstore(dst, params, foo, bar);

    assertEquals(2, result);

    Set<Tuple> expected = new LinkedHashSet<Tuple>();
    expected.add(new Tuple("b", new Double(9)));
    expected.add(new Tuple("a", new Double(7)));

    assertEquals(expected, jedisCluster.zrangeWithScores(dst, 0, 100));

    // Binary
    jedisCluster.zadd(bfoo, 1, ba);
    jedisCluster.zadd(bfoo, 2, bb);
    jedisCluster.zadd(bbar, 2, ba);
    jedisCluster.zadd(bbar, 2, bb);

    ZParams bparams = new ZParams();
    bparams.weightsByDouble(2, 2.5);
    bparams.aggregate(ZParams.Aggregate.SUM);
    long bresult = jedisCluster.zunionstore(bdst, bparams, bfoo, bbar);

    assertEquals(2, bresult);

    Set<Tuple> bexpected = new LinkedHashSet<Tuple>();
    bexpected.add(new Tuple(bb, new Double(9)));
    bexpected.add(new Tuple(ba, new Double(7)));

    assertEquals(bexpected, jedisCluster.zrangeWithScores(bdst, 0, 100));
  }

  @Test
  public void zinterstore() {
    jedisCluster.zadd(foo, 1, "a");
    jedisCluster.zadd(foo, 2, "b");
    jedisCluster.zadd(bar, 2, "a");

    long result = jedisCluster.zinterstore(dst, foo, bar);

    assertEquals(1, result);

    Set<Tuple> expected = new LinkedHashSet<Tuple>();
    expected.add(new Tuple("a", new Double(3)));

    assertEquals(expected, jedisCluster.zrangeWithScores(dst, 0, 100));

    // Binary
    jedisCluster.zadd(bfoo, 1, ba);
    jedisCluster.zadd(bfoo, 2, bb);
    jedisCluster.zadd(bbar, 2, ba);

    long bresult = jedisCluster.zinterstore(bdst, bfoo, bbar);

    assertEquals(1, bresult);

    Set<Tuple> bexpected = new LinkedHashSet<Tuple>();
    bexpected.add(new Tuple(ba, new Double(3)));

    assertEquals(bexpected, jedisCluster.zrangeWithScores(bdst, 0, 100));
  }

  @Test
  public void zintertoreParams() {
    jedisCluster.zadd(foo, 1, "a");
    jedisCluster.zadd(foo, 2, "b");
    jedisCluster.zadd(bar, 2, "a");

    ZParams params = new ZParams();
    params.weightsByDouble(2, 2.5);
    params.aggregate(ZParams.Aggregate.SUM);
    long result = jedisCluster.zinterstore(dst, params,foo, bar);

    assertEquals(1, result);

    Set<Tuple> expected = new LinkedHashSet<Tuple>();
    expected.add(new Tuple("a", new Double(7)));

    assertEquals(expected, jedisCluster.zrangeWithScores(dst, 0, 100));

    // Binary
    jedisCluster.zadd(bfoo, 1, ba);
    jedisCluster.zadd(bfoo, 2, bb);
    jedisCluster.zadd(bbar, 2, ba);

    ZParams bparams = new ZParams();
    bparams.weightsByDouble(2, 2.5);
    bparams.aggregate(ZParams.Aggregate.SUM);
    long bresult = jedisCluster.zinterstore(bdst, bparams, bfoo, bbar);

    assertEquals(1, bresult);

    Set<Tuple> bexpected = new LinkedHashSet<Tuple>();
    bexpected.add(new Tuple(ba, new Double(7)));

    assertEquals(bexpected, jedisCluster.zrangeWithScores(bdst, 0, 100));
  }

  @Test
  public void zscan() {
    jedisCluster.zadd("foo", 1, "a");
    jedisCluster.zadd("foo", 2, "b");

    ScanResult<Tuple> result = jedisCluster.zscan("foo", SCAN_POINTER_START);

    assertEquals(SCAN_POINTER_START, result.getStringCursor());
    assertFalse(result.getResult().isEmpty());

    // binary
    jedisCluster.zadd(bfoo, 1, ba);
    jedisCluster.zadd(bfoo, 1, bb);

    ScanResult<Tuple> bResult = jedisCluster.zscan(bfoo, SCAN_POINTER_START_BINARY);

    assertArrayEquals(SCAN_POINTER_START_BINARY, bResult.getCursorAsBytes());
    assertFalse(bResult.getResult().isEmpty());
  }

  @Test
  public void zscanMatch() {
    ScanParams params = new ScanParams();
    params.match("a*");

    jedisCluster.zadd("foo", 2, "b");
    jedisCluster.zadd("foo", 1, "a");
    jedisCluster.zadd("foo", 11, "aa");
    ScanResult<Tuple> result = jedisCluster.zscan("foo", SCAN_POINTER_START, params);

    assertEquals(SCAN_POINTER_START, result.getStringCursor());
    assertFalse(result.getResult().isEmpty());

    // binary
    params = new ScanParams();
    params.match(bbarstar);

    jedisCluster.zadd(bfoo, 2, bbar1);
    jedisCluster.zadd(bfoo, 1, bbar2);
    jedisCluster.zadd(bfoo, 11, bbar3);
    ScanResult<Tuple> bResult = jedisCluster.zscan(bfoo, SCAN_POINTER_START_BINARY, params);

    assertArrayEquals(SCAN_POINTER_START_BINARY, bResult.getCursorAsBytes());
    assertFalse(bResult.getResult().isEmpty());

  }

  @Test
  public void zscanCount() {
    ScanParams params = new ScanParams();
    params.count(2);

    jedisCluster.zadd("foo", 1, "a1");
    jedisCluster.zadd("foo", 2, "a2");
    jedisCluster.zadd("foo", 3, "a3");
    jedisCluster.zadd("foo", 4, "a4");
    jedisCluster.zadd("foo", 5, "a5");

    ScanResult<Tuple> result = jedisCluster.zscan("foo", SCAN_POINTER_START, params);

    assertFalse(result.getResult().isEmpty());

    // binary
    params = new ScanParams();
    params.count(2);

    jedisCluster.zadd(bfoo, 2, bbar1);
    jedisCluster.zadd(bfoo, 1, bbar2);
    jedisCluster.zadd(bfoo, 11, bbar3);

    ScanResult<Tuple> bResult = jedisCluster.zscan(bfoo, SCAN_POINTER_START_BINARY, params);

    assertFalse(bResult.getResult().isEmpty());
  }

  @Test
  public void infinity() {
    jedisCluster.zadd("key", Double.POSITIVE_INFINITY, "pos");
    assertEquals(Double.POSITIVE_INFINITY, jedisCluster.zscore("key", "pos"), 0d);
    jedisCluster.zadd("key", Double.NEGATIVE_INFINITY, "neg");
    assertEquals(Double.NEGATIVE_INFINITY, jedisCluster.zscore("key", "neg"), 0d);
    jedisCluster.zadd("key", 0d, "zero");

    Set<Tuple> set = jedisCluster.zrangeWithScores("key", 0, -1);
    Iterator<Tuple> itr = set.iterator();
    assertEquals(Double.NEGATIVE_INFINITY, itr.next().getScore(), 0d);
    assertEquals(0d, itr.next().getScore(), 0d);
    assertEquals(Double.POSITIVE_INFINITY, itr.next().getScore(), 0d);
  }
}
