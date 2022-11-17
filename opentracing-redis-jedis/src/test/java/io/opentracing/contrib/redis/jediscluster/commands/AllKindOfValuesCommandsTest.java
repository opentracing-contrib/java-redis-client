package io.opentracing.contrib.redis.jediscluster.commands;

import org.junit.Test;
import redis.clients.jedis.Protocol.Keyword;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.util.JedisClusterCRC16;
import redis.clients.util.SafeEncoder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static io.opentracing.contrib.redis.jediscluster.commands.TestKeys.*;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static redis.clients.jedis.ScanParams.SCAN_POINTER_START;
import static redis.clients.jedis.ScanParams.SCAN_POINTER_START_BINARY;

public class AllKindOfValuesCommandsTest extends JedisCommandTestBase {
    final byte[] bnx = {0x6E, 0x78};
    final byte[] bex = {0x65, 0x78};
    final long expireSeconds = 2;

//    @Test
//    public void ping() {
//        String status = jedisCluster.ping();
//        assertEquals("PONG", status);
//    }

//  @Test
//  public void pingWithMessage() {
//    String argument = "message";
//    assertEquals(argument, jedis.ping(argument));
//
//    assertArrayEquals(bfoobar, jedis.ping(bfoobar));
//  }

    @Test
    public void exists() {
        String status = jedisCluster.set("foo", "bar");
        assertEquals("OK", status);

        status = jedisCluster.set(bfoo, bbar);
        assertEquals("OK", status);

        boolean reply = jedisCluster.exists("foo");
        assertTrue(reply);

        reply = jedisCluster.exists(bfoo);
        assertTrue(reply);

        long lreply = jedisCluster.del("foo");
        assertEquals(1, lreply);

        lreply = jedisCluster.del(bfoo);
        assertEquals(1, lreply);

        reply = jedisCluster.exists("foo");
        assertFalse(reply);

        reply = jedisCluster.exists(bfoo);
        assertFalse(reply);
    }

    @Test
    public void existsMany() {
        String status = jedisCluster.set(foo, bar);
        assertEquals("OK", status);

        status = jedisCluster.set(foo2, bar2);
        assertEquals("OK", status);

        long reply = jedisCluster.exists(foo, foo2);
        assertEquals(2, reply);

        long lreply = jedisCluster.del(foo);
        assertEquals(1, lreply);

        reply = jedisCluster.exists(foo, foo2);
        assertEquals(1, reply);
    }

    @Test
    public void del() {
        jedisCluster.set(foo, bar);
        jedisCluster.set(foo2, bar3);
        jedisCluster.set(foo3, bar3);

        long reply = jedisCluster.del(foo, foo2, foo3);
        assertEquals(3, reply);

        Boolean breply = jedisCluster.exists(foo);
        assertFalse(breply);
        breply = jedisCluster.exists(foo2);
        assertFalse(breply);
        breply = jedisCluster.exists(foo3);
        assertFalse(breply);

        jedisCluster.set(foo, bar);

        reply = jedisCluster.del(foo, foo2);
        assertEquals(1, reply);

        reply = jedisCluster.del(foo, foo2);
        assertEquals(0, reply);

        // Binary ...
        jedisCluster.set(bfoo1, bbar1);
        jedisCluster.set(bfoo2, bbar2);
        jedisCluster.set(bfoo3, bbar3);

        reply = jedisCluster.del(bfoo1, bfoo2, bfoo3);
        assertEquals(3, reply);

        breply = jedisCluster.exists(bfoo1);
        assertFalse(breply);
        breply = jedisCluster.exists(bfoo2);
        assertFalse(breply);
        breply = jedisCluster.exists(bfoo3);
        assertFalse(breply);

        jedisCluster.set(bfoo1, bbar1);

        reply = jedisCluster.del(bfoo1, bfoo2);
        assertEquals(1, reply);

        reply = jedisCluster.del(bfoo1, bfoo2);
        assertEquals(0, reply);
    }

    @Test
    public void unlink() {
        jedisCluster.set(foo1, bar1);
        jedisCluster.set(foo2, bar2);
        jedisCluster.set(foo3, bar3);

        long reply = jedisCluster.unlink(foo1, foo2, foo3);
        assertEquals(3, reply);

        reply = jedisCluster.exists(foo1, foo2, foo3);
        assertEquals(0, reply);

        jedisCluster.set(foo1, bar1);

        reply = jedisCluster.unlink(foo1, foo2);
        assertEquals(1, reply);

        reply = jedisCluster.unlink(foo1, foo2);
        assertEquals(0, reply);

        // Binary ...
        jedisCluster.set(bfoo1, bbar1);
        jedisCluster.set(bfoo2, bbar2);
        jedisCluster.set(bfoo3, bbar3);

        reply = jedisCluster.unlink(bfoo1, bfoo2, bfoo3);
        assertEquals(3, reply);

        reply = jedisCluster.exists(bfoo1, bfoo2, bfoo3);
        assertEquals(0, reply);

        jedisCluster.set(bfoo1, bbar1);

        reply = jedisCluster.unlink(bfoo1, bfoo2);
        assertEquals(1, reply);

        reply = jedisCluster.unlink(bfoo1, bfoo2);
        assertEquals(0, reply);
    }

    @Test
    public void type() {
        jedisCluster.set("foo", "bar");
        String status = jedisCluster.type("foo");
        assertEquals("string", status);

        // Binary
        jedisCluster.set(bfoo, bbar);
        status = jedisCluster.type(bfoo);
        assertEquals("string", status);
    }

    @Test
    public void keys() {
        jedisCluster.set(foo, bar);
        jedisCluster.set(foobar, bar);

        Set<String> keys = jedisCluster.keys(foostar);
        Set<String> expected = new HashSet<String>();
        expected.add(foo);
        expected.add(foobar);
        assertEquals(expected, keys);

        expected = new HashSet<String>();
        keys = jedisCluster.keys(barstar);

        assertEquals(expected, keys);

        // Binary
        jedisCluster.set(bfoo, bbar);
        jedisCluster.set(bfoobar, bbar);

        Set<byte[]> bkeys = jedisCluster.keys(bfoostar);
        assertEquals(2, bkeys.size());
        assertTrue(setContains(bkeys, bfoo));
        assertTrue(setContains(bkeys, bfoobar));

        bkeys = jedisCluster.keys(bbarstar);

        assertEquals(0, bkeys.size());
    }

//  @Test
//  public void randomKey() {
//    assertNull(jedis.randomKey());
//
//    jedis.set("foo", "bar");
//
//    assertEquals("foo", jedis.randomKey());
//
//    jedis.set("bar", "foo");
//
//    String randomkey = jedis.randomKey();
//    assertTrue(randomkey.equals("foo") || randomkey.equals("bar"));
//
//    // Binary
//    jedis.del("foo");
//    jedis.del("bar");
//    assertNull(jedis.randomKey());
//
//    jedis.set(bfoo, bbar);
//
//    assertArrayEquals(bfoo, jedis.randomBinaryKey());
//
//    jedis.set(bbar, bfoo);
//
//    byte[] randomBkey = jedis.randomBinaryKey();
//    assertTrue(Arrays.equals(randomBkey, bfoo) || Arrays.equals(randomBkey, bbar));
//
//  }

    @Test
    public void rename() {
        String key1 = "foo{rename}";
        String key2 = "bar{rename}";
        jedisCluster.set(key1, key2);
        String status = jedisCluster.rename(key1, key2);
        assertEquals("OK", status);

        String value = jedisCluster.get(key1);
        assertNull(value);

        value = jedisCluster.get(key2);
        assertEquals(key2, value);

        // Binary
        byte[] bKey1 = SafeEncoder.encode("bfoo{rename}");
        byte[] bKey2 = SafeEncoder.encode("bbar{rename}");
        jedisCluster.set(bKey1, bKey2);
        String bstatus = jedisCluster.rename(bKey1, bKey2);
        assertEquals("OK", bstatus);

        byte[] bvalue = jedisCluster.get(bKey1);
        assertNull(bvalue);

        bvalue = jedisCluster.get(bKey2);
        assertArrayEquals(bKey2, bvalue);
    }

    @Test
    public void renameOldAndNewAreTheSame() {
        jedisCluster.set("foo", "bar");
        jedisCluster.rename("foo", "foo");

        // Binary
        jedisCluster.set(bfoo, bbar);
        jedisCluster.rename(bfoo, bfoo);
    }

    @Test
    public void renamenx() {
        jedisCluster.set(foo, bar);
        long status = jedisCluster.renamenx(foo, bar);
        assertEquals(1, status);

        jedisCluster.set(foo, bar);
        status = jedisCluster.renamenx(foo, bar);
        assertEquals(0, status);

        // Binary
        jedisCluster.set(bfoo, bbar);
        long bstatus = jedisCluster.renamenx(bfoo, bbar);
        assertEquals(1, bstatus);

        jedisCluster.set(bfoo, bbar);
        bstatus = jedisCluster.renamenx(bfoo, bbar);
        assertEquals(0, bstatus);

    }

//  @Test
//  public void dbSize() {
//    long size = jedisCluster.dbSize();
//    assertEquals(0, size);
//
//    jedisCluster.set("foo", "bar");
//    size = jedisCluster.dbSize();
//    assertEquals(1, size);
//
//    // Binary
//    jedisCluster.set(bfoo, bbar);
//    size = jedisCluster.dbSize();
//    assertEquals(2, size);
//  }

    @Test
    public void expire() {
        long status = jedisCluster.expire("foo", 20);
        assertEquals(0, status);

        jedisCluster.set("foo", "bar");
        status = jedisCluster.expire("foo", 20);
        assertEquals(1, status);

        // Binary
        long bstatus = jedisCluster.expire(bfoo, 20);
        assertEquals(0, bstatus);

        jedisCluster.set(bfoo, bbar);
        bstatus = jedisCluster.expire(bfoo, 20);
        assertEquals(1, bstatus);

    }

    @Test
    public void expireAt() {
        long unixTime = (System.currentTimeMillis() / 1000L) + 20;

        long status = jedisCluster.expireAt("foo", unixTime);
        assertEquals(0, status);

        jedisCluster.set("foo", "bar");
        unixTime = (System.currentTimeMillis() / 1000L) + 20;
        status = jedisCluster.expireAt("foo", unixTime);
        assertEquals(1, status);

        // Binary
        long bstatus = jedisCluster.expireAt(bfoo, unixTime);
        assertEquals(0, bstatus);

        jedisCluster.set(bfoo, bbar);
        unixTime = (System.currentTimeMillis() / 1000L) + 20;
        bstatus = jedisCluster.expireAt(bfoo, unixTime);
        assertEquals(1, bstatus);

    }

    @Test
    public void ttl() {
        long ttl = jedisCluster.ttl("foo");
        assertEquals(-2, ttl);

        jedisCluster.set("foo", "bar");
        ttl = jedisCluster.ttl("foo");
        assertEquals(-1, ttl);

        jedisCluster.expire("foo", 20);
        ttl = jedisCluster.ttl("foo");
        assertTrue(ttl >= 0 && ttl <= 20);

        // Binary
        long bttl = jedisCluster.ttl(bfoo);
        assertEquals(-2, bttl);

        jedisCluster.set(bfoo, bbar);
        bttl = jedisCluster.ttl(bfoo);
        assertEquals(-1, bttl);

        jedisCluster.expire(bfoo, 20);
        bttl = jedisCluster.ttl(bfoo);
        assertTrue(bttl >= 0 && bttl <= 20);

    }


//  @Test
//  public void select() {
//    jedisCluster.set("foo", "bar");
//    String status = jedisCluster.select(1);
//    assertEquals("OK", status);
//    assertNull(jedisCluster.get("foo"));
//    status = jedisCluster.select(0);
//    assertEquals("OK", status);
//    assertEquals("bar", jedisCluster.get("foo"));
//    // Binary
//    jedisCluster.set(bfoo, bbar);
//    String bstatus = jedisCluster.select(1);
//    assertEquals("OK", bstatus);
//    assertNull(jedisCluster.get(bfoo));
//    bstatus = jedisCluster.select(0);
//    assertEquals("OK", bstatus);
//    assertArrayEquals(bbar, jedisCluster.get(bfoo));
//  }

//    @Test
//    public void getDB() {
//        assertEquals(0, jedisCluster.getDB().longValue());
//        jedisCluster.select(1);
//        assertEquals(1, jedisCluster.getDB().longValue());
//    }

//  @Test
//  public void move() {
//    long status = jedis.move("foo", 1);
//    assertEquals(0, status);
//
//    jedis.set("foo", "bar");
//    status = jedis.move("foo", 1);
//    assertEquals(1, status);
//    assertNull(jedis.get("foo"));
//
//    jedis.select(1);
//    assertEquals("bar", jedis.get("foo"));
//
//    // Binary
//    jedis.select(0);
//    long bstatus = jedis.move(bfoo, 1);
//    assertEquals(0, bstatus);
//
//    jedis.set(bfoo, bbar);
//    bstatus = jedis.move(bfoo, 1);
//    assertEquals(1, bstatus);
//    assertNull(jedis.get(bfoo));
//
//    jedis.select(1);
//    assertArrayEquals(bbar, jedis.get(bfoo));
//
//  }
//
//  @Test
//  public void swapDB() {
//    jedisCluster.set("foo1", "bar1");
//    jedisCluster.select(1);
//    assertNull(jedisCluster.get("foo1"));
//    jedisCluster.set("foo2", "bar2");
//    String status = jedisCluster.swapDB(0, 1);
//    assertEquals("OK", status);
//    assertEquals("bar1", jedisCluster.get("foo1"));
//    assertNull(jedisCluster.get("foo2"));
//    jedisCluster.select(0);
//    assertNull(jedisCluster.get("foo1"));
//    assertEquals("bar2", jedisCluster.get("foo2"));
//
//    // Binary
//    jedisCluster.set(bfoo1, bbar1);
//    jedisCluster.select(1);
//    assertArrayEquals(null, jedisCluster.get(bfoo1));
//    jedisCluster.set(bfoo2, bbar2);
//    status = jedisCluster.swapDB(0, 1);
//    assertEquals("OK", status);
//    assertArrayEquals(bbar1, jedisCluster.get(bfoo1));
//    assertArrayEquals(null, jedisCluster.get(bfoo2));
//    jedisCluster.select(0);
//    assertArrayEquals(null, jedisCluster.get(bfoo1));
//    assertArrayEquals(bbar2, jedisCluster.get(bfoo2));
//  }

//  @Test
//  public void flushDB() {
//    jedis.set("foo", "bar");
//    assertEquals(1, jedis.dbSize().intValue());
//    jedis.set("bar", "foo");
//    jedis.move("bar", 1);
//    String status = jedis.flushDB();
//    assertEquals("OK", status);
//    assertEquals(0, jedis.dbSize().intValue());
//    jedis.select(1);
//    assertEquals(1, jedis.dbSize().intValue());
//    jedis.del("bar");
//
//    // Binary
//    jedis.select(0);
//    jedis.set(bfoo, bbar);
//    assertEquals(1, jedis.dbSize().intValue());
//    jedis.set(bbar, bfoo);
//    jedis.move(bbar, 1);
//    String bstatus = jedis.flushDB();
//    assertEquals("OK", bstatus);
//    assertEquals(0, jedis.dbSize().intValue());
//    jedis.select(1);
//    assertEquals(1, jedis.dbSize().intValue());
//
//  }

//
//  @Test
//  public void flushAll() {
//    jedis.set("foo", "bar");
//    assertEquals(1, jedis.dbSize().intValue());
//    jedis.set("bar", "foo");
//    jedis.move("bar", 1);
//    String status = jedis.flushAll();
//    assertEquals("OK", status);
//    assertEquals(0, jedis.dbSize().intValue());
//    jedis.select(1);
//    assertEquals(0, jedis.dbSize().intValue());
//
//    // Binary
//    jedis.select(0);
//    jedis.set(bfoo, bbar);
//    assertEquals(1, jedis.dbSize().intValue());
//    jedis.set(bbar, bfoo);
//    jedis.move(bbar, 1);
//    String bstatus = jedis.flushAll();
//    assertEquals("OK", bstatus);
//    assertEquals(0, jedis.dbSize().intValue());
//    jedis.select(1);
//    assertEquals(0, jedis.dbSize().intValue());
//
//  }

    @Test
    public void persist() {
        jedisCluster.setex("foo", 60 * 60, "bar");
        assertTrue(jedisCluster.ttl("foo") > 0);
        long status = jedisCluster.persist("foo");
        assertEquals(1, status);
        assertEquals(-1, jedisCluster.ttl("foo").intValue());

        // Binary
        jedisCluster.setex(bfoo, 60 * 60, bbar);
        assertTrue(jedisCluster.ttl(bfoo) > 0);
        long bstatus = jedisCluster.persist(bfoo);
        assertEquals(1, bstatus);
        assertEquals(-1, jedisCluster.ttl(bfoo).intValue());

    }

    @Test
    public void echo() {
        String result = jedisCluster.echo("hello world");
        assertEquals("hello world", result);

        // Binary
        byte[] bresult = jedisCluster.echo(SafeEncoder.encode("hello world"));
        assertArrayEquals(SafeEncoder.encode("hello world"), bresult);
    }

    @Test
    public void dumpAndRestore() {
        jedisCluster.set("foo1", "bar");
        byte[] sv = jedisCluster.dump("foo1");
        jedisCluster.restore("foo2", 0, sv);
        assertEquals("bar", jedisCluster.get("foo2"));
    }

    @Test
    public void restoreReplace() {
        // take a separate instance
        jedisCluster.set("foo", "bar");

        Map<String, String> map = new HashMap<String, String>();
        map.put("a", "A");
        map.put("b", "B");

        jedisCluster.hset("from", map);
        byte[] serialized = jedisCluster.dump("from");

        try {
            jedisCluster.restore("foo", 0, serialized);
            fail("Simple restore on a existing key should fail");
        } catch (JedisDataException e) {
            // should be here
        }
        assertEquals("bar", jedisCluster.get("foo"));

        jedisCluster.getConnectionFromSlot(JedisClusterCRC16.getSlot("foo")).restoreReplace("foo", 0, serialized);
        assertEquals(map, jedisCluster.hgetAll("foo"));
    }

    @Test
    public void pexpire() {
        long status = jedisCluster.pexpire("foo", 10000);
        assertEquals(0, status);

        jedisCluster.set("foo1", "bar1");
        status = jedisCluster.pexpire("foo1", 10000);
        assertEquals(1, status);

        jedisCluster.set("foo2", "bar2");
        status = jedisCluster.pexpire("foo2", 200000000000L);
        assertEquals(1, status);

        long pttl = jedisCluster.pttl("foo2");
        assertTrue(pttl > 100000000000L);
    }

    @Test
    public void pexpireAt() {
        long unixTime = (System.currentTimeMillis()) + 10000;

        long status = jedisCluster.pexpireAt("foo", unixTime);
        assertEquals(0, status);

        jedisCluster.set("foo", "bar");
        unixTime = (System.currentTimeMillis()) + 10000;
        status = jedisCluster.pexpireAt("foo", unixTime);
        assertEquals(1, status);
    }

    @Test
    public void pttl() {
        long pttl = jedisCluster.pttl("foo");
        assertEquals(-2, pttl);

        jedisCluster.set("foo", "bar");
        pttl = jedisCluster.pttl("foo");
        assertEquals(-1, pttl);

        jedisCluster.pexpire("foo", 20000);
        pttl = jedisCluster.pttl("foo");
        assertTrue(pttl >= 0 && pttl <= 20000);
    }

    @Test
    public void psetex() {
        long pttl = jedisCluster.pttl("foo");
        assertEquals(-2, pttl);

        String status = jedisCluster.psetex("foo", 200000000000L, "bar");
        assertTrue(Keyword.OK.name().equalsIgnoreCase(status));

        pttl = jedisCluster.pttl("foo");
        assertTrue(pttl > 100000000000L);
    }

//  @Test
//  public void scan() {
//    jedis.set("b", "b");
//    jedis.set("a", "a");
//
//    ScanResult<String> result = jedis.scan(SCAN_POINTER_START);
//
//    assertEquals(SCAN_POINTER_START, result.getStringCursor());
//    assertFalse(result.getResult().isEmpty());
//
//    // binary
//    ScanResult<byte[]> bResult = jedis.scan(SCAN_POINTER_START_BINARY);
//
//    assertArrayEquals(SCAN_POINTER_START_BINARY, bResult.getCursorAsBytes());
//    assertFalse(bResult.getResult().isEmpty());
//  }

    @Test
    public void scanMatch() {
        ScanParams params = new ScanParams();
        params.match("a{scan}*");

        jedisCluster.set("b{scan{", "b");
        jedisCluster.set("a{scan}", "a");
        jedisCluster.set("aa{scan}", "aa");
        ScanResult<String> result = jedisCluster.scan(SCAN_POINTER_START, params);

        assertEquals(SCAN_POINTER_START, result.getStringCursor());
        assertFalse(result.getResult().isEmpty());

        // binary
        params = new ScanParams();
        params.match(SafeEncoder.encode("ba{scan}*"));

        jedisCluster.set(SafeEncoder.encode("ba{scan}1"), bbar);
        jedisCluster.set(SafeEncoder.encode("ba{scan}2"), bbar);
        jedisCluster.set(SafeEncoder.encode("ba{scan}3"), bbar);

        ScanResult<byte[]> bResult = jedisCluster.scan(SCAN_POINTER_START_BINARY, params);

        assertArrayEquals(SCAN_POINTER_START_BINARY, bResult.getCursorAsBytes());
        assertFalse(bResult.getResult().isEmpty());
    }

//  @Test
//  public void scanCount() {
//    ScanParams params = new ScanParams();
//    params.count(2);
//
//    for (int i = 0; i < 10; i++) {
//      jedisCluster.set("a" + i, "a" + i);
//    }
//
//    ScanResult<String> result = jedisCluster.scan(SCAN_POINTER_START, params);
//
//    assertFalse(result.getResult().isEmpty());
//
//    // binary
//    params = new ScanParams();
//    params.count(2);
//
//    jedisCluster.set(bfoo1, bbar);
//    jedisCluster.set(bfoo2, bbar);
//    jedisCluster.set(bfoo3, bbar);
//
//    ScanResult<byte[]> bResult = jedisCluster.scan(SCAN_POINTER_START_BINARY, params);
//
//    assertFalse(bResult.getResult().isEmpty());
//  }
//
//  @Test
//  public void scanIsCompleteIteration() {
//    for (int i = 0; i < 100; i++) {
//      jedis.set("a" + i, "a" + i);
//    }
//
//    ScanResult<String> result = jedis.scan(SCAN_POINTER_START);
//    // note: in theory Redis would be allowed to already return all results on the 1st scan,
//    // but in practice this never happens for data sets greater than a few tens
//    // see: https://redis.io/commands/scan#number-of-elements-returned-at-every-scan-call
//    assertFalse(result.isCompleteIteration());
//
//    result = scanCompletely(result.getStringCursor());
//
//    assertNotNull(result);
//    assertTrue(result.isCompleteIteration());
//  }

//  private ScanResult<String> scanCompletely(String cursor) {
//    ScanResult<String> scanResult;
//    do {
//      scanResult = jedis.scan(cursor);
//      cursor = scanResult.getStringCursor();
//    } while (!SCAN_POINTER_START.equals(scanResult.getStringCursor()));
//
//    return scanResult;
//  }

    @Test
    public void setNxExAndGet() {
        String status = jedisCluster.set("hello", "world", "NX", "EX", expireSeconds);
        assertTrue(Keyword.OK.name().equalsIgnoreCase(status));
        String value = jedisCluster.get("hello");
        assertEquals("world", value);

        jedisCluster.set("hello", "bar", "NX", "EX", expireSeconds);
        value = jedisCluster.get("hello");
        assertEquals("world", value);

        long ttl = jedisCluster.ttl("hello");
        assertTrue(ttl > 0 && ttl <= expireSeconds);

        // binary
        byte[] bworld = {0x77, 0x6F, 0x72, 0x6C, 0x64};
        byte[] bhello = {0x68, 0x65, 0x6C, 0x6C, 0x6F};
        String bstatus = jedisCluster.set(bworld, bhello, bnx, bex, expireSeconds);
        assertTrue(Keyword.OK.name().equalsIgnoreCase(bstatus));
        byte[] bvalue = jedisCluster.get(bworld);
        assertTrue(Arrays.equals(bhello, bvalue));

        jedisCluster.set(bworld, bbar, bnx, bex, expireSeconds);
        bvalue = jedisCluster.get(bworld);
        assertTrue(Arrays.equals(bhello, bvalue));

        long bttl = jedisCluster.ttl(bworld);
        assertTrue(bttl > 0 && bttl <= expireSeconds);
    }
}
