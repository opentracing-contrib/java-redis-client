package io.opentracing.contrib.redis.jediscluster.commands;

import org.junit.Test;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class StringValuesCommandsTest extends JedisCommandTestBase {
    @Test
    public void setAndGet() {
        String key1 = "setAndGet:foo";
        String key2 = "setAndGet:bar";
        String status = jedisCluster.set(key1, key2);
        assertEquals("OK", status);

        String value = jedisCluster.get(key1);
        assertEquals(key2, value);

        assertNull(jedisCluster.get(key2));
    }

    @Test
    public void getSet() {
        String key1 = "getSet:foo";
        String key2 = "getSet:bar";

        String value = jedisCluster.getSet(key1, key2);
        assertNull(value);
        value = jedisCluster.get(key1);
        assertEquals(key2, value);
    }

    @Test
    public void mget() {
        String key1 = "foo{mget}";
        String key2 = "bar{mget}";
        List<String> values = jedisCluster.mget(key1, key2);
        List<String> expected = new ArrayList<String>();
        expected.add(null);
        expected.add(null);

        assertEquals(expected, values);

        jedisCluster.set(key1, key2);

        expected = new ArrayList<String>();
        expected.add(key2);
        expected.add(null);
        values = jedisCluster.mget(key1, key2);

        assertEquals(expected, values);

        jedisCluster.set(key2, key1);

        expected = new ArrayList<String>();
        expected.add(key2);
        expected.add(key1);
        values = jedisCluster.mget(key1, key2);

        assertEquals(expected, values);
    }

    @Test
    public void setnx() {
        String key1 = "setnx:foo";
        String key2 = "setnx:bar";
        long status = jedisCluster.setnx(key1, key2);
        assertEquals(1, status);
        assertEquals(key2, jedisCluster.get(key1));

        status = jedisCluster.setnx(key1, key2);
        assertEquals(0, status);
        assertEquals(key2, jedisCluster.get(key1));
    }

    @Test
    public void setex() {
        String status = jedisCluster.setex("foo", 20, "bar");
        assertEquals("OK", status);
        long ttl = jedisCluster.ttl("foo");
        assertTrue(ttl > 0 && ttl <= 20);
    }

    @Test
    public void mset() {
        String key1 = "foo{mset}";
        String key2 = "bar{mset}";
        String status = jedisCluster.mset(key1, "bar", key2, "foo");
        assertEquals("OK", status);
        assertEquals("bar", jedisCluster.get(key1));
        assertEquals("foo", jedisCluster.get(key2));
    }

    @Test
    public void msetnx() {
        String key1 = "foo{msetnx}";
        String key2 = "bar{msetnx}";
        long status = jedisCluster.msetnx(key1, "bar", key2, "foo");
        assertEquals(1, status);
        assertEquals("bar", jedisCluster.get(key1));
        assertEquals("foo", jedisCluster.get(key2));

        status = jedisCluster.msetnx(key1, "bar1", key2, "foo2");
        assertEquals(0, status);
        assertEquals("bar", jedisCluster.get(key1));
        assertEquals("foo", jedisCluster.get(key2));
    }

    @Test(expected = JedisDataException.class)
    public void incrWrongValue() {
        jedisCluster.set("foo", "bar");
        jedisCluster.incr("foo");
    }

    @Test
    public void incr() {
        long value = jedisCluster.incr("foo");
        assertEquals(1, value);
        value = jedisCluster.incr("foo");
        assertEquals(2, value);
    }

    @Test(expected = JedisDataException.class)
    public void incrByWrongValue() {
        jedisCluster.set("foo", "bar");
        jedisCluster.incrBy("foo", 2);
    }

    @Test
    public void incrBy() {
        long value = jedisCluster.incrBy("foo", 2);
        assertEquals(2, value);
        value = jedisCluster.incrBy("foo", 2);
        assertEquals(4, value);
    }

    @Test(expected = JedisDataException.class)
    public void incrByFloatWrongValue() {
        jedisCluster.set("foo", "bar");
        jedisCluster.incrByFloat("foo", 2d);
    }

    @Test(expected = JedisDataException.class)
    public void decrWrongValue() {
        jedisCluster.set("foo", "bar");
        jedisCluster.decr("foo");
    }

    @Test
    public void decr() {
        long value = jedisCluster.decr("foo");
        assertEquals(-1, value);
        value = jedisCluster.decr("foo");
        assertEquals(-2, value);
    }

    @Test(expected = JedisDataException.class)
    public void decrByWrongValue() {
        jedisCluster.set("foo", "bar");
        jedisCluster.decrBy("foo", 2);
    }

    @Test
    public void decrBy() {
        long value = jedisCluster.decrBy("foo", 2);
        assertEquals(-2, value);
        value = jedisCluster.decrBy("foo", 2);
        assertEquals(-4, value);
    }

    @Test
    public void append() {
        long value = jedisCluster.append("foo", "bar");
        assertEquals(3, value);
        assertEquals("bar", jedisCluster.get("foo"));
        value = jedisCluster.append("foo", "bar");
        assertEquals(6, value);
        assertEquals("barbar", jedisCluster.get("foo"));
    }

    @Test
    public void substr() {
        jedisCluster.set("s", "This is a string");
        assertEquals("This", jedisCluster.substr("s", 0, 3));
        assertEquals("ing", jedisCluster.substr("s", -3, -1));
        assertEquals("This is a string", jedisCluster.substr("s", 0, -1));
        assertEquals(" string", jedisCluster.substr("s", 9, 100000));
    }

    @Test
    public void strlen() {
        jedisCluster.set("s", "This is a string");
        assertEquals("This is a string".length(), jedisCluster.strlen("s").intValue());
    }

    @Test
    public void incrLargeNumbers() {
        long value = jedisCluster.incr("foo");
        assertEquals(1, value);
        assertEquals(1L + Integer.MAX_VALUE, (long) jedisCluster.incrBy("foo", Integer.MAX_VALUE));
    }

    @Test(expected = JedisDataException.class)
    public void incrReallyLargeNumbers() {
        jedisCluster.set("foo", Long.toString(Long.MAX_VALUE));
        long value = jedisCluster.incr("foo");
        assertEquals(Long.MIN_VALUE, value);
    }

    @Test
    public void incrByFloat() {
        double value = jedisCluster.incrByFloat("foo", (double) 10.5);
        assertEquals(10.5, value, 0.0);
        value = jedisCluster.incrByFloat("foo", 0.1);
        assertEquals(10.6, value, 0.0);
    }

    @Test
    public void psetex() {
        String status = jedisCluster.psetex("foo", 20000, "bar");
        assertEquals("OK", status);
        long ttl = jedisCluster.ttl("foo");
        assertTrue(ttl > 0 && ttl <= 20000);
    }
}