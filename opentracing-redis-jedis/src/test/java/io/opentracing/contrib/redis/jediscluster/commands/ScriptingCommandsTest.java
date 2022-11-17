package io.opentracing.contrib.redis.jediscluster.commands;

import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.junit.Test;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisNoScriptException;
import redis.clients.util.SafeEncoder;

import java.util.ArrayList;
import java.util.List;

import static io.opentracing.contrib.redis.jediscluster.commands.TestKeys.foo1;
import static io.opentracing.contrib.redis.jediscluster.commands.TestKeys.foo2;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class ScriptingCommandsTest extends JedisCommandTestBase {

    @SuppressWarnings("unchecked")
    @Test
    public void evalMultiBulk() {
        String script = "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2],ARGV[3]}";
        List<String> keys = new ArrayList<String>();
        keys.add(foo1);
        keys.add(foo2);

        List<String> args = new ArrayList<String>();
        args.add("first");
        args.add("second");
        args.add("third");

        List<String> response = (List<String>) jedisCluster.eval(script, keys, args);

        assertEquals(5, response.size());
        assertEquals(foo1, response.get(0));
        assertEquals(foo2, response.get(1));
        assertEquals("first", response.get(2));
        assertEquals("second", response.get(3));
        assertEquals("third", response.get(4));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void evalMultiBulkWithBinaryJedis() {
        String script = "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2],ARGV[3]}";
        List<byte[]> keys = new ArrayList<byte[]>();
        keys.add(foo1.getBytes());
        keys.add(foo2.getBytes());

        List<byte[]> args = new ArrayList<byte[]>();
        args.add("first".getBytes());
        args.add("second".getBytes());
        args.add("third".getBytes());

        List<byte[]> responses = (List<byte[]>) jedisCluster.eval(script.getBytes(), keys, args);
        assertEquals(5, responses.size());
        assertEquals(foo1, new String(responses.get(0)));
        assertEquals(foo2, new String(responses.get(1)));
        assertEquals("first", new String(responses.get(2)));
        assertEquals("second", new String(responses.get(3)));
        assertEquals("third", new String(responses.get(4)));
    }

    @Test
    public void evalBulk() {
        String script = "return KEYS[1]";
        List<String> keys = new ArrayList<String>();
        keys.add("key1");

        List<String> args = new ArrayList<String>();
        args.add("first");

        String response = (String) jedisCluster.eval(script, keys, args);

        assertEquals("key1", response);
    }

    @Test
    public void evalInt() {
        String script = "return 2";
        List<String> keys = new ArrayList<String>();
        keys.add("key1");

        Long response = (Long) jedisCluster.eval(script, keys, new ArrayList<String>());

        assertEquals(new Long(2), response);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void evalNestedLists() {
        String script = "return { {KEYS[1]} , {2} }";
        List<?> results = (List<?>) jedisCluster.eval(script, 1, "key1");

        assertThat((List<String>) results.get(0), listWithItem("key1"));
        assertThat((List<Long>) results.get(1), listWithItem(2L));
    }

    @Test
    public void evalNoArgs() {
        String script = "return KEYS[1]";
        List<String> keys = new ArrayList<String>();
        keys.add("key1");
        String response = (String) jedisCluster.eval(script, keys, new ArrayList<String>());

        assertEquals("key1", response);
    }

    @Test
    public void evalsha() {
        jedisCluster.set("foo", "bar");
        jedisCluster.eval("return redis.call('get','foo')", "foo");
        String result = (String) jedisCluster.evalsha("6b1bf486c81ceb7edf3c093f4c48582e38c0e791", "foo");

        assertEquals("bar", result);
    }

    @Test
    public void evalshaBinary() {
        jedisCluster.set(SafeEncoder.encode("foo"), SafeEncoder.encode("bar"));
        jedisCluster.eval(SafeEncoder.encode("return redis.call('get','foo')"), SafeEncoder.encode("foo"));
        byte[] result = (byte[]) jedisCluster.evalsha(SafeEncoder.encode("6b1bf486c81ceb7edf3c093f4c48582e38c0e791"), SafeEncoder.encode("foo"));

        assertArrayEquals(SafeEncoder.encode("bar"), result);
    }

    @Test(expected = JedisNoScriptException.class)
    public void evalshaShaNotFound() {
        jedisCluster.evalsha("notfound", "ffffffffffffffffffffffffffffffffffffffff");
    }

    @Test
    public void scriptFlush() {
        jedisCluster.set("foo", "bar");
        jedisCluster.eval("return redis.call('get','foo')", "foo");
        jedisCluster.scriptFlush("foo");
        assertFalse(jedisCluster.scriptExists("6b1bf486c81ceb7edf3c093f4c48582e38c0e791", "foo"));
    }

    @Test
    public void scriptExists() {
        jedisCluster.scriptLoad("return redis.call('get','foo')", "ffffffffffffffffffffffffffffffffffffffff");
        Boolean exists = jedisCluster.scriptExists("6b1bf486c81ceb7edf3c093f4c48582e38c0e791",
                "ffffffffffffffffffffffffffffffffffffffff");
        assertTrue(exists);
    }

    @Test
    public void scriptExistsBinary() {
        jedisCluster.scriptLoad(SafeEncoder.encode("return redis.call('get','foo')"), SafeEncoder.encode("ffffffffffffffffffffffffffffffffffffffff"));
        List<Long> exists = jedisCluster.scriptExists(
                SafeEncoder.encode("ffffffffffffffffffffffffffffffffffffffff"),
                SafeEncoder.encode("6b1bf486c81ceb7edf3c093f4c48582e38c0e791"));
        assertEquals(new Long(1), exists.get(0));
    }

    @Test
    public void scriptLoad() {
        jedisCluster.scriptLoad("return redis.call('get','foo')", "foo");
        assertTrue(jedisCluster.scriptExists("6b1bf486c81ceb7edf3c093f4c48582e38c0e791", "foo"));
    }

    @Test
    public void scriptLoadBinary() {
        jedisCluster.scriptLoad(SafeEncoder.encode("return redis.call('get','foo')"), SafeEncoder.encode("foo"));
        List<Long> exists = jedisCluster.scriptExists(SafeEncoder.encode("foo"), SafeEncoder.encode("6b1bf486c81ceb7edf3c093f4c48582e38c0e791"));
        assertEquals(new Long(1), exists.get(0));
    }

    @Test
    public void scriptKill() {
        try {
            jedisCluster.scriptKill("foo");
        } catch (JedisDataException e) {
            assertTrue(e.getMessage().contains("No scripts in execution right now."));
        }
    }

    @Test
    public void scriptEvalReturnNullValues() {
        jedisCluster.del(foo1);
        jedisCluster.del(foo2);

        String script = "return {redis.call('hget',KEYS[1],ARGV[1]),redis.call('hget',KEYS[2],ARGV[2])}";
        List<String> results = (List<String>) jedisCluster.eval(script, 2, foo1, foo2, "1", "2");
        assertEquals(2, results.size());
        assertNull(results.get(0));
        assertNull(results.get(1));
    }

    @Test
    public void scriptEvalShaReturnNullValues() {
        jedisCluster.del("key1");
        jedisCluster.del("key2");

        String script = "return {redis.call('hget',KEYS[1],ARGV[1]),redis.call('hget',KEYS[2],ARGV[2])}";
        String sha = jedisCluster.scriptLoad(script, "scriptTag");
        List<String> results = (List<String>) jedisCluster.evalsha(sha, 2, "scriptTag", "key1{scriptTag}", "key2{scriptTag}", "1", "2");
        assertEquals(2, results.size());
        assertNull(results.get(0));
        assertNull(results.get(1));
    }
//
//  @Test
//  public void scriptExistsWithBrokenConnection() {
//    Jedis deadClient = new Jedis(jedis.getClient().getHost(), jedis.getClient().getPort());
//    deadClient.auth("foobared");
//
//    deadClient.clientSetname("DEAD");
//
//    ClientKillerUtil.killClient(deadClient, "DEAD");
//
//    // sure, script doesn't exist, but it's just for checking connection
//    try {
//      deadClient.scriptExists("abcdefg");
//    } catch (JedisConnectionException e) {
//      // ignore it
//    }
//
//    assertEquals(true, deadClient.getClient().isBroken());
//
//    deadClient.close();
//  }

    private <T> Matcher<Iterable<? super T>> listWithItem(T expected) {
        return CoreMatchers.<T>hasItem(equalTo(expected));
    }
}
