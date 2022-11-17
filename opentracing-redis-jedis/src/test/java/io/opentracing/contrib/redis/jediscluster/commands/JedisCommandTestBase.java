package io.opentracing.contrib.redis.jediscluster.commands;

import io.opentracing.contrib.redis.HostAndPortUtil;
import io.opentracing.contrib.redis.common.RedisSpanNameProvider;
import io.opentracing.contrib.redis.common.TracingConfiguration;
import io.opentracing.contrib.redis.jedis.TracingJedisCluster;
import io.opentracing.util.GlobalTracer;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.After;
import org.junit.Before;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;

public abstract class JedisCommandTestBase {
    protected static HostAndPort hnp = HostAndPortUtil.getClusterServers().get(0);

    protected JedisCluster jedisCluster;

    @Before
    public void setUp() throws Exception {
//    jedis = new Jedis(hnp.getHost(), hnp.getPort(), 500);
//    jedis.connect();
//    jedis.auth("foobared");
//    jedis.flushAll();
//        jedis = new JedisCluster(new HashSet<>(HostAndPortUtil.getClusterServers()));
        TracingConfiguration tracingConfiguration = new TracingConfiguration.Builder(GlobalTracer.get())
                .withSpanNameProvider(RedisSpanNameProvider.PREFIX_OPERATION_NAME("testRedisName:"))
                .build();
        jedisCluster = new TracingJedisCluster(new HashSet<>(HostAndPortUtil.getClusterServers()), new GenericObjectPoolConfig(), tracingConfiguration);
    }

    @After
    public void tearDown() throws IOException {
        jedisCluster.getClusterNodes().forEach((ph, pool) -> {
            try {
                pool.getResource().flushAll();
            } catch (Exception e) {
                //ignore
            }
        });
        jedisCluster.close();
    }
//
//  protected Jedis createJedis() {
//    Jedis j = new Jedis(hnp);
//    j.connect();
//    j.auth("foobared");
//    j.flushAll();
//    return j;
//  }

    protected boolean arrayContains(List<byte[]> array, byte[] expected) {
        for (byte[] a : array) {
            try {
                assertArrayEquals(a, expected);
                return true;
            } catch (AssertionError e) {

            }
        }
        return false;
    }

    protected boolean setContains(Set<byte[]> set, byte[] expected) {
        for (byte[] a : set) {
            try {
                assertArrayEquals(a, expected);
                return true;
            } catch (AssertionError e) {

            }
        }
        return false;
    }
}
