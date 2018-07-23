package io.opentracing.contrib.redis.jedis;

import java.util.Set;
import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import io.opentracing.Tracer;
import io.opentracing.contrib.redis.common.RedisSpanNameProvider;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.Protocol;

public class TracingJedisSentinelPool extends JedisSentinelPool {

  private final Tracer tracer;
  private final boolean traceWithActiveSpanOnly;
  private Function<String, String> spanNameProvider;

  public TracingJedisSentinelPool(Tracer tracer, boolean traceWithActiveSpanOnly, String masterName, Set<String> sentinels,
      final GenericObjectPoolConfig poolConfig) {
    super(masterName, sentinels, poolConfig, Protocol.DEFAULT_TIMEOUT, null,
        Protocol.DEFAULT_DATABASE);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
  }

  public TracingJedisSentinelPool(Tracer tracer, boolean traceWithActiveSpanOnly, String masterName, Set<String> sentinels) {
    super(masterName, sentinels, new GenericObjectPoolConfig(), Protocol.DEFAULT_TIMEOUT, null,
        Protocol.DEFAULT_DATABASE);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
  }

  public TracingJedisSentinelPool(Tracer tracer, boolean traceWithActiveSpanOnly, String masterName, Set<String> sentinels, String password) {
    super(masterName, sentinels, new GenericObjectPoolConfig(), Protocol.DEFAULT_TIMEOUT, password);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
  }

  public TracingJedisSentinelPool(Tracer tracer, boolean traceWithActiveSpanOnly, String masterName, Set<String> sentinels,
      final GenericObjectPoolConfig poolConfig, int timeout, final String password) {
    super(masterName, sentinels, poolConfig, timeout, password, Protocol.DEFAULT_DATABASE);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
  }

  public TracingJedisSentinelPool(Tracer tracer, boolean traceWithActiveSpanOnly, String masterName, Set<String> sentinels,
      final GenericObjectPoolConfig poolConfig, final int timeout) {
    super(masterName, sentinels, poolConfig, timeout, null, Protocol.DEFAULT_DATABASE);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
  }

  public TracingJedisSentinelPool(Tracer tracer, boolean traceWithActiveSpanOnly, String masterName, Set<String> sentinels,
      final GenericObjectPoolConfig poolConfig, final String password) {
    super(masterName, sentinels, poolConfig, Protocol.DEFAULT_TIMEOUT, password);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
  }

  public TracingJedisSentinelPool(Tracer tracer, boolean traceWithActiveSpanOnly, String masterName, Set<String> sentinels,
      final GenericObjectPoolConfig poolConfig, int timeout, final String password,
      final int database) {
    super(masterName, sentinels, poolConfig, timeout, timeout, password, database);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
  }

  public TracingJedisSentinelPool(Tracer tracer, boolean traceWithActiveSpanOnly, String masterName, Set<String> sentinels,
      final GenericObjectPoolConfig poolConfig, int timeout, final String password,
      final int database, Function<String, String> customSpanName) {
    super(masterName, sentinels, poolConfig, timeout, timeout, password, database);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = customSpanName;
  }

  public TracingJedisSentinelPool(Tracer tracer, boolean traceWithActiveSpanOnly, String masterName, Set<String> sentinels,
      final GenericObjectPoolConfig poolConfig, int timeout, final String password,
      final int database, final String clientName) {
    super(masterName, sentinels, poolConfig, timeout, timeout, password, database, clientName);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
  }

  public TracingJedisSentinelPool(Tracer tracer, boolean traceWithActiveSpanOnly, String masterName, Set<String> sentinels,
      final GenericObjectPoolConfig poolConfig, final int timeout, final int soTimeout,
      final String password, final int database) {
    super(masterName, sentinels, poolConfig, timeout, soTimeout, password, database, null);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
  }

  public TracingJedisSentinelPool(Tracer tracer, boolean traceWithActiveSpanOnly, String masterName, Set<String> sentinels,
      final GenericObjectPoolConfig poolConfig, final int connectionTimeout, final int soTimeout,
      final String password, final int database, final String clientName) {
    super(masterName, sentinels, poolConfig, connectionTimeout, soTimeout, password, database, clientName);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
  }

  public TracingJedisSentinelPool(Tracer tracer, boolean traceWithActiveSpanOnly, String masterName, Set<String> sentinels,
      final GenericObjectPoolConfig poolConfig, final int connectionTimeout, final int soTimeout,
      final String password, final int database, final String clientName, Function<String, String> customSpanName) {
    super(masterName, sentinels, poolConfig, connectionTimeout, soTimeout, password, database, clientName);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = customSpanName;
  }

  @Override
  public Jedis getResource() {
    Jedis resource = super.getResource();
    return new TracingJedisWrapper(resource, tracer, traceWithActiveSpanOnly);
  }

  @Override
  public void returnBrokenResource(final Jedis resource) {
    super.returnBrokenResource(unwrapResource(resource));
  }

  @Override
  public void returnResource(final Jedis resource) {
    super.returnResource(unwrapResource(resource));
  }

  private Jedis unwrapResource(Jedis resource) {
    return (resource instanceof TracingJedisSentinelPool.TracingJedisWrapper)
        ? ((TracingJedisSentinelPool.TracingJedisWrapper) resource).getWrapped()
        : resource;
  }


  private class TracingJedisWrapper extends TracingJedis {
    private final Jedis wrapped;

    public TracingJedisWrapper(Jedis jedis, Tracer tracer, boolean traceWithActiveSpanOnly) {
      super(tracer, traceWithActiveSpanOnly, spanNameProvider);
      this.client = jedis.getClient();
      this.wrapped = jedis;
    }

    @Override
    public void close() {
      super.close();
      wrapped.close();
    }

    public Jedis getWrapped() {
      return wrapped;
    }
  }
}
