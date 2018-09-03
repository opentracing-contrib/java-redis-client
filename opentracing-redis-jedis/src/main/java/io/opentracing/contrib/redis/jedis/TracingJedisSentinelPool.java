/*
 * Copyright 2017-2018 The OpenTracing Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.opentracing.contrib.redis.jedis;

import io.opentracing.Tracer;
import io.opentracing.contrib.redis.common.RedisSpanNameProvider;
import io.opentracing.util.GlobalTracer;
import java.util.Set;
import java.util.function.Function;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.Protocol;

public class TracingJedisSentinelPool extends JedisSentinelPool {

  private final Tracer tracer;
  private final boolean traceWithActiveSpanOnly;
  private Function<String, String> spanNameProvider;

  public TracingJedisSentinelPool(Tracer tracer, boolean traceWithActiveSpanOnly, String masterName,
      Set<String> sentinels,
      final GenericObjectPoolConfig poolConfig) {
    super(masterName, sentinels, poolConfig, Protocol.DEFAULT_TIMEOUT, null,
        Protocol.DEFAULT_DATABASE);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingJedisSentinelPool(boolean traceWithActiveSpanOnly, String masterName,
      Set<String> sentinels,
      final GenericObjectPoolConfig poolConfig) {
    this(GlobalTracer.get(), traceWithActiveSpanOnly, masterName, sentinels, poolConfig);
  }

  public TracingJedisSentinelPool(Tracer tracer, boolean traceWithActiveSpanOnly, String masterName,
      Set<String> sentinels) {
    super(masterName, sentinels, new GenericObjectPoolConfig(), Protocol.DEFAULT_TIMEOUT, null,
        Protocol.DEFAULT_DATABASE);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingJedisSentinelPool(boolean traceWithActiveSpanOnly, String masterName,
      Set<String> sentinels) {
    this(GlobalTracer.get(), traceWithActiveSpanOnly, masterName, sentinels);
  }

  public TracingJedisSentinelPool(Tracer tracer, boolean traceWithActiveSpanOnly, String masterName,
      Set<String> sentinels, String password) {
    super(masterName, sentinels, new GenericObjectPoolConfig(), Protocol.DEFAULT_TIMEOUT, password);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingJedisSentinelPool(boolean traceWithActiveSpanOnly, String masterName,
      Set<String> sentinels, String password) {
    this(GlobalTracer.get(), traceWithActiveSpanOnly, masterName, sentinels, password);
  }

  public TracingJedisSentinelPool(Tracer tracer, boolean traceWithActiveSpanOnly, String masterName,
      Set<String> sentinels,
      final GenericObjectPoolConfig poolConfig, int timeout, final String password) {
    super(masterName, sentinels, poolConfig, timeout, password, Protocol.DEFAULT_DATABASE);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingJedisSentinelPool(boolean traceWithActiveSpanOnly, String masterName,
      Set<String> sentinels,
      final GenericObjectPoolConfig poolConfig, int timeout, final String password) {
    this(GlobalTracer.get(), traceWithActiveSpanOnly, masterName, sentinels, poolConfig, timeout,
        password);
  }

  public TracingJedisSentinelPool(Tracer tracer, boolean traceWithActiveSpanOnly, String masterName,
      Set<String> sentinels,
      final GenericObjectPoolConfig poolConfig, final int timeout) {
    super(masterName, sentinels, poolConfig, timeout, null, Protocol.DEFAULT_DATABASE);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingJedisSentinelPool(boolean traceWithActiveSpanOnly, String masterName,
      Set<String> sentinels,
      final GenericObjectPoolConfig poolConfig, final int timeout) {
    this(GlobalTracer.get(), traceWithActiveSpanOnly, masterName, sentinels, poolConfig, timeout);
  }

  public TracingJedisSentinelPool(Tracer tracer, boolean traceWithActiveSpanOnly, String masterName,
      Set<String> sentinels,
      final GenericObjectPoolConfig poolConfig, final String password) {
    super(masterName, sentinels, poolConfig, Protocol.DEFAULT_TIMEOUT, password);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingJedisSentinelPool(boolean traceWithActiveSpanOnly, String masterName,
      Set<String> sentinels,
      final GenericObjectPoolConfig poolConfig, final String password) {
    this(GlobalTracer.get(), traceWithActiveSpanOnly, masterName, sentinels, poolConfig, password);
  }

  public TracingJedisSentinelPool(Tracer tracer, boolean traceWithActiveSpanOnly, String masterName,
      Set<String> sentinels,
      final GenericObjectPoolConfig poolConfig, int timeout, final String password,
      final int database) {
    super(masterName, sentinels, poolConfig, timeout, timeout, password, database);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingJedisSentinelPool(boolean traceWithActiveSpanOnly, String masterName,
      Set<String> sentinels,
      final GenericObjectPoolConfig poolConfig, int timeout, final String password,
      final int database) {
    this(GlobalTracer.get(), traceWithActiveSpanOnly, masterName, sentinels, poolConfig, timeout,
        password, database);
  }

  public TracingJedisSentinelPool(Tracer tracer, boolean traceWithActiveSpanOnly, String masterName,
      Set<String> sentinels,
      final GenericObjectPoolConfig poolConfig, int timeout, final String password,
      final int database, Function<String, String> customSpanName) {
    super(masterName, sentinels, poolConfig, timeout, timeout, password, database);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = customSpanName;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingJedisSentinelPool(boolean traceWithActiveSpanOnly, String masterName,
      Set<String> sentinels,
      final GenericObjectPoolConfig poolConfig, int timeout, final String password,
      final int database, Function<String, String> customSpanName) {
    this(GlobalTracer.get(), traceWithActiveSpanOnly, masterName, sentinels, poolConfig, timeout,
        password, database, customSpanName);
  }

  public TracingJedisSentinelPool(Tracer tracer, boolean traceWithActiveSpanOnly, String masterName,
      Set<String> sentinels,
      final GenericObjectPoolConfig poolConfig, int timeout, final String password,
      final int database, final String clientName) {
    super(masterName, sentinels, poolConfig, timeout, timeout, password, database, clientName);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingJedisSentinelPool(boolean traceWithActiveSpanOnly, String masterName,
      Set<String> sentinels,
      final GenericObjectPoolConfig poolConfig, int timeout, final String password,
      final int database, final String clientName) {
    this(GlobalTracer.get(), traceWithActiveSpanOnly, masterName, sentinels, poolConfig, timeout,
        password, database, clientName);
  }

  public TracingJedisSentinelPool(Tracer tracer, boolean traceWithActiveSpanOnly, String masterName,
      Set<String> sentinels,
      final GenericObjectPoolConfig poolConfig, final int timeout, final int soTimeout,
      final String password, final int database) {
    super(masterName, sentinels, poolConfig, timeout, soTimeout, password, database, null);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingJedisSentinelPool(boolean traceWithActiveSpanOnly, String masterName,
      Set<String> sentinels,
      final GenericObjectPoolConfig poolConfig, final int timeout, final int soTimeout,
      final String password, final int database) {
    this(GlobalTracer.get(), traceWithActiveSpanOnly, masterName, sentinels, poolConfig, timeout,
        soTimeout, password, database);
  }

  public TracingJedisSentinelPool(Tracer tracer, boolean traceWithActiveSpanOnly, String masterName,
      Set<String> sentinels,
      final GenericObjectPoolConfig poolConfig, final int connectionTimeout, final int soTimeout,
      final String password, final int database, final String clientName) {
    super(masterName, sentinels, poolConfig, connectionTimeout, soTimeout, password, database,
        clientName);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingJedisSentinelPool(boolean traceWithActiveSpanOnly, String masterName,
      Set<String> sentinels,
      final GenericObjectPoolConfig poolConfig, final int connectionTimeout, final int soTimeout,
      final String password, final int database, final String clientName) {
    this(GlobalTracer.get(), traceWithActiveSpanOnly, masterName, sentinels, poolConfig,
        connectionTimeout,
        soTimeout, password, database, clientName);
  }

  public TracingJedisSentinelPool(Tracer tracer, boolean traceWithActiveSpanOnly, String masterName,
      Set<String> sentinels,
      final GenericObjectPoolConfig poolConfig, final int connectionTimeout, final int soTimeout,
      final String password, final int database, final String clientName,
      Function<String, String> customSpanName) {
    super(masterName, sentinels, poolConfig, connectionTimeout, soTimeout, password, database,
        clientName);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = customSpanName;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingJedisSentinelPool(boolean traceWithActiveSpanOnly, String masterName,
      Set<String> sentinels,
      final GenericObjectPoolConfig poolConfig, final int connectionTimeout, final int soTimeout,
      final String password, final int database, final String clientName,
      Function<String, String> customSpanName) {
    this(GlobalTracer.get(), traceWithActiveSpanOnly, masterName, sentinels, poolConfig,
        connectionTimeout, soTimeout,
        password, database, clientName, customSpanName);
  }

  @Override
  public Jedis getResource() {
    Jedis resource = super.getResource();
    return new TracingJedisWrapper(resource, tracer, traceWithActiveSpanOnly);
  }

  @Override
  @Deprecated
  public void returnBrokenResource(final Jedis resource) {
    super.returnBrokenResource(unwrapResource(resource));
  }

  @Override
  @Deprecated
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
