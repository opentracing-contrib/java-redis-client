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
import java.net.URI;
import java.util.function.Function;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class TracingJedisPool extends JedisPool {
  private final Tracer tracer;
  private final boolean traceWithActiveSpanOnly;
  private Function<String, String> spanNameProvider;

  public TracingJedisPool(Tracer tracer, boolean traceWithActiveSpanOnly) {
    super();
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingJedisPool(boolean traceWithActiveSpanOnly) {
    this(GlobalTracer.get(), true);
  }

  public TracingJedisPool(Tracer tracer, boolean traceWithActiveSpanOnly,
      Function<String, String> spanNameProvider) {
    super();
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    this.spanNameProvider = spanNameProvider;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingJedisPool(boolean traceWithActiveSpanOnly,
      Function<String, String> spanNameProvider) {
    this(GlobalTracer.get(), traceWithActiveSpanOnly, spanNameProvider);
  }

  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      Tracer tracer, boolean traceWithActiveSpanOnly) {
    super(poolConfig, host);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      boolean traceWithActiveSpanOnly) {
    this(poolConfig, host, GlobalTracer.get(), traceWithActiveSpanOnly);
  }

  public TracingJedisPool(final String host, final int port, Tracer tracer,
      boolean traceWithActiveSpanOnly) {
    super(host, port);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingJedisPool(final String host, final int port, boolean traceWithActiveSpanOnly) {
    this(host, port, GlobalTracer.get(), traceWithActiveSpanOnly);
  }

  public TracingJedisPool(final String host, Tracer tracer, boolean traceWithActiveSpanOnly) {
    super(host);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingJedisPool(final String host, boolean traceWithActiveSpanOnly) {
    this(host, GlobalTracer.get(), traceWithActiveSpanOnly);
  }

  public TracingJedisPool(final String host, final SSLSocketFactory sslSocketFactory,
      final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier, Tracer tracer, boolean traceWithActiveSpanOnly) {
    super(host, sslSocketFactory, sslParameters, hostnameVerifier);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingJedisPool(final String host, final SSLSocketFactory sslSocketFactory,
      final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier, boolean traceWithActiveSpanOnly) {
    this(host, sslSocketFactory, sslParameters, hostnameVerifier, GlobalTracer.get(),
        traceWithActiveSpanOnly);
  }

  public TracingJedisPool(final URI uri, Tracer tracer, boolean traceWithActiveSpanOnly) {
    super(uri);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingJedisPool(final URI uri, boolean traceWithActiveSpanOnly) {
    this(uri, GlobalTracer.get(), traceWithActiveSpanOnly);
  }

  public TracingJedisPool(final URI uri, final SSLSocketFactory sslSocketFactory,
      final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier, Tracer tracer, boolean traceWithActiveSpanOnly) {
    super(uri, sslSocketFactory, sslParameters, hostnameVerifier);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingJedisPool(final URI uri, final SSLSocketFactory sslSocketFactory,
      final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier, boolean traceWithActiveSpanOnly) {
    this(uri, sslSocketFactory, sslParameters, hostnameVerifier, GlobalTracer.get(),
        traceWithActiveSpanOnly);
  }

  public TracingJedisPool(final URI uri, final int timeout, Tracer tracer,
      boolean traceWithActiveSpanOnly) {
    super(uri, timeout);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingJedisPool(final URI uri, final int timeout,
      boolean traceWithActiveSpanOnly) {
    this(uri, timeout, GlobalTracer.get(), traceWithActiveSpanOnly);
  }

  public TracingJedisPool(final URI uri, final int timeout, final SSLSocketFactory sslSocketFactory,
      final SSLParameters sslParameters, final HostnameVerifier hostnameVerifier,
      Tracer tracer, boolean traceWithActiveSpanOnly) {
    super(uri, timeout, sslSocketFactory, sslParameters, hostnameVerifier);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingJedisPool(final URI uri, final int timeout, final SSLSocketFactory sslSocketFactory,
      final SSLParameters sslParameters, final HostnameVerifier hostnameVerifier,
      boolean traceWithActiveSpanOnly) {
    this(uri, timeout, sslSocketFactory, sslParameters, hostnameVerifier, GlobalTracer.get(),
        traceWithActiveSpanOnly);
  }


  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port,
      final int timeout, final String password,
      Tracer tracer, boolean traceWithActiveSpanOnly) {
    super(poolConfig, host, port, timeout, password);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port, final int timeout, final String password, boolean traceWithActiveSpanOnly) {
    this(poolConfig, host, port, timeout, password, GlobalTracer.get(), traceWithActiveSpanOnly);
  }

  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port,
      final int timeout, final String password, final boolean ssl,
      Tracer tracer, boolean traceWithActiveSpanOnly) {
    super(poolConfig, host, port, timeout, password, ssl);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port,
      final int timeout, final String password, final boolean ssl,
      boolean traceWithActiveSpanOnly) {
    this(poolConfig, host, port, timeout, password, ssl, GlobalTracer.get(),
        traceWithActiveSpanOnly);
  }

  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port,
      final int timeout, final String password, final boolean ssl,
      final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier, Tracer tracer, boolean traceWithActiveSpanOnly) {
    super(poolConfig, host, port, timeout, password, ssl, sslSocketFactory, sslParameters,
        hostnameVerifier);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port,
      final int timeout, final String password, final boolean ssl,
      final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier, boolean traceWithActiveSpanOnly) {
    this(poolConfig, host, port, timeout, password, ssl, sslSocketFactory, sslParameters,
        hostnameVerifier, GlobalTracer.get(), traceWithActiveSpanOnly);
  }

  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port, Tracer tracer, boolean traceWithActiveSpanOnly) {
    super(poolConfig, host, port);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port, boolean traceWithActiveSpanOnly) {
    this(poolConfig, host, port, GlobalTracer.get(), traceWithActiveSpanOnly);
  }

  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port, final boolean ssl,
      Tracer tracer, boolean traceWithActiveSpanOnly) {
    super(poolConfig, host, port, ssl);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port, final boolean ssl,
      boolean traceWithActiveSpanOnly) {
    this(poolConfig, host, port, ssl, GlobalTracer.get(), traceWithActiveSpanOnly);
  }


  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port, final boolean ssl,
      final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier, Tracer tracer, boolean traceWithActiveSpanOnly) {
    super(poolConfig, host, port, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port, final boolean ssl,
      final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier, boolean traceWithActiveSpanOnly) {
    this(poolConfig, host, port, ssl, sslSocketFactory, sslParameters, hostnameVerifier,
        GlobalTracer.get(), traceWithActiveSpanOnly);
  }

  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port, final int timeout,
      Tracer tracer, boolean traceWithActiveSpanOnly) {
    super(poolConfig, host, port, timeout);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port, final int timeout,
      boolean traceWithActiveSpanOnly) {
    this(poolConfig, host, port, timeout, GlobalTracer.get(), traceWithActiveSpanOnly);
  }

  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port, final int timeout,
      final boolean ssl, Tracer tracer, boolean traceWithActiveSpanOnly) {
    super(poolConfig, host, port, timeout, ssl);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port, final int timeout,
      final boolean ssl, boolean traceWithActiveSpanOnly) {
    this(poolConfig, host, port, timeout, ssl, GlobalTracer.get(), traceWithActiveSpanOnly);
  }

  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port, final int timeout,
      final boolean ssl, final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier, Tracer tracer, boolean traceWithActiveSpanOnly) {
    super(poolConfig, host, port, timeout, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port, final int timeout,
      final boolean ssl, final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier, boolean traceWithActiveSpanOnly) {
    this(poolConfig, host, port, timeout, ssl, sslSocketFactory, sslParameters, hostnameVerifier,
        GlobalTracer.get(), traceWithActiveSpanOnly);
  }

  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port, final int timeout,
      final String password, final int database, Tracer tracer, boolean traceWithActiveSpanOnly) {
    super(poolConfig, host, port, timeout, password, database);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port, final int timeout,
      final String password, final int database, boolean traceWithActiveSpanOnly) {
    this(poolConfig, host, port, timeout, password, database, GlobalTracer.get(),
        traceWithActiveSpanOnly);
  }

  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port, final int timeout,
      final String password, final int database, final boolean ssl,
      Tracer tracer, boolean traceWithActiveSpanOnly) {
    super(poolConfig, host, port, timeout, password, database, ssl);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port, final int timeout,
      final String password, final int database, final boolean ssl,
      boolean traceWithActiveSpanOnly) {
    this(poolConfig, host, port, timeout, password, database, ssl, GlobalTracer.get(),
        traceWithActiveSpanOnly);
  }

  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port, final int timeout,
      final String password, final int database, final boolean ssl,
      final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier, Tracer tracer, boolean traceWithActiveSpanOnly) {
    super(poolConfig, host, port, timeout, password, database, ssl, sslSocketFactory, sslParameters,
        hostnameVerifier);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port, final int timeout,
      final String password, final int database, final boolean ssl,
      final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier, boolean traceWithActiveSpanOnly) {
    this(poolConfig, host, port, timeout, password, database, ssl, sslSocketFactory, sslParameters,
        hostnameVerifier, GlobalTracer.get(), traceWithActiveSpanOnly);
  }

  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port, final int timeout,
      final String password, final int database, final String clientName,
      Tracer tracer, boolean traceWithActiveSpanOnly) {
    this(poolConfig, host, port, timeout, timeout, password, database, clientName, false,
        null, null, null, tracer, traceWithActiveSpanOnly);
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port, final int timeout,
      final String password, final int database, final String clientName,
      boolean traceWithActiveSpanOnly) {
    this(poolConfig, host, port, timeout, password, database, clientName, GlobalTracer.get(),
        traceWithActiveSpanOnly);
  }

  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port, final int timeout,
      final String password, final int database, final String clientName,
      Tracer tracer, boolean traceWithActiveSpanOnly, Function<String, String> customSpanName) {
    this(poolConfig, host, port, timeout, timeout, password, database, clientName, false,
        null, null, null, tracer, traceWithActiveSpanOnly, customSpanName);
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port, final int timeout,
      final String password, final int database, final String clientName,
      boolean traceWithActiveSpanOnly, Function<String, String> customSpanName) {
    this(poolConfig, host, port, timeout, password, database, clientName,
        GlobalTracer.get(), traceWithActiveSpanOnly, customSpanName);
  }

  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port, final int timeout,
      final String password, final int database, final String clientName, final boolean ssl,
      Tracer tracer, boolean traceWithActiveSpanOnly) {
    super(poolConfig, host, port, timeout, password, database, clientName, ssl);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port, final int timeout,
      final String password, final int database, final String clientName, final boolean ssl,
      boolean traceWithActiveSpanOnly) {
    this(poolConfig, host, port, timeout, password, database, clientName, ssl, GlobalTracer.get(),
        traceWithActiveSpanOnly);
  }

  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port, final int timeout,
      final String password, final int database, final String clientName, final boolean ssl,
      final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier, Tracer tracer, boolean traceWithActiveSpanOnly) {
    super(poolConfig, host, port, timeout, password, database, clientName, ssl, sslSocketFactory,
        sslParameters, hostnameVerifier);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port, final int timeout,
      final String password, final int database, final String clientName, final boolean ssl,
      final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier, boolean traceWithActiveSpanOnly) {
    this(poolConfig, host, port, timeout, password, database, clientName, ssl, sslSocketFactory,
        sslParameters, hostnameVerifier, GlobalTracer.get(), traceWithActiveSpanOnly);
  }

  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port,
      final int connectionTimeout, final int soTimeout, final String password, final int database,
      final String clientName, final boolean ssl, final SSLSocketFactory sslSocketFactory,
      final SSLParameters sslParameters, final HostnameVerifier hostnameVerifier,
      Tracer tracer, boolean traceWithActiveSpanOnly) {

    super(poolConfig, host, port, connectionTimeout, soTimeout, password, database, clientName, ssl,
        sslSocketFactory, sslParameters, hostnameVerifier);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    this.spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port,
      final int connectionTimeout, final int soTimeout, final String password, final int database,
      final String clientName, final boolean ssl, final SSLSocketFactory sslSocketFactory,
      final SSLParameters sslParameters, final HostnameVerifier hostnameVerifier,
      boolean traceWithActiveSpanOnly) {
    this(poolConfig, host, port, connectionTimeout, soTimeout, password, database, clientName, ssl,
        sslSocketFactory, sslParameters, hostnameVerifier, GlobalTracer.get(),
        traceWithActiveSpanOnly);
  }

  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port,
      final int connectionTimeout, final int soTimeout, final String password, final int database,
      final String clientName, final boolean ssl, final SSLSocketFactory sslSocketFactory,
      final SSLParameters sslParameters, final HostnameVerifier hostnameVerifier,
      Tracer tracer, boolean traceWithActiveSpanOnly, Function<String, String> customSpanName) {

    super(poolConfig, host, port, connectionTimeout, soTimeout, password, database, clientName, ssl,
        sslSocketFactory, sslParameters, hostnameVerifier);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    this.spanNameProvider = customSpanName;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port,
      final int connectionTimeout, final int soTimeout, final String password, final int database,
      final String clientName, final boolean ssl, final SSLSocketFactory sslSocketFactory,
      final SSLParameters sslParameters, final HostnameVerifier hostnameVerifier,
      boolean traceWithActiveSpanOnly, Function<String, String> customSpanName) {
    this(poolConfig, host, port, connectionTimeout, soTimeout, password, database, clientName,
        ssl, sslSocketFactory, sslParameters, hostnameVerifier, GlobalTracer.get(),
        traceWithActiveSpanOnly, customSpanName);
  }

  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final URI uri,
      Tracer tracer, boolean traceWithActiveSpanOnly) {
    super(poolConfig, uri);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final URI uri,
      boolean traceWithActiveSpanOnly) {
    this(poolConfig, uri, GlobalTracer.get(), traceWithActiveSpanOnly);
  }

  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final URI uri,
      final SSLSocketFactory sslSocketFactory,
      final SSLParameters sslParameters, final HostnameVerifier hostnameVerifier,
      Tracer tracer, boolean traceWithActiveSpanOnly) {
    super(poolConfig, uri, sslSocketFactory, sslParameters, hostnameVerifier);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final URI uri,
      final SSLSocketFactory sslSocketFactory,
      final SSLParameters sslParameters, final HostnameVerifier hostnameVerifier,
      boolean traceWithActiveSpanOnly) {
    this(poolConfig, uri, sslSocketFactory, sslParameters, hostnameVerifier,
        GlobalTracer.get(), traceWithActiveSpanOnly);
  }

  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final URI uri,
      final int timeout,
      Tracer tracer, boolean traceWithActiveSpanOnly) {
    super(poolConfig, uri, timeout);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final URI uri,
      final int timeout,
      boolean traceWithActiveSpanOnly) {
    this(poolConfig, uri, timeout, GlobalTracer.get(), traceWithActiveSpanOnly);
  }

  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final URI uri,
      final int timeout,
      final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier, Tracer tracer, boolean traceWithActiveSpanOnly) {
    super(poolConfig, uri, timeout, sslSocketFactory, sslParameters, hostnameVerifier);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final URI uri,
      final int timeout,
      final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier, boolean traceWithActiveSpanOnly) {
    this(poolConfig, uri, timeout, sslSocketFactory, sslParameters, hostnameVerifier,
        GlobalTracer.get(), traceWithActiveSpanOnly);
  }

  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final URI uri,
      final int connectionTimeout,
      final int soTimeout, Tracer tracer, boolean traceWithActiveSpanOnly) {
    super(poolConfig, uri, connectionTimeout, soTimeout);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final URI uri,
      final int connectionTimeout,
      final int soTimeout, boolean traceWithActiveSpanOnly) {
    this(poolConfig, uri, connectionTimeout, soTimeout, GlobalTracer.get(),
        traceWithActiveSpanOnly);
  }

  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final URI uri,
      final int connectionTimeout,
      final int soTimeout, final SSLSocketFactory sslSocketFactory,
      final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier, Tracer tracer, boolean traceWithActiveSpanOnly) {
    super(poolConfig, uri, connectionTimeout, soTimeout, sslSocketFactory, sslParameters,
        hostnameVerifier);
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final URI uri,
      final int connectionTimeout,
      final int soTimeout, final SSLSocketFactory sslSocketFactory,
      final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier, boolean traceWithActiveSpanOnly) {
    this(poolConfig, uri, connectionTimeout, soTimeout, sslSocketFactory, sslParameters,
        hostnameVerifier, GlobalTracer.get(), traceWithActiveSpanOnly);
  }

  @Override
  public Jedis getResource() {
    Jedis resource = super.getResource();

    return new TracingJedisWrapper(resource, tracer, traceWithActiveSpanOnly);
  }

  /**
   * @deprecated See {@link redis.clients.jedis.JedisPool#returnBrokenResource}
   */
  @Override
  @Deprecated
  public void returnBrokenResource(final Jedis resource) {
    super.returnBrokenResource(unwrapResource(resource));
  }

  /**
   * @deprecated See {@link redis.clients.jedis.JedisPool#returnResource}
   */
  @Override
  @Deprecated
  public void returnResource(final Jedis resource) {
    super.returnResource(unwrapResource(resource));
  }

  /**
   * @deprecated See {@link redis.clients.util.Pool#returnResourceObject}
   */
  @Override
  @Deprecated
  public void returnResourceObject(final Jedis resource) {
    super.returnResourceObject(unwrapResource(resource));
  }

  private Jedis unwrapResource(Jedis resource) {
    return (resource instanceof TracingJedisWrapper)
        ? ((TracingJedisWrapper) resource).getWrapped()
        : resource;
  }

  /**
   * TracingJedisWrapper wraps Jedis object, usually at the moment of extraction from the Pool.
   * Used to provide tracing capabilities to redis commands executed by the client provided by
   * given Jedis object.
   */
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
