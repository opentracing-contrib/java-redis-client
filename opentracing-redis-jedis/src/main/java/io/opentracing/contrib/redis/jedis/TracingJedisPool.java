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

import io.opentracing.contrib.redis.common.TracingConfiguration;
import java.net.URI;
import java.util.function.Function;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class TracingJedisPool extends JedisPool {
  private final TracingConfiguration tracingConfiguration;

  public TracingJedisPool(TracingConfiguration tracingConfiguration) {
    super();
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(TracingConfiguration tracingConfiguration,
      Function<String, String> spanNameProvider) {
    super();
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      TracingConfiguration tracingConfiguration) {
    super(poolConfig, host);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(final String host, final int port,
      TracingConfiguration tracingConfiguration) {
    super(host, port);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(final String host, TracingConfiguration tracingConfiguration) {
    super(host);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(final String host, final SSLSocketFactory sslSocketFactory,
      final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier, TracingConfiguration tracingConfiguration) {
    super(host, sslSocketFactory, sslParameters, hostnameVerifier);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(final URI uri, TracingConfiguration tracingConfiguration) {
    super(uri);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(final URI uri, final SSLSocketFactory sslSocketFactory,
      final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier, TracingConfiguration tracingConfiguration) {
    super(uri, sslSocketFactory, sslParameters, hostnameVerifier);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(final URI uri, final int timeout,
      TracingConfiguration tracingConfiguration) {
    super(uri, timeout);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(final URI uri, final int timeout, final SSLSocketFactory sslSocketFactory,
      final SSLParameters sslParameters, final HostnameVerifier hostnameVerifier,
      TracingConfiguration tracingConfiguration) {
    super(uri, timeout, sslSocketFactory, sslParameters, hostnameVerifier);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port,
      final int timeout, final String password,
      TracingConfiguration tracingConfiguration) {
    super(poolConfig, host, port, timeout, password);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port,
      final int timeout, final String password, final boolean ssl,
      TracingConfiguration tracingConfiguration) {
    super(poolConfig, host, port, timeout, password, ssl);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port,
      final int timeout, final String password, final boolean ssl,
      final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier, TracingConfiguration tracingConfiguration) {
    super(poolConfig, host, port, timeout, password, ssl, sslSocketFactory, sslParameters,
        hostnameVerifier);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port, TracingConfiguration tracingConfiguration) {
    super(poolConfig, host, port);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port, final boolean ssl,
      TracingConfiguration tracingConfiguration) {
    super(poolConfig, host, port, ssl);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port, final boolean ssl,
      final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier, TracingConfiguration tracingConfiguration) {
    super(poolConfig, host, port, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port, final int timeout,
      TracingConfiguration tracingConfiguration) {
    super(poolConfig, host, port, timeout);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port, final int timeout,
      final boolean ssl, TracingConfiguration tracingConfiguration) {
    super(poolConfig, host, port, timeout, ssl);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port, final int timeout,
      final boolean ssl, final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier, TracingConfiguration tracingConfiguration) {
    super(poolConfig, host, port, timeout, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port, final int timeout,
      final String password, final int database, TracingConfiguration tracingConfiguration) {
    super(poolConfig, host, port, timeout, password, database);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port, final int timeout,
      final String password, final int database, final boolean ssl,
      TracingConfiguration tracingConfiguration) {
    super(poolConfig, host, port, timeout, password, database, ssl);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port, final int timeout,
      final String password, final int database, final boolean ssl,
      final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier, TracingConfiguration tracingConfiguration) {
    super(poolConfig, host, port, timeout, password, database, ssl, sslSocketFactory, sslParameters,
        hostnameVerifier);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port, final int timeout,
      final String password, final int database, final String clientName,
      TracingConfiguration tracingConfiguration) {
    this(poolConfig, host, port, timeout, timeout, password, database, clientName, false,
        null, null, null, tracingConfiguration);
  }

  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port, final int timeout,
      final String password, final int database, final String clientName, final boolean ssl,
      TracingConfiguration tracingConfiguration) {
    super(poolConfig, host, port, timeout, password, database, clientName, ssl);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port, final int timeout,
      final String password, final int database, final String clientName, final boolean ssl,
      final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier, TracingConfiguration tracingConfiguration) {
    super(poolConfig, host, port, timeout, password, database, clientName, ssl, sslSocketFactory,
        sslParameters, hostnameVerifier);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final String host,
      final int port,
      final int connectionTimeout, final int soTimeout, final String password, final int database,
      final String clientName, final boolean ssl, final SSLSocketFactory sslSocketFactory,
      final SSLParameters sslParameters, final HostnameVerifier hostnameVerifier,
      TracingConfiguration tracingConfiguration) {

    super(poolConfig, host, port, connectionTimeout, soTimeout, password, database, clientName, ssl,
        sslSocketFactory, sslParameters, hostnameVerifier);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final URI uri,
      TracingConfiguration tracingConfiguration) {
    super(poolConfig, uri);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final URI uri,
      final SSLSocketFactory sslSocketFactory,
      final SSLParameters sslParameters, final HostnameVerifier hostnameVerifier,
      TracingConfiguration tracingConfiguration) {
    super(poolConfig, uri, sslSocketFactory, sslParameters, hostnameVerifier);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final URI uri,
      final int timeout,
      TracingConfiguration tracingConfiguration) {
    super(poolConfig, uri, timeout);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final URI uri,
      final int timeout,
      final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier, TracingConfiguration tracingConfiguration) {
    super(poolConfig, uri, timeout, sslSocketFactory, sslParameters, hostnameVerifier);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final URI uri,
      final int connectionTimeout,
      final int soTimeout, TracingConfiguration tracingConfiguration) {
    super(poolConfig, uri, connectionTimeout, soTimeout);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(final GenericObjectPoolConfig poolConfig, final URI uri,
      final int connectionTimeout,
      final int soTimeout, final SSLSocketFactory sslSocketFactory,
      final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier, TracingConfiguration tracingConfiguration) {
    super(poolConfig, uri, connectionTimeout, soTimeout, sslSocketFactory, sslParameters,
        hostnameVerifier);
    this.tracingConfiguration = tracingConfiguration;
  }

  @Override
  public Jedis getResource() {
    Jedis resource = super.getResource();

    return new TracingJedisWrapper(resource, tracingConfiguration);
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

    public TracingJedisWrapper(Jedis jedis, TracingConfiguration tracingConfiguration) {
      super(tracingConfiguration);
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
