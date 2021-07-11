/*
 * Copyright 2017-2020 The OpenTracing Authors
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
package io.opentracing.contrib.redis.jedis3;

import io.opentracing.contrib.redis.common.TracingConfiguration;
import java.net.URI;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSocketFactory;
import redis.clients.jedis.TracingJedisWrapper;

public class TracingJedisPool extends JedisPool {
  private final TracingConfiguration tracingConfiguration;

  public TracingJedisPool(TracingConfiguration tracingConfiguration) {
    super();
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(GenericObjectPoolConfig<Jedis> poolConfig, String host,
      TracingConfiguration tracingConfiguration) {
    super(poolConfig, host);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(String host, int port, TracingConfiguration tracingConfiguration) {
    super(host, port);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(String url, TracingConfiguration tracingConfiguration) {
    super(url);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(String url, SSLSocketFactory sslSocketFactory,
      SSLParameters sslParameters, HostnameVerifier hostnameVerifier,
      TracingConfiguration tracingConfiguration) {
    super(url, sslSocketFactory, sslParameters, hostnameVerifier);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(URI uri, TracingConfiguration tracingConfiguration) {
    super(uri);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(URI uri, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters,
      HostnameVerifier hostnameVerifier, TracingConfiguration tracingConfiguration) {
    super(uri, sslSocketFactory, sslParameters, hostnameVerifier);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(URI uri, int timeout, TracingConfiguration tracingConfiguration) {
    super(uri, timeout);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(URI uri, int timeout, SSLSocketFactory sslSocketFactory,
      SSLParameters sslParameters, HostnameVerifier hostnameVerifier,
      TracingConfiguration tracingConfiguration) {
    super(uri, timeout, sslSocketFactory, sslParameters, hostnameVerifier);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(GenericObjectPoolConfig<Jedis> poolConfig, String host, int port,
      int timeout, String password, TracingConfiguration tracingConfiguration) {
    super(poolConfig, host, port, timeout, password);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(String host, int port, String user, String password,
      TracingConfiguration tracingConfiguration) {
    super(host, port, user, password);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(GenericObjectPoolConfig<Jedis> poolConfig, String host, int port,
      String user, String password, TracingConfiguration tracingConfiguration) {
    super(poolConfig, host, port, user, password);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(GenericObjectPoolConfig<Jedis> poolConfig, String host, int port,
      int timeout, String user, String password, TracingConfiguration tracingConfiguration) {
    super(poolConfig, host, port, timeout, user, password);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(GenericObjectPoolConfig<Jedis> poolConfig, String host, int port,
      int timeout, String password, boolean ssl, TracingConfiguration tracingConfiguration) {
    super(poolConfig, host, port, timeout, password, ssl);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(GenericObjectPoolConfig<Jedis> poolConfig, String host, int port,
      int timeout, String user, String password, boolean ssl,
      TracingConfiguration tracingConfiguration) {
    super(poolConfig, host, port, timeout, user, password, ssl);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(GenericObjectPoolConfig<Jedis> poolConfig, String host, int port,
      int timeout, String password, boolean ssl, SSLSocketFactory sslSocketFactory,
      SSLParameters sslParameters, HostnameVerifier hostnameVerifier,
      TracingConfiguration tracingConfiguration) {
    super(poolConfig, host, port, timeout, password, ssl, sslSocketFactory, sslParameters,
        hostnameVerifier);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(GenericObjectPoolConfig<Jedis> poolConfig, String host, int port,
      TracingConfiguration tracingConfiguration) {
    super(poolConfig, host, port);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(GenericObjectPoolConfig<Jedis> poolConfig, String host, int port,
      boolean ssl, TracingConfiguration tracingConfiguration) {
    super(poolConfig, host, port, ssl);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(GenericObjectPoolConfig<Jedis> poolConfig, String host, int port,
      boolean ssl, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters,
      HostnameVerifier hostnameVerifier, TracingConfiguration tracingConfiguration) {
    super(poolConfig, host, port, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(GenericObjectPoolConfig<Jedis> poolConfig, String host, int port,
      int timeout, TracingConfiguration tracingConfiguration) {
    super(poolConfig, host, port, timeout);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(GenericObjectPoolConfig<Jedis> poolConfig, String host, int port,
      int timeout, boolean ssl, TracingConfiguration tracingConfiguration) {
    super(poolConfig, host, port, timeout, ssl);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(GenericObjectPoolConfig<Jedis> poolConfig, String host, int port,
      int timeout, boolean ssl, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters,
      HostnameVerifier hostnameVerifier, TracingConfiguration tracingConfiguration) {
    super(poolConfig, host, port, timeout, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(GenericObjectPoolConfig<Jedis> poolConfig, String host, int port,
      int timeout, String password, int database, TracingConfiguration tracingConfiguration) {
    super(poolConfig, host, port, timeout, password, database);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(GenericObjectPoolConfig<Jedis> poolConfig, String host, int port,
      int timeout, String user, String password, int database,
      TracingConfiguration tracingConfiguration) {
    super(poolConfig, host, port, timeout, user, password, database);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(GenericObjectPoolConfig<Jedis> poolConfig, String host, int port,
      int timeout, String password, int database, boolean ssl,
      TracingConfiguration tracingConfiguration) {
    super(poolConfig, host, port, timeout, password, database, ssl);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(GenericObjectPoolConfig<Jedis> poolConfig, String host, int port,
      int timeout, String user, String password, int database, boolean ssl,
      TracingConfiguration tracingConfiguration) {
    super(poolConfig, host, port, timeout, user, password, database, ssl);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(GenericObjectPoolConfig<Jedis> poolConfig, String host, int port,
      int timeout, String password, int database, boolean ssl, SSLSocketFactory sslSocketFactory,
      SSLParameters sslParameters, HostnameVerifier hostnameVerifier,
      TracingConfiguration tracingConfiguration) {
    super(poolConfig, host, port, timeout, password, database, ssl, sslSocketFactory, sslParameters,
        hostnameVerifier);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(GenericObjectPoolConfig<Jedis> poolConfig, String host, int port,
      int timeout, String password, int database, String clientName,
      TracingConfiguration tracingConfiguration) {
    super(poolConfig, host, port, timeout, password, database, clientName);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(GenericObjectPoolConfig<Jedis> poolConfig, String host, int port,
      int timeout, String user, String password, int database, String clientName,
      TracingConfiguration tracingConfiguration) {
    super(poolConfig, host, port, timeout, user, password, database, clientName);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(GenericObjectPoolConfig<Jedis> poolConfig, String host, int port,
      int timeout, String password, int database, String clientName, boolean ssl,
      TracingConfiguration tracingConfiguration) {
    super(poolConfig, host, port, timeout, password, database, clientName, ssl);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(GenericObjectPoolConfig<Jedis> poolConfig, String host, int port,
      int timeout, String user, String password, int database, String clientName, boolean ssl,
      TracingConfiguration tracingConfiguration) {
    super(poolConfig, host, port, timeout, user, password, database, clientName, ssl);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(GenericObjectPoolConfig<Jedis> poolConfig, String host, int port,
      int timeout, String password, int database, String clientName, boolean ssl,
      SSLSocketFactory sslSocketFactory, SSLParameters sslParameters,
      HostnameVerifier hostnameVerifier, TracingConfiguration tracingConfiguration) {
    super(poolConfig, host, port, timeout, password, database, clientName, ssl, sslSocketFactory,
        sslParameters, hostnameVerifier);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(GenericObjectPoolConfig<Jedis> poolConfig, String host, int port,
      int connectionTimeout, int soTimeout, String password, int database, String clientName,
      boolean ssl, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters,
      HostnameVerifier hostnameVerifier, TracingConfiguration tracingConfiguration) {
    super(poolConfig, host, port, connectionTimeout, soTimeout, password, database, clientName, ssl,
        sslSocketFactory, sslParameters, hostnameVerifier);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(GenericObjectPoolConfig<Jedis> poolConfig, String host, int port,
      int connectionTimeout, int soTimeout, int infiniteSoTimeout, String password, int database,
      String clientName, boolean ssl, SSLSocketFactory sslSocketFactory,
      SSLParameters sslParameters, HostnameVerifier hostnameVerifier,
      TracingConfiguration tracingConfiguration) {
    super(poolConfig, host, port, connectionTimeout, soTimeout, infiniteSoTimeout, password,
        database, clientName, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(GenericObjectPoolConfig<Jedis> poolConfig, String host, int port,
      int connectionTimeout, int soTimeout, String user, String password, int database,
      String clientName, boolean ssl, SSLSocketFactory sslSocketFactory,
      SSLParameters sslParameters, HostnameVerifier hostnameVerifier,
      TracingConfiguration tracingConfiguration) {
    super(poolConfig, host, port, connectionTimeout, soTimeout, user, password, database,
        clientName, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(GenericObjectPoolConfig<Jedis> poolConfig, String host, int port,
      int connectionTimeout, int soTimeout, int infiniteSoTimeout, String user, String password,
      int database, String clientName, boolean ssl, SSLSocketFactory sslSocketFactory,
      SSLParameters sslParameters, HostnameVerifier hostnameVerifier,
      TracingConfiguration tracingConfiguration) {
    super(poolConfig, host, port, connectionTimeout, soTimeout, infiniteSoTimeout, user, password,
        database, clientName, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(GenericObjectPoolConfig<Jedis> poolConfig, HostAndPort hostAndPort,
      JedisClientConfig clientConfig, TracingConfiguration tracingConfiguration) {
    super(poolConfig, hostAndPort, clientConfig);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(GenericObjectPoolConfig<Jedis> poolConfig,
      JedisSocketFactory jedisSocketFactory, JedisClientConfig clientConfig,
      TracingConfiguration tracingConfiguration) {
    super(poolConfig, jedisSocketFactory, clientConfig);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(GenericObjectPoolConfig<Jedis> poolConfig,
      TracingConfiguration tracingConfiguration) {
    super(poolConfig);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(String host, int port, boolean ssl,
      TracingConfiguration tracingConfiguration) {
    super(host, port, ssl);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(GenericObjectPoolConfig<Jedis> poolConfig, String host, int port,
      int connectionTimeout, int soTimeout, String password, int database, String clientName,
      TracingConfiguration tracingConfiguration) {
    super(poolConfig, host, port, connectionTimeout, soTimeout, password, database, clientName);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(GenericObjectPoolConfig<Jedis> poolConfig, String host, int port,
      int connectionTimeout, int soTimeout, String user, String password, int database,
      String clientName, TracingConfiguration tracingConfiguration) {
    super(poolConfig, host, port, connectionTimeout, soTimeout, user, password, database,
        clientName);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(GenericObjectPoolConfig<Jedis> poolConfig, String host, int port,
      int connectionTimeout, int soTimeout, int infiniteSoTimeout, String user, String password,
      int database, String clientName, TracingConfiguration tracingConfiguration) {
    super(poolConfig, host, port, connectionTimeout, soTimeout, infiniteSoTimeout, user, password,
        database, clientName);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(String host, int port, boolean ssl, SSLSocketFactory sslSocketFactory,
      SSLParameters sslParameters, HostnameVerifier hostnameVerifier,
      TracingConfiguration tracingConfiguration) {
    super(host, port, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(GenericObjectPoolConfig<Jedis> poolConfig, String host, int port,
      int connectionTimeout, int soTimeout, String password, int database, String clientName,
      boolean ssl, TracingConfiguration tracingConfiguration) {
    super(poolConfig, host, port, connectionTimeout, soTimeout, password, database, clientName,
        ssl);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(GenericObjectPoolConfig<Jedis> poolConfig, String host, int port,
      int connectionTimeout, int soTimeout, String user, String password, int database,
      String clientName, boolean ssl, TracingConfiguration tracingConfiguration) {
    super(poolConfig, host, port, connectionTimeout, soTimeout, user, password, database,
        clientName, ssl);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(GenericObjectPoolConfig<Jedis> poolConfig, URI uri,
      TracingConfiguration tracingConfiguration) {
    super(poolConfig, uri);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(GenericObjectPoolConfig<Jedis> poolConfig, URI uri,
      SSLSocketFactory sslSocketFactory, SSLParameters sslParameters,
      HostnameVerifier hostnameVerifier, TracingConfiguration tracingConfiguration) {
    super(poolConfig, uri, sslSocketFactory, sslParameters, hostnameVerifier);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(GenericObjectPoolConfig<Jedis> poolConfig, URI uri, int timeout,
      TracingConfiguration tracingConfiguration) {
    super(poolConfig, uri, timeout);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(GenericObjectPoolConfig<Jedis> poolConfig, URI uri, int timeout,
      SSLSocketFactory sslSocketFactory, SSLParameters sslParameters,
      HostnameVerifier hostnameVerifier, TracingConfiguration tracingConfiguration) {
    super(poolConfig, uri, timeout, sslSocketFactory, sslParameters, hostnameVerifier);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(GenericObjectPoolConfig<Jedis> poolConfig, URI uri, int connectionTimeout,
      int soTimeout, TracingConfiguration tracingConfiguration) {
    super(poolConfig, uri, connectionTimeout, soTimeout);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(GenericObjectPoolConfig<Jedis> poolConfig, URI uri, int connectionTimeout,
      int soTimeout, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters,
      HostnameVerifier hostnameVerifier, TracingConfiguration tracingConfiguration) {
    super(poolConfig, uri, connectionTimeout, soTimeout, sslSocketFactory, sslParameters,
        hostnameVerifier);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(GenericObjectPoolConfig<Jedis> poolConfig, URI uri, int connectionTimeout,
      int soTimeout, int infiniteSoTimeout, SSLSocketFactory sslSocketFactory,
      SSLParameters sslParameters, HostnameVerifier hostnameVerifier,
      TracingConfiguration tracingConfiguration) {
    super(poolConfig, uri, connectionTimeout, soTimeout, infiniteSoTimeout, sslSocketFactory,
        sslParameters, hostnameVerifier);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisPool(GenericObjectPoolConfig poolConfig, PooledObjectFactory<Jedis> factory,
      TracingConfiguration tracingConfiguration) {
    super(poolConfig, factory);
    this.tracingConfiguration = tracingConfiguration;
  }

  @Override
  public Jedis getResource() {
    Jedis resource = super.getResource();
    return new TracingJedisWrapper(resource, tracingConfiguration);
  }

  @Override
  public void returnBrokenResource(final Jedis resource) {
    super.returnBrokenResource(unwrapResource(resource));
  }

  @Override
  public void returnResource(final Jedis resource) {
    super.returnResource(unwrapResource(resource));
  }

  @Override
  public void returnResourceObject(final Jedis resource) {
    super.returnResourceObject(unwrapResource(resource));
  }

  private Jedis unwrapResource(Jedis resource) {
    return (resource instanceof TracingJedisWrapper)
        ? ((TracingJedisWrapper) resource).getWrapped()
        : resource;
  }
}
