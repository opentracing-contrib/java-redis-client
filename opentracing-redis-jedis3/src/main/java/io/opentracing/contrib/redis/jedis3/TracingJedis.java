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
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.JedisSocketFactory;
import redis.clients.jedis.TracingJedisWrapper;

public class TracingJedis extends TracingJedisWrapper {

  public TracingJedis(TracingConfiguration tracingConfiguration) {
    super(new Jedis(), tracingConfiguration);
  }

  public TracingJedis(String uri, TracingConfiguration tracingConfiguration) {
    super(new Jedis(uri), tracingConfiguration);
  }

  public TracingJedis(HostAndPort hp, TracingConfiguration tracingConfiguration) {
    super(new Jedis(hp), tracingConfiguration);
  }

  public TracingJedis(HostAndPort hp, JedisClientConfig config,
      TracingConfiguration tracingConfiguration) {
    super(new Jedis(hp, config), tracingConfiguration);
  }

  public TracingJedis(String host, int port, TracingConfiguration tracingConfiguration) {
    super(new Jedis(host, port), tracingConfiguration);
  }

  public TracingJedis(String host, int port, boolean ssl,
      TracingConfiguration tracingConfiguration) {
    super(new Jedis(host, port, ssl), tracingConfiguration);
  }

  public TracingJedis(String host, int port, boolean ssl, SSLSocketFactory sslSocketFactory,
      SSLParameters sslParameters, HostnameVerifier hostnameVerifier,
      TracingConfiguration tracingConfiguration) {
    super(new Jedis(host, port, ssl, sslSocketFactory, sslParameters, hostnameVerifier),
        tracingConfiguration);
  }

  public TracingJedis(String host, int port, int timeout,
      TracingConfiguration tracingConfiguration) {
    super(new Jedis(host, port, timeout), tracingConfiguration);
  }

  public TracingJedis(String host, int port, int timeout, boolean ssl,
      TracingConfiguration tracingConfiguration) {
    super(new Jedis(host, port, timeout, ssl), tracingConfiguration);
  }

  public TracingJedis(String host, int port, int timeout, boolean ssl,
      SSLSocketFactory sslSocketFactory, SSLParameters sslParameters,
      HostnameVerifier hostnameVerifier, TracingConfiguration tracingConfiguration) {
    super(new Jedis(host, port, timeout, ssl, sslSocketFactory, sslParameters, hostnameVerifier),
        tracingConfiguration);
  }

  public TracingJedis(String host, int port, int connectionTimeout, int soTimeout,
      TracingConfiguration tracingConfiguration) {
    super(new Jedis(host, port, connectionTimeout, soTimeout), tracingConfiguration);
  }

  public TracingJedis(String host, int port, int connectionTimeout, int soTimeout,
      int infiniteSoTimeout, TracingConfiguration tracingConfiguration) {
    super(new Jedis(host, port, connectionTimeout, soTimeout, infiniteSoTimeout),
        tracingConfiguration);
  }

  public TracingJedis(String host, int port, int connectionTimeout, int soTimeout, boolean ssl,
      TracingConfiguration tracingConfiguration) {
    super(new Jedis(host, port, connectionTimeout, soTimeout, ssl), tracingConfiguration);
  }

  public TracingJedis(String host, int port, int connectionTimeout, int soTimeout, boolean ssl,
      SSLSocketFactory sslSocketFactory, SSLParameters sslParameters,
      HostnameVerifier hostnameVerifier, TracingConfiguration tracingConfiguration) {
    super(new Jedis(host, port, connectionTimeout, soTimeout, ssl, sslSocketFactory, sslParameters,
        hostnameVerifier), tracingConfiguration);
  }

  public TracingJedis(String host, int port, int connectionTimeout, int soTimeout,
      int infiniteSoTimeout, boolean ssl, SSLSocketFactory sslSocketFactory,
      SSLParameters sslParameters, HostnameVerifier hostnameVerifier,
      TracingConfiguration tracingConfiguration) {
    super(new Jedis(host, port, connectionTimeout, soTimeout, infiniteSoTimeout, ssl,
        sslSocketFactory, sslParameters, hostnameVerifier), tracingConfiguration);
  }

  public TracingJedis(JedisShardInfo shardInfo, TracingConfiguration tracingConfiguration) {
    super(new Jedis(shardInfo), tracingConfiguration);
  }

  public TracingJedis(URI uri, TracingConfiguration tracingConfiguration) {
    super(new Jedis(uri), tracingConfiguration);
  }

  public TracingJedis(URI uri, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters,
      HostnameVerifier hostnameVerifier, TracingConfiguration tracingConfiguration) {
    super(new Jedis(uri, sslSocketFactory, sslParameters, hostnameVerifier), tracingConfiguration);
  }

  public TracingJedis(URI uri, int timeout, TracingConfiguration tracingConfiguration) {
    super(new Jedis(uri, timeout), tracingConfiguration);
  }

  public TracingJedis(URI uri, int timeout, SSLSocketFactory sslSocketFactory,
      SSLParameters sslParameters, HostnameVerifier hostnameVerifier,
      TracingConfiguration tracingConfiguration) {
    super(new Jedis(uri, timeout, sslSocketFactory, sslParameters, hostnameVerifier),
        tracingConfiguration);
  }

  public TracingJedis(URI uri, int connectionTimeout, int soTimeout,
      TracingConfiguration tracingConfiguration) {
    super(new Jedis(uri, connectionTimeout, soTimeout), tracingConfiguration);
  }

  public TracingJedis(URI uri, int connectionTimeout, int soTimeout,
      SSLSocketFactory sslSocketFactory, SSLParameters sslParameters,
      HostnameVerifier hostnameVerifier, TracingConfiguration tracingConfiguration) {
    super(new Jedis(uri, connectionTimeout, soTimeout, sslSocketFactory, sslParameters,
        hostnameVerifier), tracingConfiguration);
  }

  public TracingJedis(URI uri, int connectionTimeout, int soTimeout, int infiniteSoTimeout,
      SSLSocketFactory sslSocketFactory, SSLParameters sslParameters,
      HostnameVerifier hostnameVerifier, TracingConfiguration tracingConfiguration) {
    super(new Jedis(uri, connectionTimeout, soTimeout, infiniteSoTimeout, sslSocketFactory,
        sslParameters, hostnameVerifier), tracingConfiguration);
  }

  public TracingJedis(URI uri, JedisClientConfig config,
      TracingConfiguration tracingConfiguration) {
    super(new Jedis(uri, config), tracingConfiguration);
  }

  public TracingJedis(JedisSocketFactory jedisSocketFactory,
      TracingConfiguration tracingConfiguration) {
    super(new Jedis(jedisSocketFactory), tracingConfiguration);
  }

  public TracingJedis(JedisSocketFactory jedisSocketFactory, JedisClientConfig clientConfig,
      TracingConfiguration tracingConfiguration) {
    super(new Jedis(jedisSocketFactory, clientConfig), tracingConfiguration);
  }
}
