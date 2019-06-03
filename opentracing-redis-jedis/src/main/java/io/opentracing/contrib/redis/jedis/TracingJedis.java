/*
 * Copyright 2017-2019 The OpenTracing Authors
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

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;
import java.net.URI;

import io.opentracing.contrib.redis.common.TracingConfiguration;
import io.opentracing.util.GlobalTracer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.TracingJedisWrapper;

public class TracingJedis extends TracingJedisWrapper {

    public TracingJedis() {
        this(new TracingConfiguration.Builder(GlobalTracer.get()).build());
    }

    public TracingJedis(TracingConfiguration tracingConfiguration) {
        super(tracingConfiguration);
    }

    public TracingJedis(final String host, TracingConfiguration tracingConfiguration) {
        super(new Jedis(host), tracingConfiguration);
    }

    public TracingJedis(final String host, final int port,
                        TracingConfiguration tracingConfiguration) {
        super(new Jedis(host, port), tracingConfiguration);
    }

    public TracingJedis(final String host, final int port, final boolean ssl,
                        TracingConfiguration tracingConfiguration) {
        super(new Jedis(host, port, ssl), tracingConfiguration);
    }

    public TracingJedis(final String host, final int port, final boolean ssl,
                        final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
                        final HostnameVerifier hostnameVerifier, TracingConfiguration tracingConfiguration) {
        super(new Jedis(host, port, ssl, sslSocketFactory, sslParameters, hostnameVerifier), tracingConfiguration);
    }

    public TracingJedis(final String host, final int port, final int timeout,
                        TracingConfiguration tracingConfiguration) {
        super(new Jedis(host, port, timeout), tracingConfiguration);
    }

    public TracingJedis(final String host, final int port, final int timeout, final boolean ssl,
                        TracingConfiguration tracingConfiguration) {
        super(new Jedis(host, port, timeout, ssl), tracingConfiguration);
    }

    public TracingJedis(final String host, final int port, final int timeout, final boolean ssl,
                        final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
                        final HostnameVerifier hostnameVerifier, TracingConfiguration tracingConfiguration) {
        super(new Jedis(host, port, timeout, ssl, sslSocketFactory, sslParameters, hostnameVerifier), tracingConfiguration);
    }

    public TracingJedis(final String host, final int port, final int connectionTimeout,
                        final int soTimeout, TracingConfiguration tracingConfiguration) {
        super(new Jedis(host, port, connectionTimeout, soTimeout), tracingConfiguration);
    }

    public TracingJedis(final String host, final int port, final int connectionTimeout,
                        final int soTimeout,
                        final boolean ssl, TracingConfiguration tracingConfiguration) {
        super(new Jedis(host, port, connectionTimeout, soTimeout, ssl), tracingConfiguration);
    }

    public TracingJedis(final String host, final int port, final int connectionTimeout,
                        final int soTimeout,
                        final boolean ssl, final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
                        final HostnameVerifier hostnameVerifier, TracingConfiguration tracingConfiguration) {
        super(new Jedis(host, port, connectionTimeout, soTimeout, ssl, sslSocketFactory, sslParameters,
                hostnameVerifier), tracingConfiguration);
    }

    public TracingJedis(JedisShardInfo shardInfo, TracingConfiguration tracingConfiguration) {
        super(new Jedis(shardInfo), tracingConfiguration);
    }

    public TracingJedis(URI uri, TracingConfiguration tracingConfiguration) {
        super(new Jedis(uri), tracingConfiguration);
    }

    public TracingJedis(URI uri, final SSLSocketFactory sslSocketFactory,
                        final SSLParameters sslParameters,
                        final HostnameVerifier hostnameVerifier, TracingConfiguration tracingConfiguration) {
        super(new Jedis(uri, sslSocketFactory, sslParameters, hostnameVerifier), tracingConfiguration);
    }

    public TracingJedis(final URI uri, final int timeout, TracingConfiguration tracingConfiguration) {
        super(new Jedis(uri, timeout), tracingConfiguration);
    }

    public TracingJedis(final URI uri, final int timeout, final SSLSocketFactory sslSocketFactory,
                        final SSLParameters sslParameters, final HostnameVerifier hostnameVerifier,
                        TracingConfiguration tracingConfiguration) {
        super(new Jedis(uri, timeout, sslSocketFactory, sslParameters, hostnameVerifier), tracingConfiguration);
    }

    public TracingJedis(final URI uri, final int connectionTimeout, final int soTimeout,
                        TracingConfiguration tracingConfiguration) {
        super(new Jedis(uri, connectionTimeout, soTimeout), tracingConfiguration);
    }

    public TracingJedis(final URI uri, final int connectionTimeout, final int soTimeout,
                        final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
                        final HostnameVerifier hostnameVerifier, TracingConfiguration tracingConfiguration) {
        super(new Jedis(uri, connectionTimeout, soTimeout, sslSocketFactory, sslParameters, hostnameVerifier), tracingConfiguration);
    }
}
