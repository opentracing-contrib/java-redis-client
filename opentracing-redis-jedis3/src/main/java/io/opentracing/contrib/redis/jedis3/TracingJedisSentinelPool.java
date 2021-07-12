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
import java.util.Set;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.JedisFactory;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.TracingJedisWrapper;

public class TracingJedisSentinelPool extends JedisSentinelPool {

  private final TracingConfiguration tracingConfiguration;

  public TracingJedisSentinelPool(TracingConfiguration tracingConfiguration, String masterName,
      Set<String> sentinels, GenericObjectPoolConfig<Jedis> poolConfig) {
    super(masterName, sentinels, poolConfig);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisSentinelPool(TracingConfiguration tracingConfiguration, String masterName,
      Set<String> sentinels) {
    super(masterName, sentinels);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisSentinelPool(TracingConfiguration tracingConfiguration, String masterName,
      Set<String> sentinels, String password) {
    super(masterName, sentinels, password);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisSentinelPool(TracingConfiguration tracingConfiguration, String masterName,
      Set<String> sentinels, String password, String sentinelPassword) {
    super(masterName, sentinels, password, sentinelPassword);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisSentinelPool(TracingConfiguration tracingConfiguration, String masterName,
      Set<String> sentinels, GenericObjectPoolConfig<Jedis> poolConfig, int timeout,
      String password) {
    super(masterName, sentinels, poolConfig, timeout, password);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisSentinelPool(TracingConfiguration tracingConfiguration, String masterName,
      Set<String> sentinels, GenericObjectPoolConfig<Jedis> poolConfig, int timeout) {
    super(masterName, sentinels, poolConfig, timeout);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisSentinelPool(TracingConfiguration tracingConfiguration, String masterName,
      Set<String> sentinels, GenericObjectPoolConfig<Jedis> poolConfig, String password) {
    super(masterName, sentinels, poolConfig, password);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisSentinelPool(TracingConfiguration tracingConfiguration, String masterName,
      Set<String> sentinels, GenericObjectPoolConfig<Jedis> poolConfig, int timeout,
      String password, int database) {
    super(masterName, sentinels, poolConfig, timeout, password, database);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisSentinelPool(TracingConfiguration tracingConfiguration, String masterName,
      Set<String> sentinels, GenericObjectPoolConfig<Jedis> poolConfig, int timeout, String user,
      String password, int database) {
    super(masterName, sentinels, poolConfig, timeout, user, password, database);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisSentinelPool(TracingConfiguration tracingConfiguration, String masterName,
      Set<String> sentinels, GenericObjectPoolConfig<Jedis> poolConfig, int timeout,
      String password, int database, String clientName) {
    super(masterName, sentinels, poolConfig, timeout, password, database, clientName);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisSentinelPool(TracingConfiguration tracingConfiguration, String masterName,
      Set<String> sentinels, GenericObjectPoolConfig<Jedis> poolConfig, int timeout, String user,
      String password, int database, String clientName) {
    super(masterName, sentinels, poolConfig, timeout, user, password, database, clientName);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisSentinelPool(TracingConfiguration tracingConfiguration, String masterName,
      Set<String> sentinels, GenericObjectPoolConfig<Jedis> poolConfig, int connectionTimeout,
      int soTimeout, String password, int database) {
    super(masterName, sentinels, poolConfig, connectionTimeout, soTimeout, password, database);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisSentinelPool(TracingConfiguration tracingConfiguration, String masterName,
      Set<String> sentinels, GenericObjectPoolConfig<Jedis> poolConfig, int connectionTimeout,
      int soTimeout, String user, String password, int database) {
    super(masterName, sentinels, poolConfig, connectionTimeout, soTimeout, user, password,
        database);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisSentinelPool(TracingConfiguration tracingConfiguration, String masterName,
      Set<String> sentinels, GenericObjectPoolConfig<Jedis> poolConfig, int connectionTimeout,
      int soTimeout, String password, int database, String clientName) {
    super(masterName, sentinels, poolConfig, connectionTimeout, soTimeout, password, database,
        clientName);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisSentinelPool(TracingConfiguration tracingConfiguration, String masterName,
      Set<String> sentinels, GenericObjectPoolConfig<Jedis> poolConfig, int connectionTimeout,
      int soTimeout, String user, String password, int database, String clientName) {
    super(masterName, sentinels, poolConfig, connectionTimeout, soTimeout, user, password, database,
        clientName);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisSentinelPool(TracingConfiguration tracingConfiguration, String masterName,
      Set<String> sentinels, GenericObjectPoolConfig<Jedis> poolConfig, int connectionTimeout,
      int soTimeout, int infiniteSoTimeout, String user, String password, int database,
      String clientName) {
    super(masterName, sentinels, poolConfig, connectionTimeout, soTimeout, infiniteSoTimeout, user,
        password, database, clientName);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisSentinelPool(TracingConfiguration tracingConfiguration, String masterName,
      Set<String> sentinels, GenericObjectPoolConfig<Jedis> poolConfig, int connectionTimeout,
      int soTimeout, String password, int database, String clientName,
      int sentinelConnectionTimeout, int sentinelSoTimeout, String sentinelPassword,
      String sentinelClientName) {
    super(masterName, sentinels, poolConfig, connectionTimeout, soTimeout, password, database,
        clientName, sentinelConnectionTimeout, sentinelSoTimeout, sentinelPassword,
        sentinelClientName);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisSentinelPool(TracingConfiguration tracingConfiguration, String masterName,
      Set<String> sentinels, GenericObjectPoolConfig<Jedis> poolConfig, int connectionTimeout,
      int soTimeout, String user, String password, int database, String clientName,
      int sentinelConnectionTimeout, int sentinelSoTimeout, String sentinelUser,
      String sentinelPassword, String sentinelClientName) {
    super(masterName, sentinels, poolConfig, connectionTimeout, soTimeout, user, password, database,
        clientName, sentinelConnectionTimeout, sentinelSoTimeout, sentinelUser, sentinelPassword,
        sentinelClientName);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisSentinelPool(TracingConfiguration tracingConfiguration, String masterName,
      Set<String> sentinels, GenericObjectPoolConfig<Jedis> poolConfig, int connectionTimeout,
      int soTimeout, int infiniteSoTimeout, String user, String password, int database,
      String clientName, int sentinelConnectionTimeout, int sentinelSoTimeout, String sentinelUser,
      String sentinelPassword, String sentinelClientName) {
    super(masterName, sentinels, poolConfig, connectionTimeout, soTimeout, infiniteSoTimeout, user,
        password, database, clientName, sentinelConnectionTimeout, sentinelSoTimeout, sentinelUser,
        sentinelPassword, sentinelClientName);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisSentinelPool(TracingConfiguration tracingConfiguration, String masterName,
      Set<String> sentinels, GenericObjectPoolConfig<Jedis> poolConfig, JedisFactory factory) {
    super(masterName, sentinels, poolConfig, factory);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisSentinelPool(TracingConfiguration tracingConfiguration, String masterName,
      Set<HostAndPort> sentinels, GenericObjectPoolConfig<Jedis> poolConfig,
      JedisClientConfig masteClientConfig, JedisClientConfig sentinelClientConfig) {
    super(masterName, sentinels, poolConfig, masteClientConfig, sentinelClientConfig);
    this.tracingConfiguration = tracingConfiguration;
  }

  public TracingJedisSentinelPool(TracingConfiguration tracingConfiguration, String masterName,
      Set<HostAndPort> sentinels, GenericObjectPoolConfig<Jedis> poolConfig, JedisFactory factory,
      JedisClientConfig sentinelClientConfig) {
    super(masterName, sentinels, poolConfig, factory, sentinelClientConfig);
    this.tracingConfiguration = tracingConfiguration;
  }

  @Override
  public Jedis getResource() {
    Jedis resource = super.getResource();
    return new TracingJedisWrapper(resource, tracingConfiguration);
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
    return (resource instanceof TracingJedisWrapper)
        ? ((TracingJedisWrapper) resource).getWrapped()
        : resource;
  }
}
