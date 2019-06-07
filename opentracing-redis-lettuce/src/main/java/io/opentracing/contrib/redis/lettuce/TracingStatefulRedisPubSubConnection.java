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
package io.opentracing.contrib.redis.lettuce;

import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.async.RedisPubSubAsyncCommands;
import io.lettuce.core.pubsub.api.reactive.RedisPubSubReactiveCommands;
import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands;
import io.opentracing.contrib.redis.common.TracingConfiguration;

public class TracingStatefulRedisPubSubConnection<K, V> extends
    TracingStatefulRedisConnection<K, V> implements
    StatefulRedisPubSubConnection<K, V> {

  private final StatefulRedisPubSubConnection<K, V> connection;

  public TracingStatefulRedisPubSubConnection(
      StatefulRedisPubSubConnection<K, V> connection,
      TracingConfiguration tracingConfiguration) {
    super(connection, tracingConfiguration);
    this.connection = connection;
  }

  @Override
  public RedisPubSubCommands<K, V> sync() {
    return new TracingRedisPubSubCommands<>(connection.sync(), tracingConfiguration);
  }

  @Override
  public RedisPubSubAsyncCommands<K, V> async() {
    return new TracingRedisPubSubAsyncCommands<>(connection.async(), tracingConfiguration);
  }

  @Override
  public RedisPubSubReactiveCommands<K, V> reactive() {
    return connection.reactive();
  }

  @Override
  public void addListener(RedisPubSubListener<K, V> listener) {
    connection.addListener(new TracingRedisPubSubListener<>(listener, tracingConfiguration));
  }

  @Override
  public void removeListener(RedisPubSubListener<K, V> listener) {
    connection.removeListener(listener);
  }
}
