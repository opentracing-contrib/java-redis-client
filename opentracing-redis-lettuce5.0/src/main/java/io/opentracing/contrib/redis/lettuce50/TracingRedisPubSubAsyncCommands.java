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
package io.opentracing.contrib.redis.lettuce50;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.async.RedisPubSubAsyncCommands;
import io.opentracing.Span;
import io.opentracing.contrib.redis.common.TracingConfiguration;
import java.util.Arrays;

public class TracingRedisPubSubAsyncCommands<K, V> extends
    TracingRedisAsyncCommands<K, V> implements RedisPubSubAsyncCommands<K, V> {
  private final RedisPubSubAsyncCommands<K, V> commands;

  public TracingRedisPubSubAsyncCommands(
      RedisPubSubAsyncCommands<K, V> commands,
      TracingConfiguration tracingConfiguration) {
    super(commands, tracingConfiguration);
    this.commands = commands;
  }

  @Override
  public RedisFuture<Void> psubscribe(K... patterns) {
    final Span span = helper.buildSpan("psubscribe");
    span.setTag("patterns", Arrays.toString(patterns));
    return prepareRedisFuture(commands.psubscribe(patterns), span);
  }

  @Override
  public RedisFuture<Void> punsubscribe(K... patterns) {
    final Span span = helper.buildSpan("punsubscribe");
    span.setTag("patterns", Arrays.toString(patterns));
    return prepareRedisFuture(commands.punsubscribe(patterns), span);
  }

  @Override
  public RedisFuture<Void> subscribe(K... channels) {
    final Span span = helper.buildSpan("subscribe");
    span.setTag("channels", Arrays.toString(channels));
    return prepareRedisFuture(commands.subscribe(channels), span);
  }

  @Override
  public RedisFuture<Void> unsubscribe(K... channels) {
    final Span span = helper.buildSpan("unsubscribe");
    span.setTag("channels", Arrays.toString(channels));
    return prepareRedisFuture(commands.unsubscribe(channels), span);
  }

  @Override
  public StatefulRedisPubSubConnection<K, V> getStatefulConnection() {
    return new TracingStatefulRedisPubSubConnection<>(commands.getStatefulConnection(),
        tracingConfiguration);
  }
}
