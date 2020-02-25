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
package io.opentracing.contrib.redis.lettuce50;

import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands;
import io.opentracing.Span;
import io.opentracing.contrib.redis.common.TracingConfiguration;
import java.util.Arrays;

public class TracingRedisPubSubCommands<K, V> extends TracingRedisCommands<K, V> implements
    RedisPubSubCommands<K, V> {
  private final RedisPubSubCommands<K, V> commands;

  public TracingRedisPubSubCommands(RedisPubSubCommands<K, V> commands,
      TracingConfiguration tracingConfiguration) {
    super(commands, tracingConfiguration);
    this.commands = commands;
  }

  @Override
  public void psubscribe(K... patterns) {
    final Span span = helper.buildSpan("psubscribe");
    span.setTag("patterns", Arrays.toString(patterns));
    helper.decorate(span, () -> commands.psubscribe(patterns));
  }

  @Override
  public void punsubscribe(K... patterns) {
    final Span span = helper.buildSpan("punsubscribe");
    span.setTag("patterns", Arrays.toString(patterns));
    helper.decorate(span, () -> commands.punsubscribe(patterns));
  }

  @Override
  public void subscribe(K... channels) {
    final Span span = helper.buildSpan("subscribe");
    span.setTag("channels", Arrays.toString(channels));
    helper.decorate(span, () -> commands.subscribe(channels));
  }

  @Override
  public void unsubscribe(K... channels) {
    final Span span = helper.buildSpan("unsubscribe");
    span.setTag("channels", Arrays.toString(channels));
    helper.decorate(span, () -> commands.unsubscribe(channels));
  }

  @Override
  public StatefulRedisPubSubConnection<K, V> getStatefulConnection() {
    return new TracingStatefulRedisPubSubConnection<>(commands.getStatefulConnection(),
        tracingConfiguration);
  }
}
