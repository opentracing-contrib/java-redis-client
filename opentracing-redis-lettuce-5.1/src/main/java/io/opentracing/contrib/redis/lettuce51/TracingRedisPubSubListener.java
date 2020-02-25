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
package io.opentracing.contrib.redis.lettuce51;

import static io.opentracing.contrib.redis.common.TracingHelper.nullable;

import io.lettuce.core.pubsub.RedisPubSubListener;
import io.opentracing.Span;
import io.opentracing.contrib.redis.common.TracingConfiguration;
import io.opentracing.contrib.redis.common.TracingHelper;

public class TracingRedisPubSubListener<K, V> implements RedisPubSubListener<K, V> {

  private final RedisPubSubListener<K, V> listener;
  private final TracingHelper helper;

  public TracingRedisPubSubListener(RedisPubSubListener<K, V> listener,
      TracingConfiguration tracingConfiguration) {
    this.listener = listener;
    this.helper = new TracingHelper(tracingConfiguration);
  }

  @Override
  public void message(K channel, V message) {
    final Span span = helper.buildSpan("message");
    span.setTag("channel", nullable(channel));
    span.setTag("message", nullable(message));
    helper.decorate(span, () -> listener.message(channel, message));
  }

  @Override
  public void message(K pattern, K channel, V message) {
    final Span span = helper.buildSpan("message");
    span.setTag("pattern", nullable(pattern));
    span.setTag("channel", nullable(channel));
    span.setTag("message", nullable(message));
    helper.decorate(span, () -> listener.message(pattern, channel, message));
  }

  @Override
  public void subscribed(K channel, long count) {
    final Span span = helper.buildSpan("subscribed");
    span.setTag("channel", nullable(channel));
    span.setTag("count", count);
    helper.decorate(span, () -> listener.subscribed(channel, count));
  }

  @Override
  public void psubscribed(K pattern, long count) {
    final Span span = helper.buildSpan("psubscribed");
    span.setTag("pattern", nullable(pattern));
    span.setTag("count", count);
    helper.decorate(span, () -> listener.psubscribed(pattern, count));
  }

  @Override
  public void unsubscribed(K channel, long count) {
    final Span span = helper.buildSpan("unsubscribed");
    span.setTag("channel", nullable(channel));
    span.setTag("count", count);
    helper.decorate(span, () -> listener.unsubscribed(channel, count));
  }

  @Override
  public void punsubscribed(K pattern, long count) {
    final Span span = helper.buildSpan("punsubscribed");
    span.setTag("pattern", nullable(pattern));
    span.setTag("count", count);
    helper.decorate(span, () -> listener.punsubscribed(pattern, count));
  }

  @Override
  public boolean equals(Object obj) {
    return listener.equals(obj);
  }

  @Override
  public int hashCode() {
    return listener.hashCode();
  }
}
