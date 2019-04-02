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
package io.opentracing.contrib.redis.redisson;

import static io.opentracing.contrib.redis.common.TracingHelper.nullable;

import io.opentracing.Span;
import java.util.Map.Entry;
import java.util.Set;
import org.redisson.api.RSet;
import org.redisson.api.RSetMultimap;

public class TracingRSetMultimap<K, V> extends TracingRMultimap<K, V> implements
    RSetMultimap<K, V> {
  private final RSetMultimap<K, V> set;
  private final TracingRedissonHelper tracingRedissonHelper;

  public TracingRSetMultimap(RSetMultimap<K, V> set, TracingRedissonHelper tracingRedissonHelper) {
    super(set, tracingRedissonHelper);
    this.set = set;
    this.tracingRedissonHelper = tracingRedissonHelper;
  }

  @Override
  public RSet<V> get(K key) {
    Span span = tracingRedissonHelper.buildSpan("get", set);
    span.setTag("key", nullable(key));
    return tracingRedissonHelper
        .decorate(span, () -> new TracingRSet<>(set.get(key), tracingRedissonHelper));
  }

  @Override
  public Set<V> getAll(K key) {
    Span span = tracingRedissonHelper.buildSpan("getAll", set);
    span.setTag("key", nullable(key));
    return tracingRedissonHelper.decorate(span, () -> set.getAll(key));
  }

  @Override
  public Set<V> removeAll(Object key) {
    Span span = tracingRedissonHelper.buildSpan("removeAll", set);
    span.setTag("key", nullable(key));
    return tracingRedissonHelper.decorate(span, () -> set.removeAll(key));
  }

  @Override
  public Set<V> replaceValues(K key, Iterable<? extends V> values) {
    Span span = tracingRedissonHelper.buildSpan("replaceValues", set);
    span.setTag("key", nullable(key));
    span.setTag("values", nullable(values));
    return tracingRedissonHelper.decorate(span, () -> set.replaceValues(key, values));
  }

  @Override
  public Set<Entry<K, V>> entries() {
    Span span = tracingRedissonHelper.buildSpan("entries", set);
    return tracingRedissonHelper.decorate(span, set::entries);
  }

}
