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

import io.opentracing.Span;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import org.redisson.api.RFuture;
import org.redisson.api.RLocalCachedMap;

public class TracingRLocalCachedMap<K, V> extends TracingRMap<K, V> implements
    RLocalCachedMap<K, V> {
  private final RLocalCachedMap<K, V> map;
  private final TracingRedissonHelper tracingRedissonHelper;

  public TracingRLocalCachedMap(RLocalCachedMap<K, V> map,
      TracingRedissonHelper tracingRedissonHelper) {
    super(map, tracingRedissonHelper);
    this.map = map;
    this.tracingRedissonHelper = tracingRedissonHelper;
  }

  @Override
  public void preloadCache() {
    Span span = tracingRedissonHelper.buildSpan("preloadCache", map);
    tracingRedissonHelper.decorate(span, map::preloadCache);
  }

  @Override
  public RFuture<Void> clearLocalCacheAsync() {
    Span span = tracingRedissonHelper.buildSpan("clearLocalCacheAsync", map);
    return tracingRedissonHelper.prepareRFuture(span, map::clearLocalCacheAsync);
  }

  @Override
  public void clearLocalCache() {
    Span span = tracingRedissonHelper.buildSpan("clearLocalCache", map);
    tracingRedissonHelper.decorate(span, map::clearLocalCache);
  }

  @Override
  public Set<K> cachedKeySet() {
    Span span = tracingRedissonHelper.buildSpan("cachedKeySet", map);
    return tracingRedissonHelper.decorate(span, map::cachedKeySet);
  }

  @Override
  public Collection<V> cachedValues() {
    Span span = tracingRedissonHelper.buildSpan("cachedValues", map);
    return tracingRedissonHelper.decorate(span, map::cachedValues);
  }

  @Override
  public Set<Entry<K, V>> cachedEntrySet() {
    Span span = tracingRedissonHelper.buildSpan("cachedEntrySet", map);
    return tracingRedissonHelper.decorate(span, map::cachedEntrySet);
  }

  @Override
  public Map<K, V> getCachedMap() {
    Span span = tracingRedissonHelper.buildSpan("getCachedMap", map);
    return tracingRedissonHelper.decorate(span, map::getCachedMap);
  }

  @Override
  public void destroy() {
    Span span = tracingRedissonHelper.buildSpan("destroy", map);
    tracingRedissonHelper.decorate(span, map::destroy);
  }
}
