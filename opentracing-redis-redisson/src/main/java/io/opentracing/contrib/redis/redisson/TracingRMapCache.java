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
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.redisson.api.RFuture;
import org.redisson.api.RMapCache;
import org.redisson.api.map.event.MapEntryListener;

public class TracingRMapCache<K, V> extends TracingRMap<K, V> implements RMapCache<K, V> {
  private final RMapCache<K, V> cache;
  private final TracingRedissonHelper tracingRedissonHelper;

  public TracingRMapCache(RMapCache<K, V> cache, TracingRedissonHelper tracingRedissonHelper) {
    super(cache, tracingRedissonHelper);
    this.cache = cache;
    this.tracingRedissonHelper = tracingRedissonHelper;
  }

  @Override
  public void setMaxSize(int maxSize) {
    Span span = tracingRedissonHelper.buildSpan("setMaxSize", cache);
    span.setTag("maxSize", maxSize);
    tracingRedissonHelper.decorate(span, () -> cache.setMaxSize(maxSize));
  }

  @Override
  public boolean trySetMaxSize(int maxSize) {
    Span span = tracingRedissonHelper.buildSpan("trySetMaxSize", cache);
    span.setTag("maxSize", maxSize);
    return tracingRedissonHelper.decorate(span, () -> cache.trySetMaxSize(maxSize));
  }

  @Override
  public V putIfAbsent(K key, V value, long ttl, TimeUnit ttlUnit) {
    Span span = tracingRedissonHelper.buildSpan("putIfAbsent", cache);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    span.setTag("ttl", ttl);
    span.setTag("ttlUnit", nullable(ttlUnit));
    return tracingRedissonHelper.decorate(span, () -> cache.putIfAbsent(key, value, ttl, ttlUnit));
  }

  @Override
  public V putIfAbsent(K key, V value, long ttl, TimeUnit ttlUnit, long maxIdleTime,
      TimeUnit maxIdleUnit) {
    Span span = tracingRedissonHelper.buildSpan("putIfAbsent", cache);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    span.setTag("ttl", ttl);
    span.setTag("ttlUnit", nullable(ttlUnit));
    span.setTag("maxIdleTime", maxIdleTime);
    span.setTag("maxIdleUnit", nullable(maxIdleUnit));
    return tracingRedissonHelper.decorate(span,
        () -> cache.putIfAbsent(key, value, ttl, ttlUnit, maxIdleTime, maxIdleUnit));
  }

  @Override
  public V put(K key, V value, long ttl, TimeUnit unit) {
    Span span = tracingRedissonHelper.buildSpan("put", cache);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    span.setTag("ttl", ttl);
    span.setTag("unit", nullable(unit));
    return tracingRedissonHelper.decorate(span, () -> cache.put(key, value, ttl, unit));
  }

  @Override
  public V put(K key, V value, long ttl, TimeUnit ttlUnit, long maxIdleTime, TimeUnit maxIdleUnit) {
    Span span = tracingRedissonHelper.buildSpan("put", cache);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    span.setTag("ttl", ttl);
    span.setTag("ttlUnit", nullable(ttlUnit));
    span.setTag("maxIdleTime", maxIdleTime);
    span.setTag("maxIdleUnit", nullable(maxIdleUnit));
    return tracingRedissonHelper.decorate(span,
        () -> cache.put(key, value, ttl, ttlUnit, maxIdleTime, maxIdleUnit));
  }

  @Override
  public boolean fastPut(K key, V value, long ttl, TimeUnit ttlUnit) {
    Span span = tracingRedissonHelper.buildSpan("fastPut", cache);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    span.setTag("ttl", ttl);
    span.setTag("ttlUnit", nullable(ttlUnit));
    return tracingRedissonHelper.decorate(span, () -> cache.fastPut(key, value, ttl, ttlUnit));
  }

  @Override
  public boolean fastPut(K key, V value, long ttl, TimeUnit ttlUnit, long maxIdleTime,
      TimeUnit maxIdleUnit) {
    Span span = tracingRedissonHelper.buildSpan("fastPut", cache);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    span.setTag("ttl", ttl);
    span.setTag("ttlUnit", nullable(ttlUnit));
    span.setTag("maxIdleTime", maxIdleTime);
    span.setTag("maxIdleUnit", nullable(maxIdleUnit));
    return tracingRedissonHelper.decorate(span,
        () -> cache.fastPut(key, value, ttl, ttlUnit, maxIdleTime, maxIdleUnit));
  }

  @Override
  public boolean fastPutIfAbsent(K key, V value, long ttl, TimeUnit ttlUnit) {
    Span span = tracingRedissonHelper.buildSpan("fastPutIfAbsent", cache);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    span.setTag("ttl", ttl);
    span.setTag("ttlUnit", nullable(ttlUnit));
    return tracingRedissonHelper
        .decorate(span, () -> cache.fastPutIfAbsent(key, value, ttl, ttlUnit));
  }

  @Override
  public boolean fastPutIfAbsent(K key, V value, long ttl, TimeUnit ttlUnit,
      long maxIdleTime, TimeUnit maxIdleUnit) {
    Span span = tracingRedissonHelper.buildSpan("fastPutIfAbsent", cache);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    span.setTag("ttl", ttl);
    span.setTag("ttlUnit", nullable(ttlUnit));
    span.setTag("maxIdleTime", maxIdleTime);
    span.setTag("maxIdleUnit", nullable(maxIdleUnit));
    return tracingRedissonHelper.decorate(span,
        () -> cache.fastPutIfAbsent(key, value, ttl, ttlUnit, maxIdleTime, maxIdleUnit));
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> map, long ttl, TimeUnit ttlUnit) {
    Span span = tracingRedissonHelper.buildSpan("putAll", cache);
    span.setTag("ttl", ttl);
    span.setTag("ttlUnit", nullable(ttlUnit));
    tracingRedissonHelper.decorate(span, () -> cache.putAll(map, ttl, ttlUnit));
  }

  @Override
  public RFuture<Void> putAllAsync(Map<? extends K, ? extends V> map, long ttl, TimeUnit ttlUnit) {
    Span span = tracingRedissonHelper.buildSpan("putAllAsync", cache);
    span.setTag("ttl", ttl);
    span.setTag("ttlUnit", nullable(ttlUnit));
    return tracingRedissonHelper.prepareRFuture(span, () -> cache.putAllAsync(map, ttl, ttlUnit));
  }

  @Override
  public int size() {
    Span span = tracingRedissonHelper.buildSpan("size", cache);
    return tracingRedissonHelper.decorate(span, cache::size);
  }

  @Override
  public int addListener(MapEntryListener listener) {
    return cache.addListener(listener);
  }

  @Override
  public void removeListener(int listenerId) {
    cache.removeListener(listenerId);
  }

  @Override
  public long remainTimeToLive(K key) {
    Span span = tracingRedissonHelper.buildSpan("remainTimeToLive", cache);
    span.setTag("key", nullable(key));
    return tracingRedissonHelper.decorate(span, () -> cache.remainTimeToLive(key));
  }

  @Override
  public RFuture<Void> setMaxSizeAsync(int maxSize) {
    Span span = tracingRedissonHelper.buildSpan("setMaxSizeAsync", cache);
    span.setTag("maxSize", maxSize);
    return tracingRedissonHelper.prepareRFuture(span, () -> cache.setMaxSizeAsync(maxSize));
  }

  @Override
  public RFuture<Boolean> trySetMaxSizeAsync(int maxSize) {
    Span span = tracingRedissonHelper.buildSpan("trySetMaxSizeAsync", cache);
    span.setTag("maxSize", maxSize);
    return tracingRedissonHelper.prepareRFuture(span, () -> cache.trySetMaxSizeAsync(maxSize));
  }

  @Override
  public RFuture<V> putIfAbsentAsync(K key, V value, long ttl, TimeUnit unit) {
    Span span = tracingRedissonHelper.buildSpan("putIfAbsentAsync", cache);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    span.setTag("ttl", ttl);
    span.setTag("unit", nullable(unit));
    return tracingRedissonHelper
        .prepareRFuture(span, () -> cache.putIfAbsentAsync(key, value, ttl, unit));
  }

  @Override
  public RFuture<V> putIfAbsentAsync(K key, V value, long ttl, TimeUnit ttlUnit, long maxIdleTime,
      TimeUnit maxIdleUnit) {
    Span span = tracingRedissonHelper.buildSpan("putIfAbsentAsync", cache);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    span.setTag("ttl", ttl);
    span.setTag("ttlUnit", nullable(ttlUnit));
    span.setTag("maxIdleTime", maxIdleTime);
    span.setTag("maxIdleUnit", nullable(maxIdleUnit));
    return tracingRedissonHelper.prepareRFuture(
        span, () -> cache.putIfAbsentAsync(key, value, ttl, ttlUnit, maxIdleTime, maxIdleUnit));
  }

  @Override
  public RFuture<V> putAsync(K key, V value, long ttl, TimeUnit unit) {
    Span span = tracingRedissonHelper.buildSpan("putAsync", cache);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    span.setTag("ttl", ttl);
    span.setTag("unit", nullable(unit));
    return tracingRedissonHelper.prepareRFuture(span, () -> cache.putAsync(key, value, ttl, unit));
  }

  @Override
  public RFuture<V> putAsync(K key, V value, long ttl, TimeUnit ttlUnit, long maxIdleTime,
      TimeUnit maxIdleUnit) {
    Span span = tracingRedissonHelper.buildSpan("putAsync", cache);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    span.setTag("ttl", ttl);
    span.setTag("ttlUnit", nullable(ttlUnit));
    span.setTag("maxIdleTime", maxIdleTime);
    span.setTag("maxIdleUnit", nullable(maxIdleUnit));
    return tracingRedissonHelper.prepareRFuture(
        span, () -> cache.putAsync(key, value, ttl, ttlUnit, maxIdleTime, maxIdleUnit));
  }

  @Override
  public RFuture<Boolean> fastPutAsync(K key, V value, long ttl, TimeUnit unit) {
    Span span = tracingRedissonHelper.buildSpan("fastPutAsync", cache);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    span.setTag("ttl", ttl);
    span.setTag("unit", nullable(unit));
    return tracingRedissonHelper
        .prepareRFuture(span, () -> cache.fastPutAsync(key, value, ttl, unit));
  }

  @Override
  public RFuture<Boolean> fastPutAsync(K key, V value, long ttl, TimeUnit ttlUnit, long maxIdleTime,
      TimeUnit maxIdleUnit) {
    Span span = tracingRedissonHelper.buildSpan("fastPutAsync", cache);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    span.setTag("ttl", ttl);
    span.setTag("ttlUnit", nullable(ttlUnit));
    span.setTag("maxIdleTime", maxIdleTime);
    span.setTag("maxIdleUnit", nullable(maxIdleUnit));
    return tracingRedissonHelper.prepareRFuture(
        span, () -> cache.fastPutAsync(key, value, ttl, ttlUnit, maxIdleTime, maxIdleUnit));
  }

  @Override
  public RFuture<Boolean> fastPutIfAbsentAsync(K key, V value, long ttl, TimeUnit ttlUnit,
      long maxIdleTime, TimeUnit maxIdleUnit) {
    Span span = tracingRedissonHelper.buildSpan("fastPutIfAbsentAsync", cache);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    span.setTag("ttl", ttl);
    span.setTag("ttlUnit", nullable(ttlUnit));
    span.setTag("maxIdleTime", maxIdleTime);
    span.setTag("maxIdleUnit", nullable(maxIdleUnit));
    return tracingRedissonHelper.prepareRFuture(
        span, () -> cache.fastPutIfAbsentAsync(key, value, ttl, ttlUnit, maxIdleTime, maxIdleUnit));
  }

  @Override
  public RFuture<Long> remainTimeToLiveAsync(K key) {
    Span span = tracingRedissonHelper.buildSpan("remainTimeToLiveAsync", cache);
    span.setTag("key", nullable(key));
    return tracingRedissonHelper.prepareRFuture(span, () -> cache.remainTimeToLiveAsync(key));
  }

  @Override
  public void destroy() {
    Span span = tracingRedissonHelper.buildSpan("destroy", cache);
    tracingRedissonHelper.decorate(span, cache::destroy);
  }
}
