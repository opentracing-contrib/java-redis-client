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
import java.util.Arrays;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.Set;
import org.redisson.api.RCountDownLatch;
import org.redisson.api.RFuture;
import org.redisson.api.RLock;
import org.redisson.api.RMultimap;
import org.redisson.api.RPermitExpirableSemaphore;
import org.redisson.api.RReadWriteLock;
import org.redisson.api.RSemaphore;

public class TracingRMultimap<K, V> extends TracingRExpirable implements RMultimap<K, V> {
  private final RMultimap<K, V> map;
  private final TracingRedissonHelper tracingRedissonHelper;

  public TracingRMultimap(RMultimap<K, V> map, TracingRedissonHelper tracingRedissonHelper) {
    super(map, tracingRedissonHelper);
    this.map = map;
    this.tracingRedissonHelper = tracingRedissonHelper;
  }

  @Override
  public RCountDownLatch getCountDownLatch(K key) {
    return new TracingRCountDownLatch(map.getCountDownLatch(key), tracingRedissonHelper);
  }

  @Override
  public RPermitExpirableSemaphore getPermitExpirableSemaphore(K key) {
    return new TracingRPermitExpirableSemaphore(map.getPermitExpirableSemaphore(key),
        tracingRedissonHelper);
  }

  @Override
  public RSemaphore getSemaphore(K key) {
    return new TracingRSemaphore(map.getSemaphore(key), tracingRedissonHelper);
  }

  @Override
  public RLock getFairLock(K key) {
    return new TracingRLock(map.getFairLock(key), tracingRedissonHelper);
  }

  @Override
  public RReadWriteLock getReadWriteLock(K key) {
    return new TracingRReadWriteLock(map.getReadWriteLock(key), tracingRedissonHelper);
  }

  @Override
  public RLock getLock(K key) {
    return new TracingRLock(map.getLock(key), tracingRedissonHelper);
  }

  @Override
  public int size() {
    Span span = tracingRedissonHelper.buildSpan("size", map);
    return tracingRedissonHelper.decorate(span, map::size);
  }

  @Override
  public boolean isEmpty() {
    Span span = tracingRedissonHelper.buildSpan("isEmpty", map);
    return tracingRedissonHelper.decorate(span, map::isEmpty);
  }

  @Override
  public boolean containsKey(Object key) {
    Span span = tracingRedissonHelper.buildSpan("containsKey", map);
    span.setTag("key", nullable(key));
    return tracingRedissonHelper.decorate(span, () -> map.containsKey(key));
  }

  @Override
  public boolean containsValue(Object value) {
    Span span = tracingRedissonHelper.buildSpan("containsValue", map);
    span.setTag("value", nullable(value));
    return tracingRedissonHelper.decorate(span, () -> map.containsValue(value));
  }

  @Override
  public boolean containsEntry(Object key, Object value) {
    Span span = tracingRedissonHelper.buildSpan("containsEntry", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return tracingRedissonHelper.decorate(span, () -> map.containsEntry(key, value));
  }

  @Override
  public boolean put(K key, V value) {
    Span span = tracingRedissonHelper.buildSpan("put", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return tracingRedissonHelper.decorate(span, () -> map.put(key, value));
  }

  @Override
  public boolean remove(Object key, Object value) {
    Span span = tracingRedissonHelper.buildSpan("remove", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return tracingRedissonHelper.decorate(span, () -> map.remove(key, value));
  }

  @Override
  public boolean putAll(K key, Iterable<? extends V> values) {
    Span span = tracingRedissonHelper.buildSpan("putAll", map);
    span.setTag("key", nullable(key));
    span.setTag("values", nullable(values));
    return tracingRedissonHelper.decorate(span, () -> map.putAll(key, values));
  }

  @Override
  public Collection<V> replaceValues(K key, Iterable<? extends V> values) {
    Span span = tracingRedissonHelper.buildSpan("replaceValues", map);
    span.setTag("key", nullable(key));
    span.setTag("values", nullable(values));
    return tracingRedissonHelper.decorate(span, () -> map.replaceValues(key, values));
  }

  @Override
  public Collection<V> removeAll(Object key) {
    Span span = tracingRedissonHelper.buildSpan("removeAll", map);
    span.setTag("key", nullable(key));
    return tracingRedissonHelper.decorate(span, () -> map.removeAll(key));
  }

  @Override
  public void clear() {
    Span span = tracingRedissonHelper.buildSpan("clear", map);
    tracingRedissonHelper.decorate(span, map::clear);
  }

  @Override
  public Collection<V> get(K key) {
    Span span = tracingRedissonHelper.buildSpan("get", map);
    span.setTag("key", nullable(key));
    return tracingRedissonHelper.decorate(span, () -> map.get(key));
  }

  @Override
  public Collection<V> getAll(K key) {
    Span span = tracingRedissonHelper.buildSpan("getAll", map);
    span.setTag("key", nullable(key));
    return tracingRedissonHelper.decorate(span, () -> map.getAll(key));
  }

  @Override
  public Set<K> keySet() {
    Span span = tracingRedissonHelper.buildSpan("keySet", map);
    return tracingRedissonHelper.decorate(span, map::keySet);
  }

  @Override
  public int keySize() {
    Span span = tracingRedissonHelper.buildSpan("keySize", map);
    return tracingRedissonHelper.decorate(span, map::keySize);
  }

  @Override
  public Collection<V> values() {
    Span span = tracingRedissonHelper.buildSpan("values", map);
    return tracingRedissonHelper.decorate(span, map::values);
  }

  @Override
  public Collection<Entry<K, V>> entries() {
    Span span = tracingRedissonHelper.buildSpan("entries", map);
    return tracingRedissonHelper.decorate(span, map::entries);
  }

  @Override
  public long fastRemove(K... keys) {
    Span span = tracingRedissonHelper.buildSpan("fastRemove", map);
    span.setTag("keys", Arrays.toString(keys));
    return tracingRedissonHelper.decorate(span, () -> map.fastRemove(keys));
  }

  @Override
  public Set<K> readAllKeySet() {
    Span span = tracingRedissonHelper.buildSpan("readAllKeySet", map);
    return tracingRedissonHelper.decorate(span, map::readAllKeySet);
  }

  @Override
  public RFuture<Integer> sizeAsync() {
    Span span = tracingRedissonHelper.buildSpan("sizeAsync", map);
    return tracingRedissonHelper.prepareRFuture(span, map::sizeAsync);
  }

  @Override
  public RFuture<Boolean> containsKeyAsync(Object key) {
    Span span = tracingRedissonHelper.buildSpan("containsKeyAsync", map);
    span.setTag("key", nullable(key));
    return tracingRedissonHelper.prepareRFuture(span, () -> map.containsKeyAsync(key));
  }

  @Override
  public RFuture<Boolean> containsValueAsync(Object value) {
    Span span = tracingRedissonHelper.buildSpan("containsValueAsync", map);
    span.setTag("value", nullable(value));
    return tracingRedissonHelper.prepareRFuture(span, () -> map.containsValueAsync(value));
  }

  @Override
  public RFuture<Boolean> containsEntryAsync(Object key, Object value) {
    Span span = tracingRedissonHelper.buildSpan("containsEntryAsync", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return tracingRedissonHelper.prepareRFuture(span, () -> map.containsEntryAsync(key, value));
  }

  @Override
  public RFuture<Boolean> putAsync(K key, V value) {
    Span span = tracingRedissonHelper.buildSpan("putAsync", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return tracingRedissonHelper.prepareRFuture(span, () -> map.putAsync(key, value));
  }

  @Override
  public RFuture<Boolean> removeAsync(Object key, Object value) {
    Span span = tracingRedissonHelper.buildSpan("removeAsync", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return tracingRedissonHelper.prepareRFuture(span, () -> map.removeAsync(key, value));
  }

  @Override
  public RFuture<Boolean> putAllAsync(K key, Iterable<? extends V> values) {
    Span span = tracingRedissonHelper.buildSpan("putAllAsync", map);
    span.setTag("key", nullable(key));
    span.setTag("values", nullable(values));
    return tracingRedissonHelper.prepareRFuture(span, () -> map.putAllAsync(key, values));
  }

  @Override
  public RFuture<Collection<V>> replaceValuesAsync(K key, Iterable<? extends V> values) {
    Span span = tracingRedissonHelper.buildSpan("replaceValuesAsync", map);
    span.setTag("key", nullable(key));
    span.setTag("values", nullable(values));
    return tracingRedissonHelper.prepareRFuture(span, () -> map.replaceValuesAsync(key, values));
  }

  @Override
  public RFuture<Collection<V>> removeAllAsync(Object key) {
    Span span = tracingRedissonHelper.buildSpan("removeAllAsync", map);
    span.setTag("key", nullable(key));
    return tracingRedissonHelper.prepareRFuture(span, () -> map.removeAllAsync(key));
  }

  @Override
  public RFuture<Collection<V>> getAllAsync(K key) {
    Span span = tracingRedissonHelper.buildSpan("getAllAsync", map);
    span.setTag("key", nullable(key));
    return tracingRedissonHelper.prepareRFuture(span, () -> map.getAllAsync(key));
  }

  @Override
  public RFuture<Integer> keySizeAsync() {
    Span span = tracingRedissonHelper.buildSpan("keySizeAsync", map);
    return tracingRedissonHelper.prepareRFuture(span, map::keySizeAsync);
  }

  @Override
  public RFuture<Long> fastRemoveAsync(K... keys) {
    Span span = tracingRedissonHelper.buildSpan("fastRemoveAsync", map);
    span.setTag("keys", Arrays.toString(keys));
    return tracingRedissonHelper.prepareRFuture(span, () -> map.fastRemoveAsync(keys));
  }

  @Override
  public RFuture<Set<K>> readAllKeySetAsync() {
    Span span = tracingRedissonHelper.buildSpan("readAllKeySetAsync", map);
    return tracingRedissonHelper.prepareRFuture(span, map::readAllKeySetAsync);
  }

}
