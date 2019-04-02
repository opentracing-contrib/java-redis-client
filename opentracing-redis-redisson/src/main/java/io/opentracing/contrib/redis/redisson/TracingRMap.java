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

import static io.opentracing.contrib.redis.common.TracingHelper.collectionToString;
import static io.opentracing.contrib.redis.common.TracingHelper.mapToString;
import static io.opentracing.contrib.redis.common.TracingHelper.nullable;

import io.opentracing.Span;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.redisson.api.RFuture;
import org.redisson.api.RLock;
import org.redisson.api.RMap;
import org.redisson.api.RReadWriteLock;
import org.redisson.api.mapreduce.RMapReduce;

public class TracingRMap<K, V> extends TracingRExpirable implements RMap<K, V> {
  private final RMap<K, V> map;
  private final TracingRedissonHelper tracingRedissonHelper;

  public TracingRMap(RMap<K, V> map, TracingRedissonHelper tracingRedissonHelper) {
    super(map, tracingRedissonHelper);
    this.map = map;
    this.tracingRedissonHelper = tracingRedissonHelper;
  }

  @Override
  public void loadAll(boolean replaceExistingValues, int parallelism) {
    Span span = tracingRedissonHelper.buildSpan("loadAll", map);
    span.setTag("replaceExistingValues", replaceExistingValues);
    span.setTag("parallelism", parallelism);
    tracingRedissonHelper.decorate(span, () -> map.loadAll(replaceExistingValues, parallelism));
  }

  @Override
  public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, int parallelism) {
    Span span = tracingRedissonHelper.buildSpan("loadAll", map);
    span.setTag("keys", collectionToString(keys));
    span.setTag("replaceExistingValues", replaceExistingValues);
    span.setTag("parallelism", parallelism);
    tracingRedissonHelper
        .decorate(span, () -> map.loadAll(keys, replaceExistingValues, parallelism));
  }

  @Override
  public V get(Object key) {
    Span span = tracingRedissonHelper.buildSpan("get", map);
    span.setTag("key", nullable(key));
    return tracingRedissonHelper.decorate(span, () -> map.get(key));
  }

  @Override
  public V put(K key, V value) {
    Span span = tracingRedissonHelper.buildSpan("put", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return tracingRedissonHelper.decorate(span, () -> map.put(key, value));
  }

  @Override
  public V putIfAbsent(K key, V value) {
    Span span = tracingRedissonHelper.buildSpan("putIfAbsent", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return tracingRedissonHelper.decorate(span, () -> map.putIfAbsent(key, value));
  }

  @Override
  public <KOut, VOut> RMapReduce<K, V, KOut, VOut> mapReduce() {
    return new TracingRMapReduce<>(map.mapReduce(), tracingRedissonHelper);
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
  public int valueSize(K key) {
    Span span = tracingRedissonHelper.buildSpan("valueSize", map);
    span.setTag("key", nullable(key));
    return tracingRedissonHelper.decorate(span, () -> map.valueSize(key));
  }

  @Override
  public V addAndGet(K key, Number delta) {
    Span span = tracingRedissonHelper.buildSpan("addAndGet", map);
    span.setTag("key", nullable(key));
    span.setTag("delta", nullable(delta));
    return tracingRedissonHelper.decorate(span, () -> map.addAndGet(key, delta));
  }

  @Override
  public V remove(Object key) {
    Span span = tracingRedissonHelper.buildSpan("remove", map);
    span.setTag("key", nullable(key));
    return tracingRedissonHelper.decorate(span, () -> map.remove(key));
  }

  @Override
  public V replace(K key, V value) {
    Span span = tracingRedissonHelper.buildSpan("replace", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return tracingRedissonHelper.decorate(span, () -> map.replace(key, value));
  }

  @Override
  public boolean replace(K key, V oldValue, V newValue) {
    Span span = tracingRedissonHelper.buildSpan("replace", map);
    span.setTag("key", nullable(key));
    span.setTag("oldValue", nullable(oldValue));
    span.setTag("newValue", nullable(newValue));
    return tracingRedissonHelper.decorate(span, () -> map.replace(key, oldValue, newValue));
  }

  @Override
  public boolean remove(Object key, Object value) {
    Span span = tracingRedissonHelper.buildSpan("remove", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return tracingRedissonHelper.decorate(span, () -> map.remove(key, value));
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> map) {
    Span span = tracingRedissonHelper.buildSpan("putAll", this.map);
    span.setTag("map", mapToString(map));
    tracingRedissonHelper.decorate(span, () -> this.map.putAll(map));
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> map, int batchSize) {
    Span span = tracingRedissonHelper.buildSpan("putAll", this.map);
    span.setTag("map", mapToString(map));
    span.setTag("batchSize", batchSize);
    tracingRedissonHelper.decorate(span, () -> this.map.putAll(map, batchSize));
  }

  @Override
  public Map<K, V> getAll(Set<K> keys) {
    Span span = tracingRedissonHelper.buildSpan("getAll", map);
    span.setTag("keys", collectionToString(keys));
    return tracingRedissonHelper.decorate(span, () -> map.getAll(keys));
  }

  @Override
  public long fastRemove(K... keys) {
    Span span = tracingRedissonHelper.buildSpan("fastRemove", map);
    span.setTag("keys", Arrays.toString(keys));
    return tracingRedissonHelper.decorate(span, () -> map.fastRemove(keys));
  }

  @Override
  public boolean fastPut(K key, V value) {
    Span span = tracingRedissonHelper.buildSpan("fastPut", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return tracingRedissonHelper.decorate(span, () -> map.fastPut(key, value));
  }

  @Override
  public boolean fastReplace(K key, V value) {
    Span span = tracingRedissonHelper.buildSpan("fastReplace", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return tracingRedissonHelper.decorate(span, () -> map.fastReplace(key, value));
  }

  @Override
  public boolean fastPutIfAbsent(K key, V value) {
    Span span = tracingRedissonHelper.buildSpan("fastPutIfAbsent", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return tracingRedissonHelper.decorate(span, () -> map.fastPutIfAbsent(key, value));
  }

  @Override
  public Set<K> readAllKeySet() {
    Span span = tracingRedissonHelper.buildSpan("readAllKeySet", map);
    return tracingRedissonHelper.decorate(span, map::readAllKeySet);
  }

  @Override
  public Collection<V> readAllValues() {
    Span span = tracingRedissonHelper.buildSpan("readAllValues", map);
    return tracingRedissonHelper.decorate(span, map::readAllValues);
  }

  @Override
  public Set<Entry<K, V>> readAllEntrySet() {
    Span span = tracingRedissonHelper.buildSpan("readAllEntrySet", map);
    return tracingRedissonHelper.decorate(span, map::readAllEntrySet);
  }

  @Override
  public Map<K, V> readAllMap() {
    Span span = tracingRedissonHelper.buildSpan("readAllMap", map);
    return tracingRedissonHelper.decorate(span, map::readAllMap);
  }

  @Override
  public Set<K> keySet() {
    Span span = tracingRedissonHelper.buildSpan("keySet", map);
    return tracingRedissonHelper.decorate(span, () -> map.keySet());
  }

  @Override
  public Set<K> keySet(int count) {
    Span span = tracingRedissonHelper.buildSpan("keySet", map);
    span.setTag("count", count);
    return tracingRedissonHelper.decorate(span, () -> map.keySet(count));
  }

  @Override
  public Set<K> keySet(String pattern, int count) {
    Span span = tracingRedissonHelper.buildSpan("keySet", map);
    span.setTag("pattern", pattern);
    span.setTag("count", count);
    return tracingRedissonHelper.decorate(span, () -> map.keySet(pattern, count));
  }

  @Override
  public Set<K> keySet(String pattern) {
    Span span = tracingRedissonHelper.buildSpan("keySet", map);
    span.setTag("pattern", pattern);
    return tracingRedissonHelper.decorate(span, () -> map.keySet(pattern));
  }

  @Override
  public Collection<V> values() {
    Span span = tracingRedissonHelper.buildSpan("values", map);
    return tracingRedissonHelper.decorate(span, () -> map.values());
  }

  @Override
  public Collection<V> values(String keyPattern) {
    Span span = tracingRedissonHelper.buildSpan("values", map);
    span.setTag("keyPattern", keyPattern);
    return tracingRedissonHelper.decorate(span, () -> map.values(keyPattern));
  }

  @Override
  public Collection<V> values(String keyPattern, int count) {
    Span span = tracingRedissonHelper.buildSpan("values", map);
    span.setTag("keyPattern", keyPattern);
    span.setTag("count", count);
    return tracingRedissonHelper.decorate(span, () -> map.values(keyPattern, count));
  }

  @Override
  public Collection<V> values(int count) {
    Span span = tracingRedissonHelper.buildSpan("values", map);
    span.setTag("count", count);
    return tracingRedissonHelper.decorate(span, () -> map.values(count));
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    Span span = tracingRedissonHelper.buildSpan("entrySet", map);
    return tracingRedissonHelper.decorate(span, () -> map.entrySet());
  }

  @Override
  public Set<Entry<K, V>> entrySet(String keyPattern) {
    Span span = tracingRedissonHelper.buildSpan("entrySet", map);
    span.setTag("keyPattern", keyPattern);
    return tracingRedissonHelper.decorate(span, () -> map.entrySet(keyPattern));
  }

  @Override
  public Set<Entry<K, V>> entrySet(String keyPattern, int count) {
    Span span = tracingRedissonHelper.buildSpan("entrySet", map);
    span.setTag("keyPattern", keyPattern);
    span.setTag("count", count);
    return tracingRedissonHelper.decorate(span, () -> map.entrySet(keyPattern, count));
  }

  @Override
  public Set<Entry<K, V>> entrySet(int count) {
    Span span = tracingRedissonHelper.buildSpan("entrySet", map);
    span.setTag("count", count);
    return tracingRedissonHelper.decorate(span, () -> map.entrySet(count));
  }

  @Override
  public V getOrDefault(Object key, V defaultValue) {
    Span span = tracingRedissonHelper.buildSpan("getOrDefault", map);
    span.setTag("key", nullable(key));
    span.setTag("defaultValue", nullable(defaultValue));
    return tracingRedissonHelper.decorate(span, () -> map.getOrDefault(key, defaultValue));
  }

  @Override
  public void forEach(BiConsumer<? super K, ? super V> action) {
    Span span = tracingRedissonHelper.buildSpan("forEach", map);
    span.setTag("action", nullable(action));
    tracingRedissonHelper.decorate(span, () -> map.forEach(action));
  }

  @Override
  public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
    Span span = tracingRedissonHelper.buildSpan("replaceAll", map);
    span.setTag("function", nullable(function));
    tracingRedissonHelper.decorate(span, () -> map.replaceAll(function));
  }

  @Override
  public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
    Span span = tracingRedissonHelper.buildSpan("computeIfAbsent", map);
    span.setTag("key", nullable(key));
    span.setTag("mappingFunction", nullable(mappingFunction));
    return tracingRedissonHelper.decorate(span, () -> map.computeIfAbsent(key, mappingFunction));
  }

  @Override
  public V computeIfPresent(K key,
      BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    Span span = tracingRedissonHelper.buildSpan("computeIfPresent", map);
    span.setTag("key", nullable(key));
    span.setTag("remappingFunction", nullable(remappingFunction));
    return tracingRedissonHelper.decorate(span, () -> map.computeIfPresent(key, remappingFunction));
  }

  @Override
  public V compute(K key,
      BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    Span span = tracingRedissonHelper.buildSpan("compute", map);
    span.setTag("key", nullable(key));
    span.setTag("remappingFunction", nullable(remappingFunction));
    return tracingRedissonHelper.decorate(span, () -> map.compute(key, remappingFunction));
  }

  @Override
  public V merge(K key, V value,
      BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
    Span span = tracingRedissonHelper.buildSpan("merge", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    span.setTag("remappingFunction", nullable(remappingFunction));
    return tracingRedissonHelper.decorate(span, () -> map.merge(key, value, remappingFunction));
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
  public void clear() {
    Span span = tracingRedissonHelper.buildSpan("clear", map);
    tracingRedissonHelper.decorate(span, map::clear);
  }

  @Override
  public boolean equals(Object o) {
    Span span = tracingRedissonHelper.buildSpan("equals", map);
    span.setTag("object", nullable(o));
    return tracingRedissonHelper.decorate(span, () -> map.equals(o));
  }

  @Override
  public int hashCode() {
    Span span = tracingRedissonHelper.buildSpan("hashCode", map);
    return tracingRedissonHelper.decorate(span, map::hashCode);
  }

  @Override
  public RFuture<Void> loadAllAsync(boolean replaceExistingValues, int parallelism) {
    Span span = tracingRedissonHelper.buildSpan("loadAllAsync", map);
    span.setTag("replaceExistingValues", replaceExistingValues);
    span.setTag("parallelism", parallelism);
    return tracingRedissonHelper
        .prepareRFuture(span, () -> map.loadAllAsync(replaceExistingValues, parallelism));
  }

  @Override
  public RFuture<Void> loadAllAsync(Set<? extends K> keys,
      boolean replaceExistingValues, int parallelism) {
    Span span = tracingRedissonHelper.buildSpan("loadAllAsync", map);
    span.setTag("replaceExistingValues", replaceExistingValues);
    span.setTag("parallelism", parallelism);
    span.setTag("keys", collectionToString(keys));
    return tracingRedissonHelper
        .prepareRFuture(span, () -> map.loadAllAsync(keys, replaceExistingValues, parallelism));
  }

  @Override
  public RFuture<Integer> valueSizeAsync(K key) {
    Span span = tracingRedissonHelper.buildSpan("valueSizeAsync", map);
    span.setTag("key", nullable(key));
    return tracingRedissonHelper.prepareRFuture(span, () -> map.valueSizeAsync(key));
  }

  @Override
  public RFuture<Map<K, V>> getAllAsync(Set<K> keys) {
    Span span = tracingRedissonHelper.buildSpan("getAllAsync", map);
    span.setTag("keys", collectionToString(keys));
    return tracingRedissonHelper.prepareRFuture(span, () -> map.getAllAsync(keys));
  }

  @Override
  public RFuture<Void> putAllAsync(Map<? extends K, ? extends V> map) {
    Span span = tracingRedissonHelper.buildSpan("putAllAsync", this.map);
    span.setTag("map", mapToString(map));
    return tracingRedissonHelper.prepareRFuture(span, () -> this.map.putAllAsync(map));
  }

  @Override
  public RFuture<Void> putAllAsync(Map<? extends K, ? extends V> map, int batchSize) {
    Span span = tracingRedissonHelper.buildSpan("putAllAsync", this.map);
    span.setTag("map", mapToString(map));
    span.setTag("batchSize", batchSize);
    return tracingRedissonHelper.prepareRFuture(span, () -> this.map.putAllAsync(map, batchSize));
  }

  @Override
  public RFuture<V> addAndGetAsync(K key, Number value) {
    Span span = tracingRedissonHelper.buildSpan("addAndGetAsync", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return tracingRedissonHelper.prepareRFuture(span, () -> map.addAndGetAsync(key, value));
  }

  @Override
  public RFuture<Boolean> containsValueAsync(Object value) {
    Span span = tracingRedissonHelper.buildSpan("containsValueAsync", map);
    span.setTag("value", nullable(value));
    return tracingRedissonHelper.prepareRFuture(span, () -> map.containsValueAsync(value));
  }

  @Override
  public RFuture<Boolean> containsKeyAsync(Object key) {
    Span span = tracingRedissonHelper.buildSpan("containsKeyAsync", map);
    span.setTag("key", nullable(key));
    return tracingRedissonHelper.prepareRFuture(span, () -> map.containsKeyAsync(key));
  }

  @Override
  public RFuture<Integer> sizeAsync() {
    Span span = tracingRedissonHelper.buildSpan("sizeAsync", map);
    return tracingRedissonHelper.prepareRFuture(span, map::sizeAsync);
  }

  @Override
  public RFuture<Long> fastRemoveAsync(K... keys) {
    Span span = tracingRedissonHelper.buildSpan("fastRemoveAsync", map);
    span.setTag("keys", Arrays.toString(keys));
    return tracingRedissonHelper.prepareRFuture(span, () -> map.fastRemoveAsync(keys));
  }

  @Override
  public RFuture<Boolean> fastPutAsync(K key, V value) {
    Span span = tracingRedissonHelper.buildSpan("fastPutAsync", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return tracingRedissonHelper.prepareRFuture(span, () -> map.fastPutAsync(key, value));
  }

  @Override
  public RFuture<Boolean> fastReplaceAsync(K key, V value) {
    Span span = tracingRedissonHelper.buildSpan("fastReplaceAsync", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return tracingRedissonHelper.prepareRFuture(span, () -> map.fastReplaceAsync(key, value));
  }

  @Override
  public RFuture<Boolean> fastPutIfAbsentAsync(K key, V value) {
    Span span = tracingRedissonHelper.buildSpan("fastPutIfAbsentAsync", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return tracingRedissonHelper.prepareRFuture(span, () -> map.fastPutIfAbsentAsync(key, value));
  }

  @Override
  public RFuture<Set<K>> readAllKeySetAsync() {
    Span span = tracingRedissonHelper.buildSpan("readAllKeySetAsync", map);
    return tracingRedissonHelper.prepareRFuture(span, map::readAllKeySetAsync);
  }

  @Override
  public RFuture<Collection<V>> readAllValuesAsync() {
    Span span = tracingRedissonHelper.buildSpan("readAllValuesAsync", map);
    return tracingRedissonHelper.prepareRFuture(span, map::readAllValuesAsync);
  }

  @Override
  public RFuture<Set<Entry<K, V>>> readAllEntrySetAsync() {
    Span span = tracingRedissonHelper.buildSpan("readAllEntrySetAsync", map);
    return tracingRedissonHelper.prepareRFuture(span, map::readAllEntrySetAsync);
  }

  @Override
  public RFuture<Map<K, V>> readAllMapAsync() {
    Span span = tracingRedissonHelper.buildSpan("readAllMapAsync", map);
    return tracingRedissonHelper.prepareRFuture(span, map::readAllMapAsync);
  }

  @Override
  public RFuture<V> getAsync(K key) {
    Span span = tracingRedissonHelper.buildSpan("getAsync", map);
    span.setTag("key", nullable(key));
    return tracingRedissonHelper.prepareRFuture(span, () -> map.getAsync(key));
  }

  @Override
  public RFuture<V> putAsync(K key, V value) {
    Span span = tracingRedissonHelper.buildSpan("putAsync", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return tracingRedissonHelper.prepareRFuture(span, () -> map.putAsync(key, value));
  }

  @Override
  public RFuture<V> removeAsync(K key) {
    Span span = tracingRedissonHelper.buildSpan("removeAsync", map);
    span.setTag("key", nullable(key));
    return tracingRedissonHelper.prepareRFuture(span, () -> map.removeAsync(key));
  }

  @Override
  public RFuture<V> replaceAsync(K key, V value) {
    Span span = tracingRedissonHelper.buildSpan("replaceAsync", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return tracingRedissonHelper.prepareRFuture(span, () -> map.replaceAsync(key, value));
  }

  @Override
  public RFuture<Boolean> replaceAsync(K key, V oldValue, V newValue) {
    Span span = tracingRedissonHelper.buildSpan("replaceAsync", map);
    span.setTag("key", nullable(key));
    span.setTag("oldValue", nullable(oldValue));
    span.setTag("newValue", nullable(newValue));
    return tracingRedissonHelper
        .prepareRFuture(span, () -> map.replaceAsync(key, oldValue, newValue));
  }

  @Override
  public RFuture<Boolean> removeAsync(Object key, Object value) {
    Span span = tracingRedissonHelper.buildSpan("removeAsync", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return tracingRedissonHelper.prepareRFuture(span, () -> map.removeAsync(key, value));
  }

  @Override
  public RFuture<V> putIfAbsentAsync(K key, V value) {
    Span span = tracingRedissonHelper.buildSpan("putIfAbsentAsync", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return tracingRedissonHelper.prepareRFuture(span, () -> map.putIfAbsentAsync(key, value));
  }
}
