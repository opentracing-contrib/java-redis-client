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
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.redisson.api.RCountDownLatch;
import org.redisson.api.RFuture;
import org.redisson.api.RLock;
import org.redisson.api.RPermitExpirableSemaphore;
import org.redisson.api.RReadWriteLock;
import org.redisson.api.RSemaphore;
import org.redisson.api.RSetCache;
import org.redisson.api.mapreduce.RCollectionMapReduce;

public class TracingRSetCache<V> extends TracingRExpirable implements RSetCache<V> {
  private final RSetCache<V> cache;
  private final TracingRedissonHelper tracingRedissonHelper;

  public TracingRSetCache(RSetCache<V> cache, TracingRedissonHelper tracingRedissonHelper) {
    super(cache, tracingRedissonHelper);
    this.cache = cache;
    this.tracingRedissonHelper = tracingRedissonHelper;
  }

  @Override
  public RCountDownLatch getCountDownLatch(V value) {
    return new TracingRCountDownLatch(cache.getCountDownLatch(value), tracingRedissonHelper);
  }

  @Override
  public RPermitExpirableSemaphore getPermitExpirableSemaphore(V value) {
    return new TracingRPermitExpirableSemaphore(cache.getPermitExpirableSemaphore(value),
        tracingRedissonHelper);
  }

  @Override
  public RSemaphore getSemaphore(V value) {
    return new TracingRSemaphore(cache.getSemaphore(value), tracingRedissonHelper);
  }

  @Override
  public RLock getFairLock(V value) {
    return new TracingRLock(cache.getFairLock(value), tracingRedissonHelper);
  }

  @Override
  public RReadWriteLock getReadWriteLock(V value) {
    return new TracingRReadWriteLock(cache.getReadWriteLock(value), tracingRedissonHelper);
  }

  @Override
  public RLock getLock(V value) {
    return new TracingRLock(cache.getLock(value), tracingRedissonHelper);
  }

  @Override
  public Stream<V> stream(int count) {
    Span span = tracingRedissonHelper.buildSpan("stream", cache);
    span.setTag("count", count);
    return tracingRedissonHelper.decorate(span, () -> cache.stream(count));
  }

  @Override
  public Stream<V> stream(String pattern, int count) {
    Span span = tracingRedissonHelper.buildSpan("stream", cache);
    span.setTag("pattern", nullable(pattern));
    span.setTag("count", count);
    return tracingRedissonHelper.decorate(span, () -> cache.stream(pattern, count));
  }

  @Override
  public Stream<V> stream(String pattern) {
    Span span = tracingRedissonHelper.buildSpan("stream", cache);
    span.setTag("pattern", nullable(pattern));
    return tracingRedissonHelper.decorate(span, () -> cache.stream(pattern));
  }

  @Override
  public Iterator<V> iterator(int count) {
    return cache.iterator(count);
  }

  @Override
  public Iterator<V> iterator(String pattern, int count) {
    return cache.iterator(pattern, count);
  }

  @Override
  public Iterator<V> iterator(String pattern) {
    return cache.iterator(pattern);
  }

  @Override
  public <KOut, VOut> RCollectionMapReduce<V, KOut, VOut> mapReduce() {
    return new TracingRCollectionMapReduce<>(cache.mapReduce(), tracingRedissonHelper);
  }

  @Override
  public boolean add(V value, long ttl, TimeUnit unit) {
    Span span = tracingRedissonHelper.buildSpan("add", cache);
    span.setTag("value", nullable(value));
    span.setTag("ttl", ttl);
    span.setTag("unit", nullable(unit));
    return tracingRedissonHelper.decorate(span, () -> cache.add(value, ttl, unit));
  }

  @Override
  public int size() {
    Span span = tracingRedissonHelper.buildSpan("size", cache);
    return tracingRedissonHelper.decorate(span, cache::size);
  }

  @Override
  public Set<V> readAll() {
    Span span = tracingRedissonHelper.buildSpan("readAll", cache);
    return tracingRedissonHelper.decorate(span, cache::readAll);
  }

  @Override
  public boolean isEmpty() {
    Span span = tracingRedissonHelper.buildSpan("isEmpty", cache);
    return tracingRedissonHelper.decorate(span, cache::isEmpty);
  }

  @Override
  public boolean contains(Object o) {
    Span span = tracingRedissonHelper.buildSpan("contains", cache);
    span.setTag("object", nullable(o));
    return tracingRedissonHelper.decorate(span, () -> cache.contains(o));
  }

  @Override
  public Iterator<V> iterator() {
    return cache.iterator();
  }

  @Override
  public Object[] toArray() {
    Span span = tracingRedissonHelper.buildSpan("toArray", cache);
    return tracingRedissonHelper.decorate(span, () -> cache.toArray());
  }

  @Override
  public <T> T[] toArray(T[] a) {
    Span span = tracingRedissonHelper.buildSpan("toArray", cache);
    return tracingRedissonHelper.decorate(span, () -> cache.toArray(a));
  }

  @Override
  public boolean add(V element) {
    Span span = tracingRedissonHelper.buildSpan("add", cache);
    span.setTag("element", nullable(element));
    return tracingRedissonHelper.decorate(span, () -> cache.add(element));
  }

  @Override
  public boolean remove(Object o) {
    Span span = tracingRedissonHelper.buildSpan("remove", cache);
    span.setTag("object", nullable(o));
    return tracingRedissonHelper.decorate(span, () -> cache.remove(o));
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    Span span = tracingRedissonHelper.buildSpan("containsAll", cache);
    return tracingRedissonHelper.decorate(span, () -> cache.containsAll(c));
  }

  @Override
  public boolean addAll(Collection<? extends V> c) {
    Span span = tracingRedissonHelper.buildSpan("addAll", cache);
    return tracingRedissonHelper.decorate(span, () -> cache.addAll(c));
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    Span span = tracingRedissonHelper.buildSpan("retainAll", cache);
    return tracingRedissonHelper.decorate(span, () -> cache.retainAll(c));
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    Span span = tracingRedissonHelper.buildSpan("removeAll", cache);
    return tracingRedissonHelper.decorate(span, () -> cache.removeAll(c));
  }

  @Override
  public void clear() {
    Span span = tracingRedissonHelper.buildSpan("clear", cache);
    tracingRedissonHelper.decorate(span, cache::clear);
  }

  @Override
  public Spliterator<V> spliterator() {
    return cache.spliterator();
  }

  @Override
  public boolean removeIf(Predicate<? super V> filter) {
    Span span = tracingRedissonHelper.buildSpan("removeIf", cache);
    span.setTag("filter", nullable(filter));
    return tracingRedissonHelper.decorate(span, () -> cache.removeIf(filter));
  }

  @Override
  public Stream<V> stream() {
    return cache.stream();
  }

  @Override
  public Stream<V> parallelStream() {
    return cache.parallelStream();
  }

  @Override
  public void forEach(Consumer<? super V> action) {
    Span span = tracingRedissonHelper.buildSpan("forEach", cache);
    span.setTag("action", nullable(action));
    tracingRedissonHelper.decorate(span, () -> cache.forEach(action));
  }

  @Override
  public RFuture<Boolean> addAsync(V value, long ttl, TimeUnit unit) {
    Span span = tracingRedissonHelper.buildSpan("addAsync", cache);
    span.setTag("value", nullable(value));
    span.setTag("ttl", ttl);
    span.setTag("unit", nullable(unit));
    return tracingRedissonHelper.prepareRFuture(span, () -> cache.addAsync(value, ttl, unit));
  }

  @Override
  public RFuture<Integer> sizeAsync() {
    Span span = tracingRedissonHelper.buildSpan("sizeAsync", cache);
    return tracingRedissonHelper.prepareRFuture(span, cache::sizeAsync);
  }

  @Override
  public RFuture<Set<V>> readAllAsync() {
    Span span = tracingRedissonHelper.buildSpan("readAllAsync", cache);
    return tracingRedissonHelper.prepareRFuture(span, cache::readAllAsync);
  }

  @Override
  public RFuture<Boolean> retainAllAsync(Collection<?> c) {
    Span span = tracingRedissonHelper.buildSpan("retainAllAsync", cache);
    return tracingRedissonHelper.prepareRFuture(span, () -> cache.retainAllAsync(c));
  }

  @Override
  public RFuture<Boolean> removeAllAsync(Collection<?> c) {
    Span span = tracingRedissonHelper.buildSpan("removeAllAsync", cache);
    return tracingRedissonHelper.prepareRFuture(span, () -> cache.removeAllAsync(c));
  }

  @Override
  public RFuture<Boolean> containsAsync(Object o) {
    Span span = tracingRedissonHelper.buildSpan("containsAsync", cache);
    span.setTag("object", nullable(o));
    return tracingRedissonHelper.prepareRFuture(span, () -> cache.containsAsync(o));
  }

  @Override
  public RFuture<Boolean> containsAllAsync(Collection<?> c) {
    Span span = tracingRedissonHelper.buildSpan("containsAllAsync", cache);
    return tracingRedissonHelper.prepareRFuture(span, () -> cache.containsAllAsync(c));
  }

  @Override
  public RFuture<Boolean> removeAsync(Object o) {
    Span span = tracingRedissonHelper.buildSpan("removeAsync", cache);
    span.setTag("object", nullable(o));
    return tracingRedissonHelper.prepareRFuture(span, () -> cache.removeAsync(o));
  }

  @Override
  public RFuture<Boolean> addAsync(V e) {
    Span span = tracingRedissonHelper.buildSpan("addAsync", cache);
    span.setTag("element", nullable(e));
    return tracingRedissonHelper.prepareRFuture(span, () -> cache.addAsync(e));
  }

  @Override
  public RFuture<Boolean> addAllAsync(Collection<? extends V> c) {
    Span span = tracingRedissonHelper.buildSpan("addAllAsync", cache);
    return tracingRedissonHelper.prepareRFuture(span, () -> cache.addAllAsync(c));
  }

  @Override
  public void destroy() {
    Span span = tracingRedissonHelper.buildSpan("destroy", cache);
    tracingRedissonHelper.decorate(span, cache::destroy);
  }


}
