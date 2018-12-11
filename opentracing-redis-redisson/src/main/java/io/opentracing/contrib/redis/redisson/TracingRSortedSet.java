/*
 * Copyright 2017-2018 The OpenTracing Authors
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
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.redisson.api.RFuture;
import org.redisson.api.RSortedSet;
import org.redisson.api.mapreduce.RCollectionMapReduce;

public class TracingRSortedSet<V> extends TracingRObject implements RSortedSet<V> {
  private final RSortedSet<V> set;
  private final TracingRedissonHelper tracingRedissonHelper;

  public TracingRSortedSet(RSortedSet<V> set, TracingRedissonHelper tracingRedissonHelper) {
    super(set, tracingRedissonHelper);
    this.set = set;
    this.tracingRedissonHelper = tracingRedissonHelper;
  }

  @Override
  public <KOut, VOut> RCollectionMapReduce<V, KOut, VOut> mapReduce() {
    return new TracingRCollectionMapReduce<>(set.mapReduce(), tracingRedissonHelper);
  }

  @Override
  public Collection<V> readAll() {
    Span span = tracingRedissonHelper.buildSpan("readAll", set);
    return tracingRedissonHelper.decorate(span, set::readAll);
  }

  @Override
  public RFuture<Collection<V>> readAllAsync() {
    Span span = tracingRedissonHelper.buildSpan("readAllAsync", set);
    return tracingRedissonHelper.prepareRFuture(span, set::readAllAsync);
  }

  @Override
  public RFuture<Boolean> addAsync(V value) {
    Span span = tracingRedissonHelper.buildSpan("addAsync", set);
    span.setTag("value", nullable(value));
    return tracingRedissonHelper.prepareRFuture(span, () -> set.addAsync(value));
  }

  @Override
  public RFuture<Boolean> removeAsync(Object value) {
    Span span = tracingRedissonHelper.buildSpan("removeAsync", set);
    span.setTag("value", nullable(value));
    return tracingRedissonHelper.prepareRFuture(span, () -> set.removeAsync(value));
  }

  @Override
  public boolean trySetComparator(Comparator<? super V> comparator) {
    Span span = tracingRedissonHelper.buildSpan("trySetComparator", set);
    span.setTag("comparator", nullable(comparator));
    return tracingRedissonHelper.decorate(span, () -> set.trySetComparator(comparator));
  }

  @Override
  public Comparator<? super V> comparator() {
    return set.comparator();
  }

  @Override
  public SortedSet<V> subSet(V fromElement, V toElement) {
    Span span = tracingRedissonHelper.buildSpan("subSet", set);
    span.setTag("fromElement", nullable(fromElement));
    span.setTag("toElement", nullable(toElement));
    return tracingRedissonHelper.decorate(span, () -> set.subSet(fromElement, toElement));
  }

  @Override
  public SortedSet<V> headSet(V toElement) {
    Span span = tracingRedissonHelper.buildSpan("headSet", set);
    span.setTag("toElement", nullable(toElement));
    return tracingRedissonHelper.decorate(span, () -> set.headSet(toElement));
  }

  @Override
  public SortedSet<V> tailSet(V fromElement) {
    Span span = tracingRedissonHelper.buildSpan("tailSet", set);
    span.setTag("fromElement", nullable(fromElement));
    return tracingRedissonHelper.decorate(span, () -> set.tailSet(fromElement));
  }

  @Override
  public V first() {
    Span span = tracingRedissonHelper.buildSpan("first", set);
    return tracingRedissonHelper.decorate(span, set::first);
  }

  @Override
  public V last() {
    Span span = tracingRedissonHelper.buildSpan("last", set);
    return tracingRedissonHelper.decorate(span, set::last);
  }

  @Override
  public Spliterator<V> spliterator() {
    return set.spliterator();
  }

  @Override
  public int size() {
    Span span = tracingRedissonHelper.buildSpan("size", set);
    return tracingRedissonHelper.decorate(span, set::size);
  }

  @Override
  public boolean isEmpty() {
    Span span = tracingRedissonHelper.buildSpan("isEmpty", set);
    return tracingRedissonHelper.decorate(span, set::isEmpty);
  }

  @Override
  public boolean contains(Object o) {
    Span span = tracingRedissonHelper.buildSpan("contains", set);
    span.setTag("object", nullable(o));
    return tracingRedissonHelper.decorate(span, () -> set.contains(o));
  }

  @Override
  public Iterator<V> iterator() {
    return set.iterator();
  }

  @Override
  public Object[] toArray() {
    Span span = tracingRedissonHelper.buildSpan("toArray", set);
    return tracingRedissonHelper.decorate(span, () -> set.toArray());
  }

  @Override
  public <T> T[] toArray(T[] a) {
    Span span = tracingRedissonHelper.buildSpan("toArray", set);
    return tracingRedissonHelper.decorate(span, () -> set.toArray(a));
  }

  @Override
  public boolean add(V v) {
    Span span = tracingRedissonHelper.buildSpan("add", set);
    span.setTag("element", nullable(v));
    return tracingRedissonHelper.decorate(span, () -> set.add(v));
  }

  @Override
  public boolean remove(Object o) {
    Span span = tracingRedissonHelper.buildSpan("remove", set);
    span.setTag("object", nullable(o));
    return tracingRedissonHelper.decorate(span, () -> set.remove(o));
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    Span span = tracingRedissonHelper.buildSpan("containsAll", set);
    return tracingRedissonHelper.decorate(span, () -> set.containsAll(c));
  }

  @Override
  public boolean addAll(Collection<? extends V> c) {
    Span span = tracingRedissonHelper.buildSpan("addAll", set);
    return tracingRedissonHelper.decorate(span, () -> set.addAll(c));
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    Span span = tracingRedissonHelper.buildSpan("retainAll", set);
    return tracingRedissonHelper.decorate(span, () -> set.retainAll(c));
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    Span span = tracingRedissonHelper.buildSpan("removeAll", set);
    return tracingRedissonHelper.decorate(span, () -> set.removeAll(c));
  }

  @Override
  public void clear() {
    Span span = tracingRedissonHelper.buildSpan("clear", set);
    tracingRedissonHelper.decorate(span, set::clear);
  }

  @Override
  public boolean removeIf(Predicate<? super V> filter) {
    Span span = tracingRedissonHelper.buildSpan("removeIf", set);
    span.setTag("filter", nullable(filter));
    return tracingRedissonHelper.decorate(span, () -> set.removeIf(filter));
  }

  @Override
  public Stream<V> stream() {
    return set.stream();
  }

  @Override
  public Stream<V> parallelStream() {
    return set.parallelStream();
  }

  @Override
  public void forEach(Consumer<? super V> action) {
    Span span = tracingRedissonHelper.buildSpan("forEach", set);
    span.setTag("action", nullable(action));
    tracingRedissonHelper.decorate(span, () -> set.forEach(action));
  }

}
