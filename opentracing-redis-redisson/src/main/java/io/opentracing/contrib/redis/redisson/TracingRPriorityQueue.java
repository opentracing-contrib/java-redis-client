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
package io.opentracing.contrib.redis.redisson;

import static io.opentracing.contrib.redis.common.TracingHelper.nullable;

import io.opentracing.Span;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.redisson.api.RPriorityQueue;

public class TracingRPriorityQueue<V> extends TracingRObject implements RPriorityQueue<V> {
  private final RPriorityQueue<V> queue;
  private final TracingRedissonHelper tracingRedissonHelper;

  public TracingRPriorityQueue(RPriorityQueue<V> queue,
      TracingRedissonHelper tracingRedissonHelper) {
    super(queue, tracingRedissonHelper);
    this.queue = queue;
    this.tracingRedissonHelper = tracingRedissonHelper;
  }

  @Override
  public Comparator<? super V> comparator() {
    return queue.comparator();
  }

  @Override
  public List<V> readAll() {
    Span span = tracingRedissonHelper.buildSpan("readAll", queue);
    return tracingRedissonHelper.decorate(span, queue::readAll);
  }

  @Override
  public V pollLastAndOfferFirstTo(String dequeName) {
    Span span = tracingRedissonHelper.buildSpan("pollLastAndOfferFirstTo", queue);
    span.setTag("dequeName", nullable(dequeName));
    return tracingRedissonHelper.decorate(span, () -> queue.pollLastAndOfferFirstTo(dequeName));
  }

  @Override
  public boolean trySetComparator(Comparator<? super V> comparator) {
    Span span = tracingRedissonHelper.buildSpan("trySetComparator", queue);
    span.setTag("comparator", nullable(comparator));
    return tracingRedissonHelper.decorate(span, () -> queue.trySetComparator(comparator));
  }

  @Override
  public boolean add(V v) {
    Span span = tracingRedissonHelper.buildSpan("add", queue);
    span.setTag("element", nullable(v));
    return tracingRedissonHelper.decorate(span, () -> queue.add(v));
  }

  @Override
  public boolean offer(V v) {
    Span span = tracingRedissonHelper.buildSpan("offer", queue);
    span.setTag("element", nullable(v));
    return tracingRedissonHelper.decorate(span, () -> queue.offer(v));
  }

  @Override
  public V remove() {
    Span span = tracingRedissonHelper.buildSpan("remove", queue);
    return tracingRedissonHelper.decorate(span, () -> queue.remove());
  }

  @Override
  public V poll() {
    Span span = tracingRedissonHelper.buildSpan("poll", queue);
    return tracingRedissonHelper.decorate(span, queue::poll);
  }

  @Override
  public V element() {
    Span span = tracingRedissonHelper.buildSpan("element", queue);
    return tracingRedissonHelper.decorate(span, queue::element);
  }

  @Override
  public V peek() {
    Span span = tracingRedissonHelper.buildSpan("peek", queue);
    return tracingRedissonHelper.decorate(span, queue::peek);
  }

  @Override
  public int size() {
    Span span = tracingRedissonHelper.buildSpan("size", queue);
    return tracingRedissonHelper.decorate(span, queue::size);
  }

  @Override
  public boolean isEmpty() {
    Span span = tracingRedissonHelper.buildSpan("isEmpty", queue);
    return tracingRedissonHelper.decorate(span, queue::isEmpty);
  }

  @Override
  public boolean contains(Object o) {
    Span span = tracingRedissonHelper.buildSpan("contains", queue);
    span.setTag("object", nullable(o));
    return tracingRedissonHelper.decorate(span, () -> queue.contains(o));
  }

  @Override
  public Iterator<V> iterator() {
    return queue.iterator();
  }

  @Override
  public Object[] toArray() {
    Span span = tracingRedissonHelper.buildSpan("toArray", queue);
    return tracingRedissonHelper.decorate(span, () -> queue.toArray());
  }

  @Override
  public <T> T[] toArray(T[] a) {
    Span span = tracingRedissonHelper.buildSpan("toArray", queue);
    return tracingRedissonHelper.decorate(span, () -> queue.toArray(a));
  }

  @Override
  public boolean remove(Object o) {
    Span span = tracingRedissonHelper.buildSpan("remove", queue);
    span.setTag("object", nullable(o));
    return tracingRedissonHelper.decorate(span, () -> queue.remove(o));
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    Span span = tracingRedissonHelper.buildSpan("containsAll", queue);
    return tracingRedissonHelper.decorate(span, () -> queue.containsAll(c));
  }

  @Override
  public boolean addAll(Collection<? extends V> c) {
    Span span = tracingRedissonHelper.buildSpan("addAll", queue);
    return tracingRedissonHelper.decorate(span, () -> queue.addAll(c));
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    Span span = tracingRedissonHelper.buildSpan("removeAll", queue);
    return tracingRedissonHelper.decorate(span, () -> queue.removeAll(c));
  }

  @Override
  public boolean removeIf(Predicate<? super V> filter) {
    Span span = tracingRedissonHelper.buildSpan("removeIf", queue);
    span.setTag("filter", nullable(filter));
    return tracingRedissonHelper.decorate(span, () -> queue.removeIf(filter));
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    Span span = tracingRedissonHelper.buildSpan("retainAll", queue);
    return tracingRedissonHelper.decorate(span, () -> queue.retainAll(c));
  }

  @Override
  public void clear() {
    Span span = tracingRedissonHelper.buildSpan("clear", queue);
    tracingRedissonHelper.decorate(span, queue::clear);
  }

  @Override
  public Spliterator<V> spliterator() {
    return queue.spliterator();
  }

  @Override
  public Stream<V> stream() {
    return queue.stream();
  }

  @Override
  public Stream<V> parallelStream() {
    return queue.parallelStream();
  }

  @Override
  public void forEach(Consumer<? super V> action) {
    Span span = tracingRedissonHelper.buildSpan("forEach", queue);
    span.setTag("action", nullable(action));
    tracingRedissonHelper.decorate(span, () -> queue.forEach(action));
  }

}
