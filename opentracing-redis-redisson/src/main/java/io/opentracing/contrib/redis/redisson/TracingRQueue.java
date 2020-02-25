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
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.redisson.api.RFuture;
import org.redisson.api.RQueue;

public class TracingRQueue<V> extends TracingRExpirable implements RQueue<V> {
  private final RQueue<V> queue;
  private final TracingRedissonHelper tracingRedissonHelper;

  public TracingRQueue(RQueue<V> queue, TracingRedissonHelper tracingRedissonHelper) {
    super(queue, tracingRedissonHelper);
    this.queue = queue;
    this.tracingRedissonHelper = tracingRedissonHelper;
  }

  @Override
  public V pollLastAndOfferFirstTo(String dequeName) {
    Span span = tracingRedissonHelper.buildSpan("pollLastAndOfferFirstTo", queue);
    span.setTag("dequeName", nullable(dequeName));
    return tracingRedissonHelper.decorate(span, () -> queue.pollLastAndOfferFirstTo(dequeName));
  }

  @Override
  public List<V> readAll() {
    Span span = tracingRedissonHelper.buildSpan("readAll", queue);
    return tracingRedissonHelper.decorate(span, queue::readAll);
  }

  @Override
  public List<V> poll(int limit) {
    Span span = tracingRedissonHelper.buildSpan("poll", queue);
    span.setTag("limit", limit);
    return tracingRedissonHelper.decorate(span, () -> queue.poll(limit));
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
    return tracingRedissonHelper.decorate(span, () -> queue.poll());
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

  @Override
  public RFuture<V> peekAsync() {
    Span span = tracingRedissonHelper.buildSpan("peekAsync", queue);
    return tracingRedissonHelper.prepareRFuture(span, queue::peekAsync);
  }

  @Override
  public RFuture<V> pollAsync() {
    Span span = tracingRedissonHelper.buildSpan("pollAsync", queue);
    return tracingRedissonHelper.prepareRFuture(span, queue::pollAsync);
  }

  @Override
  public RFuture<Boolean> offerAsync(V e) {
    Span span = tracingRedissonHelper.buildSpan("offerAsync", queue);
    span.setTag("element", nullable(e));
    return tracingRedissonHelper.prepareRFuture(span, () -> queue.offerAsync(e));
  }

  @Override
  public RFuture<V> pollLastAndOfferFirstToAsync(String queueName) {
    Span span = tracingRedissonHelper.buildSpan("pollLastAndOfferFirstToAsync", queue);
    span.setTag("queueName", nullable(queueName));
    return tracingRedissonHelper
        .prepareRFuture(span, () -> queue.pollLastAndOfferFirstToAsync(queueName));
  }

  @Override
  public RFuture<List<V>> readAllAsync() {
    Span span = tracingRedissonHelper.buildSpan("readAllAsync", queue);
    return tracingRedissonHelper.prepareRFuture(span, queue::readAllAsync);
  }

  @Override
  public RFuture<List<V>> pollAsync(int limit) {
    Span span = tracingRedissonHelper.buildSpan("pollAsync", queue);
    span.setTag("limit", limit);
    return tracingRedissonHelper.prepareRFuture(span, () -> queue.pollAsync(limit));
  }

  @Override
  public RFuture<Boolean> retainAllAsync(Collection<?> c) {
    Span span = tracingRedissonHelper.buildSpan("retainAllAsync", queue);
    return tracingRedissonHelper.prepareRFuture(span, () -> queue.retainAllAsync(c));
  }

  @Override
  public RFuture<Boolean> removeAllAsync(Collection<?> c) {
    Span span = tracingRedissonHelper.buildSpan("removeAllAsync", queue);
    return tracingRedissonHelper.prepareRFuture(span, () -> queue.removeAllAsync(c));
  }

  @Override
  public RFuture<Boolean> containsAsync(Object o) {
    Span span = tracingRedissonHelper.buildSpan("containsAsync", queue);
    span.setTag("object", nullable(o));
    return tracingRedissonHelper.prepareRFuture(span, () -> queue.containsAsync(o));
  }

  @Override
  public RFuture<Boolean> containsAllAsync(Collection<?> c) {
    Span span = tracingRedissonHelper.buildSpan("containsAllAsync", queue);
    return tracingRedissonHelper.prepareRFuture(span, () -> queue.containsAllAsync(c));
  }

  @Override
  public RFuture<Boolean> removeAsync(Object o) {
    Span span = tracingRedissonHelper.buildSpan("removeAsync", queue);
    span.setTag("object", nullable(o));
    return tracingRedissonHelper.prepareRFuture(span, () -> queue.removeAsync(o));
  }

  @Override
  public RFuture<Integer> sizeAsync() {
    Span span = tracingRedissonHelper.buildSpan("sizeAsync", queue);
    return tracingRedissonHelper.prepareRFuture(span, queue::sizeAsync);
  }

  @Override
  public RFuture<Boolean> addAsync(V e) {
    Span span = tracingRedissonHelper.buildSpan("addAsync", queue);
    span.setTag("element", nullable(e));
    return tracingRedissonHelper.prepareRFuture(span, () -> queue.addAsync(e));
  }

  @Override
  public RFuture<Boolean> addAllAsync(Collection<? extends V> c) {
    Span span = tracingRedissonHelper.buildSpan("addAllAsync", queue);
    return tracingRedissonHelper.prepareRFuture(span, () -> queue.addAllAsync(c));
  }
}
