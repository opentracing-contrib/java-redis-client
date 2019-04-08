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
import java.util.Iterator;
import java.util.stream.Stream;
import org.redisson.api.RPriorityDeque;


public class TracingRPriorityDeque<V> extends TracingRPriorityQueue<V> implements
    RPriorityDeque<V> {

  private final RPriorityDeque<V> deque;
  private final TracingRedissonHelper tracingRedissonHelper;

  public TracingRPriorityDeque(RPriorityDeque<V> deque,
      TracingRedissonHelper tracingRedissonHelper) {
    super(deque, tracingRedissonHelper);
    this.deque = deque;
    this.tracingRedissonHelper = tracingRedissonHelper;
  }

  @Override
  public void addFirst(V v) {
    Span span = tracingRedissonHelper.buildSpan("addFirst", deque);
    span.setTag("element", nullable(v));
    tracingRedissonHelper.decorate(span, () -> deque.addFirst(v));
  }

  @Override
  public void addLast(V v) {
    Span span = tracingRedissonHelper.buildSpan("addLast", deque);
    span.setTag("element", nullable(v));
    tracingRedissonHelper.decorate(span, () -> deque.addLast(v));
  }

  @Override
  public boolean offerFirst(V v) {
    Span span = tracingRedissonHelper.buildSpan("offerFirst", deque);
    span.setTag("element", nullable(v));
    return tracingRedissonHelper.decorate(span, () -> deque.offerFirst(v));
  }

  @Override
  public boolean offerLast(V v) {
    Span span = tracingRedissonHelper.buildSpan("offerLast", deque);
    span.setTag("element", nullable(v));
    return tracingRedissonHelper.decorate(span, () -> deque.offerLast(v));
  }

  @Override
  public V removeFirst() {
    Span span = tracingRedissonHelper.buildSpan("removeFirst", deque);
    return tracingRedissonHelper.decorate(span, deque::removeFirst);
  }

  @Override
  public V removeLast() {
    Span span = tracingRedissonHelper.buildSpan("removeLast", deque);
    return tracingRedissonHelper.decorate(span, deque::removeLast);
  }

  @Override
  public V pollFirst() {
    Span span = tracingRedissonHelper.buildSpan("pollFirst", deque);
    return tracingRedissonHelper.decorate(span, deque::pollFirst);
  }

  @Override
  public V pollLast() {
    Span span = tracingRedissonHelper.buildSpan("pollLast", deque);
    return tracingRedissonHelper.decorate(span, deque::pollLast);
  }

  @Override
  public V getFirst() {
    Span span = tracingRedissonHelper.buildSpan("getFirst", deque);
    return tracingRedissonHelper.decorate(span, deque::getFirst);
  }

  @Override
  public V getLast() {
    Span span = tracingRedissonHelper.buildSpan("getLast", deque);
    return tracingRedissonHelper.decorate(span, deque::getLast);
  }

  @Override
  public V peekFirst() {
    Span span = tracingRedissonHelper.buildSpan("peekFirst", deque);
    return tracingRedissonHelper.decorate(span, deque::peekFirst);
  }

  @Override
  public V peekLast() {
    Span span = tracingRedissonHelper.buildSpan("peekLast", deque);
    return tracingRedissonHelper.decorate(span, deque::peekLast);
  }

  @Override
  public boolean removeFirstOccurrence(Object o) {
    Span span = tracingRedissonHelper.buildSpan("removeFirstOccurrence", deque);
    span.setTag("object", nullable(o));
    return tracingRedissonHelper.decorate(span, () -> deque.removeFirstOccurrence(o));
  }

  @Override
  public boolean removeLastOccurrence(Object o) {
    Span span = tracingRedissonHelper.buildSpan("removeLastOccurrence", deque);
    span.setTag("object", nullable(o));
    return tracingRedissonHelper.decorate(span, () -> deque.removeLastOccurrence(o));
  }

  @Override
  public boolean add(V v) {
    Span span = tracingRedissonHelper.buildSpan("add", deque);
    span.setTag("element", nullable(v));
    return tracingRedissonHelper.decorate(span, () -> deque.add(v));
  }

  @Override
  public boolean offer(V v) {
    Span span = tracingRedissonHelper.buildSpan("offer", deque);
    span.setTag("element", nullable(v));
    return tracingRedissonHelper.decorate(span, () -> deque.offer(v));
  }

  @Override
  public V remove() {
    Span span = tracingRedissonHelper.buildSpan("remove", deque);
    return tracingRedissonHelper.decorate(span, () -> deque.remove());
  }

  @Override
  public V poll() {
    Span span = tracingRedissonHelper.buildSpan("poll", deque);
    return tracingRedissonHelper.decorate(span, deque::poll);
  }

  @Override
  public V element() {
    Span span = tracingRedissonHelper.buildSpan("element", deque);
    return tracingRedissonHelper.decorate(span, deque::element);
  }

  @Override
  public V peek() {
    Span span = tracingRedissonHelper.buildSpan("peek", deque);
    return tracingRedissonHelper.decorate(span, deque::peek);
  }

  @Override
  public void push(V v) {
    Span span = tracingRedissonHelper.buildSpan("push", deque);
    span.setTag("element", nullable(v));
    tracingRedissonHelper.decorate(span, () -> deque.push(v));
  }

  @Override
  public V pop() {
    Span span = tracingRedissonHelper.buildSpan("pop", deque);
    return tracingRedissonHelper.decorate(span, deque::pop);
  }

  @Override
  public boolean remove(Object o) {
    Span span = tracingRedissonHelper.buildSpan("remove", deque);
    span.setTag("object", nullable(o));
    return tracingRedissonHelper.decorate(span, () -> deque.remove(o));
  }

  @Override
  public boolean contains(Object o) {
    Span span = tracingRedissonHelper.buildSpan("contains", deque);
    span.setTag("object", nullable(o));
    return tracingRedissonHelper.decorate(span, () -> deque.contains(o));
  }

  @Override
  public int size() {
    Span span = tracingRedissonHelper.buildSpan("size", deque);
    return tracingRedissonHelper.decorate(span, deque::size);
  }

  @Override
  public Iterator<V> iterator() {
    return deque.iterator();
  }

  @Override
  public Iterator<V> descendingIterator() {
    return deque.descendingIterator();
  }


  @Override
  public Stream<V> descendingStream() {
    Span span = tracingRedissonHelper.buildSpan("descendingStream", deque);
    return tracingRedissonHelper.decorate(span, deque::descendingStream);
  }
}
