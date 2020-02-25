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
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.redisson.api.RBlockingQueue;
import org.redisson.api.RFuture;

public class TracingRBlockingQueue<V> extends TracingRQueue<V> implements RBlockingQueue<V> {
  private final RBlockingQueue<V> queue;
  private final TracingRedissonHelper tracingRedissonHelper;

  public TracingRBlockingQueue(RBlockingQueue<V> queue,
      TracingRedissonHelper tracingRedissonHelper) {
    super(queue, tracingRedissonHelper);
    this.queue = queue;
    this.tracingRedissonHelper = tracingRedissonHelper;
  }

  @Override
  public V pollFromAny(long timeout, TimeUnit unit, String... queueNames)
      throws InterruptedException {
    Span span = tracingRedissonHelper.buildSpan("pollFromAny", queue);
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    span.setTag("queueNames", Arrays.toString(queueNames));
    return tracingRedissonHelper
        .decorateThrowing(span, () -> queue.pollFromAny(timeout, unit, queueNames));
  }

  @Override
  public V pollLastAndOfferFirstTo(String queueName, long timeout, TimeUnit unit)
      throws InterruptedException {
    Span span = tracingRedissonHelper.buildSpan("pollLastAndOfferFirstTo", queue);
    span.setTag("queueName", nullable(queueName));
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    return tracingRedissonHelper
        .decorateThrowing(span, () -> queue.pollLastAndOfferFirstTo(queueName, timeout, unit));
  }

  @Override
  public V takeLastAndOfferFirstTo(String queueName) throws InterruptedException {
    Span span = tracingRedissonHelper.buildSpan("takeLastAndOfferFirstTo", queue);
    span.setTag("queueName", nullable(queueName));
    return tracingRedissonHelper
        .decorateThrowing(span, () -> queue.takeLastAndOfferFirstTo(queueName));
  }

  @Override
  public int subscribeOnElements(Consumer<V> consumer) {
    Span span = tracingRedissonHelper.buildSpan("subscribeOnElements", queue);
    return tracingRedissonHelper.decorate(span, () -> queue.subscribeOnElements(consumer));
  }

  @Override
  public void unsubscribe(int listenerId) {
    Span span = tracingRedissonHelper.buildSpan("unsubscribe", queue);
    span.setTag("listenerId", listenerId);
    tracingRedissonHelper.decorate(span, () -> queue.unsubscribe(listenerId));
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
  public void put(V v) throws InterruptedException {
    Span span = tracingRedissonHelper.buildSpan("put", queue);
    span.setTag("element", nullable(v));
    tracingRedissonHelper.decorateThrowing(span, () -> queue.put(v));
  }

  @Override
  public boolean offer(V v, long timeout, TimeUnit unit) throws InterruptedException {
    Span span = tracingRedissonHelper.buildSpan("offer", queue);
    span.setTag("element", nullable(v));
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    return tracingRedissonHelper.decorateThrowing(span, () -> queue.offer(v, timeout, unit));
  }

  @Override
  public V take() throws InterruptedException {
    Span span = tracingRedissonHelper.buildSpan("take", queue);
    return tracingRedissonHelper.decorateThrowing(span, queue::take);
  }

  @Override
  public V poll(long timeout, TimeUnit unit) throws InterruptedException {
    Span span = tracingRedissonHelper.buildSpan("poll", queue);
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    return tracingRedissonHelper.decorateThrowing(span, () -> queue.poll(timeout, unit));
  }

  @Override
  public int remainingCapacity() {
    Span span = tracingRedissonHelper.buildSpan("remainingCapacity", queue);
    return tracingRedissonHelper.decorate(span, queue::remainingCapacity);
  }

  @Override
  public boolean remove(Object o) {
    Span span = tracingRedissonHelper.buildSpan("remove", queue);
    span.setTag("object", nullable(o));
    return tracingRedissonHelper.decorate(span, () -> queue.remove(o));
  }

  @Override
  public boolean contains(Object o) {
    Span span = tracingRedissonHelper.buildSpan("contains", queue);
    span.setTag("object", nullable(o));
    return tracingRedissonHelper.decorate(span, () -> queue.contains(o));
  }

  @Override
  public int drainTo(Collection<? super V> c) {
    Span span = tracingRedissonHelper.buildSpan("drainTo", queue);
    return tracingRedissonHelper.decorate(span, () -> queue.drainTo(c));
  }

  @Override
  public int drainTo(Collection<? super V> c, int maxElements) {
    Span span = tracingRedissonHelper.buildSpan("drainTo", queue);
    span.setTag("maxElements", maxElements);
    return tracingRedissonHelper.decorate(span, () -> queue.drainTo(c, maxElements));
  }

  @Override
  public RFuture<V> pollFromAnyAsync(long timeout, TimeUnit unit, String... queueNames) {
    Span span = tracingRedissonHelper.buildSpan("pollFromAnyAsync", queue);
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    span.setTag("queueNames", Arrays.toString(queueNames));
    return tracingRedissonHelper
        .prepareRFuture(span, () -> queue.pollFromAnyAsync(timeout, unit, queueNames));
  }

  @Override
  public RFuture<Integer> drainToAsync(Collection<? super V> c, int maxElements) {
    Span span = tracingRedissonHelper.buildSpan("drainToAsync", queue);
    span.setTag("maxElements", maxElements);
    return tracingRedissonHelper.prepareRFuture(span, () -> queue.drainToAsync(c, maxElements));
  }

  @Override
  public RFuture<Integer> drainToAsync(Collection<? super V> c) {
    Span span = tracingRedissonHelper.buildSpan("drainToAsync", queue);
    return tracingRedissonHelper.prepareRFuture(span, () -> queue.drainToAsync(c));
  }

  @Override
  public RFuture<V> pollLastAndOfferFirstToAsync(String queueName, long timeout, TimeUnit unit) {
    Span span = tracingRedissonHelper.buildSpan("pollLastAndOfferFirstToAsync", queue);
    span.setTag("queueName", nullable(queueName));
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    return tracingRedissonHelper
        .prepareRFuture(span, () -> queue.pollLastAndOfferFirstToAsync(queueName, timeout, unit));
  }

  @Override
  public RFuture<V> takeLastAndOfferFirstToAsync(String queueName) {
    Span span = tracingRedissonHelper.buildSpan("takeLastAndOfferFirstToAsync", queue);
    span.setTag("queueName", nullable(queueName));
    return tracingRedissonHelper
        .prepareRFuture(span, () -> queue.takeLastAndOfferFirstToAsync(queueName));
  }

  @Override
  public RFuture<V> pollAsync(long timeout, TimeUnit unit) {
    Span span = tracingRedissonHelper.buildSpan("pollAsync", queue);
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    return tracingRedissonHelper.prepareRFuture(span, () -> queue.pollAsync(timeout, unit));
  }

  @Override
  public RFuture<V> takeAsync() {
    Span span = tracingRedissonHelper.buildSpan("takeAsync", queue);
    return tracingRedissonHelper.prepareRFuture(span, queue::takeAsync);
  }

  @Override
  public RFuture<Void> putAsync(V e) {
    Span span = tracingRedissonHelper.buildSpan("putAsync", queue);
    span.setTag("element", nullable(e));
    return tracingRedissonHelper.prepareRFuture(span, () -> queue.putAsync(e));
  }

}
