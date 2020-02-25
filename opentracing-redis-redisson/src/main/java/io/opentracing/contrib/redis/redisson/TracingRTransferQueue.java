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
import java.util.concurrent.TimeUnit;
import org.redisson.api.RFuture;
import org.redisson.api.RTransferQueue;

public class TracingRTransferQueue<V> extends TracingRBlockingQueue<V> implements
    RTransferQueue<V> {
  private final RTransferQueue<V> queue;
  private final TracingRedissonHelper tracingRedissonHelper;

  public TracingRTransferQueue(RTransferQueue<V> queue,
      TracingRedissonHelper tracingRedissonHelper) {
    super(queue, tracingRedissonHelper);
    this.queue = queue;
    this.tracingRedissonHelper = tracingRedissonHelper;
  }

  @Override
  public boolean tryTransfer(V v) {
    Span span = tracingRedissonHelper.buildSpan("tryTransfer", queue);
    span.setTag("element", nullable(v));
    return tracingRedissonHelper.decorate(span, () -> queue.tryTransfer(v));
  }

  @Override
  public void transfer(V v) throws InterruptedException {
    Span span = tracingRedissonHelper.buildSpan("transfer", queue);
    span.setTag("element", nullable(v));
    tracingRedissonHelper.decorateThrowing(span, () -> queue.transfer(v));
  }

  @Override
  public boolean tryTransfer(V v, long timeout, TimeUnit unit) throws InterruptedException {
    Span span = tracingRedissonHelper.buildSpan("tryTransfer", queue);
    span.setTag("element", nullable(v));
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    return tracingRedissonHelper.decorateThrowing(span, () -> queue.tryTransfer(v, timeout, unit));
  }

  @Override
  public boolean hasWaitingConsumer() {
    Span span = tracingRedissonHelper.buildSpan("hasWaitingConsumer", queue);
    return tracingRedissonHelper.decorateThrowing(span, () -> queue.hasWaitingConsumer());
  }

  @Override
  public int getWaitingConsumerCount() {
    Span span = tracingRedissonHelper.buildSpan("getWaitingConsumerCount", queue);
    return tracingRedissonHelper.decorateThrowing(span, () -> queue.getWaitingConsumerCount());
  }


  @Override
  public RFuture<Boolean> tryTransferAsync(V e) {
    Span span = tracingRedissonHelper.buildSpan("tryTransferAsync", queue);
    span.setTag("element", nullable(e));
    return tracingRedissonHelper.prepareRFuture(span, () -> queue.tryTransferAsync(e));
  }

  @Override
  public RFuture<Void> transferAsync(V e) {
    Span span = tracingRedissonHelper.buildSpan("transferAsync", queue);
    span.setTag("element", nullable(e));
    return tracingRedissonHelper.prepareRFuture(span, () -> queue.transferAsync(e));
  }

  @Override
  public RFuture<Boolean> tryTransferAsync(V e, long timeout, TimeUnit unit) {
    Span span = tracingRedissonHelper.buildSpan("tryTransferAsync", queue);
    span.setTag("element", nullable(e));
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    return tracingRedissonHelper
        .prepareRFuture(span, () -> queue.tryTransferAsync(e, timeout, unit));
  }
}
