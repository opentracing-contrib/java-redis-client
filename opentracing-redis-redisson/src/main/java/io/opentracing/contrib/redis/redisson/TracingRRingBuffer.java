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

import io.opentracing.Span;
import org.redisson.api.RFuture;
import org.redisson.api.RRingBuffer;

public class TracingRRingBuffer<V> extends TracingRQueue<V> implements RRingBuffer<V> {
  private final RRingBuffer<V> ringBuffer;
  private final TracingRedissonHelper tracingRedissonHelper;

  public TracingRRingBuffer(RRingBuffer<V> ringBuffer,
      TracingRedissonHelper tracingRedissonHelper) {
    super(ringBuffer, tracingRedissonHelper);
    this.ringBuffer = ringBuffer;
    this.tracingRedissonHelper = tracingRedissonHelper;
  }

  @Override
  public boolean trySetCapacity(int capacity) {
    Span span = tracingRedissonHelper.buildSpan("trySetCapacity", ringBuffer);
    span.setTag("capacity", capacity);
    return tracingRedissonHelper.decorate(span, () -> ringBuffer.trySetCapacity(capacity));
  }

  @Override
  public int remainingCapacity() {
    Span span = tracingRedissonHelper.buildSpan("remainingCapacity", ringBuffer);
    return tracingRedissonHelper.decorate(span, ringBuffer::remainingCapacity);
  }

  @Override
  public int capacity() {
    Span span = tracingRedissonHelper.buildSpan("capacity", ringBuffer);
    return tracingRedissonHelper.decorate(span, ringBuffer::capacity);
  }

  @Override
  public RFuture<Boolean> trySetCapacityAsync(int capacity) {
    Span span = tracingRedissonHelper.buildSpan("trySetCapacityAsync", ringBuffer);
    span.setTag("capacity", capacity);
    return tracingRedissonHelper
        .prepareRFuture(span, () -> ringBuffer.trySetCapacityAsync(capacity));
  }

  @Override
  public RFuture<Integer> remainingCapacityAsync() {
    Span span = tracingRedissonHelper.buildSpan("remainingCapacityAsync", ringBuffer);
    return tracingRedissonHelper.prepareRFuture(span, ringBuffer::remainingCapacityAsync);
  }

  @Override
  public RFuture<Integer> capacityAsync() {
    Span span = tracingRedissonHelper.buildSpan("capacityAsync", ringBuffer);
    return tracingRedissonHelper.prepareRFuture(span, ringBuffer::capacityAsync);
  }
}
