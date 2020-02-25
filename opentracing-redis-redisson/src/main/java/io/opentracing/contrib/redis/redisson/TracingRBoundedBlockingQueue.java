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
import org.redisson.api.RBoundedBlockingQueue;
import org.redisson.api.RFuture;

public class TracingRBoundedBlockingQueue<V> extends TracingRBlockingQueue<V> implements
    RBoundedBlockingQueue<V> {
  private final RBoundedBlockingQueue<V> queue;
  private final TracingRedissonHelper tracingRedissonHelper;

  public TracingRBoundedBlockingQueue(RBoundedBlockingQueue<V> queue,
      TracingRedissonHelper tracingRedissonHelper) {
    super(queue, tracingRedissonHelper);
    this.queue = queue;
    this.tracingRedissonHelper = tracingRedissonHelper;
  }

  @Override
  public boolean trySetCapacity(int capacity) {
    Span span = tracingRedissonHelper.buildSpan("trySetCapacity", queue);
    span.setTag("capacity", capacity);
    return tracingRedissonHelper.decorate(span, () -> queue.trySetCapacity(capacity));
  }

  @Override
  public RFuture<Boolean> trySetCapacityAsync(int capacity) {
    Span span = tracingRedissonHelper.buildSpan("trySetCapacityAsync", queue);
    span.setTag("capacity", capacity);
    return tracingRedissonHelper.prepareRFuture(span, () -> queue.trySetCapacityAsync(capacity));
  }

  @Override
  public RFuture<Boolean> offerAsync(V e, long timeout, TimeUnit unit) {
    Span span = tracingRedissonHelper.buildSpan("offerAsync", queue);
    span.setTag("element", nullable(e));
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    return tracingRedissonHelper.prepareRFuture(span, () -> queue.offerAsync(e, timeout, unit));
  }

}
