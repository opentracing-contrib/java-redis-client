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
import org.redisson.api.RCountDownLatch;
import org.redisson.api.RFuture;

public class TracingRCountDownLatch extends TracingRObject implements RCountDownLatch {
  private final RCountDownLatch latch;
  private final TracingRedissonHelper tracingRedissonHelper;

  public TracingRCountDownLatch(RCountDownLatch latch,
      TracingRedissonHelper tracingRedissonHelper) {
    super(latch, tracingRedissonHelper);
    this.latch = latch;
    this.tracingRedissonHelper = tracingRedissonHelper;
  }

  @Override
  public void await() throws InterruptedException {
    Span span = tracingRedissonHelper.buildSpan("await", latch);
    tracingRedissonHelper.decorateThrowing(span, () -> latch.await());
  }

  @Override
  public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
    Span span = tracingRedissonHelper.buildSpan("await", latch);
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    return tracingRedissonHelper.decorateThrowing(span, () -> latch.await(timeout, unit));
  }

  @Override
  public void countDown() {
    Span span = tracingRedissonHelper.buildSpan("countDown", latch);
    tracingRedissonHelper.decorate(span, latch::countDown);
  }

  @Override
  public long getCount() {
    Span span = tracingRedissonHelper.buildSpan("getCount", latch);
    return tracingRedissonHelper.decorate(span, latch::getCount);
  }

  @Override
  public boolean trySetCount(long count) {
    Span span = tracingRedissonHelper.buildSpan("trySetCount", latch);
    span.setTag("count", count);
    return tracingRedissonHelper.decorateThrowing(span, () -> latch.trySetCount(count));
  }

  @Override
  public RFuture<Void> awaitAsync() {
    Span span = tracingRedissonHelper.buildSpan("awaitAsync", latch);
    return tracingRedissonHelper.prepareRFuture(span, latch::awaitAsync);
  }

  @Override
  public RFuture<Boolean> awaitAsync(long waitTime, TimeUnit unit) {
    Span span = tracingRedissonHelper.buildSpan("awaitAsync", latch);
    span.setTag("waitTime", waitTime);
    span.setTag("unit", nullable(unit));
    return tracingRedissonHelper.prepareRFuture(span, () -> latch.awaitAsync(waitTime, unit));
  }

  @Override
  public RFuture<Void> countDownAsync() {
    Span span = tracingRedissonHelper.buildSpan("countDownAsync", latch);
    return tracingRedissonHelper.prepareRFuture(span, latch::countDownAsync);
  }

  @Override
  public RFuture<Long> getCountAsync() {
    Span span = tracingRedissonHelper.buildSpan("getCountAsync", latch);
    return tracingRedissonHelper.prepareRFuture(span, latch::getCountAsync);
  }

  @Override
  public RFuture<Boolean> trySetCountAsync(long count) {
    Span span = tracingRedissonHelper.buildSpan("trySetCountAsync", latch);
    return tracingRedissonHelper.prepareRFuture(span, () -> latch.trySetCountAsync(count));
  }

}
