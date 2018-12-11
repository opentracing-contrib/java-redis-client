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
import java.util.concurrent.TimeUnit;
import org.redisson.api.RDoubleAdder;
import org.redisson.api.RFuture;

public class TracingRDoubleAdder extends TracingRExpirable implements RDoubleAdder {
  private final RDoubleAdder doubleAdder;
  private final TracingRedissonHelper tracingRedissonHelper;

  public TracingRDoubleAdder(RDoubleAdder doubleAdder,
      TracingRedissonHelper tracingRedissonHelper) {
    super(doubleAdder, tracingRedissonHelper);
    this.doubleAdder = doubleAdder;
    this.tracingRedissonHelper = tracingRedissonHelper;
  }

  @Override
  public void add(double x) {
    Span span = tracingRedissonHelper.buildSpan("add", doubleAdder);
    span.setTag("value", x);
    tracingRedissonHelper.decorate(span, () -> doubleAdder.add(x));
  }

  @Override
  public void increment() {
    Span span = tracingRedissonHelper.buildSpan("increment", doubleAdder);
    tracingRedissonHelper.decorate(span, doubleAdder::increment);
  }

  @Override
  public void decrement() {
    Span span = tracingRedissonHelper.buildSpan("decrement", doubleAdder);
    tracingRedissonHelper.decorate(span, doubleAdder::decrement);
  }

  @Override
  public double sum() {
    Span span = tracingRedissonHelper.buildSpan("sum", doubleAdder);
    return tracingRedissonHelper.decorate(span, doubleAdder::sum);
  }

  @Override
  public void reset() {
    Span span = tracingRedissonHelper.buildSpan("reset", doubleAdder);
    tracingRedissonHelper.decorate(span, doubleAdder::reset);
  }

  @Override
  public RFuture<Double> sumAsync() {
    Span span = tracingRedissonHelper.buildSpan("sumAsync", doubleAdder);
    return tracingRedissonHelper.prepareRFuture(span, doubleAdder::sumAsync);
  }

  @Override
  public RFuture<Double> sumAsync(long timeout, TimeUnit timeUnit) {
    Span span = tracingRedissonHelper.buildSpan("sumAsync", doubleAdder);
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    return tracingRedissonHelper
        .prepareRFuture(span, () -> doubleAdder.sumAsync(timeout, timeUnit));
  }

  @Override
  public RFuture<Void> resetAsync() {
    Span span = tracingRedissonHelper.buildSpan("resetAsync", doubleAdder);
    return tracingRedissonHelper.prepareRFuture(span, doubleAdder::resetAsync);
  }

  @Override
  public RFuture<Void> resetAsync(long timeout, TimeUnit timeUnit) {
    Span span = tracingRedissonHelper.buildSpan("resetAsync", doubleAdder);
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    return tracingRedissonHelper
        .prepareRFuture(span, () -> doubleAdder.resetAsync(timeout, timeUnit));
  }

  @Override
  public void destroy() {
    Span span = tracingRedissonHelper.buildSpan("destroy", doubleAdder);
    tracingRedissonHelper.decorate(span, doubleAdder::destroy);
  }

}
