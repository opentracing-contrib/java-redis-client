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

import io.opentracing.Span;
import org.redisson.api.RAtomicDouble;
import org.redisson.api.RFuture;

public class TracingRAtomicDouble extends TracingRExpirable implements RAtomicDouble {
  private final RAtomicDouble atomicDouble;
  private final TracingRedissonHelper tracingRedissonHelper;

  public TracingRAtomicDouble(RAtomicDouble atomicDouble,
      TracingRedissonHelper tracingRedissonHelper) {
    super(atomicDouble, tracingRedissonHelper);
    this.atomicDouble = atomicDouble;
    this.tracingRedissonHelper = tracingRedissonHelper;
  }

  @Override
  public double getAndDecrement() {
    Span span = tracingRedissonHelper.buildSpan("getAndDecrement", atomicDouble);
    return tracingRedissonHelper.decorate(span, atomicDouble::getAndDecrement);
  }

  @Override
  public double addAndGet(double delta) {
    Span span = tracingRedissonHelper.buildSpan("addAndGet", atomicDouble);
    span.setTag("delta", delta);
    return tracingRedissonHelper.decorate(span, () -> atomicDouble.addAndGet(delta));
  }

  @Override
  public boolean compareAndSet(double expect, double update) {
    Span span = tracingRedissonHelper.buildSpan("compareAndSet", atomicDouble);
    span.setTag("expect", expect);
    span.setTag("update", update);
    return tracingRedissonHelper.decorate(span, () -> atomicDouble.compareAndSet(expect, update));
  }

  @Override
  public double decrementAndGet() {
    Span span = tracingRedissonHelper.buildSpan("decrementAndGet", atomicDouble);
    return tracingRedissonHelper.decorate(span, atomicDouble::decrementAndGet);
  }

  @Override
  public double get() {
    Span span = tracingRedissonHelper.buildSpan("get", atomicDouble);
    return tracingRedissonHelper.decorate(span, atomicDouble::get);
  }

  @Override
  public double getAndDelete() {
    Span span = tracingRedissonHelper.buildSpan("getAndDelete", atomicDouble);
    return tracingRedissonHelper.decorate(span, atomicDouble::getAndDelete);
  }

  @Override
  public double getAndAdd(double delta) {
    Span span = tracingRedissonHelper.buildSpan("getAndAdd", atomicDouble);
    span.setTag("delta", delta);
    return tracingRedissonHelper.decorate(span, () -> atomicDouble.getAndAdd(delta));
  }

  @Override
  public double getAndSet(double newValue) {
    Span span = tracingRedissonHelper.buildSpan("getAndSet", atomicDouble);
    span.setTag("newValue", newValue);
    return tracingRedissonHelper.decorate(span, () -> atomicDouble.getAndSet(newValue));
  }

  @Override
  public double incrementAndGet() {
    Span span = tracingRedissonHelper.buildSpan("incrementAndGet", atomicDouble);
    return tracingRedissonHelper.decorate(span, atomicDouble::incrementAndGet);
  }

  @Override
  public double getAndIncrement() {
    Span span = tracingRedissonHelper.buildSpan("getAndIncrement", atomicDouble);
    return tracingRedissonHelper.decorate(span, atomicDouble::getAndIncrement);
  }

  @Override
  public void set(double newValue) {
    Span span = tracingRedissonHelper.buildSpan("set", atomicDouble);
    span.setTag("newValue", newValue);
    tracingRedissonHelper.decorate(span, () -> atomicDouble.set(newValue));
  }

  @Override
  public RFuture<Boolean> compareAndSetAsync(double expect, double update) {
    Span span = tracingRedissonHelper.buildSpan("compareAndSetAsync", atomicDouble);
    span.setTag("expect", expect);
    span.setTag("update", update);
    return tracingRedissonHelper
        .prepareRFuture(span, () -> atomicDouble.compareAndSetAsync(expect, update));
  }

  @Override
  public RFuture<Double> addAndGetAsync(double delta) {
    Span span = tracingRedissonHelper.buildSpan("addAndGetAsync", atomicDouble);
    span.setTag("delta", delta);
    return tracingRedissonHelper.prepareRFuture(span, () -> atomicDouble.addAndGetAsync(delta));
  }

  @Override
  public RFuture<Double> decrementAndGetAsync() {
    Span span = tracingRedissonHelper.buildSpan("decrementAndGetAsync", atomicDouble);
    return tracingRedissonHelper.prepareRFuture(span, atomicDouble::decrementAndGetAsync);
  }

  @Override
  public RFuture<Double> getAsync() {
    Span span = tracingRedissonHelper.buildSpan("getAsync", atomicDouble);
    return tracingRedissonHelper.prepareRFuture(span, atomicDouble::getAsync);
  }

  @Override
  public RFuture<Double> getAndDeleteAsync() {
    Span span = tracingRedissonHelper.buildSpan("getAndDeleteAsync", atomicDouble);
    return tracingRedissonHelper.prepareRFuture(span, atomicDouble::getAndDeleteAsync);
  }

  @Override
  public RFuture<Double> getAndAddAsync(double delta) {
    Span span = tracingRedissonHelper.buildSpan("getAndAddAsync", atomicDouble);
    span.setTag("delta", delta);
    return tracingRedissonHelper.prepareRFuture(span, () -> atomicDouble.getAndAddAsync(delta));
  }

  @Override
  public RFuture<Double> getAndSetAsync(double newValue) {
    Span span = tracingRedissonHelper.buildSpan("getAndSetAsync", atomicDouble);
    span.setTag("newValue", newValue);
    return tracingRedissonHelper.prepareRFuture(span, () -> atomicDouble.getAndSetAsync(newValue));
  }

  @Override
  public RFuture<Double> incrementAndGetAsync() {
    Span span = tracingRedissonHelper.buildSpan("incrementAndGetAsync", atomicDouble);
    return tracingRedissonHelper.prepareRFuture(span, atomicDouble::incrementAndGetAsync);
  }

  @Override
  public RFuture<Double> getAndIncrementAsync() {
    Span span = tracingRedissonHelper.buildSpan("getAndIncrementAsync", atomicDouble);
    return tracingRedissonHelper.prepareRFuture(span, atomicDouble::getAndIncrementAsync);
  }

  @Override
  public RFuture<Double> getAndDecrementAsync() {
    Span span = tracingRedissonHelper.buildSpan("getAndDecrementAsync", atomicDouble);
    return tracingRedissonHelper.prepareRFuture(span, atomicDouble::getAndDecrementAsync);
  }

  @Override
  public RFuture<Void> setAsync(double newValue) {
    Span span = tracingRedissonHelper.buildSpan("setAsync", atomicDouble);
    span.setTag("newValue", newValue);
    return tracingRedissonHelper.prepareRFuture(span, () -> atomicDouble.setAsync(newValue));
  }

}
