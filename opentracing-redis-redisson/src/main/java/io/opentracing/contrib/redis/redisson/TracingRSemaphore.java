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
import org.redisson.api.RSemaphore;

public class TracingRSemaphore extends TracingRExpirable implements RSemaphore {
  private final RSemaphore semaphore;
  private final TracingRedissonHelper tracingRedissonHelper;

  public TracingRSemaphore(RSemaphore semaphore, TracingRedissonHelper tracingRedissonHelper) {
    super(semaphore, tracingRedissonHelper);
    this.semaphore = semaphore;
    this.tracingRedissonHelper = tracingRedissonHelper;
  }

  @Override
  public void acquire() throws InterruptedException {
    Span span = tracingRedissonHelper.buildSpan("acquire", semaphore);
    tracingRedissonHelper.decorateThrowing(span, () -> semaphore.acquire());
  }

  @Override
  public void acquire(int permits) throws InterruptedException {
    Span span = tracingRedissonHelper.buildSpan("acquire", semaphore);
    span.setTag("permits", permits);
    tracingRedissonHelper.decorateThrowing(span, () -> semaphore.acquire(permits));
  }

  @Override
  public boolean tryAcquire() {
    Span span = tracingRedissonHelper.buildSpan("tryAcquire", semaphore);
    return tracingRedissonHelper.decorate(span, () -> semaphore.tryAcquire());
  }

  @Override
  public boolean tryAcquire(int permits) {
    Span span = tracingRedissonHelper.buildSpan("tryAcquire", semaphore);
    span.setTag("permits", permits);
    return tracingRedissonHelper.decorate(span, () -> semaphore.tryAcquire(permits));
  }

  @Override
  public boolean tryAcquire(long waitTime, TimeUnit unit) throws InterruptedException {
    Span span = tracingRedissonHelper.buildSpan("tryAcquire", semaphore);
    span.setTag("waitTime", waitTime);
    span.setTag("unit", nullable(unit));
    return tracingRedissonHelper.decorateThrowing(span, () -> semaphore.tryAcquire(waitTime, unit));
  }

  @Override
  public boolean tryAcquire(int permits, long waitTime, TimeUnit unit) throws InterruptedException {
    Span span = tracingRedissonHelper.buildSpan("tryAcquire", semaphore);
    span.setTag("permits", permits);
    span.setTag("waitTime", waitTime);
    span.setTag("unit", nullable(unit));
    return tracingRedissonHelper
        .decorateThrowing(span, () -> semaphore.tryAcquire(permits, waitTime, unit));
  }

  @Override
  public void release() {
    Span span = tracingRedissonHelper.buildSpan("release", semaphore);
    tracingRedissonHelper.decorate(span, () -> semaphore.release());
  }

  @Override
  public void release(int permits) {
    Span span = tracingRedissonHelper.buildSpan("release", semaphore);
    span.setTag("permits", permits);
    tracingRedissonHelper.decorate(span, () -> semaphore.release(permits));
  }

  @Override
  public int availablePermits() {
    Span span = tracingRedissonHelper.buildSpan("availablePermits", semaphore);
    return tracingRedissonHelper.decorate(span, semaphore::availablePermits);
  }

  @Override
  public int drainPermits() {
    Span span = tracingRedissonHelper.buildSpan("drainPermits", semaphore);
    return tracingRedissonHelper.decorate(span, semaphore::drainPermits);
  }

  @Override
  public boolean trySetPermits(int permits) {
    Span span = tracingRedissonHelper.buildSpan("trySetPermits", semaphore);
    span.setTag("permits", permits);
    return tracingRedissonHelper.decorate(span, () -> semaphore.trySetPermits(permits));
  }

  @Override
  public void reducePermits(int permits) {
    Span span = tracingRedissonHelper.buildSpan("reducePermits", semaphore);
    span.setTag("permits", permits);
    tracingRedissonHelper.decorate(span, () -> semaphore.reducePermits(permits));
  }

  @Override
  public void addPermits(int permits) {
    Span span = tracingRedissonHelper.buildSpan("addPermits", semaphore);
    span.setTag("permits", permits);
    tracingRedissonHelper.decorate(span, () -> semaphore.addPermits(permits));
  }

  @Override
  public RFuture<Boolean> tryAcquireAsync() {
    Span span = tracingRedissonHelper.buildSpan("tryAcquireAsync", semaphore);
    return tracingRedissonHelper.prepareRFuture(span, semaphore::tryAcquireAsync);
  }

  @Override
  public RFuture<Boolean> tryAcquireAsync(int permits) {
    Span span = tracingRedissonHelper.buildSpan("tryAcquireAsync", semaphore);
    span.setTag("permits", permits);
    return tracingRedissonHelper.prepareRFuture(span, () -> semaphore.tryAcquireAsync(permits));
  }

  @Override
  public RFuture<Void> acquireAsync() {
    Span span = tracingRedissonHelper.buildSpan("acquireAsync", semaphore);
    return tracingRedissonHelper.prepareRFuture(span, semaphore::acquireAsync);
  }

  @Override
  public RFuture<Void> acquireAsync(int permits) {
    Span span = tracingRedissonHelper.buildSpan("acquireAsync", semaphore);
    span.setTag("permits", permits);
    return tracingRedissonHelper.prepareRFuture(span, () -> semaphore.acquireAsync(permits));
  }

  @Override
  public RFuture<Void> releaseAsync() {
    Span span = tracingRedissonHelper.buildSpan("releaseAsync", semaphore);
    return tracingRedissonHelper.prepareRFuture(span, semaphore::releaseAsync);
  }

  @Override
  public RFuture<Void> releaseAsync(int permits) {
    Span span = tracingRedissonHelper.buildSpan("releaseAsync", semaphore);
    span.setTag("permits", permits);
    return tracingRedissonHelper.prepareRFuture(span, () -> semaphore.releaseAsync(permits));
  }

  @Override
  public RFuture<Boolean> trySetPermitsAsync(int permits) {
    Span span = tracingRedissonHelper.buildSpan("trySetPermitsAsync", semaphore);
    span.setTag("permits", permits);
    return tracingRedissonHelper.prepareRFuture(span, () -> semaphore.trySetPermitsAsync(permits));
  }

  @Override
  public RFuture<Boolean> tryAcquireAsync(long waitTime, TimeUnit unit) {
    Span span = tracingRedissonHelper.buildSpan("tryAcquireAsync", semaphore);
    span.setTag("waitTime", waitTime);
    span.setTag("unit", nullable(unit));
    return tracingRedissonHelper
        .prepareRFuture(span, () -> semaphore.tryAcquireAsync(waitTime, unit));
  }

  @Override
  public RFuture<Boolean> tryAcquireAsync(int permits, long waitTime, TimeUnit unit) {
    Span span = tracingRedissonHelper.buildSpan("tryAcquireAsync", semaphore);
    span.setTag("permits", permits);
    span.setTag("waitTime", waitTime);
    span.setTag("unit", nullable(unit));
    return tracingRedissonHelper
        .prepareRFuture(span, () -> semaphore.tryAcquireAsync(permits, waitTime, unit));
  }

  @Override
  public RFuture<Void> reducePermitsAsync(int permits) {
    Span span = tracingRedissonHelper.buildSpan("reducePermitsAsync", semaphore);
    span.setTag("permits", permits);
    return tracingRedissonHelper.prepareRFuture(span, () -> semaphore.reducePermitsAsync(permits));
  }

  @Override
  public RFuture<Void> addPermitsAsync(int permits) {
    Span span = tracingRedissonHelper.buildSpan("addPermitsAsync", semaphore);
    span.setTag("permits", permits);
    return tracingRedissonHelper.prepareRFuture(span, () -> semaphore.addPermitsAsync(permits));
  }

  @Override
  public RFuture<Integer> availablePermitsAsync() {
    Span span = tracingRedissonHelper.buildSpan("availablePermitsAsync", semaphore);
    return tracingRedissonHelper.prepareRFuture(span, () -> semaphore.availablePermitsAsync());
  }

  @Override
  public RFuture<Integer> drainPermitsAsync() {
    Span span = tracingRedissonHelper.buildSpan("drainPermitsAsync", semaphore);
    return tracingRedissonHelper.prepareRFuture(span, () -> semaphore.drainPermitsAsync());
  }

}
