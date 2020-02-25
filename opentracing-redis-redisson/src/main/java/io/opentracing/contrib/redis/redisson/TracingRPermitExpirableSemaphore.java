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
import org.redisson.api.RPermitExpirableSemaphore;

public class TracingRPermitExpirableSemaphore extends TracingRExpirable implements
    RPermitExpirableSemaphore {
  private final RPermitExpirableSemaphore semaphore;
  private final TracingRedissonHelper tracingRedissonHelper;

  public TracingRPermitExpirableSemaphore(RPermitExpirableSemaphore semaphore,
      TracingRedissonHelper tracingRedissonHelper) {
    super(semaphore, tracingRedissonHelper);
    this.semaphore = semaphore;
    this.tracingRedissonHelper = tracingRedissonHelper;
  }

  @Override
  public String acquire() throws InterruptedException {
    Span span = tracingRedissonHelper.buildSpan("acquire", semaphore);
    return tracingRedissonHelper.decorateThrowing(span, () -> semaphore.acquire());
  }

  @Override
  public String acquire(long leaseTime, TimeUnit unit) throws InterruptedException {
    Span span = tracingRedissonHelper.buildSpan("acquire", semaphore);
    span.setTag("leaseTime", leaseTime);
    span.setTag("unit", nullable(unit));
    return tracingRedissonHelper.decorateThrowing(span, () -> semaphore.acquire(leaseTime, unit));
  }

  @Override
  public String tryAcquire() {
    Span span = tracingRedissonHelper.buildSpan("tryAcquire", semaphore);
    return tracingRedissonHelper.decorate(span, () -> semaphore.tryAcquire());
  }

  @Override
  public String tryAcquire(long waitTime, TimeUnit unit) throws InterruptedException {
    Span span = tracingRedissonHelper.buildSpan("tryAcquire", semaphore);
    span.setTag("waitTime", waitTime);
    span.setTag("unit", nullable(unit));
    return tracingRedissonHelper.decorateThrowing(span, () -> semaphore.tryAcquire(waitTime, unit));
  }

  @Override
  public String tryAcquire(long waitTime, long leaseTime, TimeUnit unit)
      throws InterruptedException {
    Span span = tracingRedissonHelper.buildSpan("tryAcquire", semaphore);
    span.setTag("waitTime", waitTime);
    span.setTag("leaseTime", leaseTime);
    span.setTag("unit", nullable(unit));
    return tracingRedissonHelper
        .decorateThrowing(span, () -> semaphore.tryAcquire(waitTime, leaseTime, unit));
  }

  @Override
  public boolean tryRelease(String permitId) {
    Span span = tracingRedissonHelper.buildSpan("tryRelease", semaphore);
    span.setTag("permitId", nullable(permitId));
    return tracingRedissonHelper.decorate(span, () -> semaphore.tryRelease(permitId));
  }

  @Override
  public void release(String permitId) {
    Span span = tracingRedissonHelper.buildSpan("release", semaphore);
    span.setTag("permitId", nullable(permitId));
    tracingRedissonHelper.decorate(span, () -> semaphore.release(permitId));
  }

  @Override
  public int availablePermits() {
    Span span = tracingRedissonHelper.buildSpan("availablePermits", semaphore);
    return tracingRedissonHelper.decorate(span, semaphore::availablePermits);
  }

  @Override
  public boolean trySetPermits(int permits) {
    Span span = tracingRedissonHelper.buildSpan("trySetPermits", semaphore);
    span.setTag("permits", permits);
    return tracingRedissonHelper.decorate(span, () -> semaphore.trySetPermits(permits));
  }

  @Override
  public void addPermits(int permits) {
    Span span = tracingRedissonHelper.buildSpan("addPermits", semaphore);
    span.setTag("permits", permits);
    tracingRedissonHelper.decorate(span, () -> semaphore.addPermits(permits));
  }

  @Override
  public boolean updateLeaseTime(String permitId, long leaseTime, TimeUnit unit) {
    Span span = tracingRedissonHelper.buildSpan("updateLeaseTime", semaphore);
    span.setTag("permitId", permitId);
    span.setTag("leaseTime", leaseTime);
    span.setTag("unit", nullable(unit));
    return tracingRedissonHelper
        .decorateThrowing(span, () -> semaphore.updateLeaseTime(permitId, leaseTime, unit));
  }

  @Override
  public RFuture<String> acquireAsync() {
    Span span = tracingRedissonHelper.buildSpan("acquireAsync", semaphore);
    return tracingRedissonHelper.prepareRFuture(span, semaphore::acquireAsync);
  }

  @Override
  public RFuture<String> acquireAsync(long leaseTime, TimeUnit unit) {
    Span span = tracingRedissonHelper.buildSpan("acquireAsync", semaphore);
    span.setTag("leaseTime", leaseTime);
    span.setTag("unit", nullable(unit));
    return tracingRedissonHelper
        .prepareRFuture(span, () -> semaphore.acquireAsync(leaseTime, unit));
  }

  @Override
  public RFuture<String> tryAcquireAsync() {
    Span span = tracingRedissonHelper.buildSpan("tryAcquireAsync", semaphore);
    return tracingRedissonHelper.prepareRFuture(span, semaphore::tryAcquireAsync);
  }

  @Override
  public RFuture<String> tryAcquireAsync(long waitTime, TimeUnit unit) {
    Span span = tracingRedissonHelper.buildSpan("tryAcquireAsync", semaphore);
    span.setTag("waitTime", waitTime);
    span.setTag("unit", nullable(unit));
    return tracingRedissonHelper
        .prepareRFuture(span, () -> semaphore.tryAcquireAsync(waitTime, unit));
  }

  @Override
  public RFuture<String> tryAcquireAsync(long waitTime, long leaseTime, TimeUnit unit) {
    Span span = tracingRedissonHelper.buildSpan("tryAcquireAsync", semaphore);
    span.setTag("waitTime", waitTime);
    span.setTag("leaseTime", leaseTime);
    span.setTag("unit", nullable(unit));
    return tracingRedissonHelper
        .prepareRFuture(span, () -> semaphore.tryAcquireAsync(waitTime, leaseTime, unit));
  }

  @Override
  public RFuture<Boolean> tryReleaseAsync(String permitId) {
    Span span = tracingRedissonHelper.buildSpan("tryReleaseAsync", semaphore);
    span.setTag("permitId", permitId);
    return tracingRedissonHelper.prepareRFuture(span, () -> semaphore.tryReleaseAsync(permitId));
  }

  @Override
  public RFuture<Void> releaseAsync(String permitId) {
    Span span = tracingRedissonHelper.buildSpan("releaseAsync", semaphore);
    span.setTag("permitId", permitId);
    return tracingRedissonHelper.prepareRFuture(span, () -> semaphore.releaseAsync(permitId));
  }

  @Override
  public RFuture<Integer> availablePermitsAsync() {
    Span span = tracingRedissonHelper.buildSpan("availablePermitsAsync", semaphore);
    return tracingRedissonHelper.prepareRFuture(span, semaphore::availablePermitsAsync);
  }

  @Override
  public RFuture<Boolean> trySetPermitsAsync(int permits) {
    Span span = tracingRedissonHelper.buildSpan("trySetPermitsAsync", semaphore);
    span.setTag("permits", permits);
    return tracingRedissonHelper.prepareRFuture(span, () -> semaphore.trySetPermitsAsync(permits));
  }

  @Override
  public RFuture<Void> addPermitsAsync(int permits) {
    Span span = tracingRedissonHelper.buildSpan("addPermitsAsync", semaphore);
    span.setTag("permits", permits);
    return tracingRedissonHelper.prepareRFuture(span, () -> semaphore.addPermitsAsync(permits));
  }

  @Override
  public RFuture<Boolean> updateLeaseTimeAsync(String permitId, long leaseTime, TimeUnit unit) {
    Span span = tracingRedissonHelper.buildSpan("updateLeaseTimeAsync", semaphore);
    span.setTag("permitId", permitId);
    span.setTag("leaseTime", leaseTime);
    span.setTag("unit", nullable(unit));
    return tracingRedissonHelper
        .prepareRFuture(span, () -> semaphore.updateLeaseTimeAsync(permitId, leaseTime, unit));
  }
}
