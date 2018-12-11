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
import java.util.concurrent.locks.Condition;
import org.redisson.api.RFuture;
import org.redisson.api.RLock;

public class TracingRLock extends TracingRExpirable implements RLock {
  private final RLock lock;
  private final TracingRedissonHelper tracingRedissonHelper;

  public TracingRLock(RLock lock, TracingRedissonHelper tracingRedissonHelper) {
    super(lock, tracingRedissonHelper);
    this.lock = lock;
    this.tracingRedissonHelper = tracingRedissonHelper;
  }

  @Override
  public void lockInterruptibly(long leaseTime, TimeUnit unit) throws InterruptedException {
    Span span = tracingRedissonHelper.buildSpan("lockInterruptibly", lock);
    span.setTag("leaseTime", leaseTime);
    span.setTag("unit", nullable(unit));
    tracingRedissonHelper.decorateThrowing(span, () -> lock.lockInterruptibly(leaseTime, unit));
  }

  @Override
  public boolean tryLock(long waitTime, long leaseTime, TimeUnit unit) throws InterruptedException {
    Span span = tracingRedissonHelper.buildSpan("tryLock", lock);
    span.setTag("leaseTime", leaseTime);
    span.setTag("unit", nullable(unit));
    return tracingRedissonHelper
        .decorateThrowing(span, () -> lock.tryLock(waitTime, leaseTime, unit));
  }

  @Override
  public void lock(long leaseTime, TimeUnit unit) {
    Span span = tracingRedissonHelper.buildSpan("lock", lock);
    span.setTag("leaseTime", leaseTime);
    span.setTag("unit", nullable(unit));
    tracingRedissonHelper.decorate(span, () -> lock.lock(leaseTime, unit));
  }

  @Override
  public boolean forceUnlock() {
    Span span = tracingRedissonHelper.buildSpan("forceUnlock", lock);
    return tracingRedissonHelper.decorate(span, lock::forceUnlock);
  }

  @Override
  public boolean isLocked() {
    Span span = tracingRedissonHelper.buildSpan("isLocked", lock);
    return tracingRedissonHelper.decorate(span, lock::isLocked);
  }

  @Override
  public boolean isHeldByCurrentThread() {
    Span span = tracingRedissonHelper.buildSpan("isHeldByCurrentThread", lock);
    return tracingRedissonHelper.decorate(span, lock::isHeldByCurrentThread);
  }

  @Override
  public int getHoldCount() {
    Span span = tracingRedissonHelper.buildSpan("getHoldCount", lock);
    return tracingRedissonHelper.decorate(span, lock::getHoldCount);
  }

  @Override
  public void lock() {
    Span span = tracingRedissonHelper.buildSpan("lock", lock);
    tracingRedissonHelper.decorate(span, () -> lock.lock());
  }

  @Override
  public void lockInterruptibly() throws InterruptedException {
    Span span = tracingRedissonHelper.buildSpan("lockInterruptibly", lock);
    tracingRedissonHelper.decorateThrowing(span, () -> lock.lockInterruptibly());
  }

  @Override
  public boolean tryLock() {
    Span span = tracingRedissonHelper.buildSpan("tryLock", lock);
    return tracingRedissonHelper.decorate(span, () -> lock.tryLock());
  }

  @Override
  public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
    Span span = tracingRedissonHelper.buildSpan("tryLock", lock);
    span.setTag("time", time);
    span.setTag("unit", nullable(unit));
    return tracingRedissonHelper.decorateThrowing(span, () -> lock.tryLock(time, unit));
  }

  @Override
  public void unlock() {
    Span span = tracingRedissonHelper.buildSpan("unlock", lock);
    tracingRedissonHelper.decorate(span, lock::unlock);
  }

  @Override
  public Condition newCondition() {
    Span span = tracingRedissonHelper.buildSpan("newCondition", lock);
    return tracingRedissonHelper.decorate(span, lock::newCondition);
  }

  @Override
  public RFuture<Boolean> forceUnlockAsync() {
    Span span = tracingRedissonHelper.buildSpan("forceUnlockAsync", lock);
    return tracingRedissonHelper.prepareRFuture(span, lock::forceUnlockAsync);
  }

  @Override
  public RFuture<Void> unlockAsync() {
    Span span = tracingRedissonHelper.buildSpan("unlockAsync", lock);
    return tracingRedissonHelper.prepareRFuture(span, lock::unlockAsync);
  }

  @Override
  public RFuture<Void> unlockAsync(long threadId) {
    Span span = tracingRedissonHelper.buildSpan("unlockAsync", lock);
    span.setTag("threadId", threadId);
    return tracingRedissonHelper.prepareRFuture(span, () -> lock.unlockAsync(threadId));
  }

  @Override
  public RFuture<Boolean> tryLockAsync() {
    Span span = tracingRedissonHelper.buildSpan("tryLockAsync", lock);
    return tracingRedissonHelper.prepareRFuture(span, lock::tryLockAsync);
  }

  @Override
  public RFuture<Void> lockAsync() {
    Span span = tracingRedissonHelper.buildSpan("lockAsync", lock);
    return tracingRedissonHelper.prepareRFuture(span, lock::lockAsync);
  }

  @Override
  public RFuture<Void> lockAsync(long threadId) {
    Span span = tracingRedissonHelper.buildSpan("lockAsync", lock);
    span.setTag("threadId", threadId);
    return tracingRedissonHelper.prepareRFuture(span, () -> lock.lockAsync(threadId));
  }

  @Override
  public RFuture<Void> lockAsync(long leaseTime, TimeUnit unit) {
    Span span = tracingRedissonHelper.buildSpan("lockAsync", lock);
    span.setTag("leaseTime", leaseTime);
    span.setTag("unit", nullable(unit));
    return tracingRedissonHelper.prepareRFuture(span, () -> lock.lockAsync(leaseTime, unit));
  }

  @Override
  public RFuture<Void> lockAsync(long leaseTime, TimeUnit unit,
      long threadId) {
    Span span = tracingRedissonHelper.buildSpan("lockAsync", lock);
    span.setTag("leaseTime", leaseTime);
    span.setTag("unit", nullable(unit));
    span.setTag("threadId", threadId);
    return tracingRedissonHelper
        .prepareRFuture(span, () -> lock.lockAsync(leaseTime, unit, threadId));
  }

  @Override
  public RFuture<Boolean> tryLockAsync(long threadId) {
    Span span = tracingRedissonHelper.buildSpan("tryLockAsync", lock);
    span.setTag("threadId", threadId);
    return tracingRedissonHelper.prepareRFuture(span, () -> lock.tryLockAsync(threadId));
  }

  @Override
  public RFuture<Boolean> tryLockAsync(long waitTime, TimeUnit unit) {
    Span span = tracingRedissonHelper.buildSpan("tryLockAsync", lock);
    span.setTag("waitTime", waitTime);
    span.setTag("unit", nullable(unit));
    return tracingRedissonHelper.prepareRFuture(span, () -> lock.tryLockAsync(waitTime, unit));
  }

  @Override
  public RFuture<Boolean> tryLockAsync(long waitTime, long leaseTime, TimeUnit unit) {
    Span span = tracingRedissonHelper.buildSpan("tryLockAsync", lock);
    span.setTag("waitTime", waitTime);
    span.setTag("leaseTime", leaseTime);
    span.setTag("unit", nullable(unit));
    return tracingRedissonHelper
        .prepareRFuture(span, () -> lock.tryLockAsync(waitTime, leaseTime, unit));
  }

  @Override
  public RFuture<Boolean> tryLockAsync(long waitTime, long leaseTime,
      TimeUnit unit, long threadId) {
    Span span = tracingRedissonHelper.buildSpan("tryLockAsync", lock);
    span.setTag("waitTime", waitTime);
    span.setTag("leaseTime", leaseTime);
    span.setTag("unit", nullable(unit));
    span.setTag("threadId", threadId);
    return tracingRedissonHelper
        .prepareRFuture(span, () -> lock.tryLockAsync(waitTime, leaseTime, unit, threadId));
  }

  @Override
  public RFuture<Integer> getHoldCountAsync() {
    Span span = tracingRedissonHelper.buildSpan("getHoldCountAsync", lock);
    return tracingRedissonHelper.prepareRFuture(span, lock::getHoldCountAsync);
  }
}
