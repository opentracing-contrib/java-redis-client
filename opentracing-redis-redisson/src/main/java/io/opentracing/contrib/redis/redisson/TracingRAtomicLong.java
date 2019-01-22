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
import org.redisson.api.RAtomicLong;
import org.redisson.api.RFuture;

public class TracingRAtomicLong extends TracingRExpirable implements RAtomicLong {
    private final RAtomicLong atomicLong;
    private final TracingRedissonHelper tracingRedissonHelper;

    public TracingRAtomicLong(RAtomicLong atomicLong, TracingRedissonHelper tracingRedissonHelper) {
        super(atomicLong, tracingRedissonHelper);
        this.atomicLong = atomicLong;
        this.tracingRedissonHelper = tracingRedissonHelper;
    }

    @Override
    public long getAndDecrement() {
        Span span = tracingRedissonHelper.buildSpan("getAndDecrement", atomicLong);
        return tracingRedissonHelper.decorate(span, atomicLong::getAndDecrement);
    }

    @Override
    public long addAndGet(long delta) {
        Span span = tracingRedissonHelper.buildSpan("addAndGet", atomicLong);
        span.setTag("delta", delta);
        return tracingRedissonHelper.decorate(span, () -> atomicLong.addAndGet(delta));
    }

    @Override
    public boolean compareAndSet(long expect, long update) {
        Span span = tracingRedissonHelper.buildSpan("compareAndSet", atomicLong);
        span.setTag("expect", expect);
        span.setTag("update", update);
        return tracingRedissonHelper.decorate(span, () -> atomicLong.compareAndSet(expect, update));
    }

    @Override
    public long decrementAndGet() {
        Span span = tracingRedissonHelper.buildSpan("decrementAndGet", atomicLong);
        return tracingRedissonHelper.decorate(span, atomicLong::decrementAndGet);
    }

    @Override
    public long get() {
        Span span = tracingRedissonHelper.buildSpan("get", atomicLong);
        return tracingRedissonHelper.decorate(span, atomicLong::get);
    }

    @Override
    public long getAndDelete() {
        Span span = tracingRedissonHelper.buildSpan("getAndDelete", atomicLong);
        return tracingRedissonHelper.decorate(span, atomicLong::getAndDelete);
    }

    @Override
    public long getAndAdd(long delta) {
        Span span = tracingRedissonHelper.buildSpan("getAndAdd", atomicLong);
        span.setTag("delta", delta);
        return tracingRedissonHelper.decorate(span, () -> atomicLong.getAndAdd(delta));
    }

    @Override
    public long getAndSet(long newValue) {
        Span span = tracingRedissonHelper.buildSpan("getAndSet", atomicLong);
        span.setTag("newValue", newValue);
        return tracingRedissonHelper.decorate(span, () -> atomicLong.getAndSet(newValue));
    }

    @Override
    public long incrementAndGet() {
        Span span = tracingRedissonHelper.buildSpan("incrementAndGet", atomicLong);
        return tracingRedissonHelper.decorate(span, atomicLong::incrementAndGet);
    }

    @Override
    public long getAndIncrement() {
        Span span = tracingRedissonHelper.buildSpan("getAndIncrement", atomicLong);
        return tracingRedissonHelper.decorate(span, atomicLong::getAndIncrement);
    }

    @Override
    public void set(long newValue) {
        Span span = tracingRedissonHelper.buildSpan("set", atomicLong);
        span.setTag("newValue", newValue);
        tracingRedissonHelper.decorate(span, () -> atomicLong.set(newValue));
    }

    @Override
    public RFuture<Boolean> compareAndSetAsync(long expect, long update) {
        Span span = tracingRedissonHelper.buildSpan("compareAndSetAsync", atomicLong);
        span.setTag("expect", expect);
        span.setTag("update", update);
        return tracingRedissonHelper
                .prepareRFuture(span, () -> atomicLong.compareAndSetAsync(expect, update));
    }

    @Override
    public RFuture<Long> addAndGetAsync(long delta) {
        Span span = tracingRedissonHelper.buildSpan("addAndGetAsync", atomicLong);
        span.setTag("delta", delta);
        return tracingRedissonHelper.prepareRFuture(span, () -> atomicLong.addAndGetAsync(delta));
    }

    @Override
    public RFuture<Long> decrementAndGetAsync() {
        Span span = tracingRedissonHelper.buildSpan("decrementAndGetAsync", atomicLong);
        return tracingRedissonHelper.prepareRFuture(span, atomicLong::decrementAndGetAsync);
    }

    @Override
    public RFuture<Long> getAsync() {
        Span span = tracingRedissonHelper.buildSpan("getAsync", atomicLong);
        return tracingRedissonHelper.prepareRFuture(span, atomicLong::getAsync);
    }

    @Override
    public RFuture<Long> getAndDeleteAsync() {
        Span span = tracingRedissonHelper.buildSpan("getAndDeleteAsync", atomicLong);
        return tracingRedissonHelper.prepareRFuture(span, atomicLong::getAndDeleteAsync);
    }

    @Override
    public RFuture<Long> getAndAddAsync(long delta) {
        Span span = tracingRedissonHelper.buildSpan("getAndAddAsync", atomicLong);
        span.setTag("delta", delta);
        return tracingRedissonHelper.prepareRFuture(span, () -> atomicLong.getAndAddAsync(delta));
    }

    @Override
    public RFuture<Long> getAndSetAsync(long newValue) {
        Span span = tracingRedissonHelper.buildSpan("getAndSetAsync", atomicLong);
        span.setTag("newValue", newValue);
        return tracingRedissonHelper.prepareRFuture(span, () -> atomicLong.getAndSetAsync(newValue));
    }

    @Override
    public RFuture<Long> incrementAndGetAsync() {
        Span span = tracingRedissonHelper.buildSpan("incrementAndGetAsync", atomicLong);
        return tracingRedissonHelper.prepareRFuture(span, atomicLong::incrementAndGetAsync);
    }

    @Override
    public RFuture<Long> getAndIncrementAsync() {
        Span span = tracingRedissonHelper.buildSpan("getAndIncrementAsync", atomicLong);
        return tracingRedissonHelper.prepareRFuture(span, atomicLong::getAndIncrementAsync);
    }

    @Override
    public RFuture<Long> getAndDecrementAsync() {
        Span span = tracingRedissonHelper.buildSpan("getAndDecrementAsync", atomicLong);
        return tracingRedissonHelper.prepareRFuture(span, atomicLong::getAndDecrementAsync);
    }

    @Override
    public RFuture<Void> setAsync(long newValue) {
        Span span = tracingRedissonHelper.buildSpan("setAsync", atomicLong);
        span.setTag("newValue", newValue);
        return tracingRedissonHelper.prepareRFuture(span, () -> atomicLong.setAsync(newValue));
    }

}
