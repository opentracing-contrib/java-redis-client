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
import org.redisson.api.RBucket;
import org.redisson.api.RFuture;

import java.util.concurrent.TimeUnit;

import static io.opentracing.contrib.redis.common.TracingHelper.nullable;

public class TracingRBucket<V> extends TracingRExpirable implements RBucket<V> {
    private final RBucket<V> bucket;
    private final TracingRedissonHelper tracingRedissonHelper;

    public TracingRBucket(RBucket<V> bucket,
                          TracingRedissonHelper tracingRedissonHelper) {
        super(bucket, tracingRedissonHelper);
        this.bucket = bucket;
        this.tracingRedissonHelper = tracingRedissonHelper;
    }

    @Override
    public long size() {
        Span span = tracingRedissonHelper.buildSpan("size", bucket);
        return tracingRedissonHelper.decorate(span, bucket::size);
    }

    @Override
    public V get() {
        Span span = tracingRedissonHelper.buildSpan("get", bucket);
        return tracingRedissonHelper.decorate(span, bucket::get);
    }

    @Override
    public V getAndDelete() {
        Span span = tracingRedissonHelper.buildSpan("getAndDelete", bucket);
        return tracingRedissonHelper.decorate(span, bucket::getAndDelete);
    }

    @Override
    public boolean trySet(V value) {
        Span span = tracingRedissonHelper.buildSpan("trySet", bucket);
        span.setTag("value", nullable(value));
        return tracingRedissonHelper.decorate(span, () -> bucket.trySet(value));
    }

    @Override
    public boolean trySet(V value, long timeToLive, TimeUnit timeUnit) {
        Span span = tracingRedissonHelper.buildSpan("trySet", bucket);
        span.setTag("value", nullable(value));
        span.setTag("timeToLive", timeToLive);
        span.setTag("timeUnit", nullable(timeUnit));
        return tracingRedissonHelper.decorate(span, () -> bucket.trySet(value, timeToLive, timeUnit));
    }

    @Override
    public boolean compareAndSet(V expect, V update) {
        Span span = tracingRedissonHelper.buildSpan("compareAndSet", bucket);
        span.setTag("expect", nullable(expect));
        span.setTag("update", nullable(update));
        return tracingRedissonHelper.decorate(span, () -> bucket.compareAndSet(expect, update));
    }

    @Override
    public V getAndSet(V newValue) {
        Span span = tracingRedissonHelper.buildSpan("getAndSet", bucket);
        span.setTag("newValue", nullable(newValue));
        return tracingRedissonHelper.decorate(span, () -> bucket.getAndSet(newValue));
    }

    @Override
    public void set(V value) {
        Span span = tracingRedissonHelper.buildSpan("set", bucket);
        span.setTag("value", nullable(value));
        tracingRedissonHelper.decorate(span, () -> bucket.set(value));
    }

    @Override
    public void set(V value, long timeToLive, TimeUnit timeUnit) {
        Span span = tracingRedissonHelper.buildSpan("set", bucket);
        span.setTag("value", nullable(value));
        span.setTag("timeToLive", timeToLive);
        span.setTag("timeUnit", nullable(timeUnit));
        tracingRedissonHelper.decorate(span, () -> bucket.set(value, timeToLive, timeUnit));
    }

    @Override
    public RFuture<Long> sizeAsync() {
        Span span = tracingRedissonHelper.buildSpan("sizeAsync", bucket);
        return tracingRedissonHelper.prepareRFuture(span, bucket::sizeAsync);
    }

    @Override
    public RFuture<V> getAsync() {
        Span span = tracingRedissonHelper.buildSpan("getAsync", bucket);
        return tracingRedissonHelper.prepareRFuture(span, bucket::getAsync);
    }

    @Override
    public RFuture<V> getAndDeleteAsync() {
        Span span = tracingRedissonHelper.buildSpan("getAndDeleteAsync", bucket);
        return tracingRedissonHelper.prepareRFuture(span, bucket::getAndDeleteAsync);
    }

    @Override
    public RFuture<Boolean> trySetAsync(V value) {
        Span span = tracingRedissonHelper.buildSpan("trySetAsync", bucket);
        span.setTag("value", nullable(value));
        return tracingRedissonHelper.prepareRFuture(span, () -> bucket.trySetAsync(value));
    }

    @Override
    public RFuture<Boolean> trySetAsync(V value, long timeToLive, TimeUnit timeUnit) {
        Span span = tracingRedissonHelper.buildSpan("trySetAsync", bucket);
        span.setTag("value", nullable(value));
        span.setTag("timeToLive", timeToLive);
        span.setTag("timeUnit", nullable(timeUnit));
        return tracingRedissonHelper
                .prepareRFuture(span, () -> bucket.trySetAsync(value, timeToLive, timeUnit));
    }

    @Override
    public RFuture<Boolean> compareAndSetAsync(V expect, V update) {
        Span span = tracingRedissonHelper.buildSpan("compareAndSetAsync", bucket);
        span.setTag("expect", nullable(expect));
        span.setTag("update", nullable(update));
        return tracingRedissonHelper
                .prepareRFuture(span, () -> bucket.compareAndSetAsync(expect, update));
    }

    @Override
    public RFuture<V> getAndSetAsync(V newValue) {
        Span span = tracingRedissonHelper.buildSpan("getAndSetAsync", bucket);
        span.setTag("newValue", nullable(newValue));
        return tracingRedissonHelper.prepareRFuture(span, () -> bucket.getAndSetAsync(newValue));
    }

    @Override
    public RFuture<Void> setAsync(V value) {
        Span span = tracingRedissonHelper.buildSpan("setAsync", bucket);
        span.setTag("value", nullable(value));
        return tracingRedissonHelper.prepareRFuture(span, () -> bucket.setAsync(value));
    }

    @Override
    public RFuture<Void> setAsync(V value, long timeToLive, TimeUnit timeUnit) {
        Span span = tracingRedissonHelper.buildSpan("setAsync", bucket);
        span.setTag("value", nullable(value));
        span.setTag("timeToLive", timeToLive);
        span.setTag("timeUnit", nullable(timeUnit));
        return tracingRedissonHelper
                .prepareRFuture(span, () -> bucket.setAsync(value, timeToLive, timeUnit));
    }

}
