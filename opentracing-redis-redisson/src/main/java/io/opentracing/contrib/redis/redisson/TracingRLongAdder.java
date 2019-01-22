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
import org.redisson.api.RLongAdder;

import java.util.concurrent.TimeUnit;

import static io.opentracing.contrib.redis.common.TracingHelper.nullable;

public class TracingRLongAdder extends TracingRExpirable implements RLongAdder {
    private final RLongAdder longAdder;
    private final TracingRedissonHelper tracingRedissonHelper;

    public TracingRLongAdder(RLongAdder longAdder, TracingRedissonHelper tracingRedissonHelper) {
        super(longAdder, tracingRedissonHelper);
        this.longAdder = longAdder;
        this.tracingRedissonHelper = tracingRedissonHelper;
    }

    @Override
    public void add(long x) {
        Span span = tracingRedissonHelper.buildSpan("add", longAdder);
        span.setTag("value", x);
        tracingRedissonHelper.decorate(span, () -> longAdder.add(x));
    }

    @Override
    public void increment() {
        Span span = tracingRedissonHelper.buildSpan("increment", longAdder);
        tracingRedissonHelper.decorate(span, longAdder::increment);
    }

    @Override
    public void decrement() {
        Span span = tracingRedissonHelper.buildSpan("decrement", longAdder);
        tracingRedissonHelper.decorate(span, longAdder::decrement);
    }

    @Override
    public long sum() {
        Span span = tracingRedissonHelper.buildSpan("sum", longAdder);
        return tracingRedissonHelper.decorate(span, longAdder::sum);
    }

    @Override
    public void reset() {
        Span span = tracingRedissonHelper.buildSpan("reset", longAdder);
        tracingRedissonHelper.decorate(span, longAdder::reset);
    }

    @Override
    public RFuture<Long> sumAsync() {
        Span span = tracingRedissonHelper.buildSpan("sumAsync", longAdder);
        return tracingRedissonHelper.prepareRFuture(span, longAdder::sumAsync);
    }

    @Override
    public RFuture<Long> sumAsync(long timeout, TimeUnit timeUnit) {
        Span span = tracingRedissonHelper.buildSpan("sumAsync", longAdder);
        span.setTag("timeout", timeout);
        span.setTag("timeUnit", nullable(timeUnit));
        return tracingRedissonHelper.prepareRFuture(span, () -> longAdder.sumAsync(timeout, timeUnit));
    }

    @Override
    public RFuture<Void> resetAsync() {
        Span span = tracingRedissonHelper.buildSpan("resetAsync", longAdder);
        return tracingRedissonHelper.prepareRFuture(span, longAdder::resetAsync);
    }

    @Override
    public RFuture<Void> resetAsync(long timeout, TimeUnit timeUnit) {
        Span span = tracingRedissonHelper.buildSpan("resetAsync", longAdder);
        span.setTag("timeout", timeout);
        span.setTag("timeUnit", nullable(timeUnit));
        return tracingRedissonHelper
                .prepareRFuture(span, () -> longAdder.resetAsync(timeout, timeUnit));
    }

    @Override
    public void destroy() {
        Span span = tracingRedissonHelper.buildSpan("destroy", longAdder);
        tracingRedissonHelper.decorate(span, longAdder::destroy);
    }

}
