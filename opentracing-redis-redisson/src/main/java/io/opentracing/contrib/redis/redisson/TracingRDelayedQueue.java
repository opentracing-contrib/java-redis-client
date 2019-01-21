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
import org.redisson.api.RDelayedQueue;
import org.redisson.api.RFuture;

import java.util.concurrent.TimeUnit;

import static io.opentracing.contrib.redis.common.TracingHelper.nullable;

public class TracingRDelayedQueue<V> extends TracingRQueue<V> implements RDelayedQueue<V> {
    private final RDelayedQueue<V> queue;
    private final TracingRedissonHelper tracingRedissonHelper;

    public TracingRDelayedQueue(RDelayedQueue<V> queue, TracingRedissonHelper tracingRedissonHelper) {
        super(queue, tracingRedissonHelper);
        this.queue = queue;
        this.tracingRedissonHelper = tracingRedissonHelper;
    }

    @Override
    public void offer(V e, long delay, TimeUnit timeUnit) {
        Span span = tracingRedissonHelper.buildSpan("offer", queue);
        span.setTag("element", nullable(e));
        span.setTag("delay", delay);
        span.setTag("timeUnit", nullable(timeUnit));
        tracingRedissonHelper.decorate(span, () -> queue.offer(e, delay, timeUnit));
    }

    @Override
    public RFuture<Void> offerAsync(V e, long delay, TimeUnit timeUnit) {
        Span span = tracingRedissonHelper.buildSpan("offerAsync", queue);
        span.setTag("element", nullable(e));
        span.setTag("delay", delay);
        span.setTag("timeUnit", nullable(timeUnit));
        return tracingRedissonHelper.prepareRFuture(span, () -> queue.offerAsync(e, delay, timeUnit));
    }


    @Override
    public void destroy() {
        Span span = tracingRedissonHelper.buildSpan("destroy", queue);
        tracingRedissonHelper.decorate(span, queue::destroy);
    }
}
