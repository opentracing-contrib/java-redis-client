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
import org.redisson.api.RPriorityBlockingQueue;

import java.util.Comparator;

import static io.opentracing.contrib.redis.common.TracingHelper.nullable;

public class TracingRPriorityBlockingQueue<V> extends TracingRBlockingQueue<V> implements
        RPriorityBlockingQueue<V> {
    private final RPriorityBlockingQueue<V> queue;
    private final TracingRedissonHelper tracingRedissonHelper;

    public TracingRPriorityBlockingQueue(RPriorityBlockingQueue<V> queue,
                                         TracingRedissonHelper tracingRedissonHelper) {
        super(queue, tracingRedissonHelper);
        this.queue = queue;
        this.tracingRedissonHelper = tracingRedissonHelper;
    }

    @Override
    public Comparator<? super V> comparator() {
        return queue.comparator();
    }

    @Override
    public boolean trySetComparator(Comparator<? super V> comparator) {
        Span span = tracingRedissonHelper.buildSpan("trySetComparator", queue);
        span.setTag("comparator", nullable(comparator));
        return tracingRedissonHelper.decorate(span, () -> queue.trySetComparator(comparator));
    }
}
