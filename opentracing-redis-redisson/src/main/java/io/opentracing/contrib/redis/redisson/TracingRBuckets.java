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

import static io.opentracing.contrib.redis.common.TracingHelper.nullable;

import io.opentracing.Span;
import java.util.Arrays;
import java.util.Map;
import org.redisson.api.RBuckets;
import org.redisson.api.RFuture;

public class TracingRBuckets implements RBuckets {
  private final RBuckets buckets;
  private final TracingRedissonHelper tracingRedissonHelper;

  public TracingRBuckets(RBuckets buckets,
      TracingRedissonHelper tracingRedissonHelper) {
    this.buckets = buckets;
    this.tracingRedissonHelper = tracingRedissonHelper;
  }

  @Override
  public <V> Map<String, V> get(String... keys) {
    Span span = tracingRedissonHelper.buildSpan("get");
    span.setTag("keys", Arrays.toString(keys));
    return tracingRedissonHelper.decorate(span, () -> buckets.get(keys));
  }

  @Override
  public boolean trySet(Map<String, ?> buckets) {
    Span span = tracingRedissonHelper.buildSpan("trySet");
    span.setTag("buckets", nullable(buckets));
    return tracingRedissonHelper.decorate(span, () -> this.buckets.trySet(buckets));
  }

  @Override
  public void set(Map<String, ?> buckets) {
    Span span = tracingRedissonHelper.buildSpan("set");
    span.setTag("buckets", nullable(buckets));
    tracingRedissonHelper.decorate(span, () -> this.buckets.set(buckets));
  }

  @Override
  public <V> RFuture<Map<String, V>> getAsync(String... keys) {
    Span span = tracingRedissonHelper.buildSpan("getAsync");
    span.setTag("keys", Arrays.toString(keys));
    return tracingRedissonHelper.prepareRFuture(span, () -> buckets.getAsync(keys));
  }

  @Override
  public RFuture<Boolean> trySetAsync(Map<String, ?> buckets) {
    Span span = tracingRedissonHelper.buildSpan("trySetAsync");
    span.setTag("buckets", nullable(buckets));
    return tracingRedissonHelper.prepareRFuture(span, () -> this.buckets.trySetAsync(buckets));
  }

  @Override
  public RFuture<Void> setAsync(Map<String, ?> buckets) {
    Span span = tracingRedissonHelper.buildSpan("setAsync");
    span.setTag("buckets", nullable(buckets));
    return tracingRedissonHelper.prepareRFuture(span, () -> this.buckets.setAsync(buckets));
  }

}
