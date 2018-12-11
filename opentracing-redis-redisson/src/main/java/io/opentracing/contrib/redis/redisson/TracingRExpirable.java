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
import java.util.Date;
import java.util.concurrent.TimeUnit;
import org.redisson.api.RExpirable;
import org.redisson.api.RFuture;

public class TracingRExpirable extends TracingRObject implements RExpirable {
  private final RExpirable expirable;
  private final TracingRedissonHelper tracingRedissonHelper;

  public TracingRExpirable(RExpirable expirable, TracingRedissonHelper tracingRedissonHelper) {
    super(expirable, tracingRedissonHelper);
    this.expirable = expirable;
    this.tracingRedissonHelper = tracingRedissonHelper;
  }

  @Override
  public boolean expire(long timeToLive, TimeUnit timeUnit) {
    Span span = tracingRedissonHelper.buildSpan("expire", expirable);
    span.setTag("timeToLive", timeToLive);
    span.setTag("timeUnit", nullable(timeUnit));
    return tracingRedissonHelper.decorate(span, () -> expirable.expire(timeToLive, timeUnit));
  }

  @Override
  public boolean expireAt(long timestamp) {
    Span span = tracingRedissonHelper.buildSpan("expireAt", expirable);
    span.setTag("timestamp", timestamp);
    return tracingRedissonHelper.decorate(span, () -> expirable.expireAt(timestamp));
  }

  @Override
  public boolean expireAt(Date timestamp) {
    Span span = tracingRedissonHelper.buildSpan("expireAt", expirable);
    span.setTag("timestamp", nullable(timestamp));
    return tracingRedissonHelper.decorate(span, () -> expirable.expireAt(timestamp));
  }

  @Override
  public boolean clearExpire() {
    Span span = tracingRedissonHelper.buildSpan("clearExpire", expirable);
    return tracingRedissonHelper.decorate(span, expirable::clearExpire);
  }

  @Override
  public long remainTimeToLive() {
    Span span = tracingRedissonHelper.buildSpan("remainTimeToLive", expirable);
    return tracingRedissonHelper.decorate(span, expirable::remainTimeToLive);
  }

  @Override
  public RFuture<Boolean> expireAsync(long timeToLive,
      TimeUnit timeUnit) {
    Span span = tracingRedissonHelper.buildSpan("expireAsync", expirable);
    span.setTag("timeToLive", timeToLive);
    span.setTag("timeUnit", nullable(timeUnit));
    return tracingRedissonHelper
        .prepareRFuture(span, () -> expirable.expireAsync(timeToLive, timeUnit));
  }

  @Override
  public RFuture<Boolean> expireAtAsync(Date timestamp) {
    Span span = tracingRedissonHelper.buildSpan("expireAtAsync", expirable);
    span.setTag("timestamp", nullable(timestamp));
    return tracingRedissonHelper.prepareRFuture(span, () -> expirable.expireAtAsync(timestamp));
  }

  @Override
  public RFuture<Boolean> expireAtAsync(long timestamp) {
    Span span = tracingRedissonHelper.buildSpan("expireAtAsync", expirable);
    span.setTag("timestamp", timestamp);
    return tracingRedissonHelper.prepareRFuture(span, () -> expirable.expireAtAsync(timestamp));
  }

  @Override
  public RFuture<Boolean> clearExpireAsync() {
    Span span = tracingRedissonHelper.buildSpan("clearExpireAsync", expirable);
    return tracingRedissonHelper.prepareRFuture(span, expirable::clearExpireAsync);
  }

  @Override
  public RFuture<Long> remainTimeToLiveAsync() {
    Span span = tracingRedissonHelper.buildSpan("remainTimeToLiveAsync", expirable);
    return tracingRedissonHelper.prepareRFuture(span, expirable::remainTimeToLiveAsync);
  }

}
