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
import java.util.Map;
import org.redisson.api.RFuture;
import org.redisson.api.mapreduce.RCollator;
import org.redisson.api.mapreduce.RMapReduceExecutor;

public class TracingRMapReduceExecutor<VIn, KOut, VOut> implements
    RMapReduceExecutor<VIn, KOut, VOut> {
  private final RMapReduceExecutor<VIn, KOut, VOut> executor;
  private final TracingRedissonHelper tracingRedissonHelper;

  public TracingRMapReduceExecutor(RMapReduceExecutor<VIn, KOut, VOut> executor,
      TracingRedissonHelper tracingRedissonHelper) {
    this.executor = executor;
    this.tracingRedissonHelper = tracingRedissonHelper;
  }

  @Override
  public Map<KOut, VOut> execute() {
    Span span = tracingRedissonHelper.buildSpan("execute");
    return tracingRedissonHelper.decorate(span, () -> executor.execute());
  }

  @Override
  public RFuture<Map<KOut, VOut>> executeAsync() {
    Span span = tracingRedissonHelper.buildSpan("executeAsync");
    return tracingRedissonHelper.prepareRFuture(span, executor::executeAsync);
  }

  @Override
  public void execute(String resultMapName) {
    Span span = tracingRedissonHelper.buildSpan("execute");
    span.setTag("resultMapName", nullable(resultMapName));
    tracingRedissonHelper.decorate(span, () -> executor.execute(resultMapName));
  }

  @Override
  public RFuture<Void> executeAsync(String resultMapName) {
    Span span = tracingRedissonHelper.buildSpan("executeAsync");
    span.setTag("resultMapName", nullable(resultMapName));
    return tracingRedissonHelper.prepareRFuture(span, () -> executor.executeAsync(resultMapName));
  }

  @Override
  public <R> R execute(RCollator<KOut, VOut, R> collator) {
    Span span = tracingRedissonHelper.buildSpan("execute");
    span.setTag("collator", nullable(collator));
    return tracingRedissonHelper.decorate(span, () -> executor.execute(collator));
  }

  @Override
  public <R> RFuture<R> executeAsync(RCollator<KOut, VOut, R> collator) {
    Span span = tracingRedissonHelper.buildSpan("executeAsync");
    span.setTag("collator", nullable(collator));
    return tracingRedissonHelper.prepareRFuture(span, () -> executor.executeAsync(collator));
  }

}
