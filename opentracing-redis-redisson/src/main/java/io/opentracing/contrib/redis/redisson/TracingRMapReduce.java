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

import java.util.concurrent.TimeUnit;
import org.redisson.api.mapreduce.RMapReduce;
import org.redisson.api.mapreduce.RMapper;
import org.redisson.api.mapreduce.RReducer;

public class TracingRMapReduce<KIn, VIn, KOut, VOut> extends
    TracingRMapReduceExecutor<VIn, KOut, VOut> implements
    RMapReduce<KIn, VIn, KOut, VOut> {
  private final RMapReduce<KIn, VIn, KOut, VOut> mapReduce;
  private final TracingRedissonHelper tracingRedissonHelper;

  public TracingRMapReduce(RMapReduce<KIn, VIn, KOut, VOut> mapReduce,
      TracingRedissonHelper tracingRedissonHelper) {
    super(mapReduce, tracingRedissonHelper);
    this.mapReduce = mapReduce;
    this.tracingRedissonHelper = tracingRedissonHelper;
  }

  @Override
  public RMapReduce<KIn, VIn, KOut, VOut> timeout(long timeout, TimeUnit unit) {
    return new TracingRMapReduce<>(mapReduce.timeout(timeout, unit), tracingRedissonHelper);
  }

  @Override
  public RMapReduce<KIn, VIn, KOut, VOut> mapper(
      RMapper<KIn, VIn, KOut, VOut> mapper) {
    return new TracingRMapReduce<>(mapReduce.mapper(mapper), tracingRedissonHelper);
  }

  @Override
  public RMapReduce<KIn, VIn, KOut, VOut> reducer(
      RReducer<KOut, VOut> reducer) {
    return new TracingRMapReduce<>(mapReduce.reducer(reducer), tracingRedissonHelper);
  }

}
