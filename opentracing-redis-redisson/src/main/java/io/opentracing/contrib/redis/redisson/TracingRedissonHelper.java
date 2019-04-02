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

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.contrib.redis.common.TracingConfiguration;
import io.opentracing.contrib.redis.common.TracingHelper;
import java.util.function.Supplier;
import org.redisson.api.RFuture;
import org.redisson.api.RObject;

class TracingRedissonHelper extends TracingHelper {

  TracingRedissonHelper(TracingConfiguration tracingConfiguration) {
    super(tracingConfiguration);
  }

  Span buildSpan(String operationName, RObject rObject) {
    return buildSpan(operationName).setTag("name", rObject.getName());
  }


  private <T> RFuture<T> continueScopeSpan(RFuture<T> redisFuture) {
    Tracer tracer = getNullSafeTracer();
    Span span = tracer.activeSpan();
    CompletableRFuture<T> customRedisFuture = new CompletableRFuture<>(redisFuture);
    redisFuture.whenComplete((v, throwable) -> {
      try (Scope ignored = tracer.scopeManager().activate(span)) {
        if (throwable != null) {
          customRedisFuture.completeExceptionally(throwable);
        } else {
          customRedisFuture.complete(v);
        }
      }
    });
    return customRedisFuture;
  }

  private <V> RFuture<V> setCompleteAction(RFuture<V> future, Span span) {
    future.whenComplete((v, throwable) -> {
      if (throwable != null) {
        onError(throwable, span);
      }
      span.finish();
    });

    return future;
  }

  <V> RFuture<V> prepareRFuture(Span span, Supplier<RFuture<V>> futureSupplier) {
    RFuture<V> future;
    try {
      future = futureSupplier.get();
    } catch (Exception e) {
      onError(e, span);
      span.finish();
      throw e;
    }

    return continueScopeSpan(setCompleteAction(future, span));
  }
}
