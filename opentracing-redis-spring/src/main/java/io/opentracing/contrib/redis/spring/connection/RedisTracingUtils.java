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
package io.opentracing.contrib.redis.spring.connection;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.contrib.redis.common.TracingHelper;
import io.opentracing.noop.NoopScopeManager.NoopScope;
import io.opentracing.tag.StringTag;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import java.util.function.Supplier;


/**
 * Set of OpenTracing utility methods for Redis.
 *
 * Base on
 * https://github.com/opentracing-contrib/java-jdbc/blob/master/src/main/java/io/opentracing/contrib/jdbc/JdbcTracingUtils.java
 *
 * @author Daniel del Castillo
 */
class RedisTracingUtils {

  private static final StringTag COMMAND_TAG = new StringTag("command");
  private static final String REDIS_COMMAND = "RedisCommand";

  static <T> T doInScope(String command, Supplier<T> supplier, boolean withActiveSpanOnly,
      Tracer tracer) {
    Scope scope =
        RedisTracingUtils.buildScope(command, withActiveSpanOnly, tracer);
    try {
      return supplier.get();
    } catch (Exception e) {
      TracingHelper.onError(e, scope.span());
      throw e;
    } finally {
      scope.close();
    }
  }

  private static Scope buildScope(String command, boolean withActiveSpanOnly,
      Tracer tracer) {
    Tracer currentTracer = getNullSafeTracer(tracer);
    if (withActiveSpanOnly && currentTracer.activeSpan() == null) {
      return NoopScope.INSTANCE;
    }

    Tracer.SpanBuilder spanBuilder = currentTracer.buildSpan(RedisTracingUtils.REDIS_COMMAND)
        .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CLIENT);

    Scope scope = spanBuilder.startActive(true);
    decorate(scope.span(), command);

    return scope;
  }

  private static Tracer getNullSafeTracer(final Tracer tracer) {
    if (tracer == null) {
      return GlobalTracer.get();
    }
    return tracer;
  }

  private static void decorate(Span span, String command) {
    Tags.COMPONENT.set(span, TracingHelper.COMPONENT_NAME);
    COMMAND_TAG.set(span, command);
  }

}
