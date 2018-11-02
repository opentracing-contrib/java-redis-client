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
package io.opentracing.contrib.redis.common;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.Tracer.SpanBuilder;
import io.opentracing.noop.NoopSpan;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.Supplier;


public class TracingHelper {

  public static final String COMPONENT_NAME = "java-redis";
  private final Tracer tracer;
  private final boolean traceWithActiveSpanOnly;
  private final Function<String, String> spanNameProvider;
  private final int maxKeysLength;

  public TracingHelper(TracingConfiguration tracingConfiguration) {
    this.tracer = tracingConfiguration.getTracer();
    this.traceWithActiveSpanOnly = tracingConfiguration.isTraceWithActiveSpanOnly();
    this.spanNameProvider = tracingConfiguration.getSpanNameProvider();
    this.maxKeysLength = tracingConfiguration.getKeysMaxLength();
  }

  private static SpanBuilder builder(String operationName, Tracer tracer,
      Function<String, String> spanNameProvider) {
    return getNullSafeTracer(tracer).buildSpan(spanNameProvider.apply(operationName))
        .withTag(Tags.COMPONENT.getKey(), COMPONENT_NAME)
        .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CLIENT)
        .withTag(Tags.DB_TYPE.getKey(), "redis");
  }

  public static Span buildSpan(String operationName, boolean traceWithActiveSpanOnly,
      Tracer tracer) {
    if (traceWithActiveSpanOnly && getNullSafeTracer(tracer).activeSpan() == null) {
      return NoopSpan.INSTANCE;
    } else {
      return builder(operationName, tracer, RedisSpanNameProvider.OPERATION_NAME).start();
    }
  }

  public Span buildSpan(String operationName) {
    if (traceWithActiveSpanOnly && getNullSafeTracer(tracer).activeSpan() == null) {
      return NoopSpan.INSTANCE;
    } else {
      return builder(operationName, tracer, spanNameProvider).start();
    }
  }

  public Span buildSpan(String operationName, Object key) {
    if (traceWithActiveSpanOnly && getNullSafeTracer(tracer).activeSpan() == null) {
      return NoopSpan.INSTANCE;
    } else {
      return builder(operationName, tracer, spanNameProvider).withTag("key", nullable(key)).start();
    }
  }

  public Span buildSpan(String operationName, byte[] key) {
    if (traceWithActiveSpanOnly && getNullSafeTracer(tracer).activeSpan() == null) {
      return NoopSpan.INSTANCE;
    } else {
      return builder(operationName, tracer, spanNameProvider).withTag("key", Arrays.toString(key))
          .start();
    }
  }

  public Span buildSpan(String operationName, Object[] keys) {
    if (traceWithActiveSpanOnly && getNullSafeTracer(tracer).activeSpan() == null) {
      return NoopSpan.INSTANCE;
    } else {
      return builder(operationName, tracer, spanNameProvider).withTag("keys",
          Arrays.toString(limitKeys(keys))).start();
    }
  }

  Object[] limitKeys(Object[] keys) {
    if (keys != null && keys.length > maxKeysLength) {
      return Arrays.copyOfRange(keys, 0, maxKeysLength);
    }
    return keys;
  }

  public static void onError(Throwable throwable, Span span) {
    Tags.ERROR.set(span, Boolean.TRUE);

    if (throwable != null) {
      span.log(errorLogs(throwable));
    }
  }

  private static Map<String, Object> errorLogs(Throwable throwable) {
    Map<String, Object> errorLogs = new HashMap<>(2);
    errorLogs.put("event", Tags.ERROR.getKey());
    errorLogs.put("error.object", throwable);
    return errorLogs;
  }

  public static String nullable(Object object) {
    return object == null ? "null" : object.toString();
  }

  public static <V> String toString(Map<String, V> map) {
    List<String> list = new ArrayList<>();
    if (map != null) {
      for (Entry<String, V> entry : map.entrySet()) {
        list.add(entry.getKey() + "=" + entry.getValue());
      }
    }

    return "{" + String.join(", ", list) + "}";
  }

  public static String toString(byte[][] array) {
    if (array == null) {
      return "null";
    }

    List<String> list = new ArrayList<>();

    for (byte[] bytes : array) {
      list.add(Arrays.toString(bytes));
    }

    return "[" + String.join(", ", list) + "]";
  }

  public static String toString(Collection<byte[]> collection) {
    if (collection == null) {
      return "null";
    }
    List<String> list = new ArrayList<>();

    for (byte[] bytes : collection) {
      list.add(Arrays.toString(bytes));
    }

    return "[" + String.join(", ", list) + "]";
  }

  public static String toString(List<String> list) {
    if (list == null) {
      return "null";
    }

    return String.join(", ", list);
  }

  public static String toStringMap(Map<byte[], byte[]> map) {
    List<String> list = new ArrayList<>();
    if (map != null) {
      for (Entry<byte[], byte[]> entry : map.entrySet()) {
        list.add(Arrays.toString(entry.getKey()) + "="
            + Arrays.toString(entry.getValue()));
      }
    }

    return "{" + String.join(", ", list) + "}";

  }

  public static <V> String toStringMap2(Map<byte[], V> map) {
    List<String> list = new ArrayList<>();
    if (map != null) {
      for (Entry<byte[], V> entry : map.entrySet()) {
        list.add(Arrays.toString(entry.getKey()) + "=" + entry.getValue());
      }
    }

    return "{" + String.join(", ", list) + "}";
  }

  public static Tracer getNullSafeTracer(final Tracer tracer) {
    if (tracer == null) {
      return GlobalTracer.get();
    }
    return tracer;
  }

  public static <T extends Exception> void doInScopeExceptionally(String operationName,
      ThrowingAction<T> action,
      boolean traceWithActiveSpanOnly, Tracer tracer)
      throws T {
    Span span = buildSpan(operationName, traceWithActiveSpanOnly, tracer);
    try (Scope ignore = GlobalTracer.get().scopeManager().activate(span, false)) {
      action.execute();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  public static void doInScope(String operationName, Runnable action,
      boolean traceWithActiveSpanOnly, Tracer tracer) {
    Span span = buildSpan(operationName, traceWithActiveSpanOnly, tracer);
    try (Scope ignore = GlobalTracer.get().scopeManager().activate(span, false)) {
      action.run();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  public static <T> T doInScope(String operationName, Supplier<T> action,
      boolean traceWithActiveSpanOnly, Tracer tracer) {
    Span span = buildSpan(operationName, traceWithActiveSpanOnly, tracer);
    return doInScope(span, action);
  }

  public static <T> T doInScope(Span span, Supplier<T> action) {
    try (Scope ignore = GlobalTracer.get().scopeManager().activate(span, false)) {
      return action.get();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }
}
