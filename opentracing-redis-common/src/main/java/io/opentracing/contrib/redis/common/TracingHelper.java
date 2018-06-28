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

import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.Tracer.SpanBuilder;
import io.opentracing.noop.NoopSpan;
import io.opentracing.tag.Tags;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;


public class TracingHelper {

  private static final String COMPONENT_NAME = "java-redis";
  private final Tracer tracer;
  private final boolean traceWithActiveSpanOnly;
  private final Function<String, String> spanNameProvider;

  public TracingHelper(Tracer tracer, boolean traceWithActiveSpanOnly) {
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    this.spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
  }

  public TracingHelper(Tracer tracer, boolean traceWithActiveSpanOnly, Function<String, String> spanNameProvider) {
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    this.spanNameProvider = spanNameProvider;
  }


  private SpanBuilder builder(String operationName) {
    return tracer.buildSpan(this.spanNameProvider.apply(operationName))
        .withTag(Tags.COMPONENT.getKey(), COMPONENT_NAME)
        .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CLIENT)
        .withTag(Tags.DB_TYPE.getKey(), "redis");
  }

  public Span buildSpan(String operationName) {
    if (traceWithActiveSpanOnly && tracer.activeSpan() == null) {
      return NoopSpan.INSTANCE;
    } else {
      return builder(operationName).start();
    }
  }

  public Span buildSpan(String operationName, Object key) {
    if (traceWithActiveSpanOnly && tracer.activeSpan() == null) {
      return NoopSpan.INSTANCE;
    } else {
      return builder(operationName).withTag("key", nullable(key)).start();
    }
  }

  public Span buildSpan(String operationName, byte[] key) {
    if (traceWithActiveSpanOnly && tracer.activeSpan() == null) {
      return NoopSpan.INSTANCE;
    } else {
      return builder(operationName).withTag("key", Arrays.toString(key)).start();
    }
  }

  public Span buildSpan(String operationName, Object[] keys) {
    if (traceWithActiveSpanOnly && tracer.activeSpan() == null) {
      return NoopSpan.INSTANCE;
    } else {
      return builder(operationName).withTag("keys", Arrays.toString(keys)).start();
    }
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
}
