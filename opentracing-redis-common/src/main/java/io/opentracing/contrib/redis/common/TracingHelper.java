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
package io.opentracing.contrib.redis.common;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.Tracer.SpanBuilder;
import io.opentracing.noop.NoopSpan;
import io.opentracing.tag.Tags;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;


public class TracingHelper {

  public static final String COMPONENT_NAME = "java-redis";
  public static final String DB_TYPE = "redis";
  protected final Tracer tracer;
  private final boolean traceWithActiveSpanOnly;
  private final Function<String, String> spanNameProvider;
  private final int maxKeysLength;
  private final Map<String, String> extensionTags;

  public TracingHelper(TracingConfiguration tracingConfiguration) {
    this.tracer = tracingConfiguration.getTracer();
    this.traceWithActiveSpanOnly = tracingConfiguration.isTraceWithActiveSpanOnly();
    this.spanNameProvider = tracingConfiguration.getSpanNameProvider();
    this.maxKeysLength = tracingConfiguration.getKeysMaxLength();
    this.extensionTags = tracingConfiguration.getExtensionTags();
  }

  private SpanBuilder builder(String operationName) {
    SpanBuilder sb = tracer.buildSpan(spanNameProvider.apply(operationName))
        .withTag(Tags.COMPONENT.getKey(), COMPONENT_NAME)
        .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CLIENT)
        .withTag(Tags.DB_TYPE.getKey(), DB_TYPE);
    extensionTags.forEach(sb::withTag);
    return sb;
  }

  public Span buildSpan(String operationName, byte[][] keys) {
    if (traceWithActiveSpanOnly && tracer.activeSpan() == null) {
      return NoopSpan.INSTANCE;
    } else {
      return builder(operationName)
          .withTag("keys", toString(keys))
          .start();
    }
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
      return builder(operationName).withTag("key", Arrays.toString(key))
          .start();
    }
  }

  public Span buildSpan(String operationName, Object[] keys) {
    if (traceWithActiveSpanOnly && tracer.activeSpan() == null) {
      return NoopSpan.INSTANCE;
    } else {
      return builder(operationName).withTag("keys",
          Arrays.toString(limitKeys(keys))).start();
    }
  }

  <T> T[] limitKeys(T[] keys) {
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
    if (object == null) {
      return "";
    }
    return object.toString();
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

  private static String toStringWithDeserialization(byte[][] array) {
    if (array == null) {
      return "null";
    }

    List<String> list = new ArrayList<>();

    for (byte[] bytes : array) {
      list.add(deserialize(bytes));
    }

    return "[" + String.join(", ", list) + "]";
  }

  public static String collectionToString(Collection<?> collection) {
    if (collection == null) {
      return "";
    }
    return collection.stream().map(Object::toString).collect(Collectors.joining(", "));
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

  public static <K, V> String mapToString(Map<K, V> map) {
    if (map == null) {
      return "";
    }
    return map.entrySet()
        .stream()
        .map(entry -> entry.getKey() + " -> " + entry.getValue())
        .collect(Collectors.joining(", "));
  }

  public <T> T decorate(Span span, Supplier<T> supplier) {
    try (Scope ignore = tracer.scopeManager().activate(span)) {
      return supplier.get();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }


  public void decorate(Span span, Action action) {
    try (Scope ignore = tracer.scopeManager().activate(span)) {
      action.execute();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  public <T extends Exception> void decorateThrowing(Span span, ThrowingAction<T> action) throws T {
    try (Scope ignore = tracer.scopeManager().activate(span)) {
      action.execute();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  public <T extends Exception, V> V decorateThrowing(Span span, ThrowingSupplier<T, V> supplier)
      throws T {
    try (Scope ignore = tracer.scopeManager().activate(span)) {
      return supplier.get();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  private static String deserialize(byte[] bytes) {
    return (bytes == null ? "" : new String(bytes, StandardCharsets.UTF_8));
  }

  public <T> T doInScope(String command, byte[] key, Supplier<T> supplier) {
    Span span = buildSpan(command, deserialize(key));
    return activateAndCloseSpan(span, supplier);
  }

  public <T> T doInScope(String command, Supplier<T> supplier) {
    Span span = buildSpan(command);
    return activateAndCloseSpan(span, supplier);
  }

  public void doInScope(String command, byte[] key, Runnable runnable) {
    Span span = buildSpan(command, deserialize(key));
    activateAndCloseSpan(span, runnable);
  }

  public void doInScope(String command, Runnable runnable) {
    Span span = buildSpan(command);
    activateAndCloseSpan(span, runnable);
  }

  public <T> T doInScope(String command, byte[][] keys, Supplier<T> supplier) {
    Span span = buildSpan(command);
    span.setTag("keys", toStringWithDeserialization(limitKeys(keys)));
    return activateAndCloseSpan(span, supplier);
  }

  private <T> T activateAndCloseSpan(Span span, Supplier<T> supplier) {
    try (Scope ignored = tracer.activateSpan(span)) {
      return supplier.get();
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  private void activateAndCloseSpan(Span span, Runnable runnable) {
    try (Scope ignored = tracer.activateSpan(span)) {
      runnable.run();
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }
}
