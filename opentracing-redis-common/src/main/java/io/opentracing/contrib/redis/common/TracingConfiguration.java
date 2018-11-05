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

import io.opentracing.Tracer;
import java.util.function.Function;

public class TracingConfiguration {
  static final int DEFAULT_KEYS_MAX_LENGTH = 100;
  private final Tracer tracer;
  private final boolean traceWithActiveSpanOnly;
  private final int keysMaxLength;
  private final Function<String, String> spanNameProvider;

  private TracingConfiguration(Tracer tracer, boolean traceWithActiveSpanOnly, int keysMaxLength,
      Function<String, String> spanNameProvider) {
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    this.keysMaxLength = keysMaxLength;
    this.spanNameProvider = spanNameProvider;
  }

  public Tracer getTracer() {
    return tracer;
  }

  public boolean isTraceWithActiveSpanOnly() {
    return traceWithActiveSpanOnly;
  }

  public int getKeysMaxLength() {
    return keysMaxLength;
  }

  public Function<String, String> getSpanNameProvider() {
    return spanNameProvider;
  }

  public static class Builder {
    private final Tracer tracer;
    private boolean traceWithActiveSpanOnly;
    private int keysMaxLength = DEFAULT_KEYS_MAX_LENGTH;
    private Function<String, String> spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;

    public Builder(Tracer tracer) {
      this.tracer = tracer;
    }

    /**
     * @param traceWithActiveSpanOnly if <code>true</code> then create new spans only if there is
     * an active span
     */
    public Builder traceWithActiveSpanOnly(boolean traceWithActiveSpanOnly) {
      this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
      return this;
    }

    /**
     * Customize the span name, default is operation name ({@link RedisSpanNameProvider#OPERATION_NAME})
     *
     * @param spanNameProvider function to customize the span name
     */
    public Builder withSpanNameProvider(Function<String, String> spanNameProvider) {
      this.spanNameProvider = spanNameProvider;
      return this;
    }

    /**
     * Limit number of keys to add to span (default is 100)
     *
     * @param keysMaxLength max keys length to add to span
     */
    public Builder withKeysMaxLength(int keysMaxLength) {
      this.keysMaxLength = keysMaxLength;
      return this;
    }

    public TracingConfiguration build() {
      if (spanNameProvider == null) {
        spanNameProvider = RedisSpanNameProvider.OPERATION_NAME;
      }
      if (keysMaxLength <= 0) {
        keysMaxLength = DEFAULT_KEYS_MAX_LENGTH;
      }
      return new TracingConfiguration(tracer, traceWithActiveSpanOnly, keysMaxLength,
          spanNameProvider);
    }
  }
}
