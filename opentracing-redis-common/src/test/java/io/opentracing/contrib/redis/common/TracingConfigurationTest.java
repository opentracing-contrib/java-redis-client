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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import io.opentracing.Tracer;
import io.opentracing.mock.MockTracer;
import java.util.function.Function;
import org.junit.Test;

public class TracingConfigurationTest {

  @Test
  public void testDefaultValues() {
    Tracer tracer = new MockTracer();
    TracingConfiguration configuration = new TracingConfiguration.Builder(tracer).build();
    assertFalse(configuration.isTraceWithActiveSpanOnly());
    assertEquals(TracingConfiguration.DEFAULT_KEYS_MAX_LENGTH, configuration.getKeysMaxLength());
    assertSame(tracer, configuration.getTracer());
    assertSame(RedisSpanNameProvider.OPERATION_NAME, configuration.getSpanNameProvider());
  }

  @Test
  public void test() {
    Tracer tracer = new MockTracer();
    Function<String, String> spanNameProvider = RedisSpanNameProvider.PREFIX_OPERATION_NAME("test");

    TracingConfiguration configuration = new TracingConfiguration.Builder(tracer)
        .withKeysMaxLength(10)
        .traceWithActiveSpanOnly(true)
        .withSpanNameProvider(spanNameProvider)
        .build();

    assertTrue(configuration.isTraceWithActiveSpanOnly());
    assertEquals(10, configuration.getKeysMaxLength());
    assertSame(tracer, configuration.getTracer());
    assertSame(spanNameProvider, configuration.getSpanNameProvider());
  }

  @Test
  public void testWrongValues() {
    TracingConfiguration configuration = new TracingConfiguration.Builder(null)
        .withKeysMaxLength(-20) // will be reverted to TracingConfiguration.DEFAULT_KEYS_MAX_LENGTH
        .withSpanNameProvider(null) // will be reverted to RedisSpanNameProvider.OPERATION_NAME
        .build();

    assertFalse(configuration.isTraceWithActiveSpanOnly());
    assertEquals(TracingConfiguration.DEFAULT_KEYS_MAX_LENGTH, configuration.getKeysMaxLength());
    assertNull(configuration.getTracer());
    assertSame(RedisSpanNameProvider.OPERATION_NAME, configuration.getSpanNameProvider());
  }
}