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

import io.opentracing.Tracer;
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import java.util.function.Function;
import org.junit.Before;
import org.junit.Test;


public class TracingHelperTest {

  private Tracer mockTracer = new MockTracer();
  private MockSpan span;
  private String prefix = "redis.";
  private Function<String, String> prefixSpanName;
  private TracingHelper helperWithProvider;
  private TracingHelper helperWithoutProvider;

  @Before
  public void setUp() {
    prefixSpanName = RedisSpanNameProvider.PREFIX_OPERATION_NAME(prefix);
    helperWithProvider = new TracingHelper(
        new TracingConfiguration.Builder(mockTracer).withSpanNameProvider(prefixSpanName).build());
    helperWithoutProvider = new TracingHelper(new TracingConfiguration.Builder(mockTracer).build());
  }

  @Test
  public void testPrefix() {
    span = (MockSpan) (helperWithProvider.buildSpan("get"));
    assertEquals("redis.get", span.operationName());
  }

  @Test
  public void testDefault() {
    span = (MockSpan) (helperWithoutProvider.buildSpan("get"));
    assertEquals("get", span.operationName());
  }

  @Test
  public void limitKeys() {
    TracingHelper helper = new TracingHelper(new TracingConfiguration.Builder(mockTracer)
        .withKeysMaxLength(2).build());
    Object[] keys = {"one", "two", "three"};
    assertEquals(2, helper.limitKeys(keys).length);
  }
}
