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
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.tag.Tags;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Daniel del Castillo
 */
final class AssertionUtils {

  private static final String COMMAND_TAG = "command";


  /**
   * Make sure we get one span once we execute a Redis command.
   */
  static void commandCreatesNewSpan(MockTracer tracer, String commandName, Runnable command) {
    command.run();
    assertEquals(1, tracer.finishedSpans().size());
    assertEquals(commandName, tracer.finishedSpans().get(0).tags().get(COMMAND_TAG));
    tracer.reset();
  }

  /**
   * Make sure that a span is created when an active span exists joins the active
   */
  static void commandSpanJoinsActiveSpan(MockTracer tracer, Runnable command) {
    try (Scope ignored = tracer.buildSpan("parent").startActive(true)) {
      command.run();
      assertEquals(1, tracer.finishedSpans().size());
    }

    assertEquals(2, tracer.finishedSpans().size());
    Optional<MockSpan> redisSpan = tracer.finishedSpans().stream()
        .filter((s) -> "java-redis".equals(s.tags().get(Tags.COMPONENT.getKey()))).findFirst();

    Optional<MockSpan> parentSpan =
        tracer.finishedSpans().stream().filter((s) -> "parent".equals(s.operationName())).findFirst();

    assertTrue(redisSpan.isPresent());
    assertTrue(parentSpan.isPresent());

    assertEquals(redisSpan.get().context().traceId(), parentSpan.get().context().traceId());
    assertEquals(redisSpan.get().parentId(), parentSpan.get().context().spanId());
  }

  private AssertionUtils() {}

}
