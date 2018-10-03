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

import java.util.function.Function;
import org.junit.Before;
import org.junit.Test;


public class RedisSpanNameProviderTest {

  private String prefix = "redis.";
  private String actual = "";
  private Function<String, String> prefixSpanName;

  @Before
  public void setUp() {
    prefixSpanName = RedisSpanNameProvider.PREFIX_OPERATION_NAME(prefix);
  }

  @Test
  public void testGetPrefix() {
    actual = prefixSpanName.apply("get");
    assertEquals("redis.get", actual);
  }

  @Test
  public void testSetPrefix() {
    actual = prefixSpanName.apply("set");
    assertEquals("redis.set", actual);
  }

  @Test
  public void testPersistPrefix() {
    actual = prefixSpanName.apply("persist");
    assertEquals("redis.persist", actual);
  }
}
