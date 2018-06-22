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
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class TracingHelperTest {


    Tracer mockTracer = new MockTracer();
    private MockSpan span;
    private String get = "get";
    private String set = "set";
    private String persist = "persist";
    PrefixedFullSpanName prefixedFullSpanName = new PrefixedFullSpanName("redis");
    private String prefix = prefixedFullSpanName.getPrefix();
    TracingHelper helper = new TracingHelper(mockTracer, false, prefixedFullSpanName);

    @Test
    public void testPrefix() {
        span = (MockSpan)(helper.buildSpan(get));
        assertEquals(prefix + get, span.operationName());
        span = (MockSpan)(helper.buildSpan(set));
        assertEquals(prefix + set, span.operationName());
        span = (MockSpan)(helper.buildSpan(persist));
        assertEquals(prefix + persist, span.operationName());
    }
}
