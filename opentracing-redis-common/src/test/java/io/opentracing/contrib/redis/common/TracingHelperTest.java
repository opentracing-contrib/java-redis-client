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
    private String prefix = "redis.";
    private String get = "get";
    private String set = "set";
    private String persist = "persist";
    SpanPrefixProvider spanPrefixProvider = new PrefixedFullSpanName("redis");
    TracingHelper helper = new TracingHelper(mockTracer, false, spanPrefixProvider);

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
