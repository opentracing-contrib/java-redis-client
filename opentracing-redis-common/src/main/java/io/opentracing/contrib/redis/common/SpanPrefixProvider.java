package io.opentracing.contrib.redis.common;

public interface SpanPrefixProvider {
    public interface Builder {
        SpanPrefixProvider build();
    }

    String prefixSpanName(String method);
    String getPrefix();

}
