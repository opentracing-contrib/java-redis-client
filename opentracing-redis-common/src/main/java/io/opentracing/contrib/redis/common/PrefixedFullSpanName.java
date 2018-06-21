package io.opentracing.contrib.redis.common;


public class PrefixedFullSpanName implements SpanPrefixProvider {
    private String prefix;

    public static class Builder implements SpanPrefixProvider.Builder {

        @Override
        public SpanPrefixProvider build() {return new PrefixedFullSpanName("redis");}
        public SpanPrefixProvider build(String prefix) {return new PrefixedFullSpanName(prefix);}
    }

    PrefixedFullSpanName(String prefix) {
        if(prefix == null || prefix.equals("")) {
            this.prefix = "";
        } else {
            this.prefix = prefix + ".";
        }
    }

    @Override
    public String prefixSpanName(String method) {
        if(method == null || method.equals("")) {
            return this.prefix + "NONE";
        } else {
            return this.prefix + method;
        }
    }

    @Override
    public String getPrefix() {
        return prefix;
    }

    public static Builder newBuilder() { return new Builder();}
}
