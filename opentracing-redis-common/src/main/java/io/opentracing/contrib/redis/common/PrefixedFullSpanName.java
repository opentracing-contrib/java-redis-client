package io.opentracing.contrib.redis.common;


import java.util.function.BiFunction;

public class PrefixedFullSpanName {
    private String prefix;

    public static class Builder {

        public PrefixedFullSpanName build() {return new PrefixedFullSpanName("redis");}
        public PrefixedFullSpanName build(String prefix) {return new PrefixedFullSpanName(prefix);}
    }

    PrefixedFullSpanName(String prefix) {
        if(prefix == null || prefix.equals("")) {
            this.prefix = "";
        } else {
            this.prefix = prefix + ".";
        }
    }

    public BiFunction<String, String, String> prefixSpanName = (prefix, method) -> {
        if(method == null || method.equals("")) {
            return prefix + "NONE";
        } else {
            return prefix + method;
        }};

    public String getPrefix() {
        return prefix;
    }

    public static Builder newBuilder() { return new Builder();}
}
