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
