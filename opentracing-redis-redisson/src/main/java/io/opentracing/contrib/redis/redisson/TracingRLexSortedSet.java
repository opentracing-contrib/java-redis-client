/*
 * Copyright 2017-2019 The OpenTracing Authors
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
package io.opentracing.contrib.redis.redisson;


import io.opentracing.Span;
import org.redisson.api.RFuture;
import org.redisson.api.RLexSortedSet;

import java.util.Collection;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import static io.opentracing.contrib.redis.common.TracingHelper.nullable;

public class TracingRLexSortedSet extends TracingRSortedSet<String> implements RLexSortedSet {
    private final RLexSortedSet set;
    private final TracingRedissonHelper tracingRedissonHelper;

    public TracingRLexSortedSet(RLexSortedSet set, TracingRedissonHelper tracingRedissonHelper) {
        super(set, tracingRedissonHelper);
        this.set = set;
        this.tracingRedissonHelper = tracingRedissonHelper;
    }

    @Override
    public String pollFirst() {
        Span span = tracingRedissonHelper.buildSpan("pollFirst", set);
        return tracingRedissonHelper.decorate(span, set::pollFirst);
    }

    @Override
    public String pollLast() {
        Span span = tracingRedissonHelper.buildSpan("pollLast", set);
        return tracingRedissonHelper.decorate(span, set::pollLast);
    }

    @Override
    public Integer revRank(String o) {
        Span span = tracingRedissonHelper.buildSpan("revRank", set);
        span.setTag("object", nullable(o));
        return tracingRedissonHelper.decorate(span, () -> set.revRank(o));
    }

    @Override
    public int removeRangeTail(String fromElement, boolean fromInclusive) {
        Span span = tracingRedissonHelper.buildSpan("removeRangeTail", set);
        span.setTag("fromElement", nullable(fromElement));
        span.setTag("fromInclusive", fromInclusive);
        return tracingRedissonHelper
                .decorate(span, () -> set.removeRangeTail(fromElement, fromInclusive));
    }

    @Override
    public int removeRangeHead(String toElement, boolean toInclusive) {
        Span span = tracingRedissonHelper.buildSpan("removeRangeHead", set);
        span.setTag("toElement", nullable(toElement));
        span.setTag("toInclusive", toInclusive);
        return tracingRedissonHelper.decorate(span, () -> set.removeRangeHead(toElement, toInclusive));
    }

    @Override
    public int removeRange(String fromElement, boolean fromInclusive, String toElement,
                           boolean toInclusive) {
        Span span = tracingRedissonHelper.buildSpan("removeRange", set);
        span.setTag("fromElement", nullable(fromElement));
        span.setTag("fromInclusive", fromInclusive);
        span.setTag("toElement", nullable(toElement));
        span.setTag("toInclusive", toInclusive);
        return tracingRedissonHelper
                .decorate(span, () -> set.removeRange(fromElement, fromInclusive, toElement, toInclusive));
    }

    @Override
    public int countTail(String fromElement, boolean fromInclusive) {
        Span span = tracingRedissonHelper.buildSpan("countTail", set);
        span.setTag("fromElement", nullable(fromElement));
        span.setTag("fromInclusive", fromInclusive);
        return tracingRedissonHelper.decorate(span, () -> set.countTail(fromElement, fromInclusive));
    }

    @Override
    public int countHead(String toElement, boolean toInclusive) {
        Span span = tracingRedissonHelper.buildSpan("countHead", set);
        span.setTag("toElement", nullable(toElement));
        span.setTag("toInclusive", toInclusive);
        return tracingRedissonHelper.decorate(span, () -> set.countHead(toElement, toInclusive));
    }

    @Override
    public Collection<String> rangeTail(String fromElement, boolean fromInclusive) {
        Span span = tracingRedissonHelper.buildSpan("rangeTail", set);
        span.setTag("fromElement", nullable(fromElement));
        span.setTag("fromInclusive", fromInclusive);
        return tracingRedissonHelper.decorate(span, () -> set.rangeTail(fromElement, fromInclusive));
    }

    @Override
    public Collection<String> rangeHead(String toElement, boolean toInclusive) {
        Span span = tracingRedissonHelper.buildSpan("rangeHead", set);
        span.setTag("toElement", nullable(toElement));
        span.setTag("toInclusive", toInclusive);
        return tracingRedissonHelper.decorate(span, () -> set.rangeHead(toElement, toInclusive));
    }

    @Override
    public Collection<String> range(String fromElement, boolean fromInclusive,
                                    String toElement, boolean toInclusive) {
        Span span = tracingRedissonHelper.buildSpan("range", set);
        span.setTag("fromElement", nullable(fromElement));
        span.setTag("fromInclusive", fromInclusive);
        span.setTag("toElement", nullable(toElement));
        span.setTag("toInclusive", toInclusive);
        return tracingRedissonHelper
                .decorate(span, () -> set.range(fromElement, fromInclusive, toElement, toInclusive));
    }

    @Override
    public Collection<String> rangeTail(String fromElement, boolean fromInclusive, int offset,
                                        int count) {
        Span span = tracingRedissonHelper.buildSpan("rangeTail", set);
        span.setTag("fromElement", nullable(fromElement));
        span.setTag("fromInclusive", fromInclusive);
        span.setTag("offset", offset);
        span.setTag("count", count);
        return tracingRedissonHelper
                .decorate(span, () -> set.rangeTail(fromElement, fromInclusive, offset, count));
    }

    @Override
    public Collection<String> rangeHead(String toElement, boolean toInclusive, int offset,
                                        int count) {
        Span span = tracingRedissonHelper.buildSpan("rangeHead", set);
        span.setTag("toElement", nullable(toElement));
        span.setTag("toInclusive", toInclusive);
        span.setTag("offset", offset);
        span.setTag("count", count);
        return tracingRedissonHelper
                .decorate(span, () -> set.rangeHead(toElement, toInclusive, offset, count));
    }

    @Override
    public Collection<String> range(String fromElement, boolean fromInclusive,
                                    String toElement, boolean toInclusive, int offset, int count) {
        Span span = tracingRedissonHelper.buildSpan("range", set);
        span.setTag("fromElement", nullable(fromElement));
        span.setTag("fromInclusive", fromInclusive);
        span.setTag("toElement", nullable(toElement));
        span.setTag("toInclusive", toInclusive);
        span.setTag("offset", offset);
        span.setTag("count", count);
        return tracingRedissonHelper
                .decorate(span,
                        () -> set.range(fromElement, fromInclusive, toElement, toInclusive, offset, count));
    }

    @Override
    public Collection<String> rangeTailReversed(String fromElement, boolean fromInclusive) {
        Span span = tracingRedissonHelper.buildSpan("rangeTailReversed", set);
        span.setTag("fromElement", nullable(fromElement));
        span.setTag("fromInclusive", fromInclusive);
        return tracingRedissonHelper
                .decorate(span, () -> set.rangeTailReversed(fromElement, fromInclusive));
    }

    @Override
    public Collection<String> rangeHeadReversed(String toElement, boolean toInclusive) {
        Span span = tracingRedissonHelper.buildSpan("rangeHeadReversed", set);
        span.setTag("toElement", nullable(toElement));
        span.setTag("toInclusive", toInclusive);
        return tracingRedissonHelper
                .decorate(span, () -> set.rangeHeadReversed(toElement, toInclusive));
    }

    @Override
    public Collection<String> rangeReversed(String fromElement, boolean fromInclusive,
                                            String toElement, boolean toInclusive) {
        Span span = tracingRedissonHelper.buildSpan("rangeReversed", set);
        span.setTag("fromElement", nullable(fromElement));
        span.setTag("fromInclusive", fromInclusive);
        span.setTag("toElement", nullable(toElement));
        span.setTag("toInclusive", toInclusive);
        return tracingRedissonHelper
                .decorate(span,
                        () -> set.rangeReversed(fromElement, fromInclusive, toElement, toInclusive));
    }

    @Override
    public Collection<String> rangeTailReversed(String fromElement, boolean fromInclusive,
                                                int offset, int count) {
        Span span = tracingRedissonHelper.buildSpan("rangeTailReversed", set);
        span.setTag("fromElement", nullable(fromElement));
        span.setTag("fromInclusive", fromInclusive);
        span.setTag("offset", offset);
        span.setTag("count", count);
        return tracingRedissonHelper
                .decorate(span, () -> set.rangeTailReversed(fromElement, fromInclusive, offset, count));
    }

    @Override
    public Collection<String> rangeHeadReversed(String toElement, boolean toInclusive,
                                                int offset, int count) {
        Span span = tracingRedissonHelper.buildSpan("rangeHeadReversed", set);
        span.setTag("toElement", nullable(toElement));
        span.setTag("toInclusive", toInclusive);
        span.setTag("offset", offset);
        span.setTag("count", count);
        return tracingRedissonHelper
                .decorate(span, () -> set.rangeHeadReversed(toElement, toInclusive, offset, count));
    }

    @Override
    public Collection<String> rangeReversed(String fromElement, boolean fromInclusive,
                                            String toElement, boolean toInclusive, int offset, int count) {
        Span span = tracingRedissonHelper.buildSpan("rangeReversed", set);
        span.setTag("fromElement", nullable(fromElement));
        span.setTag("fromInclusive", fromInclusive);
        span.setTag("toElement", nullable(toElement));
        span.setTag("toInclusive", toInclusive);
        span.setTag("offset", offset);
        span.setTag("count", count);
        return tracingRedissonHelper.decorate(span,
                () -> set.rangeReversed(fromElement, fromInclusive, toElement, toInclusive, offset, count));
    }

    @Override
    public int count(String fromElement, boolean fromInclusive, String toElement,
                     boolean toInclusive) {
        Span span = tracingRedissonHelper.buildSpan("count", set);
        span.setTag("fromElement", nullable(fromElement));
        span.setTag("fromInclusive", fromInclusive);
        span.setTag("toElement", nullable(toElement));
        span.setTag("toInclusive", toInclusive);
        return tracingRedissonHelper.decorate(span,
                () -> set.count(fromElement, fromInclusive, toElement, toInclusive));
    }

    @Override
    public Integer rank(String o) {
        Span span = tracingRedissonHelper.buildSpan("rank", set);
        span.setTag("object", nullable(o));
        return tracingRedissonHelper.decorate(span, () -> set.rank(o));
    }

    @Override
    public Collection<String> range(int startIndex, int endIndex) {
        Span span = tracingRedissonHelper.buildSpan("range", set);
        span.setTag("startIndex", startIndex);
        span.setTag("endIndex", endIndex);
        return tracingRedissonHelper.decorate(span, () -> set.range(startIndex, endIndex));
    }

    @Override
    public RFuture<String> pollLastAsync() {
        Span span = tracingRedissonHelper.buildSpan("pollLastAsync", set);
        return tracingRedissonHelper.prepareRFuture(span, set::pollLastAsync);
    }

    @Override
    public RFuture<String> pollFirstAsync() {
        Span span = tracingRedissonHelper.buildSpan("pollFirstAsync", set);
        return tracingRedissonHelper.prepareRFuture(span, set::pollFirstAsync);
    }

    @Override
    public RFuture<String> firstAsync() {
        Span span = tracingRedissonHelper.buildSpan("firstAsync", set);
        return tracingRedissonHelper.prepareRFuture(span, set::firstAsync);
    }

    @Override
    public RFuture<String> lastAsync() {
        Span span = tracingRedissonHelper.buildSpan("lastAsync", set);
        return tracingRedissonHelper.prepareRFuture(span, set::lastAsync);
    }

    @Override
    public RFuture<Collection<String>> readAllAsync() {
        Span span = tracingRedissonHelper.buildSpan("readAllAsync", set);
        return tracingRedissonHelper.prepareRFuture(span, set::readAllAsync);
    }

    @Override
    public RFuture<Integer> removeRangeAsync(String fromElement, boolean fromInclusive,
                                             String toElement, boolean toInclusive) {
        Span span = tracingRedissonHelper.buildSpan("removeRangeAsync", set);
        span.setTag("fromElement", nullable(fromElement));
        span.setTag("fromInclusive", fromInclusive);
        span.setTag("toElement", nullable(toElement));
        span.setTag("toInclusive", toInclusive);
        return tracingRedissonHelper.prepareRFuture(span,
                () -> set.removeRangeAsync(fromElement, fromInclusive, toElement, toInclusive));
    }

    @Override
    public RFuture<Integer> removeRangeTailAsync(String fromElement, boolean fromInclusive) {
        Span span = tracingRedissonHelper.buildSpan("removeRangeTailAsync", set);
        span.setTag("fromElement", nullable(fromElement));
        span.setTag("fromInclusive", fromInclusive);
        return tracingRedissonHelper.prepareRFuture(span,
                () -> set.removeRangeTailAsync(fromElement, fromInclusive));
    }

    @Override
    public RFuture<Integer> removeRangeHeadAsync(String toElement, boolean toInclusive) {
        Span span = tracingRedissonHelper.buildSpan("removeRangeHeadAsync", set);
        span.setTag("toElement", nullable(toElement));
        span.setTag("toInclusive", toInclusive);
        return tracingRedissonHelper.prepareRFuture(span,
                () -> set.removeRangeHeadAsync(toElement, toInclusive));
    }

    @Override
    public RFuture<Integer> countTailAsync(String fromElement, boolean fromInclusive) {
        Span span = tracingRedissonHelper.buildSpan("countTailAsync", set);
        span.setTag("fromElement", nullable(fromElement));
        span.setTag("fromInclusive", fromInclusive);
        return tracingRedissonHelper.prepareRFuture(span,
                () -> set.countTailAsync(fromElement, fromInclusive));
    }

    @Override
    public RFuture<Integer> countHeadAsync(String toElement, boolean toInclusive) {
        Span span = tracingRedissonHelper.buildSpan("countHeadAsync", set);
        span.setTag("toElement", nullable(toElement));
        span.setTag("toInclusive", toInclusive);
        return tracingRedissonHelper.prepareRFuture(span,
                () -> set.countHeadAsync(toElement, toInclusive));
    }

    @Override
    public RFuture<Collection<String>> rangeTailAsync(String fromElement, boolean fromInclusive) {
        Span span = tracingRedissonHelper.buildSpan("rangeTailAsync", set);
        span.setTag("fromElement", nullable(fromElement));
        span.setTag("fromInclusive", fromInclusive);
        return tracingRedissonHelper.prepareRFuture(span,
                () -> set.rangeTailAsync(fromElement, fromInclusive));
    }

    @Override
    public RFuture<Collection<String>> rangeHeadAsync(
            String toElement, boolean toInclusive) {
        Span span = tracingRedissonHelper.buildSpan("rangeHeadAsync", set);
        span.setTag("toElement", nullable(toElement));
        span.setTag("toInclusive", toInclusive);
        return tracingRedissonHelper.prepareRFuture(span,
                () -> set.rangeHeadAsync(toElement, toInclusive));
    }

    @Override
    public RFuture<Collection<String>> rangeAsync(
            String fromElement, boolean fromInclusive, String toElement, boolean toInclusive) {
        Span span = tracingRedissonHelper.buildSpan("rangeAsync", set);
        span.setTag("fromElement", nullable(fromElement));
        span.setTag("fromInclusive", fromInclusive);
        span.setTag("toElement", nullable(toElement));
        span.setTag("toInclusive", toInclusive);
        return tracingRedissonHelper.prepareRFuture(span,
                () -> set.rangeAsync(fromElement, fromInclusive, toElement, toInclusive));
    }

    @Override
    public RFuture<Collection<String>> rangeTailAsync(
            String fromElement, boolean fromInclusive, int offset, int count) {
        Span span = tracingRedissonHelper.buildSpan("rangeTailAsync", set);
        span.setTag("fromElement", nullable(fromElement));
        span.setTag("fromInclusive", fromInclusive);
        span.setTag("offset", offset);
        span.setTag("count", count);
        return tracingRedissonHelper.prepareRFuture(span,
                () -> set.rangeTailAsync(fromElement, fromInclusive, offset, count));
    }

    @Override
    public RFuture<Collection<String>> rangeHeadAsync(
            String toElement, boolean toInclusive, int offset, int count) {
        Span span = tracingRedissonHelper.buildSpan("rangeHeadAsync", set);
        span.setTag("toElement", nullable(toElement));
        span.setTag("toInclusive", toInclusive);
        span.setTag("offset", offset);
        span.setTag("count", count);
        return tracingRedissonHelper.prepareRFuture(span,
                () -> set.rangeHeadAsync(toElement, toInclusive, offset, count));
    }

    @Override
    public RFuture<Collection<String>> rangeAsync(
            String fromElement, boolean fromInclusive, String toElement, boolean toInclusive, int offset,
            int count) {
        Span span = tracingRedissonHelper.buildSpan("rangeAsync", set);
        span.setTag("fromElement", nullable(fromElement));
        span.setTag("fromInclusive", fromInclusive);
        span.setTag("toElement", nullable(toElement));
        span.setTag("toInclusive", toInclusive);
        span.setTag("offset", offset);
        span.setTag("count", count);
        return tracingRedissonHelper.prepareRFuture(span,
                () -> set.rangeAsync(fromElement, fromInclusive, toElement, toInclusive, offset, count));
    }

    @Override
    public RFuture<Collection<String>> rangeTailReversedAsync(
            String fromElement, boolean fromInclusive) {
        Span span = tracingRedissonHelper.buildSpan("rangeTailReversedAsync", set);
        span.setTag("fromElement", nullable(fromElement));
        span.setTag("fromInclusive", fromInclusive);
        return tracingRedissonHelper.prepareRFuture(span,
                () -> set.rangeTailReversedAsync(fromElement, fromInclusive));
    }

    @Override
    public RFuture<Collection<String>> rangeHeadReversedAsync(
            String toElement, boolean toInclusive) {
        Span span = tracingRedissonHelper.buildSpan("rangeHeadReversedAsync", set);
        span.setTag("toElement", nullable(toElement));
        span.setTag("toInclusive", toInclusive);
        return tracingRedissonHelper.prepareRFuture(span,
                () -> set.rangeHeadReversedAsync(toElement, toInclusive));
    }

    @Override
    public RFuture<Collection<String>> rangeReversedAsync(
            String fromElement, boolean fromInclusive, String toElement, boolean toInclusive) {
        Span span = tracingRedissonHelper.buildSpan("rangeReversedAsync", set);
        span.setTag("fromElement", nullable(fromElement));
        span.setTag("fromInclusive", fromInclusive);
        span.setTag("toElement", nullable(toElement));
        span.setTag("toInclusive", toInclusive);
        return tracingRedissonHelper.prepareRFuture(span,
                () -> set.rangeReversedAsync(fromElement, fromInclusive, toElement, toInclusive));
    }

    @Override
    public RFuture<Collection<String>> rangeTailReversedAsync(
            String fromElement, boolean fromInclusive, int offset, int count) {
        Span span = tracingRedissonHelper.buildSpan("rangeTailReversedAsync", set);
        span.setTag("fromElement", nullable(fromElement));
        span.setTag("fromInclusive", fromInclusive);
        span.setTag("offset", offset);
        span.setTag("count", count);
        return tracingRedissonHelper.prepareRFuture(span,
                () -> set.rangeTailReversedAsync(fromElement, fromInclusive, offset, count));
    }

    @Override
    public RFuture<Collection<String>> rangeHeadReversedAsync(
            String toElement, boolean toInclusive, int offset, int count) {
        Span span = tracingRedissonHelper.buildSpan("rangeHeadReversedAsync", set);
        span.setTag("toElement", nullable(toElement));
        span.setTag("toInclusive", toInclusive);
        span.setTag("offset", offset);
        span.setTag("count", count);
        return tracingRedissonHelper.prepareRFuture(span,
                () -> set.rangeHeadReversedAsync(toElement, toInclusive, offset, count));
    }

    @Override
    public RFuture<Collection<String>> rangeReversedAsync(
            String fromElement, boolean fromInclusive, String toElement, boolean toInclusive, int offset,
            int count) {
        Span span = tracingRedissonHelper.buildSpan("rangeReversedAsync", set);
        span.setTag("fromElement", nullable(fromElement));
        span.setTag("fromInclusive", fromInclusive);
        span.setTag("toElement", nullable(toElement));
        span.setTag("toInclusive", toInclusive);
        span.setTag("offset", offset);
        span.setTag("count", count);
        return tracingRedissonHelper.prepareRFuture(span,
                () -> set
                        .rangeReversedAsync(fromElement, fromInclusive, toElement, toInclusive, offset, count));
    }

    @Override
    public RFuture<Integer> countAsync(String fromElement, boolean fromInclusive,
                                       String toElement, boolean toInclusive) {
        Span span = tracingRedissonHelper.buildSpan("countAsync", set);
        span.setTag("fromElement", nullable(fromElement));
        span.setTag("fromInclusive", fromInclusive);
        span.setTag("toElement", nullable(toElement));
        span.setTag("toInclusive", toInclusive);
        return tracingRedissonHelper.prepareRFuture(span,
                () -> set.countAsync(fromElement, fromInclusive, toElement, toInclusive));
    }

    @Override
    public RFuture<Integer> rankAsync(String o) {
        Span span = tracingRedissonHelper.buildSpan("rankAsync", set);
        span.setTag("object", nullable(o));
        return tracingRedissonHelper.prepareRFuture(span, () -> set.rankAsync(o));
    }

    @Override
    public RFuture<Collection<String>> rangeAsync(int startIndex, int endIndex) {
        Span span = tracingRedissonHelper.buildSpan("rankAsync", set);
        span.setTag("startIndex", startIndex);
        span.setTag("endIndex", endIndex);
        return tracingRedissonHelper.prepareRFuture(span, () -> set.rangeAsync(startIndex, endIndex));
    }

    @Override
    public RFuture<Integer> revRankAsync(String o) {
        return set.revRankAsync(o);
    }

    @Override
    public RFuture<Boolean> retainAllAsync(Collection<?> c) {
        Span span = tracingRedissonHelper.buildSpan("retainAllAsync", set);
        return tracingRedissonHelper.prepareRFuture(span, () -> set.retainAllAsync(c));
    }

    @Override
    public RFuture<Boolean> removeAllAsync(Collection<?> c) {
        Span span = tracingRedissonHelper.buildSpan("removeAllAsync", set);
        return tracingRedissonHelper.prepareRFuture(span, () -> set.removeAllAsync(c));
    }

    @Override
    public RFuture<Boolean> containsAsync(Object o) {
        Span span = tracingRedissonHelper.buildSpan("containsAsync", set);
        span.setTag("object", nullable(o));
        return tracingRedissonHelper.prepareRFuture(span, () -> set.containsAsync(o));
    }

    @Override
    public RFuture<Boolean> containsAllAsync(Collection<?> c) {
        Span span = tracingRedissonHelper.buildSpan("containsAllAsync", set);
        return tracingRedissonHelper.prepareRFuture(span, () -> set.containsAllAsync(c));
    }

    @Override
    public RFuture<Boolean> removeAsync(Object o) {
        Span span = tracingRedissonHelper.buildSpan("removeAsync", set);
        span.setTag("object", nullable(o));
        return tracingRedissonHelper.prepareRFuture(span, () -> set.removeAsync(o));
    }

    @Override
    public RFuture<Integer> sizeAsync() {
        Span span = tracingRedissonHelper.buildSpan("sizeAsync", set);
        return tracingRedissonHelper.prepareRFuture(span, set::sizeAsync);
    }

    @Override
    public RFuture<Boolean> addAsync(String e) {
        Span span = tracingRedissonHelper.buildSpan("addAsync", set);
        span.setTag("element", nullable(e));
        return tracingRedissonHelper.prepareRFuture(span, () -> set.addAsync(e));
    }

    @Override
    public RFuture<Boolean> addAllAsync(Collection<? extends String> c) {
        Span span = tracingRedissonHelper.buildSpan("addAllAsync", set);
        return tracingRedissonHelper.prepareRFuture(span, () -> set.addAllAsync(c));
    }

    @Override
    public boolean expire(long timeToLive, TimeUnit timeUnit) {
        Span span = tracingRedissonHelper.buildSpan("expire", set);
        span.setTag("timeToLive", timeToLive);
        span.setTag("timeUnit", nullable(timeUnit));
        return tracingRedissonHelper.decorate(span, () -> set.expire(timeToLive, timeUnit));
    }

    @Override
    public boolean expireAt(long timestamp) {
        Span span = tracingRedissonHelper.buildSpan("expireAt", set);
        span.setTag("timestamp", timestamp);
        return tracingRedissonHelper.decorate(span, () -> set.expireAt(timestamp));
    }

    @Override
    public boolean expireAt(Date timestamp) {
        Span span = tracingRedissonHelper.buildSpan("expireAt", set);
        span.setTag("timestamp", nullable(timestamp));
        return tracingRedissonHelper.decorate(span, () -> set.expireAt(timestamp));
    }

    @Override
    public boolean clearExpire() {
        Span span = tracingRedissonHelper.buildSpan("clearExpire", set);
        return tracingRedissonHelper.decorate(span, set::clearExpire);
    }

    @Override
    public long remainTimeToLive() {
        Span span = tracingRedissonHelper.buildSpan("remainTimeToLive", set);
        return tracingRedissonHelper.decorate(span, set::remainTimeToLive);
    }

    @Override
    public RFuture<Boolean> expireAsync(long timeToLive,
                                        TimeUnit timeUnit) {
        Span span = tracingRedissonHelper.buildSpan("expireAsync", set);
        span.setTag("timeToLive", timeToLive);
        span.setTag("timeUnit", nullable(timeUnit));
        return tracingRedissonHelper.prepareRFuture(span, () -> set.expireAsync(timeToLive, timeUnit));
    }

    @Override
    public RFuture<Boolean> expireAtAsync(Date timestamp) {
        Span span = tracingRedissonHelper.buildSpan("expireAtAsync", set);
        span.setTag("timestamp", nullable(timestamp));
        return tracingRedissonHelper.prepareRFuture(span, () -> set.expireAtAsync(timestamp));
    }

    @Override
    public RFuture<Boolean> expireAtAsync(long timestamp) {
        Span span = tracingRedissonHelper.buildSpan("expireAtAsync", set);
        span.setTag("timestamp", timestamp);
        return tracingRedissonHelper.prepareRFuture(span, () -> set.expireAtAsync(timestamp));
    }

    @Override
    public RFuture<Boolean> clearExpireAsync() {
        Span span = tracingRedissonHelper.buildSpan("clearExpireAsync", set);
        return tracingRedissonHelper.prepareRFuture(span, set::clearExpireAsync);
    }

    @Override
    public RFuture<Long> remainTimeToLiveAsync() {
        Span span = tracingRedissonHelper.buildSpan("remainTimeToLiveAsync", set);
        return tracingRedissonHelper.prepareRFuture(span, set::remainTimeToLiveAsync);
    }


}
