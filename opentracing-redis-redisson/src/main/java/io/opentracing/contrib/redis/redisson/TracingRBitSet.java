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
package io.opentracing.contrib.redis.redisson;


import static io.opentracing.contrib.redis.common.TracingHelper.nullable;

import io.opentracing.Span;
import java.util.Arrays;
import java.util.BitSet;
import org.redisson.api.RBitSet;
import org.redisson.api.RFuture;

public class TracingRBitSet extends TracingRExpirable implements RBitSet {
  private final RBitSet bitSet;
  private final TracingRedissonHelper tracingRedissonHelper;

  public TracingRBitSet(RBitSet bitSet,
      TracingRedissonHelper tracingRedissonHelper) {
    super(bitSet, tracingRedissonHelper);
    this.bitSet = bitSet;
    this.tracingRedissonHelper = tracingRedissonHelper;
  }

  @Override
  public long length() {
    Span span = tracingRedissonHelper.buildSpan("length", bitSet);
    return tracingRedissonHelper.decorate(span, bitSet::length);
  }

  @Override
  public void set(long fromIndex, long toIndex, boolean value) {
    Span span = tracingRedissonHelper.buildSpan("set", bitSet);
    span.setTag("fromIndex", fromIndex);
    span.setTag("toIndex", toIndex);
    span.setTag("value", value);
    tracingRedissonHelper.decorate(span, () -> bitSet.set(fromIndex, toIndex, value));
  }

  @Override
  public void clear(long fromIndex, long toIndex) {
    Span span = tracingRedissonHelper.buildSpan("clear", bitSet);
    span.setTag("fromIndex", fromIndex);
    span.setTag("toIndex", toIndex);
    tracingRedissonHelper.decorate(span, () -> bitSet.clear(fromIndex, toIndex));
  }

  @Override
  public void set(BitSet bs) {
    Span span = tracingRedissonHelper.buildSpan("set", bitSet);
    span.setTag("bs", nullable(bs));
    tracingRedissonHelper.decorate(span, () -> bitSet.set(bs));
  }

  @Override
  public void not() {
    Span span = tracingRedissonHelper.buildSpan("not", bitSet);
    tracingRedissonHelper.decorate(span, bitSet::not);
  }

  @Override
  public void set(long fromIndex, long toIndex) {
    Span span = tracingRedissonHelper.buildSpan("set", bitSet);
    span.setTag("fromIndex", fromIndex);
    span.setTag("toIndex", toIndex);
    tracingRedissonHelper.decorate(span, () -> bitSet.set(fromIndex, toIndex));
  }

  @Override
  public long size() {
    Span span = tracingRedissonHelper.buildSpan("size", bitSet);
    return tracingRedissonHelper.decorate(span, bitSet::size);
  }

  @Override
  public boolean get(long bitIndex) {
    Span span = tracingRedissonHelper.buildSpan("get", bitSet);
    span.setTag("bitIndex", bitIndex);
    return tracingRedissonHelper.decorate(span, () -> bitSet.get(bitIndex));
  }

  @Override
  public boolean set(long bitIndex) {
    Span span = tracingRedissonHelper.buildSpan("set", bitSet);
    span.setTag("bitIndex", bitIndex);
    return tracingRedissonHelper.decorate(span, () -> bitSet.set(bitIndex));
  }

  @Override
  public void set(long bitIndex, boolean value) {
    Span span = tracingRedissonHelper.buildSpan("set", bitSet);
    span.setTag("bitIndex", bitIndex);
    span.setTag("value", value);
    tracingRedissonHelper.decorate(span, () -> bitSet.set(bitIndex, value));
  }

  @Override
  public byte[] toByteArray() {
    Span span = tracingRedissonHelper.buildSpan("toByteArray", bitSet);
    return tracingRedissonHelper.decorate(span, bitSet::toByteArray);
  }

  @Override
  public long cardinality() {
    Span span = tracingRedissonHelper.buildSpan("cardinality", bitSet);
    return tracingRedissonHelper.decorate(span, bitSet::cardinality);
  }

  @Override
  public boolean clear(long bitIndex) {
    Span span = tracingRedissonHelper.buildSpan("clear", bitSet);
    span.setTag("bitIndex", bitIndex);
    return tracingRedissonHelper.decorate(span, () -> bitSet.clear(bitIndex));
  }

  @Override
  public void clear() {
    Span span = tracingRedissonHelper.buildSpan("clear", bitSet);
    tracingRedissonHelper.decorate(span, () -> bitSet.clear());
  }

  @Override
  public BitSet asBitSet() {
    Span span = tracingRedissonHelper.buildSpan("asBitSet", bitSet);
    return tracingRedissonHelper.decorate(span, bitSet::asBitSet);
  }

  @Override
  public void or(String... bitSetNames) {
    Span span = tracingRedissonHelper.buildSpan("or", bitSet);
    span.setTag("bitSetNames", Arrays.toString(bitSetNames));
    tracingRedissonHelper.decorate(span, () -> bitSet.or(bitSetNames));
  }

  @Override
  public void and(String... bitSetNames) {
    Span span = tracingRedissonHelper.buildSpan("and", bitSet);
    span.setTag("bitSetNames", Arrays.toString(bitSetNames));
    tracingRedissonHelper.decorate(span, () -> bitSet.and(bitSetNames));
  }

  @Override
  public void xor(String... bitSetNames) {
    Span span = tracingRedissonHelper.buildSpan("xor", bitSet);
    span.setTag("bitSetNames", Arrays.toString(bitSetNames));
    tracingRedissonHelper.decorate(span, () -> bitSet.xor(bitSetNames));
  }

  @Override
  public RFuture<byte[]> toByteArrayAsync() {
    Span span = tracingRedissonHelper.buildSpan("toByteArrayAsync", bitSet);
    return tracingRedissonHelper.prepareRFuture(span, bitSet::toByteArrayAsync);
  }

  @Override
  public RFuture<Long> lengthAsync() {
    Span span = tracingRedissonHelper.buildSpan("lengthAsync", bitSet);
    return tracingRedissonHelper.prepareRFuture(span, bitSet::lengthAsync);
  }

  @Override
  public RFuture<Void> setAsync(long fromIndex, long toIndex, boolean value) {
    Span span = tracingRedissonHelper.buildSpan("setAsync", bitSet);
    span.setTag("fromIndex", fromIndex);
    span.setTag("toIndex", toIndex);
    span.setTag("value", value);
    return tracingRedissonHelper
        .prepareRFuture(span, () -> bitSet.setAsync(fromIndex, toIndex, value));
  }

  @Override
  public RFuture<Void> clearAsync(long fromIndex, long toIndex) {
    Span span = tracingRedissonHelper.buildSpan("clearAsync", bitSet);
    span.setTag("fromIndex", fromIndex);
    span.setTag("toIndex", toIndex);
    return tracingRedissonHelper.prepareRFuture(span, () -> bitSet.clearAsync(fromIndex, toIndex));
  }

  @Override
  public RFuture<Void> setAsync(BitSet bs) {
    Span span = tracingRedissonHelper.buildSpan("setAsync", bitSet);
    span.setTag("bs", nullable(bs));
    return tracingRedissonHelper.prepareRFuture(span, () -> bitSet.setAsync(bs));
  }

  @Override
  public RFuture<Void> notAsync() {
    Span span = tracingRedissonHelper.buildSpan("notAsync", bitSet);
    return tracingRedissonHelper.prepareRFuture(span, bitSet::notAsync);
  }

  @Override
  public RFuture<Void> setAsync(long fromIndex, long toIndex) {
    Span span = tracingRedissonHelper.buildSpan("setAsync", bitSet);
    span.setTag("fromIndex", fromIndex);
    span.setTag("toIndex", toIndex);
    return tracingRedissonHelper.prepareRFuture(span, () -> bitSet.setAsync(fromIndex, toIndex));
  }

  @Override
  public RFuture<Long> sizeAsync() {
    Span span = tracingRedissonHelper.buildSpan("sizeAsync", bitSet);
    return tracingRedissonHelper.prepareRFuture(span, bitSet::sizeAsync);
  }

  @Override
  public RFuture<Boolean> getAsync(long bitIndex) {
    Span span = tracingRedissonHelper.buildSpan("getAsync", bitSet);
    span.setTag("bitIndex", bitIndex);
    return tracingRedissonHelper.prepareRFuture(span, () -> bitSet.getAsync(bitIndex));
  }

  @Override
  public RFuture<Boolean> setAsync(long bitIndex) {
    Span span = tracingRedissonHelper.buildSpan("setAsync", bitSet);
    span.setTag("bitIndex", bitIndex);
    return tracingRedissonHelper.prepareRFuture(span, () -> bitSet.setAsync(bitIndex));
  }

  @Override
  public RFuture<Boolean> setAsync(long bitIndex, boolean value) {
    Span span = tracingRedissonHelper.buildSpan("setAsync", bitSet);
    span.setTag("bitIndex", bitIndex);
    span.setTag("value", value);
    return tracingRedissonHelper.prepareRFuture(span, () -> bitSet.setAsync(bitIndex, value));
  }

  @Override
  public RFuture<Long> cardinalityAsync() {
    Span span = tracingRedissonHelper.buildSpan("cardinalityAsync", bitSet);
    return tracingRedissonHelper.prepareRFuture(span, bitSet::cardinalityAsync);
  }

  @Override
  public RFuture<Boolean> clearAsync(long bitIndex) {
    Span span = tracingRedissonHelper.buildSpan("clearAsync", bitSet);
    span.setTag("bitIndex", bitIndex);
    return tracingRedissonHelper.prepareRFuture(span, () -> bitSet.clearAsync(bitIndex));
  }

  @Override
  public RFuture<Void> clearAsync() {
    Span span = tracingRedissonHelper.buildSpan("clearAsync", bitSet);
    return tracingRedissonHelper.prepareRFuture(span, bitSet::clearAsync);
  }

  @Override
  public RFuture<Void> orAsync(String... bitSetNames) {
    Span span = tracingRedissonHelper.buildSpan("orAsync", bitSet);
    span.setTag("bitSetNames", Arrays.toString(bitSetNames));
    return tracingRedissonHelper.prepareRFuture(span, () -> bitSet.orAsync(bitSetNames));
  }

  @Override
  public RFuture<Void> andAsync(String... bitSetNames) {
    Span span = tracingRedissonHelper.buildSpan("andAsync", bitSet);
    span.setTag("bitSetNames", Arrays.toString(bitSetNames));
    return tracingRedissonHelper.prepareRFuture(span, () -> bitSet.andAsync(bitSetNames));
  }

  @Override
  public RFuture<Void> xorAsync(String... bitSetNames) {
    Span span = tracingRedissonHelper.buildSpan("xorAsync", bitSet);
    span.setTag("bitSetNames", Arrays.toString(bitSetNames));
    return tracingRedissonHelper.prepareRFuture(span, () -> bitSet.xorAsync(bitSetNames));
  }

}
