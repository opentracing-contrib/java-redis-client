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

import static io.opentracing.contrib.redis.common.TracingHelper.collectionToString;
import static io.opentracing.contrib.redis.common.TracingHelper.nullable;

import io.opentracing.Span;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.redisson.api.RFuture;
import org.redisson.api.RLock;
import org.redisson.api.RSet;
import org.redisson.api.SortOrder;
import org.redisson.api.mapreduce.RCollectionMapReduce;

public class TracingRSet<V> extends TracingRExpirable implements RSet<V> {
  private final RSet<V> set;
  private final TracingRedissonHelper tracingRedissonHelper;

  public TracingRSet(RSet<V> set, TracingRedissonHelper tracingRedissonHelper) {
    super(set, tracingRedissonHelper);
    this.set = set;
    this.tracingRedissonHelper = tracingRedissonHelper;
  }

  @Override
  public RLock getLock(V value) {
    return new TracingRLock(set.getLock(value), tracingRedissonHelper);
  }

  @Override
  public Iterator<V> iterator(int count) {
    return set.iterator(count);
  }

  @Override
  public Iterator<V> iterator(String pattern, int count) {
    return set.iterator(pattern, count);
  }

  @Override
  public Iterator<V> iterator(String pattern) {
    return set.iterator(pattern);
  }

  @Override
  public <KOut, VOut> RCollectionMapReduce<V, KOut, VOut> mapReduce() {
    return new TracingRCollectionMapReduce<>(set.mapReduce(), tracingRedissonHelper);
  }

  @Override
  public Set<V> removeRandom(int amount) {
    Span span = tracingRedissonHelper.buildSpan("removeRandom", set);
    span.setTag("amount", amount);
    return tracingRedissonHelper.decorate(span, () -> set.removeRandom(amount));
  }

  @Override
  public V removeRandom() {
    Span span = tracingRedissonHelper.buildSpan("removeRandom", set);
    return tracingRedissonHelper.decorate(span, () -> set.removeRandom());
  }

  @Override
  public V random() {
    Span span = tracingRedissonHelper.buildSpan("random", set);
    return tracingRedissonHelper.decorate(span, () -> set.random());
  }

  @Override
  public Set<V> random(int count) {
    Span span = tracingRedissonHelper.buildSpan("random", set);
    span.setTag("count", count);
    return tracingRedissonHelper.decorate(span, () -> set.random(count));
  }

  @Override
  public boolean move(String destination, V member) {
    Span span = tracingRedissonHelper.buildSpan("move", set);
    span.setTag("destination", nullable(destination));
    span.setTag("member", nullable(member));
    return tracingRedissonHelper.decorate(span, () -> set.move(destination, member));
  }

  @Override
  public Set<V> readAll() {
    Span span = tracingRedissonHelper.buildSpan("readAll", set);
    return tracingRedissonHelper.decorate(span, set::readAll);
  }

  @Override
  public int union(String... names) {
    Span span = tracingRedissonHelper.buildSpan("union", set);
    span.setTag("names", Arrays.toString(names));
    return tracingRedissonHelper.decorate(span, () -> set.union(names));
  }

  @Override
  public Set<V> readUnion(String... names) {
    Span span = tracingRedissonHelper.buildSpan("readUnion", set);
    span.setTag("names", Arrays.toString(names));
    return tracingRedissonHelper.decorate(span, () -> set.readUnion(names));
  }

  @Override
  public int diff(String... names) {
    Span span = tracingRedissonHelper.buildSpan("diff", set);
    span.setTag("names", Arrays.toString(names));
    return tracingRedissonHelper.decorate(span, () -> set.diff(names));
  }

  @Override
  public Set<V> readDiff(String... names) {
    Span span = tracingRedissonHelper.buildSpan("readDiff", set);
    span.setTag("names", Arrays.toString(names));
    return tracingRedissonHelper.decorate(span, () -> set.readDiff(names));
  }

  @Override
  public int intersection(String... names) {
    Span span = tracingRedissonHelper.buildSpan("intersection", set);
    span.setTag("names", Arrays.toString(names));
    return tracingRedissonHelper.decorate(span, () -> set.intersection(names));
  }

  @Override
  public Set<V> readIntersection(String... names) {
    Span span = tracingRedissonHelper.buildSpan("readIntersection", set);
    span.setTag("names", Arrays.toString(names));
    return tracingRedissonHelper.decorate(span, () -> set.readIntersection(names));
  }

  @Override
  public int size() {
    Span span = tracingRedissonHelper.buildSpan("size", set);
    return tracingRedissonHelper.decorate(span, set::size);
  }

  @Override
  public boolean isEmpty() {
    Span span = tracingRedissonHelper.buildSpan("isEmpty", set);
    return tracingRedissonHelper.decorate(span, set::isEmpty);
  }

  @Override
  public boolean contains(Object o) {
    Span span = tracingRedissonHelper.buildSpan("contains", set);
    span.setTag("object", nullable(o));
    return tracingRedissonHelper.decorate(span, () -> set.contains(o));
  }

  @Override
  public Iterator<V> iterator() {
    return set.iterator();
  }

  @Override
  public Object[] toArray() {
    Span span = tracingRedissonHelper.buildSpan("toArray", set);
    return tracingRedissonHelper.decorate(span, () -> set.toArray());
  }

  @Override
  public <T> T[] toArray(T[] a) {
    Span span = tracingRedissonHelper.buildSpan("toArray", set);
    return tracingRedissonHelper.decorate(span, () -> set.toArray(a));
  }

  @Override
  public boolean add(V element) {
    Span span = tracingRedissonHelper.buildSpan("add", set);
    span.setTag("element", nullable(element));
    return tracingRedissonHelper.decorate(span, () -> set.add(element));
  }

  @Override
  public boolean remove(Object o) {
    Span span = tracingRedissonHelper.buildSpan("remove", set);
    span.setTag("object", nullable(o));
    return tracingRedissonHelper.decorate(span, () -> set.remove(o));
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    Span span = tracingRedissonHelper.buildSpan("containsAll", set);
    return tracingRedissonHelper.decorate(span, () -> set.containsAll(c));
  }

  @Override
  public boolean addAll(Collection<? extends V> c) {
    Span span = tracingRedissonHelper.buildSpan("addAll", set);
    return tracingRedissonHelper.decorate(span, () -> set.addAll(c));
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    Span span = tracingRedissonHelper.buildSpan("retainAll", set);
    return tracingRedissonHelper.decorate(span, () -> set.retainAll(c));
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    Span span = tracingRedissonHelper.buildSpan("removeAll", set);
    return tracingRedissonHelper.decorate(span, () -> set.removeAll(c));
  }

  @Override
  public void clear() {
    Span span = tracingRedissonHelper.buildSpan("clear", set);
    tracingRedissonHelper.decorate(span, set::clear);
  }

  @Override
  public Spliterator<V> spliterator() {
    return set.spliterator();
  }

  @Override
  public boolean removeIf(Predicate<? super V> filter) {
    Span span = tracingRedissonHelper.buildSpan("removeIf", set);
    span.setTag("filter", nullable(filter));
    return tracingRedissonHelper.decorate(span, () -> set.removeIf(filter));
  }

  @Override
  public Stream<V> stream() {
    return set.stream();
  }

  @Override
  public Stream<V> parallelStream() {
    return set.parallelStream();
  }

  @Override
  public void forEach(Consumer<? super V> action) {
    Span span = tracingRedissonHelper.buildSpan("forEach", set);
    span.setTag("action", nullable(action));
    tracingRedissonHelper.decorate(span, () -> set.forEach(action));
  }

  @Override
  public RFuture<Set<V>> removeRandomAsync(int amount) {
    Span span = tracingRedissonHelper.buildSpan("removeRandomAsync", set);
    span.setTag("amount", amount);
    return tracingRedissonHelper.prepareRFuture(span, () -> set.removeRandomAsync(amount));
  }

  @Override
  public RFuture<V> removeRandomAsync() {
    Span span = tracingRedissonHelper.buildSpan("removeRandomAsync", set);
    return tracingRedissonHelper.prepareRFuture(span, set::removeRandomAsync);
  }

  @Override
  public RFuture<V> randomAsync() {
    Span span = tracingRedissonHelper.buildSpan("randomAsync", set);
    return tracingRedissonHelper.prepareRFuture(span, set::randomAsync);
  }

  @Override
  public RFuture<Set<V>> randomAsync(int count) {
    Span span = tracingRedissonHelper.buildSpan("randomAsync", set);
    span.setTag("count", count);
    return tracingRedissonHelper.prepareRFuture(span, () -> set.randomAsync(count));
  }

  @Override
  public RFuture<Boolean> moveAsync(String destination, V member) {
    Span span = tracingRedissonHelper.buildSpan("moveAsync", set);
    span.setTag("destination", nullable(destination));
    span.setTag("member", nullable(member));
    return tracingRedissonHelper.prepareRFuture(span, () -> set.moveAsync(destination, member));
  }

  @Override
  public RFuture<Set<V>> readAllAsync() {
    Span span = tracingRedissonHelper.buildSpan("readAllAsync", set);
    return tracingRedissonHelper.prepareRFuture(span, set::readAllAsync);
  }

  @Override
  public RFuture<Integer> unionAsync(String... names) {
    Span span = tracingRedissonHelper.buildSpan("unionAsync", set);
    span.setTag("names", Arrays.toString(names));
    return tracingRedissonHelper.prepareRFuture(span, () -> set.unionAsync(names));
  }

  @Override
  public RFuture<Set<V>> readUnionAsync(String... names) {
    Span span = tracingRedissonHelper.buildSpan("readUnionAsync", set);
    span.setTag("names", Arrays.toString(names));
    return tracingRedissonHelper.prepareRFuture(span, () -> set.readUnionAsync(names));
  }

  @Override
  public RFuture<Integer> diffAsync(String... names) {
    Span span = tracingRedissonHelper.buildSpan("diffAsync", set);
    span.setTag("names", Arrays.toString(names));
    return tracingRedissonHelper.prepareRFuture(span, () -> set.diffAsync(names));
  }

  @Override
  public RFuture<Set<V>> readDiffAsync(String... names) {
    Span span = tracingRedissonHelper.buildSpan("readDiffAsync", set);
    span.setTag("names", Arrays.toString(names));
    return tracingRedissonHelper.prepareRFuture(span, () -> set.readDiffAsync(names));
  }

  @Override
  public RFuture<Integer> intersectionAsync(String... names) {
    Span span = tracingRedissonHelper.buildSpan("intersectionAsync", set);
    span.setTag("names", Arrays.toString(names));
    return tracingRedissonHelper.prepareRFuture(span, () -> set.intersectionAsync(names));
  }

  @Override
  public RFuture<Set<V>> readIntersectionAsync(String... names) {
    Span span = tracingRedissonHelper.buildSpan("readIntersectionAsync", set);
    span.setTag("names", Arrays.toString(names));
    return tracingRedissonHelper.prepareRFuture(span, () -> set.readIntersectionAsync(names));
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
  public RFuture<Boolean> addAsync(V e) {
    Span span = tracingRedissonHelper.buildSpan("addAsync", set);
    span.setTag("element", nullable(e));
    return tracingRedissonHelper.prepareRFuture(span, () -> set.addAsync(e));
  }

  @Override
  public RFuture<Boolean> addAllAsync(Collection<? extends V> c) {
    Span span = tracingRedissonHelper.buildSpan("addAllAsync", set);
    return tracingRedissonHelper.prepareRFuture(span, () -> set.addAllAsync(c));
  }

  @Override
  public RFuture<Set<V>> readSortAsync(SortOrder order) {
    Span span = tracingRedissonHelper.buildSpan("readSortAsync", set);
    span.setTag("order", nullable(order));
    return tracingRedissonHelper.prepareRFuture(span, () -> set.readSortAsync(order));
  }

  @Override
  public RFuture<Set<V>> readSortAsync(SortOrder order, int offset, int count) {
    Span span = tracingRedissonHelper.buildSpan("readSortAsync", set);
    span.setTag("order", nullable(order));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingRedissonHelper
        .prepareRFuture(span, () -> set.readSortAsync(order, offset, count));
  }

  @Override
  public RFuture<Set<V>> readSortAsync(String byPattern, SortOrder order) {
    Span span = tracingRedissonHelper.buildSpan("readSortAsync", set);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    return tracingRedissonHelper.prepareRFuture(span, () -> set.readSortAsync(byPattern, order));
  }

  @Override
  public RFuture<Set<V>> readSortAsync(String byPattern, SortOrder order, int offset, int count) {
    Span span = tracingRedissonHelper.buildSpan("readSortAsync", set);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingRedissonHelper
        .prepareRFuture(span, () -> set.readSortAsync(byPattern, order, offset, count));
  }

  @Override
  public <T> RFuture<Collection<T>> readSortAsync(String byPattern, List<String> getPatterns,
      SortOrder order) {
    Span span = tracingRedissonHelper.buildSpan("readSortAsync", set);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    span.setTag("getPatterns", collectionToString(getPatterns));
    return tracingRedissonHelper
        .prepareRFuture(span, () -> set.readSortAsync(byPattern, getPatterns, order));
  }

  @Override
  public <T> RFuture<Collection<T>> readSortAsync(
      String byPattern, List<String> getPatterns, SortOrder order, int offset, int count) {
    Span span = tracingRedissonHelper.buildSpan("readSortAsync", set);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    span.setTag("getPatterns", collectionToString(getPatterns));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingRedissonHelper
        .prepareRFuture(span, () -> set.readSortAsync(byPattern, getPatterns, order, offset, count)
        );
  }

  @Override
  public RFuture<Set<V>> readSortAlphaAsync(SortOrder order) {
    Span span = tracingRedissonHelper.buildSpan("readSortAlphaAsync", set);
    span.setTag("order", nullable(order));
    return tracingRedissonHelper.prepareRFuture(span, () -> set.readSortAlphaAsync(order));
  }

  @Override
  public RFuture<Set<V>> readSortAlphaAsync(SortOrder order, int offset, int count) {
    Span span = tracingRedissonHelper.buildSpan("readSortAlphaAsync", set);
    span.setTag("order", nullable(order));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingRedissonHelper
        .prepareRFuture(span, () -> set.readSortAlphaAsync(order, offset, count));
  }

  @Override
  public RFuture<Set<V>> readSortAlphaAsync(String byPattern, SortOrder order) {
    Span span = tracingRedissonHelper.buildSpan("readSortAlphaAsync", set);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    return tracingRedissonHelper
        .prepareRFuture(span, () -> set.readSortAlphaAsync(byPattern, order));
  }

  @Override
  public RFuture<Set<V>> readSortAlphaAsync(String byPattern, SortOrder order, int offset,
      int count) {
    Span span = tracingRedissonHelper.buildSpan("readSortAlphaAsync", set);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingRedissonHelper
        .prepareRFuture(span, () -> set.readSortAlphaAsync(byPattern, order, offset, count));
  }

  @Override
  public <T> RFuture<Collection<T>> readSortAlphaAsync(String byPattern, List<String> getPatterns,
      SortOrder order) {
    Span span = tracingRedissonHelper.buildSpan("readSortAlphaAsync", set);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    span.setTag("getPatterns", collectionToString(getPatterns));
    return tracingRedissonHelper
        .prepareRFuture(span, () -> set.readSortAlphaAsync(byPattern, getPatterns, order));
  }

  @Override
  public <T> RFuture<Collection<T>> readSortAlphaAsync(String byPattern, List<String> getPatterns,
      SortOrder order, int offset, int count) {
    Span span = tracingRedissonHelper.buildSpan("readSortAlphaAsync", set);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    span.setTag("getPatterns", collectionToString(getPatterns));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingRedissonHelper
        .prepareRFuture(span,
            () -> set.readSortAlphaAsync(byPattern, getPatterns, order, offset, count)
        );
  }

  @Override
  public RFuture<Integer> sortToAsync(String destName, SortOrder order) {
    Span span = tracingRedissonHelper.buildSpan("sortToAsync", set);
    span.setTag("destName", nullable(destName));
    span.setTag("order", nullable(order));
    return tracingRedissonHelper.prepareRFuture(span, () -> set.sortToAsync(destName, order));
  }

  @Override
  public RFuture<Integer> sortToAsync(String destName,
      SortOrder order, int offset, int count) {
    Span span = tracingRedissonHelper.buildSpan("sortToAsync", set);
    span.setTag("destName", nullable(destName));
    span.setTag("order", nullable(order));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingRedissonHelper
        .prepareRFuture(span, () -> set.sortToAsync(destName, order, offset, count));
  }

  @Override
  public RFuture<Integer> sortToAsync(String destName, String byPattern,
      SortOrder order) {
    Span span = tracingRedissonHelper.buildSpan("sortToAsync", set);
    span.setTag("destName", nullable(destName));
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    return tracingRedissonHelper
        .prepareRFuture(span, () -> set.sortToAsync(destName, byPattern, order));
  }

  @Override
  public RFuture<Integer> sortToAsync(String destName, String byPattern,
      SortOrder order, int offset, int count) {
    Span span = tracingRedissonHelper.buildSpan("sortToAsync", set);
    span.setTag("destName", nullable(destName));
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingRedissonHelper
        .prepareRFuture(span, () -> set.sortToAsync(destName, byPattern, order, offset, count));
  }

  @Override
  public RFuture<Integer> sortToAsync(String destName, String byPattern,
      List<String> getPatterns, SortOrder order) {
    Span span = tracingRedissonHelper.buildSpan("sortToAsync", set);
    span.setTag("destName", nullable(destName));
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    span.setTag("getPatterns", collectionToString(getPatterns));
    return tracingRedissonHelper
        .prepareRFuture(span, () -> set.sortToAsync(destName, byPattern, getPatterns, order));
  }

  @Override
  public RFuture<Integer> sortToAsync(String destName, String byPattern,
      List<String> getPatterns, SortOrder order, int offset, int count) {
    Span span = tracingRedissonHelper.buildSpan("sortToAsync", set);
    span.setTag("destName", nullable(destName));
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    span.setTag("getPatterns", collectionToString(getPatterns));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingRedissonHelper
        .prepareRFuture(
            span, () -> set.sortToAsync(destName, byPattern, getPatterns, order, offset, count));
  }

  @Override
  public Set<V> readSort(SortOrder order) {
    Span span = tracingRedissonHelper.buildSpan("readSort", set);
    span.setTag("order", nullable(order));
    return tracingRedissonHelper.decorate(span, () -> set.readSort(order));
  }

  @Override
  public Set<V> readSort(SortOrder order, int offset, int count) {
    Span span = tracingRedissonHelper.buildSpan("readSort", set);
    span.setTag("order", nullable(order));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingRedissonHelper.decorate(span, () -> set.readSort(order, offset, count));
  }

  @Override
  public Set<V> readSort(String byPattern, SortOrder order) {
    Span span = tracingRedissonHelper.buildSpan("readSort", set);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    return tracingRedissonHelper.decorate(span, () -> set.readSort(byPattern, order));
  }

  @Override
  public Set<V> readSort(String byPattern, SortOrder order, int offset, int count) {
    Span span = tracingRedissonHelper.buildSpan("readSort", set);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingRedissonHelper
        .decorate(span, () -> set.readSort(byPattern, order, offset, count));
  }

  @Override
  public <T> Collection<T> readSort(String byPattern, List<String> getPatterns, SortOrder order) {
    Span span = tracingRedissonHelper.buildSpan("readSort", set);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("getPatterns", collectionToString(getPatterns));
    span.setTag("order", nullable(order));
    return tracingRedissonHelper.decorate(span, () -> set.readSort(byPattern, getPatterns, order));
  }

  @Override
  public <T> Collection<T> readSort(String byPattern, List<String> getPatterns, SortOrder order,
      int offset, int count) {
    Span span = tracingRedissonHelper.buildSpan("readSort", set);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("getPatterns", collectionToString(getPatterns));
    span.setTag("order", nullable(order));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingRedissonHelper
        .decorate(span, () -> set.readSort(byPattern, getPatterns, order, offset, count));
  }

  @Override
  public Set<V> readSortAlpha(SortOrder order) {
    Span span = tracingRedissonHelper.buildSpan("readSortAlpha", set);
    span.setTag("order", nullable(order));
    return tracingRedissonHelper.decorate(span, () -> set.readSortAlpha(order));
  }

  @Override
  public Set<V> readSortAlpha(SortOrder order, int offset, int count) {
    Span span = tracingRedissonHelper.buildSpan("readSortAlpha", set);
    span.setTag("order", nullable(order));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingRedissonHelper.decorate(span, () -> set.readSortAlpha(order, offset, count));
  }

  @Override
  public Set<V> readSortAlpha(String byPattern, SortOrder order) {
    Span span = tracingRedissonHelper.buildSpan("readSortAlpha", set);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    return tracingRedissonHelper.decorate(span, () -> set.readSortAlpha(byPattern, order));
  }

  @Override
  public Set<V> readSortAlpha(String byPattern, SortOrder order, int offset, int count) {
    Span span = tracingRedissonHelper.buildSpan("readSortAlpha", set);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingRedissonHelper
        .decorate(span, () -> set.readSortAlpha(byPattern, order, offset, count));
  }

  @Override
  public <T> Collection<T> readSortAlpha(String byPattern, List<String> getPatterns,
      SortOrder order) {
    Span span = tracingRedissonHelper.buildSpan("readSortAlpha", set);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("getPatterns", collectionToString(getPatterns));
    span.setTag("order", nullable(order));
    return tracingRedissonHelper
        .decorate(span, () -> set.readSortAlpha(byPattern, getPatterns, order));
  }

  @Override
  public <T> Collection<T> readSortAlpha(String byPattern,
      List<String> getPatterns, SortOrder order, int offset, int count) {
    Span span = tracingRedissonHelper.buildSpan("readSortAlpha", set);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("getPatterns", collectionToString(getPatterns));
    span.setTag("order", nullable(order));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingRedissonHelper
        .decorate(span, () -> set.readSortAlpha(byPattern, getPatterns, order, offset, count));
  }

  @Override
  public int sortTo(String destName, SortOrder order) {
    Span span = tracingRedissonHelper.buildSpan("sortTo", set);
    span.setTag("destName", nullable(destName));
    span.setTag("order", nullable(order));
    return tracingRedissonHelper.decorate(span, () -> set.sortTo(destName, order));
  }

  @Override
  public int sortTo(String destName, SortOrder order, int offset, int count) {
    Span span = tracingRedissonHelper.buildSpan("sortTo", set);
    span.setTag("destName", nullable(destName));
    span.setTag("order", nullable(order));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingRedissonHelper.decorate(span, () -> set.sortTo(destName, order, offset, count));
  }

  @Override
  public int sortTo(String destName, String byPattern, SortOrder order) {
    Span span = tracingRedissonHelper.buildSpan("sortTo", set);
    span.setTag("destName", nullable(destName));
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    return tracingRedissonHelper.decorate(span, () -> set.sortTo(destName, byPattern, order));
  }

  @Override
  public int sortTo(String destName, String byPattern, SortOrder order, int offset, int count) {
    Span span = tracingRedissonHelper.buildSpan("sortTo", set);
    span.setTag("destName", nullable(destName));
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingRedissonHelper
        .decorate(span, () -> set.sortTo(destName, byPattern, order, offset, count));
  }

  @Override
  public int sortTo(String destName, String byPattern, List<String> getPatterns,
      SortOrder order) {
    Span span = tracingRedissonHelper.buildSpan("sortTo", set);
    span.setTag("destName", nullable(destName));
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("getPatterns", collectionToString(getPatterns));
    span.setTag("order", nullable(order));
    return tracingRedissonHelper
        .decorate(span, () -> set.sortTo(destName, byPattern, getPatterns, order));
  }

  @Override
  public int sortTo(String destName, String byPattern, List<String> getPatterns,
      SortOrder order, int offset, int count) {
    Span span = tracingRedissonHelper.buildSpan("sortTo", set);
    span.setTag("destName", nullable(destName));
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("getPatterns", collectionToString(getPatterns));
    span.setTag("order", nullable(order));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingRedissonHelper
        .decorate(span, () -> set.sortTo(destName, byPattern, getPatterns, order, offset, count));
  }

}
