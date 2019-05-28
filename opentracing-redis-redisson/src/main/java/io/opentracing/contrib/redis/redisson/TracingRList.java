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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import org.redisson.api.RFuture;
import org.redisson.api.RList;
import org.redisson.api.SortOrder;
import org.redisson.api.mapreduce.RCollectionMapReduce;

public class TracingRList<V> extends TracingRExpirable implements RList<V> {
  private final RList<V> list;
  private final TracingRedissonHelper tracingRedissonHelper;

  public TracingRList(RList<V> list, TracingRedissonHelper tracingRedissonHelper) {
    super(list, tracingRedissonHelper);
    this.list = list;
    this.tracingRedissonHelper = tracingRedissonHelper;
  }

  @Override
  public List<V> get(int... indexes) {
    Span span = tracingRedissonHelper.buildSpan("get", list);
    span.setTag("indexes", Arrays.toString(indexes));
    return tracingRedissonHelper.decorate(span, () -> list.get(indexes));
  }

  @Override
  public <KOut, VOut> RCollectionMapReduce<V, KOut, VOut> mapReduce() {
    return new TracingRCollectionMapReduce<>(list.mapReduce(), tracingRedissonHelper);
  }

  @Override
  public int addAfter(V elementToFind, V element) {
    Span span = tracingRedissonHelper.buildSpan("addAfter", list);
    span.setTag("elementToFind", nullable(elementToFind));
    span.setTag("element", nullable(element));
    return tracingRedissonHelper.decorate(span, () -> list.addAfter(elementToFind, element));
  }

  @Override
  public int addBefore(V elementToFind, V element) {
    Span span = tracingRedissonHelper.buildSpan("addBefore", list);
    span.setTag("elementToFind", nullable(elementToFind));
    span.setTag("element", nullable(element));
    return tracingRedissonHelper.decorate(span, () -> list.addBefore(elementToFind, element));
  }

  @Override
  public void fastSet(int index, V element) {
    Span span = tracingRedissonHelper.buildSpan("fastSet", list);
    span.setTag("index", index);
    span.setTag("element", nullable(element));
    tracingRedissonHelper.decorate(span, () -> list.fastSet(index, element));
  }

  @Override
  public RList<V> subList(int fromIndex, int toIndex) {
    return new TracingRList<>(list.subList(fromIndex, toIndex), tracingRedissonHelper);
  }

  @Override
  public List<V> readAll() {
    Span span = tracingRedissonHelper.buildSpan("readAll", list);
    return tracingRedissonHelper.decorate(span, list::readAll);
  }

  @Override
  public void trim(int fromIndex, int toIndex) {
    Span span = tracingRedissonHelper.buildSpan("trim", list);
    span.setTag("fromIndex", fromIndex);
    span.setTag("toIndex", toIndex);
    tracingRedissonHelper.decorate(span, () -> list.trim(fromIndex, toIndex));
  }

  @Override
  public List<V> range(int toIndex) {
    Span span = tracingRedissonHelper.buildSpan("range", list);
    span.setTag("toIndex", toIndex);
    return tracingRedissonHelper.decorate(span, () -> list.range(toIndex));
  }

  @Override
  public List<V> range(int fromIndex, int toIndex) {
    Span span = tracingRedissonHelper.buildSpan("range", list);
    span.setTag("fromIndex", fromIndex);
    span.setTag("toIndex", toIndex);
    return tracingRedissonHelper.decorate(span, () -> list.range(fromIndex, toIndex));
  }

  @Override
  public void fastRemove(int index) {
    Span span = tracingRedissonHelper.buildSpan("fastRemove", list);
    span.setTag("index", index);
    tracingRedissonHelper.decorate(span, () -> list.fastRemove(index));
  }

  @Override
  public boolean remove(Object object, int count) {
    Span span = tracingRedissonHelper.buildSpan("remove", list);
    span.setTag("object", nullable(object));
    span.setTag("count", count);
    return tracingRedissonHelper.decorate(span, () -> list.remove(object, count));
  }

  @Override
  public int size() {
    Span span = tracingRedissonHelper.buildSpan("size", list);
    return tracingRedissonHelper.decorate(span, list::size);
  }

  @Override
  public boolean isEmpty() {
    Span span = tracingRedissonHelper.buildSpan("isEmpty", list);
    return tracingRedissonHelper.decorate(span, list::isEmpty);
  }

  @Override
  public boolean contains(Object object) {
    Span span = tracingRedissonHelper.buildSpan("contains", list);
    span.setTag("object", nullable(object));
    return tracingRedissonHelper.decorate(span, () -> list.contains(object));
  }

  @Override
  public Iterator<V> iterator() {
    return list.iterator();
  }

  @Override
  public Object[] toArray() {
    Span span = tracingRedissonHelper.buildSpan("toArray", list);
    return tracingRedissonHelper.decorate(span, () -> list.toArray());
  }

  @Override
  public <T> T[] toArray(T[] a) {
    Span span = tracingRedissonHelper.buildSpan("toArray", list);
    return tracingRedissonHelper.decorate(span, () -> list.toArray(a));
  }

  @Override
  public boolean add(V element) {
    Span span = tracingRedissonHelper.buildSpan("add", list);
    span.setTag("element", nullable(element));
    return tracingRedissonHelper.decorate(span, () -> list.add(element));
  }

  @Override
  public boolean remove(Object object) {
    Span span = tracingRedissonHelper.buildSpan("remove", list);
    span.setTag("object", nullable(object));
    return tracingRedissonHelper.decorate(span, () -> list.remove(object));
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    Span span = tracingRedissonHelper.buildSpan("containsAll", list);
    return tracingRedissonHelper.decorate(span, () -> list.containsAll(c));
  }

  @Override
  public boolean addAll(Collection<? extends V> c) {
    Span span = tracingRedissonHelper.buildSpan("addAll", list);
    return tracingRedissonHelper.decorate(span, () -> list.addAll(c));
  }

  @Override
  public boolean addAll(int index, Collection<? extends V> c) {
    Span span = tracingRedissonHelper.buildSpan("addAll", list);
    span.setTag("index", index);
    return tracingRedissonHelper.decorate(span, () -> list.addAll(index, c));
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    Span span = tracingRedissonHelper.buildSpan("removeAll", list);
    return tracingRedissonHelper.decorate(span, () -> list.removeAll(c));
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    Span span = tracingRedissonHelper.buildSpan("retainAll", list);
    return tracingRedissonHelper.decorate(span, () -> list.retainAll(c));
  }

  @Override
  public void replaceAll(UnaryOperator<V> operator) {
    Span span = tracingRedissonHelper.buildSpan("replaceAll", list);
    span.setTag("operator", nullable(operator));
    tracingRedissonHelper.decorate(span, () -> list.replaceAll(operator));
  }

  @Override
  public void sort(Comparator<? super V> comparator) {
    Span span = tracingRedissonHelper.buildSpan("sort", list);
    span.setTag("comparator", nullable(comparator));
    tracingRedissonHelper.decorate(span, () -> list.sort(comparator));
  }

  @Override
  public void clear() {
    Span span = tracingRedissonHelper.buildSpan("clear", list);
    tracingRedissonHelper.decorate(span, list::clear);
  }

  @Override
  public boolean equals(Object object) {
    Span span = tracingRedissonHelper.buildSpan("equals", list);
    span.setTag("object", nullable(object));
    return tracingRedissonHelper.decorate(span, () -> list.equals(object));
  }

  @Override
  public int hashCode() {
    Span span = tracingRedissonHelper.buildSpan("hashCode", list);
    return tracingRedissonHelper.decorate(span, list::hashCode);
  }

  @Override
  public V get(int index) {
    Span span = tracingRedissonHelper.buildSpan("get", list);
    span.setTag("index", index);
    return tracingRedissonHelper.decorate(span, () -> list.get(index));
  }

  @Override
  public V set(int index, V element) {
    Span span = tracingRedissonHelper.buildSpan("set", list);
    span.setTag("index", index);
    span.setTag("element", nullable(element));
    return tracingRedissonHelper.decorate(span, () -> list.set(index, element));
  }

  @Override
  public void add(int index, V element) {
    Span span = tracingRedissonHelper.buildSpan("add", list);
    span.setTag("index", index);
    span.setTag("element", nullable(element));
    tracingRedissonHelper.decorate(span, () -> list.add(index, element));
  }

  @Override
  public V remove(int index) {
    Span span = tracingRedissonHelper.buildSpan("remove", list);
    span.setTag("index", index);
    return tracingRedissonHelper.decorate(span, () -> list.remove(index));
  }

  @Override
  public int indexOf(Object o) {
    Span span = tracingRedissonHelper.buildSpan("indexOf", list);
    span.setTag("object", nullable(o));
    return tracingRedissonHelper.decorate(span, () -> list.indexOf(o));
  }

  @Override
  public int lastIndexOf(Object o) {
    Span span = tracingRedissonHelper.buildSpan("lastIndexOf", list);
    span.setTag("object", nullable(o));
    return tracingRedissonHelper.decorate(span, () -> list.lastIndexOf(o));
  }

  @Override
  public ListIterator<V> listIterator() {
    return list.listIterator();
  }

  @Override
  public ListIterator<V> listIterator(int index) {
    return list.listIterator(index);
  }

  @Override
  public Spliterator<V> spliterator() {
    return list.spliterator();
  }

  @Override
  public boolean removeIf(Predicate<? super V> filter) {
    Span span = tracingRedissonHelper.buildSpan("removeIf", list);
    span.setTag("filter", nullable(filter));
    return tracingRedissonHelper.decorate(span, () -> list.removeIf(filter));
  }

  @Override
  public Stream<V> stream() {
    return list.stream();
  }

  @Override
  public Stream<V> parallelStream() {
    return list.parallelStream();
  }

  @Override
  public void forEach(Consumer<? super V> action) {
    Span span = tracingRedissonHelper.buildSpan("forEach", list);
    span.setTag("action", nullable(action));
    tracingRedissonHelper.decorate(span, () -> list.forEach(action));
  }

  @Override
  public RFuture<List<V>> getAsync(int... indexes) {
    Span span = tracingRedissonHelper.buildSpan("getAsync", list);
    span.setTag("indexes", Arrays.toString(indexes));
    return tracingRedissonHelper.prepareRFuture(span, () -> list.getAsync(indexes));
  }

  @Override
  public RFuture<Integer> addAfterAsync(V elementToFind, V element) {
    Span span = tracingRedissonHelper.buildSpan("addAfterAsync", list);
    span.setTag("elementToFind", nullable(elementToFind));
    span.setTag("element", nullable(element));
    return tracingRedissonHelper
        .prepareRFuture(span, () -> list.addAfterAsync(elementToFind, element));
  }

  @Override
  public RFuture<Integer> addBeforeAsync(V elementToFind, V element) {
    Span span = tracingRedissonHelper.buildSpan("addBeforeAsync", list);
    span.setTag("elementToFind", nullable(elementToFind));
    span.setTag("element", nullable(element));
    return tracingRedissonHelper
        .prepareRFuture(span, () -> list.addBeforeAsync(elementToFind, element));
  }

  @Override
  public RFuture<Boolean> addAsync(int index, V element) {
    Span span = tracingRedissonHelper.buildSpan("addAsync", list);
    span.setTag("index", index);
    span.setTag("element", nullable(element));
    return tracingRedissonHelper.prepareRFuture(span, () -> list.addAsync(index, element));
  }

  @Override
  public RFuture<Boolean> addAllAsync(int index, Collection<? extends V> coll) {
    Span span = tracingRedissonHelper.buildSpan("addAllAsync", list);
    span.setTag("index", index);
    return tracingRedissonHelper.prepareRFuture(span, () -> list.addAllAsync(index, coll));
  }

  @Override
  public RFuture<Integer> lastIndexOfAsync(Object o) {
    Span span = tracingRedissonHelper.buildSpan("lastIndexOfAsync", list);
    span.setTag("object", nullable(o));
    return tracingRedissonHelper.prepareRFuture(span, () -> list.lastIndexOfAsync(o));
  }

  @Override
  public RFuture<Integer> indexOfAsync(Object o) {
    Span span = tracingRedissonHelper.buildSpan("indexOfAsync", list);
    span.setTag("object", nullable(o));
    return tracingRedissonHelper.prepareRFuture(span, () -> list.indexOfAsync(o));
  }

  @Override
  public RFuture<Void> fastSetAsync(int index, V element) {
    Span span = tracingRedissonHelper.buildSpan("fastSetAsync", list);
    span.setTag("index", index);
    span.setTag("element", nullable(element));
    return tracingRedissonHelper.prepareRFuture(span, () -> list.fastSetAsync(index, element));
  }

  @Override
  public RFuture<V> setAsync(int index, V element) {
    Span span = tracingRedissonHelper.buildSpan("setAsync", list);
    span.setTag("index", index);
    span.setTag("element", nullable(element));
    return tracingRedissonHelper.prepareRFuture(span, () -> list.setAsync(index, element));
  }

  @Override
  public RFuture<V> getAsync(int index) {
    Span span = tracingRedissonHelper.buildSpan("getAsync", list);
    span.setTag("index", index);
    return tracingRedissonHelper.prepareRFuture(span, () -> list.getAsync(index));
  }

  @Override
  public RFuture<List<V>> readAllAsync() {
    Span span = tracingRedissonHelper.buildSpan("readAllAsync", list);
    return tracingRedissonHelper.prepareRFuture(span, list::readAllAsync);
  }

  @Override
  public RFuture<Void> trimAsync(int fromIndex, int toIndex) {
    Span span = tracingRedissonHelper.buildSpan("trimAsync", list);
    span.setTag("fromIndex", fromIndex);
    span.setTag("toIndex", toIndex);
    return tracingRedissonHelper.prepareRFuture(span, () -> list.trimAsync(fromIndex, toIndex));
  }

  @Override
  public RFuture<Void> fastRemoveAsync(int index) {
    Span span = tracingRedissonHelper.buildSpan("fastRemoveAsync", list);
    span.setTag("index", index);
    return tracingRedissonHelper.prepareRFuture(span, () -> list.fastRemoveAsync(index));
  }

  @Override
  public RFuture<V> removeAsync(int index) {
    Span span = tracingRedissonHelper.buildSpan("removeAsync", list);
    span.setTag("index", index);
    return tracingRedissonHelper.prepareRFuture(span, () -> list.removeAsync(index));
  }

  @Override
  public RFuture<Boolean> removeAsync(Object o, int count) {
    Span span = tracingRedissonHelper.buildSpan("removeAsync", list);
    span.setTag("object", nullable(o));
    span.setTag("count", count);
    return tracingRedissonHelper.prepareRFuture(span, () -> list.removeAsync(o, count));
  }

  @Override
  public RFuture<List<V>> rangeAsync(int toIndex) {
    Span span = tracingRedissonHelper.buildSpan("rangeAsync", list);
    span.setTag("toIndex", toIndex);
    return tracingRedissonHelper.prepareRFuture(span, () -> list.rangeAsync(toIndex));
  }

  @Override
  public RFuture<List<V>> rangeAsync(int fromIndex, int toIndex) {
    Span span = tracingRedissonHelper.buildSpan("rangeAsync", list);
    span.setTag("fromIndex", fromIndex);
    span.setTag("toIndex", toIndex);
    return tracingRedissonHelper.prepareRFuture(span, () -> list.rangeAsync(fromIndex, toIndex));
  }

  @Override
  public RFuture<Boolean> retainAllAsync(Collection<?> c) {
    Span span = tracingRedissonHelper.buildSpan("retainAllAsync", list);
    return tracingRedissonHelper.prepareRFuture(span, () -> list.retainAllAsync(c));
  }

  @Override
  public RFuture<Boolean> removeAllAsync(Collection<?> c) {
    Span span = tracingRedissonHelper.buildSpan("retainAllAsync", list);
    return tracingRedissonHelper.prepareRFuture(span, () -> list.removeAllAsync(c));
  }

  @Override
  public RFuture<Boolean> containsAsync(Object o) {
    Span span = tracingRedissonHelper.buildSpan("containsAsync", list);
    span.setTag("object", nullable(o));
    return tracingRedissonHelper.prepareRFuture(span, () -> list.containsAsync(o));
  }

  @Override
  public RFuture<Boolean> containsAllAsync(Collection<?> c) {
    Span span = tracingRedissonHelper.buildSpan("containsAllAsync", list);
    return tracingRedissonHelper.prepareRFuture(span, () -> list.containsAllAsync(c));
  }

  @Override
  public RFuture<Boolean> removeAsync(Object o) {
    Span span = tracingRedissonHelper.buildSpan("removeAsync", list);
    span.setTag("object", nullable(o));
    return tracingRedissonHelper.prepareRFuture(span, () -> list.removeAsync(o));
  }

  @Override
  public RFuture<Integer> sizeAsync() {
    Span span = tracingRedissonHelper.buildSpan("sizeAsync", list);
    return tracingRedissonHelper.prepareRFuture(span, list::sizeAsync);
  }

  @Override
  public RFuture<Boolean> addAsync(V e) {
    Span span = tracingRedissonHelper.buildSpan("addAsync", list);
    span.setTag("element", nullable(e));
    return tracingRedissonHelper.prepareRFuture(span, () -> list.addAsync(e));
  }

  @Override
  public RFuture<Boolean> addAllAsync(Collection<? extends V> c) {
    Span span = tracingRedissonHelper.buildSpan("addAllAsync", list);
    return tracingRedissonHelper.prepareRFuture(span, () -> list.addAllAsync(c));
  }

  @Override
  public RFuture<List<V>> readSortAsync(SortOrder order) {
    Span span = tracingRedissonHelper.buildSpan("readSortAsync", list);
    span.setTag("order", nullable(order));
    return tracingRedissonHelper.prepareRFuture(span, () -> list.readSortAsync(order));
  }

  @Override
  public RFuture<List<V>> readSortAsync(SortOrder order, int offset, int count) {
    Span span = tracingRedissonHelper.buildSpan("readSortAsync", list);
    span.setTag("order", nullable(order));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingRedissonHelper
        .prepareRFuture(span, () -> list.readSortAsync(order, offset, count));
  }

  @Override
  public RFuture<List<V>> readSortAsync(String byPattern, SortOrder order) {
    Span span = tracingRedissonHelper.buildSpan("readSortAsync", list);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    return tracingRedissonHelper.prepareRFuture(span, () -> list.readSortAsync(byPattern, order));
  }

  @Override
  public RFuture<List<V>> readSortAsync(String byPattern, SortOrder order, int offset, int count) {
    Span span = tracingRedissonHelper.buildSpan("readSortAsync", list);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingRedissonHelper
        .prepareRFuture(span, () -> list.readSortAsync(byPattern, order, offset, count));
  }

  @Override
  public <T> RFuture<Collection<T>> readSortAsync(String byPattern, List<String> getPatterns,
      SortOrder order) {
    Span span = tracingRedissonHelper.buildSpan("readSortAsync", list);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    span.setTag("getPatterns", collectionToString(getPatterns));
    return tracingRedissonHelper
        .prepareRFuture(span, () -> list.readSortAsync(byPattern, getPatterns, order));
  }

  @Override
  public <T> RFuture<Collection<T>> readSortAsync(
      String byPattern, List<String> getPatterns, SortOrder order, int offset, int count) {
    Span span = tracingRedissonHelper.buildSpan("readSortAsync", list);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    span.setTag("getPatterns", collectionToString(getPatterns));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingRedissonHelper
        .prepareRFuture(span, () -> list.readSortAsync(byPattern, getPatterns, order, offset, count)
        );
  }

  @Override
  public RFuture<List<V>> readSortAlphaAsync(SortOrder order) {
    Span span = tracingRedissonHelper.buildSpan("readSortAlphaAsync", list);
    span.setTag("order", nullable(order));
    return tracingRedissonHelper.prepareRFuture(span, () -> list.readSortAlphaAsync(order));
  }

  @Override
  public RFuture<List<V>> readSortAlphaAsync(SortOrder order, int offset, int count) {
    Span span = tracingRedissonHelper.buildSpan("readSortAlphaAsync", list);
    span.setTag("order", nullable(order));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingRedissonHelper
        .prepareRFuture(span, () -> list.readSortAlphaAsync(order, offset, count));
  }

  @Override
  public RFuture<List<V>> readSortAlphaAsync(String byPattern, SortOrder order) {
    Span span = tracingRedissonHelper.buildSpan("readSortAlphaAsync", list);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    return tracingRedissonHelper
        .prepareRFuture(span, () -> list.readSortAlphaAsync(byPattern, order));
  }

  @Override
  public RFuture<List<V>> readSortAlphaAsync(String byPattern, SortOrder order, int offset,
      int count) {
    Span span = tracingRedissonHelper.buildSpan("readSortAlphaAsync", list);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingRedissonHelper
        .prepareRFuture(span, () -> list.readSortAlphaAsync(byPattern, order, offset, count));
  }

  @Override
  public <T> RFuture<Collection<T>> readSortAlphaAsync(String byPattern, List<String> getPatterns,
      SortOrder order) {
    Span span = tracingRedissonHelper.buildSpan("readSortAlphaAsync", list);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    span.setTag("getPatterns", collectionToString(getPatterns));
    return tracingRedissonHelper
        .prepareRFuture(span, () -> list.readSortAlphaAsync(byPattern, getPatterns, order));
  }

  @Override
  public <T> RFuture<Collection<T>> readSortAlphaAsync(String byPattern, List<String> getPatterns,
      SortOrder order, int offset, int count) {
    Span span = tracingRedissonHelper.buildSpan("readSortAlphaAsync", list);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    span.setTag("getPatterns", collectionToString(getPatterns));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingRedissonHelper
        .prepareRFuture(span,
            () -> list.readSortAlphaAsync(byPattern, getPatterns, order, offset, count)
        );
  }

  @Override
  public RFuture<Integer> sortToAsync(String destName, SortOrder order) {
    Span span = tracingRedissonHelper.buildSpan("sortToAsync", list);
    span.setTag("destName", nullable(destName));
    span.setTag("order", nullable(order));
    return tracingRedissonHelper.prepareRFuture(span, () -> list.sortToAsync(destName, order));
  }

  @Override
  public RFuture<Integer> sortToAsync(String destName,
      SortOrder order, int offset, int count) {
    Span span = tracingRedissonHelper.buildSpan("sortToAsync", list);
    span.setTag("destName", nullable(destName));
    span.setTag("order", nullable(order));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingRedissonHelper
        .prepareRFuture(span, () -> list.sortToAsync(destName, order, offset, count));
  }

  @Override
  public RFuture<Integer> sortToAsync(String destName, String byPattern,
      SortOrder order) {
    Span span = tracingRedissonHelper.buildSpan("sortToAsync", list);
    span.setTag("destName", nullable(destName));
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    return tracingRedissonHelper
        .prepareRFuture(span, () -> list.sortToAsync(destName, byPattern, order));
  }

  @Override
  public RFuture<Integer> sortToAsync(String destName, String byPattern,
      SortOrder order, int offset, int count) {
    Span span = tracingRedissonHelper.buildSpan("sortToAsync", list);
    span.setTag("destName", nullable(destName));
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingRedissonHelper
        .prepareRFuture(span, () -> list.sortToAsync(destName, byPattern, order, offset, count));
  }

  @Override
  public RFuture<Integer> sortToAsync(String destName, String byPattern,
      List<String> getPatterns, SortOrder order) {
    Span span = tracingRedissonHelper.buildSpan("sortToAsync", list);
    span.setTag("destName", nullable(destName));
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    span.setTag("getPatterns", collectionToString(getPatterns));
    return tracingRedissonHelper
        .prepareRFuture(span, () -> list.sortToAsync(destName, byPattern, getPatterns, order));
  }

  @Override
  public RFuture<Integer> sortToAsync(String destName, String byPattern,
      List<String> getPatterns, SortOrder order, int offset, int count) {
    Span span = tracingRedissonHelper.buildSpan("sortToAsync", list);
    span.setTag("destName", nullable(destName));
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    span.setTag("getPatterns", collectionToString(getPatterns));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingRedissonHelper
        .prepareRFuture(
            span, () -> list.sortToAsync(destName, byPattern, getPatterns, order, offset, count));
  }

  @Override
  public List<V> readSort(SortOrder order) {
    Span span = tracingRedissonHelper.buildSpan("readSort", list);
    span.setTag("order", nullable(order));
    return tracingRedissonHelper.decorate(span, () -> list.readSort(order));
  }

  @Override
  public List<V> readSort(SortOrder order, int offset, int count) {
    Span span = tracingRedissonHelper.buildSpan("readSort", list);
    span.setTag("order", nullable(order));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingRedissonHelper.decorate(span, () -> list.readSort(order, offset, count));
  }

  @Override
  public List<V> readSort(String byPattern, SortOrder order) {
    Span span = tracingRedissonHelper.buildSpan("readSort", list);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    return tracingRedissonHelper.decorate(span, () -> list.readSort(byPattern, order));
  }

  @Override
  public List<V> readSort(String byPattern, SortOrder order, int offset, int count) {
    Span span = tracingRedissonHelper.buildSpan("readSort", list);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingRedissonHelper
        .decorate(span, () -> list.readSort(byPattern, order, offset, count));
  }

  @Override
  public <T> Collection<T> readSort(String byPattern, List<String> getPatterns, SortOrder order) {
    Span span = tracingRedissonHelper.buildSpan("readSort", list);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("getPatterns", collectionToString(getPatterns));
    span.setTag("order", nullable(order));
    return tracingRedissonHelper.decorate(span, () -> list.readSort(byPattern, getPatterns, order));
  }

  @Override
  public <T> Collection<T> readSort(String byPattern, List<String> getPatterns, SortOrder order,
      int offset, int count) {
    Span span = tracingRedissonHelper.buildSpan("readSort", list);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("getPatterns", collectionToString(getPatterns));
    span.setTag("order", nullable(order));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingRedissonHelper
        .decorate(span, () -> list.readSort(byPattern, getPatterns, order, offset, count));
  }

  @Override
  public List<V> readSortAlpha(SortOrder order) {
    Span span = tracingRedissonHelper.buildSpan("readSortAlpha", list);
    span.setTag("order", nullable(order));
    return tracingRedissonHelper.decorate(span, () -> list.readSortAlpha(order));
  }

  @Override
  public List<V> readSortAlpha(SortOrder order, int offset, int count) {
    Span span = tracingRedissonHelper.buildSpan("readSortAlpha", list);
    span.setTag("order", nullable(order));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingRedissonHelper.decorate(span, () -> list.readSortAlpha(order, offset, count));
  }

  @Override
  public List<V> readSortAlpha(String byPattern, SortOrder order) {
    Span span = tracingRedissonHelper.buildSpan("readSortAlpha", list);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    return tracingRedissonHelper.decorate(span, () -> list.readSortAlpha(byPattern, order));
  }

  @Override
  public List<V> readSortAlpha(String byPattern, SortOrder order, int offset, int count) {
    Span span = tracingRedissonHelper.buildSpan("readSortAlpha", list);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingRedissonHelper
        .decorate(span, () -> list.readSortAlpha(byPattern, order, offset, count));
  }

  @Override
  public <T> Collection<T> readSortAlpha(String byPattern, List<String> getPatterns,
      SortOrder order) {
    Span span = tracingRedissonHelper.buildSpan("readSortAlpha", list);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("getPatterns", collectionToString(getPatterns));
    span.setTag("order", nullable(order));
    return tracingRedissonHelper
        .decorate(span, () -> list.readSortAlpha(byPattern, getPatterns, order));
  }

  @Override
  public <T> Collection<T> readSortAlpha(String byPattern,
      List<String> getPatterns, SortOrder order, int offset, int count) {
    Span span = tracingRedissonHelper.buildSpan("readSortAlpha", list);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("getPatterns", collectionToString(getPatterns));
    span.setTag("order", nullable(order));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingRedissonHelper
        .decorate(span, () -> list.readSortAlpha(byPattern, getPatterns, order, offset, count));
  }

  @Override
  public int sortTo(String destName, SortOrder order) {
    Span span = tracingRedissonHelper.buildSpan("sortTo", list);
    span.setTag("destName", nullable(destName));
    span.setTag("order", nullable(order));
    return tracingRedissonHelper.decorate(span, () -> list.sortTo(destName, order));
  }

  @Override
  public int sortTo(String destName, SortOrder order, int offset, int count) {
    Span span = tracingRedissonHelper.buildSpan("sortTo", list);
    span.setTag("destName", nullable(destName));
    span.setTag("order", nullable(order));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingRedissonHelper.decorate(span, () -> list.sortTo(destName, order, offset, count));
  }

  @Override
  public int sortTo(String destName, String byPattern, SortOrder order) {
    Span span = tracingRedissonHelper.buildSpan("sortTo", list);
    span.setTag("destName", nullable(destName));
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    return tracingRedissonHelper.decorate(span, () -> list.sortTo(destName, byPattern, order));
  }

  @Override
  public int sortTo(String destName, String byPattern, SortOrder order, int offset, int count) {
    Span span = tracingRedissonHelper.buildSpan("sortTo", list);
    span.setTag("destName", nullable(destName));
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingRedissonHelper
        .decorate(span, () -> list.sortTo(destName, byPattern, order, offset, count));
  }

  @Override
  public int sortTo(String destName, String byPattern, List<String> getPatterns,
      SortOrder order) {
    Span span = tracingRedissonHelper.buildSpan("sortTo", list);
    span.setTag("destName", nullable(destName));
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("getPatterns", collectionToString(getPatterns));
    span.setTag("order", nullable(order));
    return tracingRedissonHelper
        .decorate(span, () -> list.sortTo(destName, byPattern, getPatterns, order));
  }

  @Override
  public int sortTo(String destName, String byPattern, List<String> getPatterns,
      SortOrder order, int offset, int count) {
    Span span = tracingRedissonHelper.buildSpan("sortTo", list);
    span.setTag("destName", nullable(destName));
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("getPatterns", collectionToString(getPatterns));
    span.setTag("order", nullable(order));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingRedissonHelper
        .decorate(span, () -> list.sortTo(destName, byPattern, getPatterns, order, offset, count));
  }
}
