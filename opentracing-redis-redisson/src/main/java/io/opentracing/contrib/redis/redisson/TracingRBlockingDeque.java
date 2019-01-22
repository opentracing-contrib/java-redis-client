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
import org.redisson.api.RBlockingDeque;
import org.redisson.api.RFuture;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import static io.opentracing.contrib.redis.common.TracingHelper.nullable;

public class TracingRBlockingDeque<V> extends TracingRDeque<V> implements RBlockingDeque<V> {
    private final RBlockingDeque<V> deque;
    private final TracingRedissonHelper tracingRedissonHelper;

    public TracingRBlockingDeque(RBlockingDeque<V> deque,
                                 TracingRedissonHelper tracingRedissonHelper) {
        super(deque, tracingRedissonHelper);
        this.deque = deque;
        this.tracingRedissonHelper = tracingRedissonHelper;
    }

    @Override
    public V pollFirstFromAny(long timeout, TimeUnit unit, String... queueNames)
            throws InterruptedException {
        Span span = tracingRedissonHelper.buildSpan("pollFirstFromAny", deque);
        span.setTag("timeout", timeout);
        span.setTag("unit", nullable(unit));
        span.setTag("queueNames", Arrays.toString(queueNames));
        return tracingRedissonHelper
                .decorateThrowing(span, () -> deque.pollFirstFromAny(timeout, unit, queueNames));
    }

    @Override
    public V pollLastFromAny(long timeout, TimeUnit unit, String... queueNames)
            throws InterruptedException {
        Span span = tracingRedissonHelper.buildSpan("pollLastFromAny", deque);
        span.setTag("timeout", timeout);
        span.setTag("unit", nullable(unit));
        span.setTag("queueNames", Arrays.toString(queueNames));
        return tracingRedissonHelper
                .decorateThrowing(span, () -> deque.pollLastFromAny(timeout, unit, queueNames));
    }

    @Override
    public void addFirst(V v) {
        Span span = tracingRedissonHelper.buildSpan("addFirst", deque);
        span.setTag("element", nullable(v));
        tracingRedissonHelper.decorate(span, () -> deque.addFirst(v));
    }

    @Override
    public void addLast(V v) {
        Span span = tracingRedissonHelper.buildSpan("addLast", deque);
        span.setTag("element", nullable(v));
        tracingRedissonHelper.decorate(span, () -> deque.addLast(v));
    }

    @Override
    public boolean offerFirst(V v) {
        Span span = tracingRedissonHelper.buildSpan("offerFirst", deque);
        span.setTag("element", nullable(v));
        return tracingRedissonHelper.decorate(span, () -> deque.offerFirst(v));
    }

    @Override
    public boolean offerLast(V v) {
        Span span = tracingRedissonHelper.buildSpan("offerLast", deque);
        span.setTag("element", nullable(v));
        return tracingRedissonHelper.decorate(span, () -> deque.offerLast(v));
    }

    @Override
    public void putFirst(V v) throws InterruptedException {
        Span span = tracingRedissonHelper.buildSpan("putFirst", deque);
        span.setTag("element", nullable(v));
        tracingRedissonHelper.decorateThrowing(span, () -> deque.putFirst(v));
    }

    @Override
    public void putLast(V v) throws InterruptedException {
        Span span = tracingRedissonHelper.buildSpan("putLast", deque);
        span.setTag("element", nullable(v));
        tracingRedissonHelper.decorateThrowing(span, () -> deque.putLast(v));
    }

    @Override
    public boolean offerFirst(V v, long timeout, TimeUnit unit) throws InterruptedException {
        Span span = tracingRedissonHelper.buildSpan("offerFirst", deque);
        span.setTag("element", nullable(v));
        span.setTag("timeout", timeout);
        span.setTag("unit", nullable(unit));
        return tracingRedissonHelper.decorateThrowing(span, () -> deque.offerFirst(v, timeout, unit));
    }

    @Override
    public boolean offerLast(V v, long timeout, TimeUnit unit) throws InterruptedException {
        Span span = tracingRedissonHelper.buildSpan("offerLast", deque);
        span.setTag("element", nullable(v));
        span.setTag("timeout", timeout);
        span.setTag("unit", nullable(unit));
        return tracingRedissonHelper.decorateThrowing(span, () -> deque.offerLast(v, timeout, unit));
    }

    @Override
    public V takeFirst() throws InterruptedException {
        Span span = tracingRedissonHelper.buildSpan("takeFirst", deque);
        return tracingRedissonHelper.decorateThrowing(span, deque::takeFirst);
    }

    @Override
    public V takeLast() throws InterruptedException {
        Span span = tracingRedissonHelper.buildSpan("takeLast", deque);
        return tracingRedissonHelper.decorateThrowing(span, deque::takeLast);
    }

    @Override
    public V pollFirst(long timeout, TimeUnit unit) throws InterruptedException {
        Span span = tracingRedissonHelper.buildSpan("pollFirst", deque);
        span.setTag("timeout", timeout);
        span.setTag("unit", nullable(unit));
        return tracingRedissonHelper.decorateThrowing(span, () -> deque.pollFirst(timeout, unit));
    }

    @Override
    public V pollLast(long timeout, TimeUnit unit) throws InterruptedException {
        Span span = tracingRedissonHelper.buildSpan("pollLast", deque);
        span.setTag("timeout", timeout);
        span.setTag("unit", nullable(unit));
        return tracingRedissonHelper.decorateThrowing(span, () -> deque.pollLast(timeout, unit));
    }

    @Override
    public boolean removeFirstOccurrence(Object o) {
        Span span = tracingRedissonHelper.buildSpan("removeFirstOccurrence", deque);
        span.setTag("object", nullable(o));
        return tracingRedissonHelper.decorate(span, () -> deque.removeFirstOccurrence(o));
    }

    @Override
    public boolean removeLastOccurrence(Object o) {
        Span span = tracingRedissonHelper.buildSpan("removeLastOccurrence", deque);
        span.setTag("object", nullable(o));
        return tracingRedissonHelper.decorate(span, () -> deque.removeLastOccurrence(o));
    }

    @Override
    public boolean add(V v) {
        Span span = tracingRedissonHelper.buildSpan("add", deque);
        span.setTag("element", nullable(v));
        return tracingRedissonHelper.decorate(span, () -> deque.add(v));
    }

    @Override
    public boolean offer(V v) {
        Span span = tracingRedissonHelper.buildSpan("offer", deque);
        span.setTag("element", nullable(v));
        return tracingRedissonHelper.decorate(span, () -> deque.offer(v));
    }

    @Override
    public void put(V v) throws InterruptedException {
        Span span = tracingRedissonHelper.buildSpan("put", deque);
        span.setTag("element", nullable(v));
        tracingRedissonHelper.decorateThrowing(span, () -> deque.put(v));
    }

    @Override
    public boolean offer(V v, long timeout, TimeUnit unit) throws InterruptedException {
        Span span = tracingRedissonHelper.buildSpan("offer", deque);
        span.setTag("element", nullable(v));
        span.setTag("timeout", timeout);
        span.setTag("unit", nullable(unit));
        return tracingRedissonHelper.decorateThrowing(span, () -> deque.offer(v, timeout, unit));
    }

    @Override
    public V remove() {
        Span span = tracingRedissonHelper.buildSpan("remove", deque);
        return tracingRedissonHelper.decorate(span, () -> deque.remove());
    }

    @Override
    public V poll() {
        Span span = tracingRedissonHelper.buildSpan("poll", deque);
        return tracingRedissonHelper.decorate(span, () -> deque.poll());
    }

    @Override
    public V take() throws InterruptedException {
        Span span = tracingRedissonHelper.buildSpan("take", deque);
        return tracingRedissonHelper.decorateThrowing(span, deque::take);
    }

    @Override
    public V poll(long timeout, TimeUnit unit) throws InterruptedException {
        Span span = tracingRedissonHelper.buildSpan("poll", deque);
        span.setTag("timeout", timeout);
        span.setTag("unit", nullable(unit));
        return tracingRedissonHelper.decorateThrowing(span, () -> deque.poll(timeout, unit));
    }

    @Override
    public V element() {
        Span span = tracingRedissonHelper.buildSpan("element", deque);
        return tracingRedissonHelper.decorate(span, deque::element);
    }

    @Override
    public V peek() {
        Span span = tracingRedissonHelper.buildSpan("peek", deque);
        return tracingRedissonHelper.decorate(span, deque::peek);
    }

    @Override
    public boolean remove(Object o) {
        Span span = tracingRedissonHelper.buildSpan("remove", deque);
        span.setTag("object", nullable(o));
        return tracingRedissonHelper.decorate(span, () -> deque.remove(o));
    }

    @Override
    public boolean contains(Object o) {
        Span span = tracingRedissonHelper.buildSpan("contains", deque);
        span.setTag("object", nullable(o));
        return tracingRedissonHelper.decorate(span, () -> deque.contains(o));
    }

    @Override
    public int size() {
        Span span = tracingRedissonHelper.buildSpan("size", deque);
        return tracingRedissonHelper.decorate(span, deque::size);
    }

    @Override
    public Iterator<V> iterator() {
        return deque.iterator();
    }

    @Override
    public void push(V v) {
        Span span = tracingRedissonHelper.buildSpan("push", deque);
        span.setTag("element", nullable(v));
        tracingRedissonHelper.decorate(span, () -> deque.push(v));
    }

    @Override
    public int remainingCapacity() {
        Span span = tracingRedissonHelper.buildSpan("remainingCapacity", deque);
        return tracingRedissonHelper.decorate(span, deque::remainingCapacity);
    }

    @Override
    public int drainTo(Collection<? super V> c) {
        Span span = tracingRedissonHelper.buildSpan("drainTo", deque);
        return tracingRedissonHelper.decorate(span, () -> deque.drainTo(c));
    }

    @Override
    public int drainTo(Collection<? super V> c, int maxElements) {
        Span span = tracingRedissonHelper.buildSpan("drainTo", deque);
        span.setTag("maxElements", maxElements);
        return tracingRedissonHelper.decorate(span, () -> deque.drainTo(c, maxElements));
    }

    @Override
    public V pollFromAny(long timeout, TimeUnit unit, String... queueNames)
            throws InterruptedException {
        Span span = tracingRedissonHelper.buildSpan("pollFromAny", deque);
        span.setTag("timeout", timeout);
        span.setTag("unit", nullable(unit));
        span.setTag("queueNames", Arrays.toString(queueNames));
        return tracingRedissonHelper
                .decorateThrowing(span, () -> deque.pollFromAny(timeout, unit, queueNames));
    }

    @Override
    public V pollLastAndOfferFirstTo(String queueName, long timeout, TimeUnit unit)
            throws InterruptedException {
        Span span = tracingRedissonHelper.buildSpan("pollLastAndOfferFirstTo", deque);
        span.setTag("queueName", nullable(queueName));
        span.setTag("timeout", timeout);
        span.setTag("unit", nullable(unit));
        return tracingRedissonHelper
                .decorateThrowing(span, () -> deque.pollLastAndOfferFirstTo(queueName, timeout, unit));
    }

    @Override
    public V takeLastAndOfferFirstTo(String queueName) throws InterruptedException {
        Span span = tracingRedissonHelper.buildSpan("takeLastAndOfferFirstTo", deque);
        span.setTag("queueName", nullable(queueName));
        return tracingRedissonHelper
                .decorateThrowing(span, () -> deque.takeLastAndOfferFirstTo(queueName));
    }

    @Override
    public RFuture<V> pollFromAnyAsync(long timeout, TimeUnit unit, String... queueNames) {
        Span span = tracingRedissonHelper.buildSpan("pollFromAnyAsync", deque);
        span.setTag("timeout", timeout);
        span.setTag("unit", nullable(unit));
        span.setTag("queueNames", Arrays.toString(queueNames));
        return tracingRedissonHelper
                .prepareRFuture(span, () -> deque.pollFromAnyAsync(timeout, unit, queueNames));
    }

    @Override
    public RFuture<Integer> drainToAsync(Collection<? super V> c, int maxElements) {
        Span span = tracingRedissonHelper.buildSpan("drainToAsync", deque);
        span.setTag("maxElements", maxElements);
        return tracingRedissonHelper.prepareRFuture(span, () -> deque.drainToAsync(c, maxElements));
    }

    @Override
    public RFuture<Integer> drainToAsync(Collection<? super V> c) {
        Span span = tracingRedissonHelper.buildSpan("drainToAsync", deque);
        return tracingRedissonHelper.prepareRFuture(span, () -> deque.drainToAsync(c));
    }

    @Override
    public RFuture<V> pollLastAndOfferFirstToAsync(String queueName, long timeout, TimeUnit unit) {
        Span span = tracingRedissonHelper.buildSpan("pollLastAndOfferFirstToAsync", deque);
        span.setTag("queueName", nullable(queueName));
        span.setTag("timeout", timeout);
        span.setTag("unit", nullable(unit));
        return tracingRedissonHelper
                .prepareRFuture(span, () -> deque.pollLastAndOfferFirstToAsync(queueName, timeout, unit));
    }

    @Override
    public RFuture<V> takeLastAndOfferFirstToAsync(String queueName) {
        Span span = tracingRedissonHelper.buildSpan("takeLastAndOfferFirstToAsync", deque);
        span.setTag("queueName", nullable(queueName));
        return tracingRedissonHelper
                .prepareRFuture(span, () -> deque.takeLastAndOfferFirstToAsync(queueName));
    }

    @Override
    public RFuture<V> pollAsync(long timeout, TimeUnit unit) {
        Span span = tracingRedissonHelper.buildSpan("pollAsync", deque);
        span.setTag("timeout", timeout);
        span.setTag("unit", nullable(unit));
        return tracingRedissonHelper.prepareRFuture(span, () -> deque.pollAsync(timeout, unit));
    }

    @Override
    public RFuture<V> takeAsync() {
        Span span = tracingRedissonHelper.buildSpan("takeAsync", deque);
        return tracingRedissonHelper.prepareRFuture(span, deque::takeAsync);
    }

    @Override
    public RFuture<Void> putAsync(V e) {
        Span span = tracingRedissonHelper.buildSpan("putAsync", deque);
        span.setTag("element", nullable(e));
        return tracingRedissonHelper.prepareRFuture(span, () -> deque.putAsync(e));
    }

    @Override
    public RFuture<V> pollFirstFromAnyAsync(long timeout, TimeUnit unit, String... queueNames) {
        Span span = tracingRedissonHelper.buildSpan("pollFirstFromAnyAsync", deque);
        span.setTag("timeout", timeout);
        span.setTag("unit", nullable(unit));
        span.setTag("queueNames", Arrays.toString(queueNames));
        return tracingRedissonHelper
                .prepareRFuture(span, () -> deque.pollFirstFromAnyAsync(timeout, unit, queueNames));
    }

    @Override
    public RFuture<V> pollLastFromAnyAsync(long timeout, TimeUnit unit, String... queueNames) {
        Span span = tracingRedissonHelper.buildSpan("pollLastFromAnyAsync", deque);
        span.setTag("timeout", timeout);
        span.setTag("unit", nullable(unit));
        span.setTag("queueNames", Arrays.toString(queueNames));
        return tracingRedissonHelper
                .prepareRFuture(span, () -> deque.pollLastFromAnyAsync(timeout, unit, queueNames));
    }

    @Override
    public RFuture<Void> putFirstAsync(V e) {
        Span span = tracingRedissonHelper.buildSpan("putFirstAsync", deque);
        span.setTag("element", nullable(e));
        return tracingRedissonHelper.prepareRFuture(span, () -> deque.putFirstAsync(e));
    }

    @Override
    public RFuture<Void> putLastAsync(V e) {
        Span span = tracingRedissonHelper.buildSpan("putLastAsync", deque);
        span.setTag("element", nullable(e));
        return tracingRedissonHelper.prepareRFuture(span, () -> deque.putLastAsync(e));
    }

    @Override
    public RFuture<V> pollLastAsync(long timeout, TimeUnit unit) {
        Span span = tracingRedissonHelper.buildSpan("pollLastAsync", deque);
        span.setTag("timeout", timeout);
        span.setTag("unit", nullable(unit));
        return tracingRedissonHelper
                .prepareRFuture(span, () -> deque.pollLastAsync(timeout, unit));
    }

    @Override
    public RFuture<V> takeLastAsync() {
        Span span = tracingRedissonHelper.buildSpan("takeLastAsync", deque);
        return tracingRedissonHelper.prepareRFuture(span, deque::takeLastAsync);
    }

    @Override
    public RFuture<V> pollFirstAsync(long timeout, TimeUnit unit) {
        Span span = tracingRedissonHelper.buildSpan("pollFirstAsync", deque);
        span.setTag("timeout", timeout);
        span.setTag("unit", nullable(unit));
        return tracingRedissonHelper
                .prepareRFuture(span, () -> deque.pollFirstAsync(timeout, unit));
    }

    @Override
    public RFuture<V> takeFirstAsync() {
        Span span = tracingRedissonHelper.buildSpan("takeFirstAsync", deque);
        return tracingRedissonHelper.prepareRFuture(span, deque::takeFirstAsync);
    }

}
