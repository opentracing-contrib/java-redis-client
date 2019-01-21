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

import io.netty.util.concurrent.FutureListener;
import org.redisson.api.RFuture;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class CompletableRFuture<T> extends CompletableFuture<T> implements RFuture<T> {
    private RFuture<T> wrappedFuture;

    public CompletableRFuture(RFuture<T> wrappedFuture) {
        this.wrappedFuture = wrappedFuture;
    }

    @Override
    public boolean isSuccess() {
        return wrappedFuture.isSuccess();
    }

    @Override
    public Throwable cause() {
        return wrappedFuture.cause();
    }

    @Override
    public T getNow() {
        return wrappedFuture.getNow();
    }

    @Override
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        return wrappedFuture.await(timeout, unit);
    }

    @Override
    public boolean await(long timeoutMillis) throws InterruptedException {
        return wrappedFuture.await(timeoutMillis);
    }

    @Override
    public RFuture<T> addListener(FutureListener<? super T> listener) {
        return wrappedFuture.addListener(listener);
    }

    @Override
    public RFuture<T> addListeners(FutureListener<? super T>... listeners) {
        return wrappedFuture.addListeners(listeners);
    }

    @Override
    public RFuture<T> removeListener(FutureListener<? super T> listener) {
        return wrappedFuture.removeListener(listener);
    }

    @Override
    public RFuture<T> removeListeners(FutureListener<? super T>... listeners) {
        return wrappedFuture.removeListeners(listeners);
    }

    @Override
    public RFuture<T> sync() throws InterruptedException {
        return wrappedFuture.sync();
    }

    @Override
    public RFuture<T> syncUninterruptibly() {
        return wrappedFuture.syncUninterruptibly();
    }

    @Override
    public RFuture<T> await() throws InterruptedException {
        return wrappedFuture.await();
    }

    @Override
    public RFuture<T> awaitUninterruptibly() {
        return wrappedFuture.awaitUninterruptibly();
    }

    @Override
    public boolean awaitUninterruptibly(long timeout, TimeUnit unit) {
        return wrappedFuture.awaitUninterruptibly(timeout, unit);
    }

    @Override
    public boolean awaitUninterruptibly(long timeoutMillis) {
        return wrappedFuture.awaitUninterruptibly(timeoutMillis);
    }
}