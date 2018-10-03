package io.opentracing.contrib.redis.lettuce;

import io.lettuce.core.RedisFuture;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class CompletableRedisFuture<T> extends CompletableFuture<T> implements RedisFuture<T> {

    private RedisFuture<T> wrappedFuture;

    public CompletableRedisFuture(RedisFuture<T> wrappedFuture) {
        this.wrappedFuture = wrappedFuture;
    }

    @Override
    public String getError() {
        return wrappedFuture.getError();
    }

    @Override
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        return wrappedFuture.await(timeout,unit);
    }
}