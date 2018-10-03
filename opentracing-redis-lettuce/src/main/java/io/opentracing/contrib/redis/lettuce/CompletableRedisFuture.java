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
    return wrappedFuture.await(timeout, unit);
  }
}