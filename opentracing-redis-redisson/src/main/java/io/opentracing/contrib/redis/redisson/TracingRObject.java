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

import static io.opentracing.contrib.redis.common.TracingHelper.nullable;

import io.opentracing.Span;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.redisson.api.RFuture;
import org.redisson.api.RObject;
import org.redisson.client.codec.Codec;

public class TracingRObject implements RObject {
  private final RObject object;
  private final TracingRedissonHelper tracingRedissonHelper;

  public TracingRObject(RObject object, TracingRedissonHelper tracingRedissonHelper) {
    this.object = object;
    this.tracingRedissonHelper = tracingRedissonHelper;
  }

  @Override
  public void restore(byte[] state) {
    Span span = tracingRedissonHelper.buildSpan("restore", object);
    span.setTag("state", Arrays.toString(state));
    tracingRedissonHelper.decorate(span, () -> object.restore(state));
  }

  @Override
  public void restore(byte[] state, long timeToLive, TimeUnit timeUnit) {
    Span span = tracingRedissonHelper.buildSpan("restore", object);
    span.setTag("state", Arrays.toString(state));
    span.setTag("timeToLive", timeToLive);
    span.setTag("timeUnit", nullable(timeUnit));
    tracingRedissonHelper.decorate(span, () -> object.restore(state, timeToLive, timeUnit));
  }

  @Override
  public void restoreAndReplace(byte[] state) {
    Span span = tracingRedissonHelper.buildSpan("restoreAndReplace", object);
    span.setTag("state", Arrays.toString(state));
    tracingRedissonHelper.decorate(span, () -> object.restoreAndReplace(state));
  }

  @Override
  public void restoreAndReplace(byte[] state, long timeToLive, TimeUnit timeUnit) {
    Span span = tracingRedissonHelper.buildSpan("restoreAndReplace", object);
    span.setTag("state", Arrays.toString(state));
    span.setTag("timeToLive", timeToLive);
    span.setTag("timeUnit", nullable(timeUnit));
    tracingRedissonHelper
        .decorate(span, () -> object.restoreAndReplace(state, timeToLive, timeUnit));
  }

  @Override
  public byte[] dump() {
    Span span = tracingRedissonHelper.buildSpan("dump", object);
    return tracingRedissonHelper.decorate(span, object::dump);
  }

  @Override
  public boolean touch() {
    Span span = tracingRedissonHelper.buildSpan("touch", object);
    return tracingRedissonHelper.decorate(span, object::touch);
  }

  @Override
  public void migrate(String host, int port, int database, long timeout) {
    Span span = tracingRedissonHelper.buildSpan("migrate", object);
    span.setTag("host", nullable(host));
    span.setTag("port", port);
    span.setTag("database", database);
    span.setTag("timeout", timeout);
    tracingRedissonHelper.decorate(span, () -> object.migrate(host, port, database, timeout));
  }

  @Override
  public void copy(String host, int port, int database, long timeout) {
    Span span = tracingRedissonHelper.buildSpan("copy", object);
    span.setTag("host", nullable(host));
    span.setTag("port", port);
    span.setTag("database", database);
    span.setTag("timeout", timeout);
    tracingRedissonHelper.decorate(span, () -> object.copy(host, port, database, timeout));
  }

  @Override
  public boolean move(int database) {
    Span span = tracingRedissonHelper.buildSpan("move", object);
    span.setTag("database", database);
    return tracingRedissonHelper.decorate(span, () -> object.move(database));
  }

  @Override
  public String getName() {
    return object.getName();
  }

  @Override
  public boolean delete() {
    Span span = tracingRedissonHelper.buildSpan("delete", object);
    return tracingRedissonHelper.decorate(span, object::delete);
  }

  @Override
  public boolean unlink() {
    Span span = tracingRedissonHelper.buildSpan("unlink", object);
    return tracingRedissonHelper.decorate(span, object::unlink);
  }

  @Override
  public void rename(String newName) {
    Span span = tracingRedissonHelper.buildSpan("rename", object);
    span.setTag("newName", nullable(newName));
    tracingRedissonHelper.decorate(span, () -> object.rename(newName));
  }

  @Override
  public boolean renamenx(String newName) {
    Span span = tracingRedissonHelper.buildSpan("renamenx", object);
    span.setTag("newName", nullable(newName));
    return tracingRedissonHelper.decorate(span, () -> object.renamenx(newName));
  }

  @Override
  public boolean isExists() {
    Span span = tracingRedissonHelper.buildSpan("isExists", object);
    return tracingRedissonHelper.decorate(span, object::isExists);
  }

  @Override
  public Codec getCodec() {
    return object.getCodec();
  }

  @Override
  public RFuture<Void> restoreAsync(byte[] state) {
    Span span = tracingRedissonHelper.buildSpan("restoreAsync", object);
    span.setTag("state", Arrays.toString(state));
    return tracingRedissonHelper.prepareRFuture(span, () -> object.restoreAsync(state));
  }

  @Override
  public RFuture<Void> restoreAsync(byte[] state, long timeToLive,
      TimeUnit timeUnit) {
    Span span = tracingRedissonHelper.buildSpan("restoreAsync", object);
    span.setTag("state", Arrays.toString(state));
    span.setTag("timeToLive", timeToLive);
    span.setTag("timeUnit", nullable(timeUnit));
    return tracingRedissonHelper
        .prepareRFuture(span, () -> object.restoreAsync(state, timeToLive, timeUnit));
  }

  @Override
  public RFuture<Void> restoreAndReplaceAsync(byte[] state) {
    Span span = tracingRedissonHelper.buildSpan("restoreAndReplaceAsync", object);
    span.setTag("state", Arrays.toString(state));
    return tracingRedissonHelper.prepareRFuture(span, () -> object.restoreAndReplaceAsync(state));
  }

  @Override
  public RFuture<Void> restoreAndReplaceAsync(byte[] state, long timeToLive,
      TimeUnit timeUnit) {
    Span span = tracingRedissonHelper.buildSpan("restoreAndReplaceAsync", object);
    span.setTag("state", Arrays.toString(state));
    span.setTag("timeToLive", timeToLive);
    span.setTag("timeUnit", nullable(timeUnit));
    return tracingRedissonHelper
        .prepareRFuture(span, () -> object.restoreAndReplaceAsync(state, timeToLive, timeUnit));
  }

  @Override
  public RFuture<byte[]> dumpAsync() {
    Span span = tracingRedissonHelper.buildSpan("dumpAsync", object);
    return tracingRedissonHelper.prepareRFuture(span, object::dumpAsync);
  }

  @Override
  public RFuture<Boolean> touchAsync() {
    Span span = tracingRedissonHelper.buildSpan("touchAsync", object);
    return tracingRedissonHelper.prepareRFuture(span, object::touchAsync);
  }

  @Override
  public RFuture<Void> migrateAsync(String host, int port, int database,
      long timeout) {
    Span span = tracingRedissonHelper.buildSpan("migrateAsync", object);
    span.setTag("host", nullable(host));
    span.setTag("port", port);
    span.setTag("database", database);
    span.setTag("timeout", timeout);
    return tracingRedissonHelper
        .prepareRFuture(span, () -> object.migrateAsync(host, port, database, timeout));
  }

  @Override
  public RFuture<Void> copyAsync(String host, int port, int database, long timeout) {
    Span span = tracingRedissonHelper.buildSpan("copyAsync", object);
    span.setTag("host", nullable(host));
    span.setTag("port", port);
    span.setTag("database", database);
    span.setTag("timeout", timeout);
    return tracingRedissonHelper
        .prepareRFuture(span, () -> object.copyAsync(host, port, database, timeout));
  }

  @Override
  public RFuture<Boolean> moveAsync(int database) {
    Span span = tracingRedissonHelper.buildSpan("moveAsync", object);
    span.setTag("database", database);
    return tracingRedissonHelper.prepareRFuture(span, () -> object.moveAsync(database));
  }

  @Override
  public RFuture<Boolean> deleteAsync() {
    Span span = tracingRedissonHelper.buildSpan("deleteAsync", object);
    return tracingRedissonHelper.prepareRFuture(span, object::deleteAsync);
  }

  @Override
  public RFuture<Boolean> unlinkAsync() {
    Span span = tracingRedissonHelper.buildSpan("unlinkAsync", object);
    return tracingRedissonHelper.prepareRFuture(span, object::unlinkAsync);
  }

  @Override
  public RFuture<Void> renameAsync(String newName) {
    Span span = tracingRedissonHelper.buildSpan("renameAsync", object);
    span.setTag("newName", nullable(newName));
    return tracingRedissonHelper.prepareRFuture(span, () -> object.renameAsync(newName));
  }

  @Override
  public RFuture<Boolean> renamenxAsync(String newName) {
    Span span = tracingRedissonHelper.buildSpan("renamenxAsync", object);
    span.setTag("newName", nullable(newName));
    return tracingRedissonHelper.prepareRFuture(span, () -> object.renamenxAsync(newName));
  }

  @Override
  public RFuture<Boolean> isExistsAsync() {
    Span span = tracingRedissonHelper.buildSpan("isExistsAsync", object);
    return tracingRedissonHelper.prepareRFuture(span, object::isExistsAsync);
  }

  @Override
  public boolean equals(Object o) {
    Span span = tracingRedissonHelper.buildSpan("equals", object);
    span.setTag("object", nullable(o));
    return tracingRedissonHelper.decorate(span, () -> object.equals(o));
  }

  @Override
  public int hashCode() {
    Span span = tracingRedissonHelper.buildSpan("hashCode", object);
    return tracingRedissonHelper.decorate(span, object::hashCode);
  }
}
