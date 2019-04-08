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
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.redisson.api.RFuture;
import org.redisson.api.RKeys;
import org.redisson.api.RObject;
import org.redisson.api.RType;

public class TracingRKeys implements RKeys {
  private final RKeys keys;
  private final TracingRedissonHelper tracingRedissonHelper;

  public TracingRKeys(RKeys keys, TracingRedissonHelper tracingRedissonHelper) {
    this.keys = keys;
    this.tracingRedissonHelper = tracingRedissonHelper;
  }

  @Override
  public boolean move(String name, int database) {
    Span span = tracingRedissonHelper.buildSpan("move");
    span.setTag("name", nullable(name));
    span.setTag("database", database);
    return tracingRedissonHelper.decorate(span, () -> keys.move(name, database));
  }

  @Override
  public void migrate(String name, String host, int port, int database, long timeout) {
    Span span = tracingRedissonHelper.buildSpan("migrate");
    span.setTag("name", nullable(name));
    span.setTag("host", nullable(host));
    span.setTag("port", port);
    span.setTag("database", database);
    span.setTag("timeout", timeout);
    tracingRedissonHelper.decorate(span, () -> keys.migrate(name, host, port, database, timeout));
  }

  @Override
  public void copy(String name, String host, int port, int database, long timeout) {
    Span span = tracingRedissonHelper.buildSpan("copy");
    span.setTag("name", nullable(name));
    span.setTag("host", nullable(host));
    span.setTag("port", port);
    span.setTag("database", database);
    span.setTag("timeout", timeout);
    tracingRedissonHelper.decorate(span, () -> keys.copy(name, host, port, database, timeout));
  }

  @Override
  public boolean expire(String name, long timeToLive, TimeUnit timeUnit) {
    Span span = tracingRedissonHelper.buildSpan("expire");
    span.setTag("name", nullable(name));
    span.setTag("timeToLive", timeToLive);
    span.setTag("timeUnit", nullable(timeUnit));
    return tracingRedissonHelper.decorate(span, () -> keys.expire(name, timeToLive, timeUnit));
  }

  @Override
  public boolean expireAt(String name, long timestamp) {
    Span span = tracingRedissonHelper.buildSpan("expireAt");
    span.setTag("name", nullable(name));
    span.setTag("timestamp", timestamp);
    return tracingRedissonHelper.decorate(span, () -> keys.expireAt(name, timestamp));
  }

  @Override
  public boolean clearExpire(String name) {
    Span span = tracingRedissonHelper.buildSpan("clearExpire");
    span.setTag("name", nullable(name));
    return tracingRedissonHelper.decorate(span, () -> keys.clearExpire(name));
  }

  @Override
  public boolean renamenx(String oldName, String newName) {
    Span span = tracingRedissonHelper.buildSpan("renamenx");
    span.setTag("oldName", nullable(oldName));
    span.setTag("newName", nullable(newName));
    return tracingRedissonHelper.decorate(span, () -> keys.renamenx(oldName, newName));
  }

  @Override
  public void rename(String currentName, String newName) {
    Span span = tracingRedissonHelper.buildSpan("rename");
    span.setTag("currentName", nullable(currentName));
    span.setTag("newName", nullable(newName));
    tracingRedissonHelper.decorate(span, () -> keys.rename(currentName, newName));
  }

  @Override
  public long remainTimeToLive(String name) {
    Span span = tracingRedissonHelper.buildSpan("remainTimeToLive");
    span.setTag("name", nullable(name));
    return tracingRedissonHelper.decorate(span, () -> keys.remainTimeToLive(name));
  }

  @Override
  public long touch(String... names) {
    Span span = tracingRedissonHelper.buildSpan("touch");
    span.setTag("names", Arrays.toString(names));
    return tracingRedissonHelper.decorate(span, () -> keys.touch(names));
  }

  @Override
  public long countExists(String... names) {
    Span span = tracingRedissonHelper.buildSpan("countExists");
    span.setTag("names", Arrays.toString(names));
    return tracingRedissonHelper.decorate(span, () -> keys.countExists(names));
  }

  @Override
  public RType getType(String key) {
    Span span = tracingRedissonHelper.buildSpan("getType");
    span.setTag("key", nullable(key));
    return tracingRedissonHelper.decorate(span, () -> keys.getType(key));
  }

  @Override
  public int getSlot(String key) {
    Span span = tracingRedissonHelper.buildSpan("getSlot");
    span.setTag("key", nullable(key));
    return tracingRedissonHelper.decorate(span, () -> keys.getSlot(key));
  }

  @Override
  public Iterable<String> getKeysByPattern(String pattern) {
    Span span = tracingRedissonHelper.buildSpan("getKeysByPattern");
    span.setTag("pattern", nullable(pattern));
    return tracingRedissonHelper.decorate(span, () -> keys.getKeysByPattern(pattern));
  }

  @Override
  public Iterable<String> getKeysByPattern(String pattern, int count) {
    Span span = tracingRedissonHelper.buildSpan("getKeysByPattern");
    span.setTag("pattern", nullable(pattern));
    span.setTag("count", count);
    return tracingRedissonHelper.decorate(span, () -> keys.getKeysByPattern(pattern, count));
  }

  @Override
  public Iterable<String> getKeys() {
    Span span = tracingRedissonHelper.buildSpan("getKeys");
    return tracingRedissonHelper.decorate(span, () -> keys.getKeys());
  }

  @Override
  public Iterable<String> getKeys(int count) {
    Span span = tracingRedissonHelper.buildSpan("getKeys");
    span.setTag("count", count);
    return tracingRedissonHelper.decorate(span, () -> keys.getKeys(count));
  }

  @Override
  public Stream<String> getKeysStreamByPattern(String pattern) {
    Span span = tracingRedissonHelper.buildSpan("getKeysStreamByPattern");
    span.setTag("pattern", nullable(pattern));
    return tracingRedissonHelper.decorate(span, () -> keys.getKeysStreamByPattern(pattern));
  }

  @Override
  public Stream<String> getKeysStreamByPattern(String pattern, int count) {
    Span span = tracingRedissonHelper.buildSpan("getKeysStreamByPattern");
    span.setTag("pattern", nullable(pattern));
    span.setTag("count", count);
    return tracingRedissonHelper.decorate(span, () -> keys.getKeysStreamByPattern(pattern, count));
  }

  @Override
  public Stream<String> getKeysStream() {
    Span span = tracingRedissonHelper.buildSpan("getKeysStream");
    return tracingRedissonHelper.decorate(span, () -> keys.getKeysStream());
  }

  @Override
  public Stream<String> getKeysStream(int count) {
    Span span = tracingRedissonHelper.buildSpan("getKeysStream");
    span.setTag("count", count);
    return tracingRedissonHelper.decorate(span, () -> keys.getKeysStream(count));
  }

  @Override
  public String randomKey() {
    Span span = tracingRedissonHelper.buildSpan("randomKey");
    return tracingRedissonHelper.decorate(span, keys::randomKey);
  }

  @Override
  @Deprecated
  public Collection<String> findKeysByPattern(String pattern) {
    Span span = tracingRedissonHelper.buildSpan("findKeysByPattern");
    span.setTag("pattern", nullable(pattern));
    return tracingRedissonHelper.decorate(span, () -> keys.findKeysByPattern(pattern));
  }

  @Override
  public long deleteByPattern(String pattern) {
    Span span = tracingRedissonHelper.buildSpan("deleteByPattern");
    span.setTag("pattern", nullable(pattern));
    return tracingRedissonHelper.decorate(span, () -> keys.deleteByPattern(pattern));
  }

  @Override
  public long delete(RObject... objects) {
    Span span = tracingRedissonHelper.buildSpan("delete");
    span.setTag("objects", Arrays.toString(objects));
    return tracingRedissonHelper.decorate(span, () -> keys.delete(objects));
  }

  @Override
  public long delete(String... keys) {
    Span span = tracingRedissonHelper.buildSpan("delete");
    span.setTag("keys", Arrays.toString(keys));
    return tracingRedissonHelper.decorate(span, () -> this.keys.delete(keys));
  }

  @Override
  public long unlink(String... keys) {
    Span span = tracingRedissonHelper.buildSpan("unlink");
    span.setTag("keys", Arrays.toString(keys));
    return tracingRedissonHelper.decorate(span, () -> this.keys.unlink(keys));
  }

  @Override
  public long count() {
    Span span = tracingRedissonHelper.buildSpan("count");
    return tracingRedissonHelper.decorate(span, keys::count);
  }

  @Override
  public void flushdb() {
    Span span = tracingRedissonHelper.buildSpan("flushdb");
    tracingRedissonHelper.decorate(span, keys::flushdb);
  }

  @Override
  public void flushdbParallel() {
    Span span = tracingRedissonHelper.buildSpan("flushdbParallel");
    tracingRedissonHelper.decorate(span, keys::flushdbParallel);
  }

  @Override
  public void flushall() {
    Span span = tracingRedissonHelper.buildSpan("flushall");
    tracingRedissonHelper.decorate(span, keys::flushall);
  }

  @Override
  public void flushallParallel() {
    Span span = tracingRedissonHelper.buildSpan("flushallParallel");
    tracingRedissonHelper.decorate(span, keys::flushallParallel);
  }

  @Override
  public RFuture<Boolean> moveAsync(String name, int database) {
    Span span = tracingRedissonHelper.buildSpan("moveAsync");
    span.setTag("name", nullable(name));
    span.setTag("database", database);
    return tracingRedissonHelper.prepareRFuture(span, () -> keys.moveAsync(name, database));
  }

  @Override
  public RFuture<Void> migrateAsync(String name, String host, int port, int database,
      long timeout) {
    Span span = tracingRedissonHelper.buildSpan("migrateAsync");
    span.setTag("name", nullable(name));
    span.setTag("host", nullable(host));
    span.setTag("port", port);
    span.setTag("database", database);
    span.setTag("timeout", timeout);
    return tracingRedissonHelper
        .prepareRFuture(span, () -> keys.migrateAsync(name, host, port, database, timeout));
  }

  @Override
  public RFuture<Void> copyAsync(String name, String host, int port, int database, long timeout) {
    Span span = tracingRedissonHelper.buildSpan("copyAsync");
    span.setTag("name", nullable(name));
    span.setTag("host", nullable(host));
    span.setTag("port", port);
    span.setTag("database", database);
    span.setTag("timeout", timeout);
    return tracingRedissonHelper
        .prepareRFuture(span, () -> keys.copyAsync(name, host, port, database, timeout));
  }

  @Override
  public RFuture<Boolean> expireAsync(String name, long timeToLive, TimeUnit timeUnit) {
    Span span = tracingRedissonHelper.buildSpan("expireAsync");
    span.setTag("name", nullable(name));
    span.setTag("timeToLive", timeToLive);
    span.setTag("timeUnit", nullable(timeUnit));
    return tracingRedissonHelper
        .prepareRFuture(span, () -> keys.expireAsync(name, timeToLive, timeUnit));
  }

  @Override
  public RFuture<Boolean> expireAtAsync(String name, long timestamp) {
    Span span = tracingRedissonHelper.buildSpan("expireAtAsync");
    span.setTag("name", nullable(name));
    span.setTag("timestamp", timestamp);
    return tracingRedissonHelper.prepareRFuture(span, () -> keys.expireAtAsync(name, timestamp));
  }

  @Override
  public RFuture<Boolean> clearExpireAsync(String name) {
    Span span = tracingRedissonHelper.buildSpan("clearExpireAsync");
    span.setTag("name", nullable(name));
    return tracingRedissonHelper.prepareRFuture(span, () -> keys.clearExpireAsync(name));
  }

  @Override
  public RFuture<Boolean> renamenxAsync(String oldName, String newName) {
    Span span = tracingRedissonHelper.buildSpan("renamenxAsync");
    span.setTag("oldName", nullable(oldName));
    span.setTag("newName", nullable(newName));
    return tracingRedissonHelper.prepareRFuture(span, () -> keys.renamenxAsync(oldName, newName));
  }

  @Override
  public RFuture<Void> renameAsync(String currentName, String newName) {
    Span span = tracingRedissonHelper.buildSpan("renameAsync");
    span.setTag("currentName", nullable(currentName));
    span.setTag("newName", nullable(newName));
    return tracingRedissonHelper.prepareRFuture(span, () -> keys.renameAsync(currentName, newName));
  }

  @Override
  public RFuture<Long> remainTimeToLiveAsync(String name) {
    Span span = tracingRedissonHelper.buildSpan("remainTimeToLiveAsync");
    span.setTag("name", nullable(name));
    return tracingRedissonHelper.prepareRFuture(span, () -> keys.remainTimeToLiveAsync(name));
  }

  @Override
  public RFuture<Long> touchAsync(String... names) {
    Span span = tracingRedissonHelper.buildSpan("touchAsync");
    span.setTag("names", Arrays.toString(names));
    return tracingRedissonHelper.prepareRFuture(span, () -> keys.touchAsync(names));
  }

  @Override
  public RFuture<Long> countExistsAsync(String... names) {
    Span span = tracingRedissonHelper.buildSpan("countExistsAsync");
    span.setTag("names", Arrays.toString(names));
    return tracingRedissonHelper.prepareRFuture(span, () -> keys.countExistsAsync(names));
  }

  @Override
  public RFuture<RType> getTypeAsync(String key) {
    Span span = tracingRedissonHelper.buildSpan("getTypeAsync");
    span.setTag("key", nullable(key));
    return tracingRedissonHelper.prepareRFuture(span, () -> keys.getTypeAsync(key));
  }

  @Override
  public RFuture<Integer> getSlotAsync(String key) {
    Span span = tracingRedissonHelper.buildSpan("getSlotAsync");
    span.setTag("key", nullable(key));
    return tracingRedissonHelper.prepareRFuture(span, () -> keys.getSlotAsync(key));
  }

  @Override
  public RFuture<String> randomKeyAsync() {
    Span span = tracingRedissonHelper.buildSpan("randomKeyAsync");
    return tracingRedissonHelper.prepareRFuture(span, keys::randomKeyAsync);
  }

  @Override
  @Deprecated
  public RFuture<Collection<String>> findKeysByPatternAsync(String pattern) {
    Span span = tracingRedissonHelper.buildSpan("findKeysByPatternAsync");
    span.setTag("pattern", nullable(pattern));
    return tracingRedissonHelper.prepareRFuture(span, () -> keys.findKeysByPatternAsync(pattern));
  }

  @Override
  public RFuture<Long> deleteByPatternAsync(String pattern) {
    Span span = tracingRedissonHelper.buildSpan("deleteByPatternAsync");
    span.setTag("pattern", nullable(pattern));
    return tracingRedissonHelper.prepareRFuture(span, () -> keys.deleteByPatternAsync(pattern));
  }

  @Override
  public RFuture<Long> deleteAsync(RObject... objects) {
    Span span = tracingRedissonHelper.buildSpan("deleteAsync");
    span.setTag("objects", Arrays.toString(objects));
    return tracingRedissonHelper.prepareRFuture(span, () -> keys.deleteAsync(objects));
  }

  @Override
  public RFuture<Long> deleteAsync(String... keys) {
    Span span = tracingRedissonHelper.buildSpan("deleteAsync");
    span.setTag("keys", Arrays.toString(keys));
    return tracingRedissonHelper.prepareRFuture(span, () -> this.keys.deleteAsync(keys));
  }

  @Override
  public RFuture<Long> unlinkAsync(String... keys) {
    Span span = tracingRedissonHelper.buildSpan("unlinkAsync");
    span.setTag("keys", Arrays.toString(keys));
    return tracingRedissonHelper.prepareRFuture(span, () -> this.keys.unlinkAsync(keys));
  }

  @Override
  public RFuture<Long> countAsync() {
    Span span = tracingRedissonHelper.buildSpan("countAsync");
    return tracingRedissonHelper.prepareRFuture(span, keys::countAsync);
  }

  @Override
  public RFuture<Void> flushdbAsync() {
    Span span = tracingRedissonHelper.buildSpan("flushdbAsync");
    return tracingRedissonHelper.prepareRFuture(span, keys::flushdbAsync);
  }

  @Override
  public RFuture<Void> flushallAsync() {
    Span span = tracingRedissonHelper.buildSpan("flushallAsync");
    return tracingRedissonHelper.prepareRFuture(span, keys::flushallAsync);
  }

  @Override
  public RFuture<Void> flushdbParallelAsync() {
    Span span = tracingRedissonHelper.buildSpan("flushdbParallelAsync");
    return tracingRedissonHelper.prepareRFuture(span, keys::flushdbParallelAsync);
  }

  @Override
  public RFuture<Void> flushallParallelAsync() {
    Span span = tracingRedissonHelper.buildSpan("flushallParallelAsync");
    return tracingRedissonHelper.prepareRFuture(span, keys::flushallParallelAsync);
  }

}
