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
package io.opentracing.contrib.redis.spring.connection;

import io.opentracing.Tracer;
import org.springframework.data.redis.connection.ReactiveGeoCommands;
import org.springframework.data.redis.connection.ReactiveHashCommands;
import org.springframework.data.redis.connection.ReactiveHyperLogLogCommands;
import org.springframework.data.redis.connection.ReactiveKeyCommands;
import org.springframework.data.redis.connection.ReactiveListCommands;
import org.springframework.data.redis.connection.ReactiveNumberCommands;
import org.springframework.data.redis.connection.ReactiveRedisConnection;
import org.springframework.data.redis.connection.ReactiveScriptingCommands;
import org.springframework.data.redis.connection.ReactiveServerCommands;
import org.springframework.data.redis.connection.ReactiveSetCommands;
import org.springframework.data.redis.connection.ReactiveStringCommands;
import org.springframework.data.redis.connection.ReactiveZSetCommands;

public class TracingReactiveRedisConnection implements ReactiveRedisConnection {
  private final ReactiveRedisConnection reactiveRedisConnection;
  private final boolean withActiveSpanOnly;
  private final Tracer tracer;

  public TracingReactiveRedisConnection(
      ReactiveRedisConnection reactiveRedisConnection, boolean withActiveSpanOnly,
      Tracer tracer) {
    this.reactiveRedisConnection = reactiveRedisConnection;
    this.withActiveSpanOnly = withActiveSpanOnly;
    this.tracer = tracer;
  }

  @Override
  public void close() {
    reactiveRedisConnection.close();
  }

  @Override
  public ReactiveKeyCommands keyCommands() {
    return reactiveRedisConnection.keyCommands();
  }

  @Override
  public ReactiveStringCommands stringCommands() {
    return reactiveRedisConnection.stringCommands();
  }

  @Override
  public ReactiveNumberCommands numberCommands() {
    return reactiveRedisConnection.numberCommands();
  }

  @Override
  public ReactiveListCommands listCommands() {
    return reactiveRedisConnection.listCommands();
  }

  @Override
  public ReactiveSetCommands setCommands() {
    return reactiveRedisConnection.setCommands();
  }

  @Override
  public ReactiveZSetCommands zSetCommands() {
    return reactiveRedisConnection.zSetCommands();
  }

  @Override
  public ReactiveHashCommands hashCommands() {
    return reactiveRedisConnection.hashCommands();
  }

  @Override
  public ReactiveGeoCommands geoCommands() {
    return reactiveRedisConnection.geoCommands();
  }

  @Override
  public ReactiveHyperLogLogCommands hyperLogLogCommands() {
    return reactiveRedisConnection.hyperLogLogCommands();
  }

  @Override
  public ReactiveScriptingCommands scriptingCommands() {
    return reactiveRedisConnection.scriptingCommands();
  }

  @Override
  public ReactiveServerCommands serverCommands() {
    return reactiveRedisConnection.serverCommands();
  }

  @Override
  public reactor.core.publisher.Mono<String> ping() {
    return RedisTracingUtils.doInScope(RedisCommand.PING, reactiveRedisConnection::ping,
        withActiveSpanOnly, tracer);
  }

}
